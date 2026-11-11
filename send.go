package meshcore

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

// SendResult — итог отправки личного сообщения.
type SendResult struct {
	Acked          bool
	Attempts       int
	DirectAttempts int
	FloodAttempts  int
	Route          string // "direct" или "flood" успешной попытки
	Tag            uint32
	TripTime       time.Duration
	Err            error
}

// SendDirectMessage отправляет личное сообщение контакту по имени.
func (c *Client) SendDirectMessage(ctx context.Context, contactName, text string) (*SendResult, error) {
	ct, err := c.ResolveContact(ctx, contactName)
	if err != nil {
		return nil, err
	}
	return c.SendDirectMessageTo(ctx, ct, text)
}

// SendDirectMessageTo отправляет личное сообщение с политикой повторов:
// сначала cfg.DirectAttempts попыток по последнему известному маршруту,
// затем маршрут сбрасывается и делается cfg.FloodAttempts широковещательных.
func (c *Client) SendDirectMessageTo(ctx context.Context, ct *Contact, text string) (*SendResult, error) {
	if err := checkText(text); err != nil {
		return nil, err
	}
	res := &SendResult{}
	direct, flood := c.cfg.DirectAttempts, c.cfg.FloodAttempts
	total := direct + flood
	// Одинаковая метка времени на все попытки: уникальность пакету даёт номер
	// попытки, что позволяет получателям распознавать ретрансли.
	ts := uint32(time.Now().Unix())
	flooded := false

	for i := 0; i < total; i++ {
		if err := ctx.Err(); err != nil {
			res.Err = err
			return res, err
		}
		if !flooded && i == direct && flood > 0 {
			if err := c.ResetPath(ctx, ct); err != nil {
				c.cfg.Logf("meshcore: сброс маршрута: %v", err)
			}
			flooded = true
			c.cfg.Logf("meshcore: прямой маршрут исчерпан (%d), переход на flood", direct)
		}
		res.Attempts = i + 1

		// Подписываемся на ACK ДО отправки: подтверждение может прийти
		// сразу вслед за SENT.
		ack := c.arm(func(f []byte) bool {
			return len(f) >= 5 && f[0] == pushSendConfirmed
		})

		payload := make([]byte, 0, 13+len(text))
		payload = append(payload, cmdSendTxtMsg, TxtTypePlain, byte(i))
		var tb [4]byte
		binary.LittleEndian.PutUint32(tb[:], ts)
		payload = append(payload, tb[:]...)
		payload = append(payload, ct.Prefix()...)
		payload = append(payload, []byte(text)...)

		sent, err := c.tx([]byte{respSent, respErr}, payload, c.cfg.CommandTimeout)
		if err != nil {
			ack.release()
			res.Err = err
			c.cfg.Logf("meshcore: попытка %d: %v", i+1, err)
			c.sleep(c.cfg.RetryDelay)
			continue
		}
		if sent[0] == respErr {
			ack.release()
			res.Err = frameError(sent)
			if errors.Is(res.Err, ErrNotFound) {
				return res, res.Err
			}
			c.sleep(c.cfg.RetryDelay)
			continue
		}
		if len(sent) < 10 {
			ack.release()
			res.Err = fmt.Errorf("meshcore: короткий кадр SENT: %d", len(sent))
			c.sleep(c.cfg.RetryDelay)
			continue
		}

		tag := binary.LittleEndian.Uint32(sent[2:6])
		est := binary.LittleEndian.Uint32(sent[6:10])
		res.Tag = tag
		if sent[1] == 1 {
			res.Route = "flood"
			res.FloodAttempts++
		} else {
			res.Route = "direct"
			res.DirectAttempts++
		}

		wait := c.clampWait(est)
		c.cfg.Logf("meshcore: попытка %d (%s, tag=%08x), жду ACK %s",
			i+1, res.Route, tag, wait)

		f, err := ack.collect(wait, func(fr []byte) bool {
			return len(fr) >= 5 && binary.LittleEndian.Uint32(fr[1:5]) == tag
		})
		if err == nil {
			res.Acked = true
			res.Err = nil
			if len(f) >= 9 {
				res.TripTime = time.Duration(binary.LittleEndian.Uint32(f[5:9])) * time.Millisecond
			}
			return res, nil
		}
		res.Err = ErrNoAck
		c.sleep(c.cfg.RetryDelay)
	}

	if res.Err == nil {
		res.Err = ErrNoAck
	}
	return res, res.Err
}

// ---------------------------------------------------------------------------
// Групповые (хэштег) каналы
// ---------------------------------------------------------------------------

// ChannelOptions — параметры отправки в канал.
type ChannelOptions struct {
	// Scope — регион рассылки, например "vostok". Пусто = scope узла.
	Scope string
	// Unscoped — классическая неограниченная рассылка (игнорирует Scope).
	Unscoped bool
	// Attempts переопределяет cfg.ChannelAttempts.
	Attempts int
	// EchoWindow переопределяет cfg.EchoWindow.
	EchoWindow time.Duration
	// RequireRepeats — сколько эхо одного пакета считать подтверждением.
	RequireRepeats int
	// RestoreScope — вернуть прежнее состояние scope после отправки (по умолч. true).
	RestoreScope *bool
}

func (o ChannelOptions) restore() bool { return o.RestoreScope == nil || *o.RestoreScope }

// ChannelSendResult — итог отправки в канал.
type ChannelSendResult struct {
	Sent     bool // прошивка приняла сообщение (RESP_CODE_OK)
	Heard    bool // хотя бы один ретранслятор повторил пакет
	Attempts int
	Echoes   []Echo
	Err      error
}

// SendChannelMessage отправляет сообщение в хэштег-канал, при необходимости
// задавая регион, и повторяет отправку, пока пакет не повторит ретранслятор.
//
// ВАЖНО об определении «услышанности»: для групповых сообщений прошивка
// отвечает лишь RESP_CODE_OK — ни tag, ни expected_ack не возвращаются
// (в отличие от личных сообщений). Поэтому подтверждение строится на эхо из
// rx_log (0x88): ретранслятор, повторивший пакет, наращивает в нём маршрут,
// и мы видим этот повтор с SNR/RSSI. Атрибуция эхо нашему пакету — эвристика:
// тип полезной нагрузки GRP_TXT, непустой маршрут и кластеризация по
// побайтовому совпадению payload (ретрансляторы payload не меняют).
func (c *Client) SendChannelMessage(ctx context.Context, ch Channel, text string,
	opts ChannelOptions) (*ChannelSendResult, error) {
	if err := checkText(text); err != nil {
		return nil, err
	}
	res := &ChannelSendResult{}

	attempts := opts.Attempts
	if attempts <= 0 {
		attempts = c.cfg.ChannelAttempts
	}
	window := opts.EchoWindow
	if window <= 0 {
		window = c.cfg.EchoWindow
	}
	require := opts.RequireRepeats
	if require <= 0 {
		require = c.cfg.RequireRepeats
	}

	// Регион.
	scopeChanged := false
	if opts.Unscoped {
		if err := c.SetUnscoped(ctx); err != nil {
			return nil, err
		}
		scopeChanged = true
	} else if opts.Scope != "" {
		if err := c.SetFloodScope(ctx, opts.Scope); err != nil {
			return nil, err
		}
		scopeChanged = true
	}
	if scopeChanged && opts.restore() {
		defer func() {
			if err := c.ClearFloodScope(context.Background()); err != nil {
				c.cfg.Logf("meshcore: возврат scope: %v", err)
			}
		}()
	}

	// Подписка на rx_log нужна ДО первой отправки, иначе первое эхо теряется.
	sub := c.SubscribeBuffered(1024, EventRxLog)
	defer sub.Cancel()

	for i := 0; i < attempts; i++ {
		if err := ctx.Err(); err != nil {
			res.Err = err
			return res, err
		}
		res.Attempts = i + 1
		ts := uint32(time.Now().Unix())

		payload := make([]byte, 0, 7+len(text))
		payload = append(payload, cmdSendChannelTxt, TxtTypePlain, ch.Index)
		var tb [4]byte
		binary.LittleEndian.PutUint32(tb[:], ts)
		payload = append(payload, tb[:]...)
		payload = append(payload, []byte(text)...)

		f, err := c.tx([]byte{respOK, respErr}, payload, c.cfg.CommandTimeout)
		if err != nil {
			res.Err = err
			c.cfg.Logf("meshcore: канал, попытка %d: %v", i+1, err)
			continue
		}
		if f[0] == respErr {
			res.Err = frameError(f)
			c.cfg.Logf("meshcore: канал, попытка %d: %v", i+1, res.Err)
			if errors.Is(res.Err, ErrNotFound) {
				return res, res.Err
			}
			continue
		}
		res.Sent = true

		echoes, err := c.waitForEchoes(ctx, sub, window, require)
		if err != nil {
			res.Err = err
			c.cfg.Logf("meshcore: канал, попытка %d: эхо не дождались", i+1)
			continue
		}
		res.Heard = true
		res.Echoes = echoes
		res.Err = nil
		return res, nil
	}

	if res.Err == nil {
		res.Err = ErrNotHeard
	}
	return res, res.Err
}

// waitForEchoes собирает эхо GRP_TXT-пакетов и возвращает первый кластер,
// достигший require повторов. Кластеризация — по побайтовому совпадению
// payload: ретрансляторы меняют только маршрут, полезная нагрузка остаётся.
func (c *Client) waitForEchoes(ctx context.Context, sub *Subscription, window time.Duration,
	require int) ([]Echo, error) {
	deadline := time.After(window)
	clusters := map[string][]Echo{}

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-deadline:
			return nil, ErrTimeout
		case ev, ok := <-sub.Events():
			if !ok {
				// подписку отменили извне — иначе цикл ушёл бы в busy-loop
				return nil, ErrClosed
			}
			if ev.RxLog == nil || ev.RxLog.Pkt == nil {
				continue
			}
			pkt := ev.RxLog.Pkt
			// Только групповые текстовые пакеты, повторённые ретранслятором.
			if pkt.PayloadType != payloadGrpTxt || !pkt.Path.Repeated() {
				continue
			}
			key := hex.EncodeToString(pkt.Payload)
			clusters[key] = append(clusters[key], Echo{
				At:   ev.RxLog.At,
				SNR:  ev.RxLog.SNR,
				RSSI: ev.RxLog.RSSI,
				Path: pkt.Path,
				Pkt:  pkt,
			})
			if len(clusters[key]) >= require {
				return clusters[key], nil
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Вспомогательное
// ---------------------------------------------------------------------------

// SplitText разбивает длинный текст на части не длиннее MaxTextLen,
// добавляя индикатор "[i/n]".
func SplitText(text string) []string {
	limit := MaxTextLen - len("[00/00] ")
	if limit < 20 {
		limit = 20
	}
	runes := []rune(text)
	var chunks []string
	for len(runes) > limit {
		chunks = append(chunks, string(runes[:limit]))
		runes = runes[limit:]
	}
	chunks = append(chunks, string(runes))
	if len(chunks) == 1 {
		return chunks
	}
	out := make([]string, len(chunks))
	for i, ch := range chunks {
		out[i] = fmt.Sprintf("[%d/%d] %s", i+1, len(chunks), ch)
	}
	return out
}

// Login выполняет вход на репитер или room-сервер. Нужен только для
// административных действий; телеметрия и сообщения его не требуют.
func (c *Client) Login(ctx context.Context, ct *Contact, password string) error {
	push := c.arm(matchAny([]byte{pushLoginSuccess, pushLoginFail}))
	defer push.release()

	payload := make([]byte, 0, 33+len(password))
	payload = append(payload, cmdSendLogin)
	payload = append(payload, ct.PublicKey[:]...)
	payload = append(payload, []byte(password)...)

	sent, err := c.tx([]byte{respSent, respErr}, payload, c.cfg.CommandTimeout)
	if err != nil {
		return err
	}
	if sent[0] == respErr {
		return frameError(sent)
	}
	est := uint32(0)
	if len(sent) >= 10 {
		est = binary.LittleEndian.Uint32(sent[6:10])
	}
	f, err := push.wait(c.clampWait(est))
	if err != nil {
		return err
	}
	if f[0] == pushLoginFail {
		return errors.New("meshcore: вход отклонён (неверный пароль)")
	}
	return nil
}
