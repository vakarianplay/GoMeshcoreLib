package meshcore

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Config — параметры клиента и политики повторов.
type Config struct {
	// Политика повторов для личных сообщений и запросов телеметрии.
	DirectAttempts int // попыток по последнему известному маршруту (по умолч. 5)
	FloodAttempts  int // затем широковещательных попыток (по умолч. 5)

	// Политика повторов для групповых (хэштег) каналов.
	ChannelAttempts int // попыток, пока пакет не повторит ретранслятор (3)

	RetryDelay time.Duration // пауза между попытками
	// EchoWindow — сколько ждать эхо от ретрансляторов после отправки в канал.
	EchoWindow time.Duration
	// RequireRepeats — сколько эхо одного пакета считать подтверждением.
	RequireRepeats int

	CommandTimeout time.Duration // на короткий обмен команда/ответ
	AckSlack       float64       // множитель к suggested_timeout (1.25)
	MinWait        time.Duration // нижняя граница ожидания mesh-ответа
	MaxWait        time.Duration // верхняя граница

	AppName     string
	Verbose     bool
	Logf        func(format string, args ...any)
	EventBuffer int
}

// DefaultConfig возвращает конфиг со значениями по умолчанию.
func DefaultConfig() Config {
	return Config{
		DirectAttempts:  5,
		FloodAttempts:   5,
		ChannelAttempts: 3,
		RetryDelay:      0,
		EchoWindow:      8 * time.Second,
		RequireRepeats:  1,
		CommandTimeout:  15 * time.Second,
		AckSlack:        1.25,
		MinWait:         5 * time.Second,
		MaxWait:         5 * time.Minute,
		AppName:         "go-meshcore",
		EventBuffer:     256,
	}
}

func (c Config) withDefaults() Config {
	d := DefaultConfig()
	if c.DirectAttempts <= 0 {
		c.DirectAttempts = d.DirectAttempts
	}
	if c.FloodAttempts < 0 {
		c.FloodAttempts = 0
	}
	if c.ChannelAttempts <= 0 {
		c.ChannelAttempts = d.ChannelAttempts
	}
	if c.EchoWindow <= 0 {
		c.EchoWindow = d.EchoWindow
	}
	if c.RequireRepeats <= 0 {
		c.RequireRepeats = d.RequireRepeats
	}
	if c.CommandTimeout <= 0 {
		c.CommandTimeout = d.CommandTimeout
	}
	if c.AckSlack <= 0 {
		c.AckSlack = d.AckSlack
	}
	if c.MinWait <= 0 {
		c.MinWait = d.MinWait
	}
	if c.MaxWait <= 0 {
		c.MaxWait = d.MaxWait
	}
	if c.AppName == "" {
		c.AppName = d.AppName
	}
	if c.EventBuffer <= 0 {
		c.EventBuffer = d.EventBuffer
	}
	if c.Logf == nil {
		c.Logf = func(string, ...any) {}
	}
	return c
}

// Client — соединение с компаньоном MeshCore.
// Все методы безопасны для одновременного использования.
type Client struct {
	cfg Config
	tr  Transport
	br  *bufio.Reader

	wmu sync.Mutex // сериализует записи кадров
	cmu sync.Mutex // одна короткая транзакция «команда -> ответ» за раз

	stop     chan struct{}
	closed   chan struct{}
	stopOnce sync.Once
	connErr  error

	pmu     sync.Mutex
	pseq    uint64
	pending map[uint64]*pending

	smu  sync.RWMutex
	sseq uint64
	subs map[uint64]*Subscription

	selfMu sync.RWMutex
	self   SelfInfo
	dev    DeviceInfo

	ctcMu    sync.RWMutex
	contacts []Contact
	ctcReady bool

	chMu     sync.RWMutex
	channels []Channel

	drainMu sync.Mutex

	dropped atomic.Int64
}

// Dial подключается к компаньону и выполняет инициализацию сессии.
func Dial(tr Transport, cfg Config) (*Client, error) {
	cfg = cfg.withDefaults()
	c := &Client{
		cfg:     cfg,
		tr:      tr,
		br:      bufio.NewReaderSize(tr, 4096),
		stop:    make(chan struct{}),
		closed:  make(chan struct{}),
		pending: map[uint64]*pending{},
		subs:    map[uint64]*Subscription{},
	}
	go c.readLoop()

	if err := c.handshake(); err != nil {
		_ = c.Close()
		return nil, err
	}
	go c.pumpLoop()
	return c, nil
}

// DialTCPClient — подключение по WiFi/TCP одной строкой.
func DialTCPClient(host string, port int, cfg Config) (*Client, error) {
	tr, err := DialTCP(host, port)
	if err != nil {
		return nil, err
	}
	return Dial(tr, cfg)
}

// DialSerialClient — подключение по USB/COM-порту одной строкой.
func DialSerialClient(port string, baud int, cfg Config) (*Client, error) {
	tr, err := DialSerial(port, baud)
	if err != nil {
		return nil, err
	}
	return Dial(tr, cfg)
}

// Close закрывает соединение и освобождает ресурсы.
func (c *Client) Close() error {
	var err error
	c.stopOnce.Do(func() {
		close(c.stop)
		err = c.tr.Close()
		<-c.closed
		c.smu.Lock()
		for id, s := range c.subs {
			close(s.ch)
			delete(c.subs, id)
		}
		c.smu.Unlock()
	})
	return err
}

// Dropped возвращает число событий, отброшенных из-за переполнения буфера.
func (c *Client) Dropped() int64 { return c.dropped.Load() }

// ---------------------------------------------------------------------------
// Чтение и рассылка кадров
// ---------------------------------------------------------------------------

func (c *Client) readLoop() {
	defer close(c.closed)
	fr := &frameReader{br: c.br}
	for {
		select {
		case <-c.stop:
			return
		default:
		}
		f, err := fr.next()
		if err != nil {
			select {
			case <-c.stop:
			default:
				c.connErr = err
				c.cfg.Logf("meshcore: чтение прервано: %v", err)
			}
			return
		}
		c.dispatch(f)
	}
}

func (c *Client) dispatch(f []byte) {
	if len(f) == 0 {
		return
	}
	if c.cfg.Verbose {
		c.cfg.Logf("<< [%d] %s (%s)", len(f), hex.EncodeToString(f), frameName(f[0]))
	}

	// 1) синхронные ожидания (request/response и armed push-waiters)
	c.pmu.Lock()
	for _, p := range c.pending {
		if p.match(f) {
			select {
			case p.ch <- append([]byte(nil), f...):
			default:
			}
		}
	}
	c.pmu.Unlock()

	// 2) подписчики
	c.broadcast(f)
}

func (c *Client) broadcast(f []byte) {
	ev := c.parseEvent(f)
	c.smu.RLock()
	defer c.smu.RUnlock()
	for _, s := range c.subs {
		if s.wants(ev.Kind) {
			select {
			case s.ch <- ev:
			default:
				c.dropped.Add(1)
			}
		}
	}
}

func (c *Client) writeFrame(payload []byte) error {
	if len(payload) > maxFrameSize {
		return fmt.Errorf("meshcore: кадр слишком большой: %d", len(payload))
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if c.cfg.Verbose {
		c.cfg.Logf(">> [%d] %s", len(payload), hex.EncodeToString(payload))
	}
	_, err := c.tr.Write(encodeFrame(payload))
	return err
}

// send записывает кадр, удерживая блокировку команд (без ожидания ответа).
func (c *Client) send(payload []byte) error {
	c.cmu.Lock()
	defer c.cmu.Unlock()
	return c.writeFrame(payload)
}

// tx — короткая транзакция: отправить команду и дождаться одного из want.
func (c *Client) tx(want []byte, payload []byte, timeout time.Duration) ([]byte, error) {
	c.cmu.Lock()
	defer c.cmu.Unlock()
	p := c.arm(matchAny(want))
	defer p.release()
	if err := c.writeFrame(payload); err != nil {
		return nil, err
	}
	return p.next(timeout)
}

// ---------------------------------------------------------------------------
// Ожидания
// ---------------------------------------------------------------------------

type pending struct {
	c     *Client
	id    uint64
	match func([]byte) bool
	ch    chan []byte
}

func (c *Client) arm(match func([]byte) bool) *pending {
	p := &pending{c: c, match: match, ch: make(chan []byte, 64)}
	c.pmu.Lock()
	c.pseq++
	p.id = c.pseq
	c.pending[p.id] = p
	c.pmu.Unlock()
	return p
}

func (p *pending) release() {
	p.c.pmu.Lock()
	delete(p.c.pending, p.id)
	p.c.pmu.Unlock()
}

// next возвращает один кадр, НЕ снимая ожидание с регистрации.
func (p *pending) next(d time.Duration) ([]byte, error) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case f := <-p.ch:
		return f, nil
	case <-t.C:
		return nil, ErrTimeout
	case <-p.c.stop:
		return nil, ErrClosed
	}
}

// nextCtx ждёт кадр с учётом отмены через контекст. Побеждает то, что
// случится раньше: кадр, таймаут, ctx или закрытие клиента.
func (p *pending) nextCtx(ctx context.Context, d time.Duration) ([]byte, error) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case f := <-p.ch:
		return f, nil
	case <-t.C:
		return nil, ErrTimeout
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-p.c.stop:
		return nil, ErrClosed
	}
}

// wait — next + автоматическое снятие с регистрации.
func (p *pending) wait(d time.Duration) ([]byte, error) {
	defer p.release()
	return p.next(d)
}

// collect ждёт кадр, удовлетворяющий ok, игнорируя прочие.
func (p *pending) collect(d time.Duration, ok func([]byte) bool) ([]byte, error) {
	defer p.release()
	deadline := time.Now().Add(d)
	for {
		rem := time.Until(deadline)
		if rem <= 0 {
			return nil, ErrTimeout
		}
		f, err := p.next(rem)
		if err != nil {
			return nil, err
		}
		if ok(f) {
			return f, nil
		}
	}
}

func matchAny(codes []byte) func([]byte) bool {
	set := make(map[byte]bool, len(codes))
	for _, c := range codes {
		set[c] = true
	}
	return func(f []byte) bool { return len(f) > 0 && set[f[0]] }
}

// ---------------------------------------------------------------------------
// Подписки на события
// ---------------------------------------------------------------------------

// EventKind — тип события.
type EventKind int

const (
	EventUnknown EventKind = iota
	EventDirectMessage
	EventChannelMessage
	EventChannelData
	EventRxLog
	EventACK
	EventTelemetry
	EventStatus
	EventBinaryResponse
	EventAdvert
	EventNewAdvert
	EventPathUpdate
	EventMsgWaiting
	EventLoginSuccess
	EventLoginFail
	EventTraceData
	EventControlData
	EventRawData
)

// Event — событие, доставляемое подписчику.
type Event struct {
	Kind    EventKind
	Frame   []byte
	Direct  *DirectMessage
	Channel *ChannelMessage
	RxLog   *RxLogEntry
	Telem   *Telemetry
	PubKey  []byte
}

// Subscription — подписка на события.
type Subscription struct {
	c     *Client
	id    uint64
	all   bool
	kinds map[EventKind]bool
	ch    chan Event
	once  sync.Once
}

// Subscribe регистрирует подписку. Без аргументов — на все события.
func (c *Client) Subscribe(kinds ...EventKind) *Subscription {
	return c.SubscribeBuffered(c.cfg.EventBuffer, kinds...)
}

// SubscribeBuffered — то же, но с явно заданным размером буфера.
func (c *Client) SubscribeBuffered(size int, kinds ...EventKind) *Subscription {
	if size <= 0 {
		size = c.cfg.EventBuffer
	}
	s := &Subscription{c: c, kinds: map[EventKind]bool{}, ch: make(chan Event, size)}
	if len(kinds) == 0 {
		s.all = true
	}
	for _, k := range kinds {
		s.kinds[k] = true
	}
	c.smu.Lock()
	c.sseq++
	s.id = c.sseq
	c.subs[s.id] = s
	c.smu.Unlock()
	return s
}

func (s *Subscription) wants(k EventKind) bool { return s.all || s.kinds[k] }

// Events возвращает канал событий.
func (s *Subscription) Events() <-chan Event { return s.ch }

// Cancel отписывается. Повторные вызовы безопасны.
func (s *Subscription) Cancel() {
	s.once.Do(func() {
		s.c.smu.Lock()
		if _, ok := s.c.subs[s.id]; ok {
			delete(s.c.subs, s.id)
			close(s.ch)
		}
		s.c.smu.Unlock()
	})
}

func (c *Client) parseEvent(f []byte) Event {
	ev := Event{Kind: EventUnknown, Frame: f}
	if len(f) == 0 {
		return ev
	}
	switch f[0] {
	case respContactMsg, respContactMsgV3:
		ev.Kind = EventDirectMessage
		if m, ok := parseDirectMessage(f); ok {
			m.FromName = c.nameByPrefix(m.From[:])
			ev.Direct = m
		}
	case respChannelMsg, respChannelMsgV3:
		ev.Kind = EventChannelMessage
		if m, ok := parseChannelMessage(f); ok {
			m.ChannelName = c.channelName(m.ChannelIndex)
			ev.Channel = m
		}
	case respChannelData:
		ev.Kind = EventChannelData
	case pushRxLog:
		ev.Kind = EventRxLog
		if e, ok := parseRxLog(f); ok {
			ev.RxLog = e
		}
	case pushSendConfirmed:
		ev.Kind = EventACK
	case pushTelemetry:
		ev.Kind = EventTelemetry
		if t, ok := parseTelemetryPush(f); ok {
			t.FromName = c.nameByPrefix(t.From[:])
			ev.Telem = t
		}
	case pushStatusResp:
		ev.Kind = EventStatus
	case pushBinaryResp:
		ev.Kind = EventBinaryResponse
	case pushAdvert:
		ev.Kind = EventAdvert
		if len(f) >= 7 {
			ev.PubKey = append([]byte(nil), f[1:7]...)
		}
	case pushNewAdvert:
		ev.Kind = EventNewAdvert
	case pushPathUpdated:
		ev.Kind = EventPathUpdate
	case pushMsgWaiting:
		ev.Kind = EventMsgWaiting
	case pushLoginSuccess:
		ev.Kind = EventLoginSuccess
	case pushLoginFail:
		ev.Kind = EventLoginFail
	case pushTraceData:
		ev.Kind = EventTraceData
	case pushControlData:
		ev.Kind = EventControlData
	case pushRawData:
		ev.Kind = EventRawData
	}
	return ev
}

// ---------------------------------------------------------------------------
// Насос сообщений
// ---------------------------------------------------------------------------

// pumpLoop вычитывает очередь сообщений прошивки по «щелчкам» MSG_WAITING.
func (c *Client) pumpLoop() {
	sub := c.Subscribe(EventMsgWaiting)
	defer sub.Cancel()
	for {
		select {
		case <-c.stop:
			return
		case _, ok := <-sub.Events():
			if !ok {
				return
			}
			c.DrainMessages()
		}
	}
}

// DrainMessages вычитывает все накопленные сообщения из очереди прошивки.
func (c *Client) DrainMessages() {
	c.drainMu.Lock()
	defer c.drainMu.Unlock()
	for i := 0; i < 512; i++ {
		f, err := c.tx(
			[]byte{respContactMsg, respContactMsgV3, respChannelMsg, respChannelMsgV3,
				respChannelData, respNoMoreMsgs, respErr},
			[]byte{cmdSyncNextMsg}, c.cfg.CommandTimeout)
		if err != nil || len(f) == 0 {
			return
		}
		if f[0] == respNoMoreMsgs || f[0] == respErr {
			return
		}
		// только подписчикам: pending-ожидания уже увидели кадр внутри tx
		c.broadcast(f)
	}
}

// ---------------------------------------------------------------------------
// Инициализация
// ---------------------------------------------------------------------------

func (c *Client) handshake() error {
	deadline := c.cfg.CommandTimeout * 2

	// APP_START: 1 + 7 зарезервированных + имя приложения.
	// Побочный эффект: прошивка сбрасывает _iter_started, поэтому
	// GET_CONTACTS не вернёт BAD_STATE из-за чужого незакрытого списка.
	start := make([]byte, 8, 40)
	start[0] = cmdAppStart
	start = append(start, []byte(c.cfg.AppName)...)
	f, err := c.tx([]byte{respSelfInfo, respErr}, start, deadline)
	if err != nil {
		return fmt.Errorf("meshcore: APP_START: %w", err)
	}
	if f[0] == respErr {
		return frameError(f)
	}
	if si, ok := parseSelfInfo(f); ok {
		c.setSelf(si)
	}

	// DEVICE_QUERY: сообщаем версию протокола (>= 3 -> кадры V3 с SNR).
	f, err = c.tx([]byte{respDeviceInfo, respErr}, []byte{cmdDeviceQuery, appTargetVer}, deadline)
	if err != nil {
		return fmt.Errorf("meshcore: DEVICE_QUERY: %w", err)
	}
	if f[0] == respDeviceInfo {
		if di, ok := parseDeviceInfo(f); ok {
			c.setDevice(di)
		}
	}

	// SET_DEVICE_TIME: прошивка принимает только время «вперёд», ошибку игнорируем.
	ts := make([]byte, 5)
	ts[0] = cmdSetDeviceTime
	binary.LittleEndian.PutUint32(ts[1:], uint32(time.Now().Unix()))
	_, _ = c.tx([]byte{respOK, respErr}, ts, deadline)

	// Кэшируем контакты и каналы.
	if _, err := c.RefreshContacts(context.Background()); err != nil {
		c.cfg.Logf("meshcore: загрузка контактов: %v", err)
	}
	if _, err := c.RefreshChannels(); err != nil {
		c.cfg.Logf("meshcore: загрузка каналов: %v", err)
	}
	c.DrainMessages()
	return nil
}

// clampWait приводит suggested_timeout прошивки к рабочим границам.
// meshcore_py считает suggested_timeout/800 секунд, что эквивалентно
// миллисекундам × 1.25 — отсюда AckSlack по умолчанию.
func (c *Client) clampWait(suggestedMillis uint32) time.Duration {
	w := time.Duration(suggestedMillis) * time.Millisecond
	if c.cfg.AckSlack > 0 {
		w = time.Duration(float64(w) * c.cfg.AckSlack)
	}
	if w < c.cfg.MinWait {
		w = c.cfg.MinWait
	}
	if w > c.cfg.MaxWait {
		w = c.cfg.MaxWait
	}
	return w
}

func (c *Client) sleep(d time.Duration) {
	if d <= 0 {
		return
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-c.stop:
	case <-t.C:
	}
}
