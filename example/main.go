// Бот MeshCore: слушает хэштег-канал, по команде !reps отдаёт телеметрию
// репитеров, по команде !ping — данные о маршруте и качестве сигнала.
//
//	go run ./example -host mesh.lan -port 5000 -channel test -scope vostok -v
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"meshcore"
)

// payloadGrpTxt — тип полезной нагрузки группового текстового пакета
// (PAYLOAD_TYPE_GRP_TXT в Packet.h). Дублируется здесь, потому что в
// библиотеке константа не экспортируется.
const payloadGrpTxt uint8 = 0x05

// ---------------------------------------------------------------------------
// Конфигурация
// ---------------------------------------------------------------------------

type botConfig struct {
	host       string
	port       int
	serialPort string
	baud       int

	channel  string // имя хэштег-канала без '#'
	scope    string // регион рассылки канала
	repsCmd  string // команда телеметрии репитеров
	pingCmd  string // команда пинга
	reps     []string
	direct   int
	flood    int
	chanAtt  int
	echoWin  time.Duration
	globalSc bool
	cooldown time.Duration
	verbose  bool
}

func parseFlags() botConfig {
	var cfg botConfig
	var reps string

	flag.StringVar(&cfg.host, "host", "mesh.lan", "хост компаньона (WiFi/TCP)")
	flag.IntVar(&cfg.port, "port", 5000, "TCP-порт компаньона")
	flag.StringVar(&cfg.serialPort, "serial", "", "COM-порт (если задан — вместо TCP)")
	flag.IntVar(&cfg.baud, "baud", 115200, "скорость COM-порта")

	flag.StringVar(&cfg.channel, "channel", "test", "имя хэштег-канала (без '#')")
	flag.StringVar(&cfg.scope, "scope", "est", "регион рассылки (пусто = scope узла)")
	flag.StringVar(&cfg.repsCmd, "command", "!reps", "команда: телеметрия репитеров")
	flag.StringVar(&cfg.pingCmd, "ping", "!ping", "команда: пинг")
	flag.StringVar(&reps, "reps", "MO EST 08,MO EST 13,MO EST 14 📡, MO EST MQTT 09",
		"имена репитеров через запятую")

	flag.IntVar(&cfg.direct, "direct", 5, "попыток телеметрии по известному маршруту")
	flag.IntVar(&cfg.flood, "flood", 5, "затем широковещательных попыток")
	flag.IntVar(&cfg.chanAtt, "chan-attempts", 3, "попыток отправки в канал")
	flag.DurationVar(&cfg.echoWin, "echo-window", 6*time.Second,
		"сколько ждать эхо от ретрансляторов")

	flag.BoolVar(&cfg.globalSc, "global-scope", false,
		"задать регион один раз на сессию (иначе — вокруг каждой отправки)")
	flag.DurationVar(&cfg.cooldown, "cooldown", 20*time.Second,
		"минимальный интервал между обработками одной команды")
	flag.BoolVar(&cfg.verbose, "v", false, "дамп кадров в stderr")

	flag.Parse()

	for _, name := range strings.Split(reps, ",") {
		if n := strings.TrimSpace(name); n != "" {
			cfg.reps = append(cfg.reps, n)
		}
	}
	if len(cfg.reps) == 0 {
		fmt.Fprintln(os.Stderr, "ОШИБКА: -reps пуст")
		os.Exit(2)
	}
	if cfg.channel == "" {
		fmt.Fprintln(os.Stderr, "ОШИБКА: -channel пуст")
		os.Exit(2)
	}
	if strings.EqualFold(strings.TrimSpace(cfg.repsCmd),
		strings.TrimSpace(cfg.pingCmd)) {
		fmt.Fprintln(os.Stderr, "ОШИБКА: -command и -ping должны различаться")
		os.Exit(2)
	}
	return cfg
}

// ---------------------------------------------------------------------------
// Вспомогательное
// ---------------------------------------------------------------------------

func logf(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "%s %s\n",
		time.Now().Format("15:04:05"), fmt.Sprintf(format, a...))
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func minDur(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

// splitForMesh режет текст на части не длиннее 133 БАЙТ, не разрывая руны.
func splitForMesh(text string) []string {
	if len(text) <= meshcore.MaxTextLen {
		return []string{text}
	}
	const reserve = 8 // "[01/02] "
	body := meshcore.MaxTextLen - reserve

	runes := []rune(text)
	var chunks []string
	for start := 0; start < len(runes); {
		end, n := start, 0
		for end < len(runes) {
			sz := len(string(runes[end]))
			if n+sz > body {
				break
			}
			n += sz
			end++
		}
		if end == start { // одиночная руна больше body — гарантируем прогресс
			end = start + 1
		}
		chunks = append(chunks, string(runes[start:end]))
		start = end
	}
	out := make([]string, len(chunks))
	for i, ch := range chunks {
		out[i] = fmt.Sprintf("[%d/%d] %s", i+1, len(chunks), ch)
	}
	return out
}

// splitSender отделяет имя отправителя от текста.
//
// Прошивка форматирует групповое сообщение как "<sender_name>: <text>"
// (BaseChatMesh::sendGroupMessage). Символ ':' запрещён в именах узлов
// (isValidName), поэтому разбиение по первому ':' однозначно.
// Если префикса нет — имя считается неизвестным.
func splitSender(text string) (sender, body string) {
	if i := strings.Index(text, ": "); i > 0 {
		return strings.TrimSpace(text[:i]), text[i+2:]
	}
	if i := strings.Index(text, ":"); i > 0 {
		return strings.TrimSpace(text[:i]), text[i+1:]
	}
	return "", strings.TrimSpace(text)
}

// isCommand — терпимое распознавание команды по токенам.
func isCommand(text, cmd string) bool {
	target := strings.ToLower(strings.TrimSpace(cmd))
	if target == "" {
		return false
	}
	for _, tok := range strings.Fields(strings.ToLower(text)) {
		if tok == target {
			return true
		}
	}
	return false
}

func shortErr(err error) string {
	if err == nil {
		return "ok"
	}
	s := err.Error()
	if len(s) > 90 {
		s = s[:87] + "..."
	}
	return s
}

// ---------------------------------------------------------------------------
// Кольцевой буфер rx_log
// ---------------------------------------------------------------------------

// rxRing хранит последние сырые пакеты из rx_log (push 0x88).
//
// Только rx_log несёт RSSI и сами хэши хопов: кадр группового сообщения
// содержит лишь SNR и байт path_len. Поэтому для !ping мы ищем здесь сырой
// пакет, соответствующий принятому сообщению.
type rxRing struct {
	mu    sync.Mutex
	items []meshcore.RxLogEntry
	max   int
	ttl   time.Duration
}

func newRxRing(max int, ttl time.Duration) *rxRing {
	return &rxRing{items: make([]meshcore.RxLogEntry, 0, max), max: max, ttl: ttl}
}

func (r *rxRing) add(e meshcore.RxLogEntry) {
	if e.Pkt == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	r.items = append(r.items, e)
	if len(r.items) > r.max {
		r.items = r.items[len(r.items)-r.max:]
		return
	}
	// подрезаем устаревшее
	cut := 0
	lim := time.Now().Add(-r.ttl)
	for cut < len(r.items) && r.items[cut].At.Before(lim) {
		cut++
	}
	if cut > 0 {
		r.items = append(r.items[:0], r.items[cut:]...)
	}
}

// find ищет сырой пакет, соответствующий принятому групповому сообщению.
//
// Критерии совпадения: тип нагрузки GRP_TXT, одинаковый SNR (обе величины
// вычисляются прошивкой как int8(snr*4) из одного и того же приёма) и
// согласованный path_len. Ищем с конца — берём самое свежее совпадение.
func (r *rxRing) find(m *meshcore.ChannelMessage) (meshcore.RxLogEntry, bool) {
	var none meshcore.RxLogEntry
	if m == nil {
		return none, false
	}
	wantSNR := snrCode(m.SNR)
	lim := time.Now().Add(-r.ttl)

	r.mu.Lock()
	defer r.mu.Unlock()

	for i := len(r.items) - 1; i >= 0; i-- {
		e := r.items[i]
		if e.At.Before(lim) || e.Pkt == nil {
			continue
		}
		if e.Pkt.PayloadType != payloadGrpTxt {
			continue
		}
		if snrCode(e.SNR) != wantSNR {
			continue
		}
		if !pathMatches(m.Path, e.Pkt) {
			continue
		}
		return e, true
	}
	return none, false
}

// snrCode приводит float SNR обратно к целому коду прошивки (int8, ×4).
func snrCode(snr float64) int {
	v := int(snr * 4)
	if snr < 0 {
		v = -int((-snr)*4 + 0.5)
	} else {
		v = int(snr*4 + 0.5)
	}
	if v > 127 {
		v = 127
	}
	if v < -128 {
		v = -128
	}
	return v
}

// pathMatches сверяет байт path_len из кадра сообщения с маршрутом сырого пакета.
// В кадре direct представлен маркером 0xFF, а в самом пакете — реальным
// path_len при direct-типе маршрута, поэтому сравниваем по типу маршрута.
func pathMatches(p meshcore.PathInfo, pkt *meshcore.RawPacket) bool {
	if !p.Flood {
		return !pkt.IsFlood()
	}
	return pkt.IsFlood() && pkt.PathLenByte == p.Encoded
}

// routeText формирует человекочитаемый маршрут.
func routeText(p meshcore.PathInfo) string {
	if len(p.Hops) == 0 {
		if p.Flood {
			return "flood (без хопов)"
		}
		return "direct"
	}
	return strings.Join(p.Hops, "→")
}

// ---------------------------------------------------------------------------
// Форматирование телеметрии
// ---------------------------------------------------------------------------

var fieldOrder = []struct {
	key   string
	label string
	unit  string
	prec  int
}{
	{"voltage", "U", "V", 2},
	{"temperature", "T", "C", 1},
	{"relative_humidity", "RH", "%", 1},
	{"pressure", "P", "hPa", 1},
	{"percentage", "Bat", "%", 0},
	{"luminosity", "Lux", "", 0},
	{"altitude", "Alt", "m", 0},
	{"current", "I", "A", 2},
	{"power", "W", "W", 0},
	{"concentration", "ppm", "", 0},
}

func formatTelemetry(name string, t *meshcore.Telemetry) string {
	used := make(map[int]bool, len(t.LPP.Fields))
	var parts []string

	for _, spec := range fieldOrder {
		for i := range t.LPP.Fields {
			f := t.LPP.Fields[i]
			if used[i] || f.Name != spec.key {
				continue
			}
			used[i] = true
			parts = append(parts,
				fmt.Sprintf("%s=%.*f%s", spec.label, spec.prec, f.Value, spec.unit))
		}
	}

	var lat, lon *float64
	for i := range t.LPP.Fields {
		f := t.LPP.Fields[i]
		v := f.Value
		switch f.Name {
		case "gps.lat":
			lat = &v
		case "gps.lon":
			lon = &v
		}
	}
	if lat != nil && lon != nil && (*lat != 0 || *lon != 0) {
		parts = append(parts, fmt.Sprintf("GPS=%.4f/%.4f", *lat, *lon))
	}

	for i := range t.LPP.Fields {
		f := t.LPP.Fields[i]
		if used[i] || strings.HasPrefix(f.Name, "gps.") {
			continue
		}
		parts = append(parts, fmt.Sprintf("%s=%.2f", f.Name, f.Value))
	}

	var b strings.Builder
	b.WriteString(name)
	b.WriteString("\n")
	if len(parts) == 0 {
		b.WriteString("нет данных")
	} else {
		b.WriteString(strings.Join(parts, " "))
	}
	b.WriteString(fmt.Sprintf("\n(%d поп., %s)", t.Attempts, t.Route))
	if t.LPP.Tail != "" {
		b.WriteString(" [?]")
	}
	return b.String()
}

func failText(name string, err error) string {
	return fmt.Sprintf("%s\nтелеметрия недоступна\n(%s)", name, shortErr(err))
}

// ---------------------------------------------------------------------------
// Сессия
// ---------------------------------------------------------------------------

type resolvedRep struct {
	requested string
	contact   *meshcore.Contact
	ok        bool
}

type session struct {
	cfg      botConfig
	client   *meshcore.Client
	ch       meshcore.Channel
	selfName string
	rx       *rxRing

	// Запросы телеметрии строго последовательны: прошивка хранит только один
	// pending_telemetry и сбрасывает его при каждом новом запросе.
	teleMu sync.Mutex
	repsMu sync.Mutex

	busyReps atomic.Bool
	busyPing atomic.Bool

	lastMu sync.Mutex
	last   map[string]time.Time

	lastDropped atomic.Int64
}

func newSession(ctx context.Context, cfg botConfig) (*session, error) {
	var (
		tr  meshcore.Transport
		err error
	)
	if cfg.serialPort != "" {
		tr, err = meshcore.DialSerial(cfg.serialPort, cfg.baud)
	} else {
		tr, err = meshcore.DialTCP(cfg.host, cfg.port)
	}
	if err != nil {
		return nil, err
	}

	mcCfg := meshcore.DefaultConfig()
	mcCfg.DirectAttempts = cfg.direct
	mcCfg.FloodAttempts = cfg.flood
	mcCfg.ChannelAttempts = cfg.chanAtt
	mcCfg.EchoWindow = cfg.echoWin
	mcCfg.Verbose = cfg.verbose
	mcCfg.RetryDelay = 500 * time.Millisecond
	mcCfg.AppName = "reps-bot"
	mcCfg.Logf = func(f string, a ...any) {
		if cfg.verbose {
			logf(f, a...)
		}
	}

	client, err := meshcore.Dial(tr, mcCfg)
	if err != nil {
		_ = tr.Close()
		return nil, err
	}

	s := &session{
		cfg:      cfg,
		client:   client,
		selfName: client.Self().Name,
		rx:       newRxRing(512, 90*time.Second),
		last:     map[string]time.Time{},
	}

	dev := client.Device()
	logf("подключено: узел=%q модель=%q прошивка=%s",
		s.selfName, dev.Model, dev.Version)
	logf("радио: %s", client.RadioPrefs())

	// Канал. Без записи в слоте компаньон не расшифрует трафик #test,
	// поэтому создаём его, если его ещё нет. Ключ = sha256("#test")[0:16].
	ch, err := client.EnsureHashtagChannel(ctx, cfg.channel)
	if err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("канал #%s: %w", cfg.channel, err)
	}
	s.ch = ch
	logf("канал: %s (слот %d)", ch.Name, ch.Index)

	if cfg.scope != "" && cfg.globalSc {
		if err := client.SetFloodScope(ctx, cfg.scope); err != nil {
			logf("ВНИМАНИЕ: не удалось задать регион %q: %v", cfg.scope, err)
		} else {
			logf("регион сессии: #%s", meshcore.NormalizeHashtag(cfg.scope))
		}
	} else if cfg.scope != "" {
		logf("регион #%s будет задаваться вокруг каждой отправки",
			meshcore.NormalizeHashtag(cfg.scope))
	}

	reps := s.resolveTargets(ctx)
	for _, r := range reps {
		if r.ok {
			logf("репитер: %s (type=%d)", r.contact.Name, r.contact.Type)
		} else {
			logf("ВНИМАНИЕ: %q не найден в контактах", r.requested)
		}
	}

	return s, nil
}

// resolveTargets сопоставляет имена из конфига с контактами. Если кого-то нет —
// обновляет книгу контактов и пробует снова (репитер могли услышать позже).
func (s *session) resolveTargets(ctx context.Context) []resolvedRep {
	out := make([]resolvedRep, 0, len(s.cfg.reps))
	missing := false
	for _, name := range s.cfg.reps {
		ct, err := s.client.ResolveContact(ctx, name)
		if err != nil {
			missing = true
			out = append(out, resolvedRep{requested: name})
			continue
		}
		if ct.Name != name {
			logf("примечание: %q найден как %q", name, ct.Name)
		}
		out = append(out, resolvedRep{requested: name, contact: ct, ok: true})
	}

	if missing {
		logf("кого-то не хватает — обновляю книгу контактов…")
		if _, err := s.client.RefreshContacts(ctx); err != nil {
			logf("обновление контактов: %v", err)
			return out
		}
		for i := range out {
			if out[i].ok {
				continue
			}
			if ct, err := s.client.ResolveContact(ctx, out[i].requested); err == nil {
				out[i].contact, out[i].ok = ct, true
			}
		}
	}
	return out
}

// allowRun реализует cooldown для каждой команды отдельно.
func (s *session) allowRun(key string) bool {
	s.lastMu.Lock()
	defer s.lastMu.Unlock()
	if prev, ok := s.last[key]; ok {
		if since := time.Since(prev); since < s.cfg.cooldown {
			logf("%s: cooldown, осталось %s", key,
				(s.cfg.cooldown - since).Round(time.Second))
			return false
		}
	}
	s.last[key] = time.Now()
	return true
}

// noteDropped предупреждает о потерях событий (rx_log — это поток всех
// принятых пакетов, буфер может переполняться при плотном эфире).
func (s *session) noteDropped() {
	cur := s.client.Dropped()
	prev := s.lastDropped.Swap(cur)
	if cur > prev {
		logf("ВНИМАНИЕ: отброшено событий: %d (всего %d) — увеличьте буфер подписки",
			cur-prev, cur)
	}
}

// post отправляет текст в канал.
func (s *session) post(ctx context.Context, text string) {
	// При глобальном регионе не трогаем scope вокруг отправки, иначе
	// библиотека сбросит его в defer и последующие сообщения уйдут без региона.
	scope := s.cfg.scope
	if s.cfg.globalSc {
		scope = ""
	}

	for _, part := range splitForMesh(text) {
		res, err := s.client.SendChannelMessage(ctx, s.ch, part, meshcore.ChannelOptions{
			Scope:      scope,
			EchoWindow: s.cfg.echoWin,
			Attempts:   s.cfg.chanAtt,
		})
		sent := res != nil && res.Sent
		switch {
		case err == nil:
			logf("-> %s: %q (эхо=%d, попыток=%d)",
				s.ch.Name, part, len(res.Echoes), res.Attempts)
		case sent:
			// Пакет ушёл в эфир, но эхо от ретрансляторов не подтверждено.
			logf("-> %s: отправлено, эхо не подтверждено (%d попыток): %v",
				s.ch.Name, res.Attempts, err)
		default:
			logf("!! не удалось отправить в %s: %v", s.ch.Name, err)
			return
		}
		if ctx.Err() != nil {
			return
		}
	}
}

// ---------------------------------------------------------------------------
// Обработчик !ping
// ---------------------------------------------------------------------------

// handlePing отвечает упоминанием отправителя, числом хопов, маршрутом,
// SNR и RSSI принятой команды.
//
// SNR и число хопов есть в самом кадре сообщения. RSSI и хэши хопов берутся
// из rx_log (push 0x88) — единственного источника этих данных; если
// сопоставить сырой пакет не удалось, отвечаем по данным кадра.
func (s *session) handlePing(ctx context.Context, m *meshcore.ChannelMessage, sender string) {
	defer s.busyPing.Store(false)
	s.noteDropped()

	entry, corr := s.rx.find(m)

	var (
		hops  int
		route string
		snr   float64
		rssi  float64
		hasRS bool
		pkt   *meshcore.RawPacket
	)

	if corr {
		pkt = entry.Pkt
		hops = pkt.Path.HashCount
		route = routeText(pkt.Path)
		snr = entry.SNR
		rssi = entry.RSSI
		hasRS = true
	} else {
		hops = m.Path.HashCount
		route = routeText(m.Path)
		snr = m.SNR
		logf("!ping: rx_log не сопоставлен, отвечаю по данным кадра")
	}

	var b strings.Builder
	if sender != "" {
		b.WriteString("@[" + sender + "] ")
	}
	b.WriteString("pong\n")
	b.WriteString(fmt.Sprintf("хопов: %d\n", hops))
	b.WriteString("маршрут: " + route + "\n")
	if m.HasSNR || corr {
		b.WriteString(fmt.Sprintf("SNR: %.2f dB\n", snr))
	} else {
		b.WriteString("SNR: н/д\n")
	}
	if hasRS {
		b.WriteString(fmt.Sprintf("RSSI: %.0f dBm", rssi))
	} else {
		b.WriteString("RSSI: н/д (rx_log не сопоставлен)")
	}
	// Код региона доступен только из сырого пакета и только для scoped-flood.
	// Имя региона из него не восстанавливается: это усечённый HMAC, а не ID.
	if pkt != nil && pkt.HasTransport && pkt.TransportCodes[0] != 0 {
		b.WriteString(fmt.Sprintf("\nрегион: код 0x%04x", pkt.TransportCodes[0]))
	}

	s.post(ctx, b.String())
	logf("!ping -> %q (хопов=%d, корреляция=%v)", sender, hops, corr)
}

// ---------------------------------------------------------------------------
// Обработчик !reps
// ---------------------------------------------------------------------------

// handleReps опрашивает репитеров и публикует телеметрию по готовности.
func (s *session) handleReps(ctx context.Context) {
	defer s.busyReps.Store(false)
	s.teleMu.Lock()
	defer s.teleMu.Unlock()
	s.noteDropped()

	reps := s.resolveTargets(ctx)
	logf("%s: опрашиваю %d репитеров", s.cfg.repsCmd, len(reps))

	var okCount int
	for i := range reps {
		if ctx.Err() != nil {
			return
		}
		r := reps[i]

		if !r.ok {
			s.post(ctx, fmt.Sprintf("%s\nнет в контактах", r.requested))
			continue
		}

		name := r.contact.Name
		start := time.Now()

		// RequestTelemetryFrom уже содержит политику повторов:
		// cfg.DirectAttempts по известному маршруту, затем ResetPath
		// и cfg.FloodAttempts широковещательно.
		t, err := s.client.RequestTelemetryFrom(ctx, r.contact)
		elapsed := time.Since(start).Round(time.Second)

		if err != nil || t == nil {
			logf("%s: НЕ получено за %s: %v", name, elapsed, shortErr(err))
			s.post(ctx, failText(name, err))
			continue
		}

		okCount++
		logf("%s: получено за %s (%d попыток, %s, %d полей)",
			name, elapsed, t.Attempts, t.Route, len(t.LPP.Fields))
		s.post(ctx, formatTelemetry(name, t))
	}

	logf("%s: готово, получено %d из %d", s.cfg.repsCmd, okCount, len(reps))
}

// ---------------------------------------------------------------------------
// Приём сообщений
// ---------------------------------------------------------------------------

func (s *session) handleChannelMessage(ctx context.Context, m *meshcore.ChannelMessage) {
	sender, body := splitSender(m.Text)

	// Не отвечаем сами себе: это наш ответ или его эхо от ретранслятора.
	if s.selfName != "" && strings.EqualFold(sender, s.selfName) {
		return
	}

	if s.cfg.verbose {
		logf("[CH %s] от=%q snr=%.2f %s: %q",
			m.ChannelName, sender, m.SNR, m.Path, m.Text)
	}

	switch {
	case isCommand(body, s.cfg.pingCmd):
		if !s.allowRun("ping") {
			return
		}
		if !s.busyPing.CompareAndSwap(false, true) {
			logf("%s уже обрабатывается — пропускаю", s.cfg.pingCmd)
			return
		}
		logf("получена команда %q от %q (snr=%.2f, %s)",
			s.cfg.pingCmd, sender, m.SNR, m.Path)
		go s.handlePing(ctx, m, sender)

	case isCommand(body, s.cfg.repsCmd):
		if !s.allowRun("reps") {
			return
		}
		if !s.busyReps.CompareAndSwap(false, true) {
			logf("%s уже обрабатывается — пропускаю", s.cfg.repsCmd)
			return
		}
		logf("получена команда %q от %q", s.cfg.repsCmd, sender)
		go s.handleReps(ctx)
	}
}

func (s *session) run(ctx context.Context) error {
	// rx_log — поток ВСЕХ принятых пакетов, поэтому буфер берём большой:
	// при переполнении события теряются и !ping теряет RSSI с маршрутом.
	sub := s.client.SubscribeBuffered(4096,
		meshcore.EventChannelMessage,
		meshcore.EventDirectMessage,
		meshcore.EventRxLog,
	)
	defer sub.Cancel()

	logf("слушаю %s (регион %q, команды %q и %q)…",
		s.ch.Name, s.cfg.scope, s.cfg.repsCmd, s.cfg.pingCmd)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case ev, ok := <-sub.Events():
			if !ok {
				return meshcore.ErrClosed
			}

			switch ev.Kind {
			case meshcore.EventRxLog:
				if ev.RxLog != nil {
					s.rx.add(*ev.RxLog)
				}

			case meshcore.EventDirectMessage:
				if ev.Direct != nil {
					logf("[DM] %s: %s", ev.Direct.FromName, ev.Direct.Text)
				}

			case meshcore.EventChannelMessage:
				if ev.Channel == nil || ev.Channel.ChannelIndex != s.ch.Index {
					continue
				}
				s.handleChannelMessage(ctx, ev.Channel)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// main: супервизор с переподключением
// ---------------------------------------------------------------------------

func main() {
	cfg := parseFlags()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	logf("запуск: канал=#%s регион=%q команды=%q,%q репитеров=%d",
		cfg.channel, cfg.scope, cfg.repsCmd, cfg.pingCmd, len(cfg.reps))

	backoff := 2 * time.Second
	for {
		if ctx.Err() != nil {
			logf("остановлено")
			return
		}

		err := runSession(ctx, cfg)
		if ctx.Err() != nil {
			logf("остановлено")
			return
		}

		logf("сессия прервана: %v — переподключение через %s", err, backoff)
		if !sleepCtx(ctx, backoff) {
			return
		}
		backoff = minDur(backoff*2, 60*time.Second)
	}
}

func runSession(ctx context.Context, cfg botConfig) error {
	s, err := newSession(ctx, cfg)
	if err != nil {
		return err
	}
	defer s.client.Close()

	return s.run(ctx)
}