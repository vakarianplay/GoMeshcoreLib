package GoMeshcoreLib

import (
	"bufio"
	"context"
	"encoding/hex"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Config holds client parameters and retry policies.
type Config struct {
	// Retry policy for direct messages and telemetry requests.
	DirectAttempts int // tries over the last known path
	FloodAttempts  int // then tries over flood

	// Retry policy for hashtag channels.
	ChannelAttempts int // tries until a repeater echoes the packet

	RetryDelay     time.Duration // pause between tries
	EchoWindow     time.Duration // how long to wait for repeater echoes
	RequireRepeats int           // echoes of one packet counted as proof

	CommandTimeout time.Duration // short command/response exchanges
	AckSlack       float64       // multiplier applied to suggested_timeout
	MinWait        time.Duration // lower bound for mesh responses
	MaxWait        time.Duration // upper bound for mesh responses

	AppName     string
	Verbose     bool
	Logf        func(format string, args ...any)
	EventBuffer int
}

// DefaultConfig returns a Config with the library defaults.
func DefaultConfig() Config {
	return Config{
		DirectAttempts:  defaultDirectAttempts,
		FloodAttempts:   defaultFloodAttempts,
		ChannelAttempts: defaultChannelAttempts,
		EchoWindow:      defaultEchoWindow,
		RequireRepeats:  defaultRequireRepeats,
		CommandTimeout:  defaultCommandTimeout,
		AckSlack:        defaultAckSlack,
		MinWait:         defaultMinWait,
		MaxWait:         defaultMaxWait,
		AppName:         "go-meshcore",
		EventBuffer:     defaultEventBuffer,
	}
}

// withDefaults fills zero fields from DefaultConfig.
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

// Client is a connection to a MeshCore companion radio. All methods are safe
// for concurrent use.
type Client struct {
	cfg Config
	tr  Transport
	br  *bufio.Reader

	wmu sync.Mutex // serialises frame writes
	cmu sync.Mutex // one short command/response transaction at a time

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

// Dial connects to a companion and initialises the session.
func Dial(tr Transport, cfg Config) (*Client, error) {
	c := &Client{
		cfg:     cfg.withDefaults(),
		tr:      tr,
		br:      bufio.NewReaderSize(tr, readBufSize),
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

// DialTCPClient connects over WiFi/TCP.
func DialTCPClient(host string, port int, cfg Config) (*Client, error) {
	tr, err := DialTCP(host, port)
	if err != nil {
		return nil, err
	}
	return Dial(tr, cfg)
}

// DialSerialClient connects over USB.
func DialSerialClient(port string, baud int, cfg Config) (*Client, error) {
	tr, err := DialSerial(port, baud)
	if err != nil {
		return nil, err
	}
	return Dial(tr, cfg)
}

// Close shuts the connection down and releases resources.
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

// Dropped reports how many events were lost to a full subscription buffer.
func (c *Client) Dropped() int64 { return c.dropped.Load() }

// ---------------------------------------------------------------------------
// Framing and dispatch
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
				c.cfg.Logf("meshcore: read loop stopped: %v", err)
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
	c.matchPending(f)
	c.broadcast(f)
}

// matchPending delivers a frame to every armed waiter whose predicate accepts.
func (c *Client) matchPending(f []byte) {
	c.pmu.Lock()
	defer c.pmu.Unlock()
	for _, p := range c.pending {
		if !p.match(f) {
			continue
		}
		select {
		case p.ch <- append([]byte(nil), f...):
		default:
		}
	}
}

func (c *Client) broadcast(f []byte) {
	ev := c.parseEvent(f)
	c.smu.RLock()
	defer c.smu.RUnlock()
	for _, s := range c.subs {
		if !s.wants(ev.Kind) {
			continue
		}
		select {
		case s.ch <- ev:
		default:
			c.dropped.Add(1)
		}
	}
}

func (c *Client) writeFrame(payload []byte) error {
	if len(payload) > maxFrameSize {
		return fmt.Errorf("meshcore: frame too large: %d bytes", len(payload))
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if c.cfg.Verbose {
		c.cfg.Logf(">> [%d] %s", len(payload), hex.EncodeToString(payload))
	}
	_, err := c.tr.Write(encodeFrame(payload))
	return err
}

// send writes a frame without waiting for a reply.
func (c *Client) send(payload []byte) error {
	c.cmu.Lock()
	defer c.cmu.Unlock()
	return c.writeFrame(payload)
}

// tx runs one short transaction: send a command, wait for one of want.
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

// txOK sends a command that must answer OK.
func (c *Client) txOK(payload []byte) error {
	f, err := c.tx([]byte{respOK, respErr}, payload, c.cfg.CommandTimeout)
	switch {
	case err != nil:
		return err
	case f[0] == respErr:
		return frameError(f)
	}
	return nil
}

// txOne sends a command expecting exactly one reply code besides ERR.
func (c *Client) txOne(want byte, payload []byte) ([]byte, error) {
	f, err := c.tx([]byte{want, respErr}, payload, c.cfg.CommandTimeout)
	switch {
	case err != nil:
		return nil, err
	case f[0] == respErr:
		return nil, frameError(f)
	}
	return f, nil
}

// ---------------------------------------------------------------------------
// Waiters
// ---------------------------------------------------------------------------

type pending struct {
	c     *Client
	id    uint64
	match func([]byte) bool
	ch    chan []byte
}

func (c *Client) arm(match func([]byte) bool) *pending {
	p := &pending{c: c, match: match, ch: make(chan []byte, pendingBuffer)}
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

// next returns one frame and keeps the waiter armed.
func (p *pending) next(d time.Duration) ([]byte, error) {
	return p.waitSelect(d, nil)
}

// nextCtx is next with context cancellation.
func (p *pending) nextCtx(ctx context.Context, d time.Duration) ([]byte, error) {
	return p.waitSelect(d, ctx.Done())
}

func (p *pending) waitSelect(d time.Duration, done <-chan struct{}) ([]byte, error) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case f := <-p.ch:
		return f, nil
	case <-t.C:
		return nil, ErrTimeout
	case <-done:
		return nil, context.Canceled
	case <-p.c.stop:
		return nil, ErrClosed
	}
}

// wait returns one frame and disarms the waiter.
func (p *pending) wait(d time.Duration) ([]byte, error) {
	defer p.release()
	return p.next(d)
}

// collect waits for a frame accepted by ok, ignoring the rest.
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
	set := make(map[byte]struct{}, len(codes))
	for _, c := range codes {
		set[c] = struct{}{}
	}
	return func(f []byte) bool {
		_, ok := set[f[0]]
		return len(f) > 0 && ok
	}
}

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

// EventKind identifies an event.
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

// Event is delivered to subscribers.
type Event struct {
	Kind    EventKind
	Frame   []byte
	Direct  *DirectMessage
	Channel *ChannelMessage
	RxLog   *RxLogEntry
	Telem   *Telemetry
	PubKey  []byte
}

// Subscription receives events.
type Subscription struct {
	c     *Client
	id    uint64
	all   bool
	kinds map[EventKind]bool
	ch    chan Event
	once  sync.Once
}

// Subscribe registers a subscription. Without kinds it receives everything.
func (c *Client) Subscribe(kinds ...EventKind) *Subscription {
	return c.SubscribeBuffered(c.cfg.EventBuffer, kinds...)
}

// SubscribeBuffered registers a subscription with an explicit buffer size.
func (c *Client) SubscribeBuffered(size int, kinds ...EventKind) *Subscription {
	s := &Subscription{
		c:     c,
		kinds: make(map[EventKind]bool, len(kinds)),
		ch:    make(chan Event, max(size, c.cfg.EventBuffer)),
		all:   len(kinds) == 0,
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

// Events returns the event channel.
func (s *Subscription) Events() <-chan Event { return s.ch }

// Cancel unsubscribes. Repeated calls are safe.
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
		if len(f) > pubKeyPrefixLen {
			ev.PubKey = append([]byte(nil), f[1:1+pubKeyPrefixLen]...)
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
// Message pump
// ---------------------------------------------------------------------------

// pumpLoop drains the firmware message queue on every MSG_WAITING tick.
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

// DrainMessages pulls every queued message out of the firmware.
func (c *Client) DrainMessages() {
	c.drainMu.Lock()
	defer c.drainMu.Unlock()

	want := []byte{respContactMsg, respContactMsgV3, respChannelMsg, respChannelMsgV3,
		respChannelData, respNoMoreMsgs, respErr}

	for i := 0; i < drainLimit; i++ {
		f, err := c.tx(want, []byte{cmdSyncNextMsg}, c.cfg.CommandTimeout)
		if err != nil || len(f) == 0 || f[0] == respNoMoreMsgs || f[0] == respErr {
			return
		}
		c.broadcast(f) // waiters already saw the frame inside tx
	}
}

// ---------------------------------------------------------------------------
// Session start
// ---------------------------------------------------------------------------

func (c *Client) handshake() error {
	timeout := c.cfg.CommandTimeout * handshakeFactor

	// APP_START also resets the firmware contact iterator, so GET_CONTACTS
	// cannot fail with BAD_STATE left over from another client.
	payload := append([]byte{cmdAppStart}, zeros(handshakeReservedLen)...)
	payload = append(payload, []byte(c.cfg.AppName)...)

	f, err := c.tx([]byte{respSelfInfo, respErr}, payload, timeout)
	switch {
	case err != nil:
		return fmt.Errorf("meshcore: APP_START: %w", err)
	case f[0] == respErr:
		return frameError(f)
	}
	if si, ok := parseSelfInfo(f); ok {
		c.setSelf(si)
	}

	// DEVICE_QUERY announces the protocol version we understand (>= 3 gives
	// V3 frames carrying SNR).
	f, err = c.tx([]byte{respDeviceInfo, respErr},
		[]byte{cmdDeviceQuery, appTargetVer}, timeout)
	if err != nil {
		return fmt.Errorf("meshcore: DEVICE_QUERY: %w", err)
	}
	if di, ok := parseDeviceInfo(f); ok {
		c.setDevice(di)
	}

	// SET_DEVICE_TIME only accepts forward jumps; an error here is harmless.
	_, _ = c.tx([]byte{respOK, respErr},
		frame(cmdSetDeviceTime, u32le(uint32(time.Now().Unix()))), timeout)

	if _, err := c.RefreshContacts(context.Background()); err != nil {
		c.cfg.Logf("meshcore: contact load failed: %v", err)
	}
	if _, err := c.RefreshChannels(); err != nil {
		c.cfg.Logf("meshcore: channel load failed: %v", err)
	}
	c.DrainMessages()
	return nil
}

// clampWait scales the firmware suggested_timeout into working bounds.
// meshcore_py uses suggested_timeout/800 seconds, i.e. millis * 1.25, which is
// what the default AckSlack reproduces.
func (c *Client) clampWait(suggestedMillis uint32) time.Duration {
	w := time.Duration(suggestedMillis) * time.Millisecond
	if c.cfg.AckSlack > 0 {
		w = time.Duration(float64(w) * c.cfg.AckSlack)
	}
	return min(max(w, c.cfg.MinWait), c.cfg.MaxWait)
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
