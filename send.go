package GoMeshcoreLib

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

// SendResult is the outcome of a direct message send.
type SendResult struct {
	Acked          bool
	Attempts       int
	DirectAttempts int
	FloodAttempts  int
	Route          string
	Tag            uint32
	TripTime       time.Duration
	Err            error
}

// SendDirectMessage sends a direct message to a contact given by name.
func (c *Client) SendDirectMessage(ctx context.Context, contactName, text string) (*SendResult, error) {
	ct, err := c.ResolveContact(ctx, contactName)
	if err != nil {
		return nil, err
	}
	return c.SendDirectMessageTo(ctx, ct, text)
}

// SendDirectMessageTo sends a direct message using the retry policy:
// DirectAttempts over the last known path, then ResetPath and FloodAttempts
// over flood.
func (c *Client) SendDirectMessageTo(ctx context.Context, ct *Contact, text string) (*SendResult, error) {
	if err := checkText(text); err != nil {
		return nil, err
	}
	res := &SendResult{}
	total := c.cfg.DirectAttempts + c.cfg.FloodAttempts
	// One timestamp for every try: the attempt number is what makes each
	// packet unique and lets receivers spot retries.
	ts := uint32(time.Now().Unix())
	flooded := false

	for i := 0; i < total; i++ {
		if err := ctx.Err(); err != nil {
			res.Err = err
			return res, err
		}
		if !flooded && i == c.cfg.DirectAttempts && c.cfg.FloodAttempts > 0 {
			if err := c.ResetPath(ctx, ct); err != nil {
				c.cfg.Logf("meshcore: path reset failed: %v", err)
			}
			flooded = true
			c.cfg.Logf("meshcore: direct path exhausted (%d tries), switching to flood",
				c.cfg.DirectAttempts)
		}
		res.Attempts = i + 1

		// Arm before sending: the ACK can follow SENT immediately.
		ack := c.arm(func(f []byte) bool {
			return len(f) >= ackFrameLen && f[0] == pushSendConfirmed
		})

		payload := frame(cmdSendTxtMsg, []byte{TxtTypePlain, byte(i)},
			u32le(ts), ct.Prefix(), []byte(text))

		sent, err := c.tx([]byte{respSent, respErr}, payload, c.cfg.CommandTimeout)
		if err != nil || (len(sent) > 0 && sent[0] == respErr) {
			ack.release()
			if err == nil {
				err = frameError(sent)
			}
			res.Err = err
			c.cfg.Logf("meshcore: attempt %d failed: %v", i+1, err)
			if errors.Is(err, ErrNotFound) {
				return res, err
			}
			c.sleep(c.cfg.RetryDelay)
			continue
		}
		if len(sent) < sentFrameLen {
			ack.release()
			res.Err = fmt.Errorf("meshcore: short SENT frame: %d bytes", len(sent))
			c.sleep(c.cfg.RetryDelay)
			continue
		}

		res.Tag = binary.LittleEndian.Uint32(sent[sentTagIdx : sentTagIdx+4])
		res.Route, _ = c.recordRoute(res, sent)
		wait := c.clampWait(binary.LittleEndian.Uint32(sent[sentTimeoutIdx : sentTimeoutIdx+4]))
		c.cfg.Logf("meshcore: attempt %d (%s, tag=%08x), waiting %s for ACK",
			i+1, res.Route, res.Tag, wait)

		tag := res.Tag
		f, err := ack.collect(wait, func(fr []byte) bool {
			return len(fr) >= ackFrameLen &&
				binary.LittleEndian.Uint32(fr[1:5]) == tag
		})
		if err != nil {
			res.Err = ErrNoAck
			c.sleep(c.cfg.RetryDelay)
			continue
		}

		res.Acked, res.Err = true, nil
		if len(f) >= ackTripFrameLen {
			res.TripTime = time.Duration(binary.LittleEndian.Uint32(f[5:9])) * time.Millisecond
		}
		return res, nil
	}

	if res.Err == nil {
		res.Err = ErrNoAck
	}
	return res, res.Err
}

// recordRoute notes which route a SENT frame used and returns its name.
func (c *Client) recordRoute(res *SendResult, sent []byte) (string, bool) {
	flood := sent[sentRouteIdx] == sentRouteFlood
	if flood {
		res.FloodAttempts++
		return "flood", true
	}
	res.DirectAttempts++
	return "direct", false
}

// ---------------------------------------------------------------------------
// Group (hashtag) channels
// ---------------------------------------------------------------------------

// ChannelOptions configures a channel send.
type ChannelOptions struct {
	Scope          string        // region, e.g. "vostok"; empty keeps the node default
	Unscoped       bool          // classic unscoped flood, ignores Scope
	Attempts       int           // overrides cfg.ChannelAttempts
	EchoWindow     time.Duration // overrides cfg.EchoWindow
	RequireRepeats int           // echoes of one packet counted as proof
	RestoreScope   *bool         // restore the previous scope afterwards (default true)
}

func (o ChannelOptions) restore() bool { return o.RestoreScope == nil || *o.RestoreScope }

// ChannelSendResult is the outcome of a channel send.
type ChannelSendResult struct {
	Sent     bool // firmware accepted the message (RESP_CODE_OK)
	Heard    bool // at least one repeater echoed the packet
	Attempts int
	Echoes   []Echo
	Err      error
}

// SendChannelMessage posts to a hashtag channel, optionally setting a region,
// and retries until a repeater echoes the packet.
//
// Channel messages get no tag back — the firmware answers RESP_CODE_OK only —
// so "heard" is derived from rx_log echoes: a repeater that forwards the packet
// appends its hop hash, and we see that copy with SNR and RSSI. Matching an
// echo to our packet is heuristic: GRP_TXT payload, non-empty path, and
// byte-identical payload clustering (repeaters never alter the payload).
func (c *Client) SendChannelMessage(ctx context.Context, ch Channel, text string,
	opts ChannelOptions) (*ChannelSendResult, error) {
	if err := checkText(text); err != nil {
		return nil, err
	}
	res := &ChannelSendResult{}
	attempts, window, require := c.channelPolicy(opts)

	restore, err := c.applyScope(ctx, opts)
	if err != nil {
		return nil, err
	}
	if restore {
		defer c.restoreScope()
	}

	// Subscribe before the first send so the earliest echo is not lost.
	sub := c.SubscribeBuffered(defaultEchoBuffer, EventRxLog)
	defer sub.Cancel()

	for i := 0; i < attempts; i++ {
		if err := ctx.Err(); err != nil {
			res.Err = err
			return res, err
		}
		res.Attempts = i + 1

		payload := frame(cmdSendChannelTxt, []byte{TxtTypePlain, ch.Index},
			u32le(uint32(time.Now().Unix())), []byte(text))

		f, err := c.tx([]byte{respOK, respErr}, payload, c.cfg.CommandTimeout)
		if err != nil || (len(f) > 0 && f[0] == respErr) {
			if err == nil {
				err = frameError(f)
			}
			res.Err = err
			c.cfg.Logf("meshcore: channel attempt %d failed: %v", i+1, err)
			if errors.Is(err, ErrNotFound) {
				return res, err
			}
			continue
		}
		res.Sent = true

		echoes, err := c.waitForEchoes(ctx, sub, window, require)
		if err != nil {
			res.Err = err
			c.cfg.Logf("meshcore: channel attempt %d: no echo confirmed", i+1)
			continue
		}
		res.Heard, res.Echoes, res.Err = true, echoes, nil
		return res, nil
	}

	if res.Err == nil {
		res.Err = ErrNotHeard
	}
	return res, res.Err
}

func (c *Client) channelPolicy(o ChannelOptions) (attempts int, window time.Duration, require int) {
	attempts, window, require = c.cfg.ChannelAttempts, c.cfg.EchoWindow, c.cfg.RequireRepeats
	if o.Attempts > 0 {
		attempts = o.Attempts
	}
	if o.EchoWindow > 0 {
		window = o.EchoWindow
	}
	if o.RequireRepeats > 0 {
		require = o.RequireRepeats
	}
	return
}

// applyScope sets the region for this send and reports whether it must be
// restored afterwards.
func (c *Client) applyScope(ctx context.Context, o ChannelOptions) (bool, error) {
	switch {
	case o.Unscoped:
		return true, c.SetUnscoped(ctx)
	case o.Scope != "":
		return true, c.SetFloodScope(ctx, o.Scope)
	}
	return false, nil
}

func (c *Client) restoreScope() {
	if err := c.ClearFloodScope(context.Background()); err != nil {
		c.cfg.Logf("meshcore: scope restore failed: %v", err)
	}
}

// waitForEchoes collects GRP_TXT echoes and returns the first cluster reaching
// require repeats. Clustering is by byte-identical payload.
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
				return nil, ErrClosed // cancelled elsewhere: avoid a busy loop
			}
			if !isRepeatedGroupPacket(ev.RxLog) {
				continue
			}
			key := hex.EncodeToString(ev.RxLog.Pkt.Payload)
			clusters[key] = append(clusters[key], Echo{
				At: ev.RxLog.At, SNR: ev.RxLog.SNR, RSSI: ev.RxLog.RSSI,
				Path: ev.RxLog.Pkt.Path, Pkt: ev.RxLog.Pkt,
			})
			if len(clusters[key]) >= require {
				return clusters[key], nil
			}
		}
	}
}

// isRepeatedGroupPacket reports whether an rx_log entry is a group text packet
// that at least one repeater forwarded.
func isRepeatedGroupPacket(e *RxLogEntry) bool {
	return e != nil && e.Pkt != nil &&
		e.Pkt.PayloadType == payloadGrpTxt && e.Pkt.Path.Repeated()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// SplitText splits long text into chunks of at most MaxTextLen runes, tagged
// "[i/n]".
func SplitText(text string) []string {
	limit := max(MaxTextLen-len("[00/00] "), chunkTagMin)

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
		out[i] = fmt.Sprintf(chunkTagFormat, i+1, len(chunks), ch)
	}
	return out
}

// Login signs into a repeater or room server. Only administrative actions need
// it; messages and telemetry do not.
func (c *Client) Login(ctx context.Context, ct *Contact, password string) error {
	push := c.arm(matchAny([]byte{pushLoginSuccess, pushLoginFail}))
	defer push.release()

	payload := frame(cmdSendLogin, ct.PublicKey[:], []byte(password))
	sent, err := c.tx([]byte{respSent, respErr}, payload, c.cfg.CommandTimeout)
	switch {
	case err != nil:
		return err
	case sent[0] == respErr:
		return frameError(sent)
	}

	est := uint32(0)
	if len(sent) >= sentFrameLen {
		est = binary.LittleEndian.Uint32(sent[sentTimeoutIdx : sentTimeoutIdx+4])
	}
	f, err := push.wait(c.clampWait(est))
	switch {
	case err != nil:
		return err
	case f[0] == pushLoginFail:
		return errors.New("meshcore: login rejected (bad password)")
	}
	return nil
}
