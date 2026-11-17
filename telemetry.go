package GoMeshcoreLib

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
)

// RequestTelemetry asks a node for telemetry by name, using the same retry
// policy as direct messages: DirectAttempts over the known path, then
// FloodAttempts over flood.
//
// No login is needed: onContactRequest only checks the node telemetry modes
// and the contact flags.
func (c *Client) RequestTelemetry(ctx context.Context, name string) (*Telemetry, error) {
	ct, err := c.ResolveContact(ctx, name)
	if err != nil {
		return nil, err
	}
	return c.RequestTelemetryFrom(ctx, ct)
}

// RequestTelemetryFrom is RequestTelemetry for an already resolved contact.
func (c *Client) RequestTelemetryFrom(ctx context.Context, ct *Contact) (*Telemetry, error) {
	total := c.cfg.DirectAttempts + c.cfg.FloodAttempts
	prefix := ct.Prefix()
	flooded := false
	var lastErr error

	for i := 0; i < total; i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !flooded && i == c.cfg.DirectAttempts && c.cfg.FloodAttempts > 0 {
			if err := c.ResetPath(ctx, ct); err != nil {
				c.cfg.Logf("meshcore: path reset failed: %v", err)
			}
			flooded = true
			c.cfg.Logf("meshcore: telemetry switching to flood after %d tries",
				c.cfg.DirectAttempts)
		}

		// The 0x8B frame carries no tag (the firmware strips it), so match on
		// the 6 byte public key prefix instead.
		push := c.arm(func(f []byte) bool {
			return len(f) >= telemetryMinLen && f[0] == pushTelemetry &&
				bytes.Equal(f[telemetryKeyIdx:telemetryKeyIdx+pubKeyPrefixLen], prefix)
		})

		payload := frame(cmdSendTelemetry, zeros(telemReqReservedLen), ct.PublicKey[:])
		sent, err := c.tx([]byte{respSent, respErr}, payload, c.cfg.CommandTimeout)
		if err != nil || (len(sent) > 0 && sent[0] == respErr) {
			push.release()
			if err == nil {
				err = frameError(sent)
			}
			lastErr = err
			c.cfg.Logf("meshcore: telemetry attempt %d failed: %v", i+1, err)
			if errors.Is(err, ErrNotFound) {
				return nil, err
			}
			c.sleep(c.cfg.RetryDelay)
			continue
		}
		if len(sent) < sentFrameLen {
			push.release()
			lastErr = errors.New("meshcore: short SENT frame")
			c.sleep(c.cfg.RetryDelay)
			continue
		}

		route, _ := "direct", false
		if sent[sentRouteIdx] == sentRouteFlood {
			route = "flood"
		}
		wait := c.clampWait(binary.LittleEndian.Uint32(sent[sentTimeoutIdx : sentTimeoutIdx+4]))
		c.cfg.Logf("meshcore: telemetry attempt %d (%s), waiting %s", i+1, route, wait)

		f, err := push.wait(wait)
		if err != nil {
			lastErr = err
			c.sleep(c.cfg.RetryDelay)
			continue
		}
		t, ok := parseTelemetryPush(f)
		if !ok {
			lastErr = errors.New("meshcore: cannot parse telemetry frame")
			c.sleep(c.cfg.RetryDelay)
			continue
		}
		t.Attempts, t.Route, t.FromName = i+1, route, ct.Name
		return t, nil
	}

	if lastErr == nil {
		lastErr = ErrTimeout
	}
	return nil, lastErr
}

// RequestSelfTelemetry asks the companion for its own telemetry. The request is
// exactly 4 bytes with no public key, and the firmware answers immediately
// without using the radio.
func (c *Client) RequestSelfTelemetry(ctx context.Context) (*Telemetry, error) {
	push := c.arm(func(f []byte) bool {
		return len(f) >= telemetryMinLen && f[0] == pushTelemetry
	})
	defer push.release()

	if err := c.send(frame(cmdSendTelemetry, zeros(telemReqReservedLen))); err != nil {
		return nil, err
	}
	f, err := push.wait(c.cfg.CommandTimeout)
	if err != nil {
		return nil, err
	}
	t, ok := parseTelemetryPush(f)
	if !ok {
		return nil, errors.New("meshcore: cannot parse telemetry frame")
	}
	t.Attempts, t.Route, t.FromName = 1, "self", c.Self().Name
	return t, nil
}

// BatteryStorage is the companion battery and storage report.
type BatteryStorage struct {
	Millivolts uint16
	UsedKB     uint32
	TotalKB    uint32
}

// Battery returns the local node battery voltage and storage usage.
func (c *Client) Battery(ctx context.Context) (BatteryStorage, error) {
	var b BatteryStorage
	f, err := c.txOne(respBattStorage, []byte{cmdGetBattStorage})
	if err != nil {
		return b, err
	}
	if len(f) < batteryFrameLen {
		return b, errors.New("meshcore: short BATTERY frame")
	}
	r := newReader(f[1:])
	b.Millivolts = r.u16()
	b.UsedKB = r.u32()
	b.TotalKB = r.u32()
	return b, nil
}
