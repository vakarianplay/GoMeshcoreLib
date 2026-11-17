package GoMeshcoreLib

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Self returns information about the local node, captured at connect time.
func (c *Client) Self() SelfInfo {
	c.selfMu.RLock()
	defer c.selfMu.RUnlock()
	return c.self
}

// Device returns the companion firmware information.
func (c *Client) Device() DeviceInfo {
	c.selfMu.RLock()
	defer c.selfMu.RUnlock()
	return c.dev
}

func (c *Client) setSelf(s SelfInfo) {
	c.selfMu.Lock()
	c.self = s
	c.selfMu.Unlock()
}

func (c *Client) setDevice(d DeviceInfo) {
	c.selfMu.Lock()
	c.dev = d
	c.selfMu.Unlock()
}

// NodeName returns the local node name without using the radio.
func (c *Client) NodeName() string { return c.Self().Name }

// RadioPrefs are the local radio parameters, taken from the cached handshake.
type RadioPrefs struct {
	FreqMHz         float64
	BandwidthKHz    float64
	SpreadingFactor int
	CodingRate      int
	TXPowerDBm      int
	MultiAcks       byte
	PathHashMode    byte
}

// RadioPrefs returns the companion radio settings from the cached handshake.
func (c *Client) RadioPrefs() RadioPrefs {
	s, d := c.Self(), c.Device()
	return RadioPrefs{
		FreqMHz:         s.FreqMHz,
		BandwidthKHz:    s.BandwidthKHz,
		SpreadingFactor: s.SpreadingFactor,
		CodingRate:      s.CodingRate,
		TXPowerDBm:      int(s.TXPowerDBm),
		MultiAcks:       s.MultiAcks,
		PathHashMode:    d.PathHashMode,
	}
}

func (r RadioPrefs) String() string {
	return fmt.Sprintf("%.4f MHz, bw %.1f kHz, SF%d, CR%d, TX %d dBm",
		r.FreqMHz, r.BandwidthKHz, r.SpreadingFactor, r.CodingRate, r.TXPowerDBm)
}

// CLI runs a text command on the local companion (firmware v14+).
func (c *Client) CLI(ctx context.Context, command string) (string, error) {
	f, err := c.txOne(respCLIReply, frame(cmdRunCLI, []byte(command)))
	if err != nil {
		return "", err
	}
	return strings.TrimRight(string(f[1:]), "\x00"), nil
}

// RemoteCLI sends a text command to another node over the air. The node runs it
// only if the contact was granted Remote CLI; otherwise the text reaches its
// owner as a plain message.
func (c *Client) RemoteCLI(ctx context.Context, ct *Contact, command string) error {
	txtType := TxtTypeCLIData
	if ct.IsChat() {
		txtType = TxtTypeCLICmd
	}
	payload := frame(cmdSendTxtMsg, []byte{txtType, 0}, u32le(0),
		ct.Prefix(), []byte(command))

	f, err := c.tx([]byte{respSent, respErr}, payload, c.cfg.CommandTimeout)
	switch {
	case err != nil:
		return err
	case f[0] == respErr:
		return frameError(f)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Statistics
// ---------------------------------------------------------------------------

// StatsKind selects a CMD_GET_STATS sub-report.
type StatsKind byte

const (
	StatsCore    StatsKind = 0
	StatsRadio   StatsKind = 1
	StatsPackets StatsKind = 2
)

// CoreStats: battery, uptime, queue length, error flags.
type CoreStats struct {
	BatteryMV uint16
	UptimeSec uint32
	ErrFlags  uint16
	QueueLen  uint8
}

// RadioStats: noise floor, last RSSI/SNR, airtime.
type RadioStats struct {
	NoiseFloor int16
	LastRSSI   int8
	LastSNR    float64
	TXAirSecs  uint32
	RXAirSecs  uint32
}

// PacketStats: packet counters.
type PacketStats struct {
	Recv       uint32
	Sent       uint32
	SentFlood  uint32
	SentDirect uint32
	RecvFlood  uint32
	RecvDirect uint32
	RecvErrors uint32
}

func (c *Client) statsReader(ctx context.Context, kind StatsKind, minLen int) (reader, error) {
	f, err := c.txOne(respStats, []byte{cmdGetStats, byte(kind)})
	if err != nil {
		return reader{}, err
	}
	if len(f) < minLen {
		return reader{}, fmt.Errorf("meshcore: short STATS frame: %d bytes", len(f))
	}
	r := newReader(f[statsDataIdx:]) // skip code and sub-type
	return r, nil
}

// StatsCore returns local system statistics.
func (c *Client) StatsCore(ctx context.Context) (CoreStats, error) {
	var s CoreStats
	r, err := c.statsReader(ctx, StatsCore, statsCoreLen)
	if err != nil {
		return s, err
	}
	s.BatteryMV = r.u16()
	s.UptimeSec = r.u32()
	s.ErrFlags = r.u16()
	s.QueueLen = r.u8()
	return s, nil
}

// StatsRadio returns local radio statistics.
func (c *Client) StatsRadio(ctx context.Context) (RadioStats, error) {
	var s RadioStats
	r, err := c.statsReader(ctx, StatsRadio, statsRadioLen)
	if err != nil {
		return s, err
	}
	s.NoiseFloor = int16(r.u16())
	s.LastRSSI = r.i8()
	s.LastSNR = float64(r.i8()) / snrScale
	s.TXAirSecs = r.u32()
	s.RXAirSecs = r.u32()
	return s, nil
}

// StatsPackets returns local packet counters.
func (c *Client) StatsPackets(ctx context.Context) (PacketStats, error) {
	var s PacketStats
	r, err := c.statsReader(ctx, StatsPackets, statsPacketsLen)
	if err != nil {
		return s, err
	}
	for _, dst := range []*uint32{&s.Recv, &s.Sent, &s.SentFlood, &s.SentDirect,
		&s.RecvFlood, &s.RecvDirect, &s.RecvErrors} {
		*dst = r.u32()
	}
	return s, nil
}

// CurrentTime returns the device clock as unix seconds (UTC).
func (c *Client) CurrentTime(ctx context.Context) (uint32, error) {
	f, err := c.txOne(respCurrTime, []byte{cmdGetDeviceTime})
	if err != nil {
		return 0, err
	}
	if len(f) < currTimeFrameLen {
		return 0, errors.New("meshcore: short CURR_TIME frame")
	}
	return binary.LittleEndian.Uint32(f[1:5]), nil
}

// SyncClock sets the device clock to the host time.
func (c *Client) SyncClock(ctx context.Context) error {
	return c.txOK(frame(cmdSetDeviceTime, u32le(uint32(time.Now().Unix()))))
}

// ParseCLIValue splits a reply such as "> 910.5250244,62.5,7,5" into fields.
func ParseCLIValue(s string) []string {
	s = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(s), ">"))
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

// ParseCLIFloat is ParseCLIValue with the first field converted to float.
func ParseCLIFloat(s string) (float64, error) {
	parts := ParseCLIValue(s)
	if len(parts) == 0 {
		return 0, errors.New("meshcore: empty CLI reply")
	}
	return strconv.ParseFloat(parts[0], 64)
}
