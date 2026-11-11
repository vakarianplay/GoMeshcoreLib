package meshcore

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Self возвращает информацию о собственном узле, полученную при подключении.
func (c *Client) Self() SelfInfo {
	c.selfMu.RLock()
	defer c.selfMu.RUnlock()
	return c.self
}

// Device возвращает информацию о прошивке компаньона.
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

// NodeName возвращает имя собственного узла (без выхода в эфир).
func (c *Client) NodeName() string { return c.Self().Name }

// RadioPrefs — радиопараметры собственного узла (без выхода в эфир).
type RadioPrefs struct {
	FreqMHz         float64
	BandwidthKHz    float64
	SpreadingFactor int
	CodingRate      int
	TXPowerDBm      int
	MultiAcks       byte
	PathHashMode    byte
}

// RadioPrefs возвращает радионастройки компаньона из кэша SELF_INFO/DEVICE_INFO.
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

// CLI отправляет произвольную текстовую команду собственному компаньону
// (CMD_RUN_CLI_COMMAND, прошивка v14+). Ответ — RESP_CODE_CLI_REPLY.
func (c *Client) CLI(ctx context.Context, command string) (string, error) {
	payload := make([]byte, 0, 1+len(command))
	payload = append(payload, cmdRunCLI)
	payload = append(payload, []byte(command)...)

	f, err := c.tx([]byte{respCLIReply, respErr}, payload, c.cfg.CommandTimeout)
	if err != nil {
		return "", err
	}
	if f[0] == respErr {
		return "", frameError(f)
	}
	return strings.TrimRight(string(f[1:]), "\x00"), nil
}

// RemoteCLI отправляет текстовую команду ДРУГОМУ узлу через эфир.
// Узел выполнит её только если контакту выдано право Remote CLI;
// иначе текст уйдёт владельцу узла как обычное сообщение.
func (c *Client) RemoteCLI(ctx context.Context, ct *Contact, command string) error {
	txtType := TxtTypeCLIData
	if ct.IsChat() {
		txtType = TxtTypeCLICmd
	}
	payload := make([]byte, 0, 13+len(command))
	payload = append(payload, cmdSendTxtMsg, txtType, 0)
	var tb [4]byte
	binary.LittleEndian.PutUint32(tb[:], 0)
	payload = append(payload, tb[:]...)
	payload = append(payload, ct.Prefix()...)
	payload = append(payload, []byte(command)...)

	f, err := c.tx([]byte{respSent, respErr}, payload, c.cfg.CommandTimeout)
	if err != nil {
		return err
	}
	if f[0] == respErr {
		return frameError(f)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Статистика
// ---------------------------------------------------------------------------

// StatsKind — подтип CMD_GET_STATS.
type StatsKind byte

const (
	StatsCore    StatsKind = 0
	StatsRadio   StatsKind = 1
	StatsPackets StatsKind = 2
)

// CoreStats — батарея, аптайм, очередь, флаги ошибок.
type CoreStats struct {
	BatteryMV uint16
	UptimeSec uint32
	ErrFlags  uint16
	QueueLen  uint8
}

// RadioStats — шум, последние RSSI/SNR, эфирное время.
type RadioStats struct {
	NoiseFloor int16
	LastRSSI   int8
	LastSNR    float64
	TXAirSecs  uint32
	RXAirSecs  uint32
}

// PacketStats — счётчики пакетов.
type PacketStats struct {
	Recv       uint32
	Sent       uint32
	SentFlood  uint32
	SentDirect uint32
	RecvFlood  uint32
	RecvDirect uint32
	RecvErrors uint32
}

func (c *Client) statsFrame(ctx context.Context, kind StatsKind) ([]byte, error) {
	f, err := c.tx([]byte{respStats, respErr}, []byte{cmdGetStats, byte(kind)}, c.cfg.CommandTimeout)
	if err != nil {
		return nil, err
	}
	if f[0] == respErr {
		return nil, frameError(f)
	}
	return f, nil
}

// StatsCore возвращает системную статистику собственного узла.
func (c *Client) StatsCore(ctx context.Context) (CoreStats, error) {
	var s CoreStats
	f, err := c.statsFrame(ctx, StatsCore)
	if err != nil {
		return s, err
	}
	// code + type + batt(2) + uptime(4) + err(2) + queue(1) = 11
	if len(f) < 11 {
		return s, errors.New("meshcore: короткий кадр STATS(core)")
	}
	s.BatteryMV = binary.LittleEndian.Uint16(f[2:4])
	s.UptimeSec = binary.LittleEndian.Uint32(f[4:8])
	s.ErrFlags = binary.LittleEndian.Uint16(f[8:10])
	s.QueueLen = f[10]
	return s, nil
}

// StatsRadio возвращает радиостатистику собственного узла.
func (c *Client) StatsRadio(ctx context.Context) (RadioStats, error) {
	var s RadioStats
	f, err := c.statsFrame(ctx, StatsRadio)
	if err != nil {
		return s, err
	}
	// code + type + noise(2) + rssi(1) + snr(1) + tx_air(4) + rx_air(4) = 14
	if len(f) < 14 {
		return s, errors.New("meshcore: короткий кадр STATS(radio)")
	}
	s.NoiseFloor = int16(binary.LittleEndian.Uint16(f[2:4]))
	s.LastRSSI = int8(f[4])
	s.LastSNR = float64(int8(f[5])) / 4.0
	s.TXAirSecs = binary.LittleEndian.Uint32(f[6:10])
	s.RXAirSecs = binary.LittleEndian.Uint32(f[10:14])
	return s, nil
}

// StatsPackets возвращает счётчики пакетов собственного узла.
func (c *Client) StatsPackets(ctx context.Context) (PacketStats, error) {
	var s PacketStats
	f, err := c.statsFrame(ctx, StatsPackets)
	if err != nil {
		return s, err
	}
	// code + type + 7 × uint32 = 30
	if len(f) < 30 {
		return s, errors.New("meshcore: короткий кадр STATS(packets)")
	}
	s.Recv = binary.LittleEndian.Uint32(f[2:6])
	s.Sent = binary.LittleEndian.Uint32(f[6:10])
	s.SentFlood = binary.LittleEndian.Uint32(f[10:14])
	s.SentDirect = binary.LittleEndian.Uint32(f[14:18])
	s.RecvFlood = binary.LittleEndian.Uint32(f[18:22])
	s.RecvDirect = binary.LittleEndian.Uint32(f[22:26])
	s.RecvErrors = binary.LittleEndian.Uint32(f[26:30])
	return s, nil
}

// CurrentTime возвращает время на устройстве (UTC, unix seconds).
func (c *Client) CurrentTime(ctx context.Context) (uint32, error) {
	f, err := c.tx([]byte{respCurrTime, respErr}, []byte{cmdGetDeviceTime}, c.cfg.CommandTimeout)
	if err != nil {
		return 0, err
	}
	if f[0] == respErr {
		return 0, frameError(f)
	}
	if len(f) < 5 {
		return 0, errors.New("meshcore: короткий кадр CURR_TIME")
	}
	return binary.LittleEndian.Uint32(f[1:5]), nil
}

// SyncClock выставляет время устройства равным времени хоста.
func (c *Client) SyncClock(ctx context.Context) error {
	payload := make([]byte, 5)
	payload[0] = cmdSetDeviceTime
	binary.LittleEndian.PutUint32(payload[1:], uint32(time.Now().Unix()))
	f, err := c.tx([]byte{respOK, respErr}, payload, c.cfg.CommandTimeout)
	if err != nil {
		return err
	}
	if f[0] == respErr {
		return frameError(f)
	}
	return nil
}

// ParseCLIValue выделяет поля из ответа вида "> 910.5250244,62.5,7,5".
func ParseCLIValue(s string) []string {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, ">")
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

// ParseCLIFloat — то же, но с приведением к float.
func ParseCLIFloat(s string) (float64, error) {
	parts := ParseCLIValue(s)
	if len(parts) == 0 {
		return 0, errors.New("meshcore: пустой ответ CLI")
	}
	return strconv.ParseFloat(parts[0], 64)
}
