package meshcore

import (
	"encoding/binary"
	"time"
)

// SelfInfo — ответ на CMD_APP_START (RESP_CODE_SELF_INFO).
type SelfInfo struct {
	AdvType           byte
	TXPowerDBm        int8
	MaxTXPowerDBm     int8
	PublicKey         [32]byte
	Lat               float64
	Lon               float64
	MultiAcks         byte
	AdvertLocPolicy   byte
	TelemetryModeEnv  byte
	TelemetryModeLoc  byte
	TelemetryModeBase byte
	ManualAddContacts bool
	FreqMHz           float64
	BandwidthKHz      float64
	SpreadingFactor   int
	CodingRate        int
	Name              string
}

// DeviceInfo — ответ на CMD_DEVICE_QUERY (RESP_CODE_DEVICE_INFO).
type DeviceInfo struct {
	FirmwareVerCode byte
	MaxContacts     int
	MaxChannels     int
	BLEPin          uint32
	FirmwareBuild   string
	Model           string
	Version         string
	ClientRepeat    byte
	PathHashMode    byte
}

func parseSelfInfo(f []byte) (SelfInfo, bool) {
	var s SelfInfo
	// Минимум без имени: 1+3+32+8+4+10 = 58
	if len(f) < 58 || f[0] != respSelfInfo {
		return s, false
	}
	i := 1
	s.AdvType = f[i]
	s.TXPowerDBm = int8(f[i+1])
	s.MaxTXPowerDBm = int8(f[i+2])
	i += 3
	copy(s.PublicKey[:], f[i:i+32])
	i += 32
	lat := int32(binary.LittleEndian.Uint32(f[i:]))
	lon := int32(binary.LittleEndian.Uint32(f[i+4:]))
	s.Lat = float64(lat) / 1e6
	s.Lon = float64(lon) / 1e6
	i += 8
	s.MultiAcks = f[i]
	s.AdvertLocPolicy = f[i+1]
	tm := f[i+2]
	s.TelemetryModeEnv = (tm >> 4) & 0x03
	s.TelemetryModeLoc = (tm >> 2) & 0x03
	s.TelemetryModeBase = tm & 0x03
	s.ManualAddContacts = f[i+3] > 0
	i += 4
	freq := binary.LittleEndian.Uint32(f[i:])
	bw := binary.LittleEndian.Uint32(f[i+4:])
	s.FreqMHz = float64(freq) / 1000.0
	s.BandwidthKHz = float64(bw) / 1000.0
	s.SpreadingFactor = int(f[i+8])
	s.CodingRate = int(f[i+9])
	i += 10
	if i < len(f) {
		s.Name = trimNUL(string(f[i:]))
	}
	return s, true
}

func parseDeviceInfo(f []byte) (DeviceInfo, bool) {
	var d DeviceInfo
	if len(f) < 2 || f[0] != respDeviceInfo {
		return d, false
	}
	d.FirmwareVerCode = f[1]
	if d.FirmwareVerCode < 3 || len(f) < 80 {
		return d, true
	}
	d.MaxContacts = int(f[2]) * 2
	d.MaxChannels = int(f[3])
	d.BLEPin = binary.LittleEndian.Uint32(f[4:8])
	d.FirmwareBuild = trimNUL(string(f[8:20]))
	d.Model = trimNUL(string(f[20:60]))
	d.Version = trimNUL(string(f[60:80]))
	if len(f) > 80 {
		d.ClientRepeat = f[80]
	}
	if len(f) > 81 {
		d.PathHashMode = f[81]
	}
	return d, true
}

func trimNUL(s string) string {
	for i := 0; i < len(s); i++ {
		if s[i] == 0 {
			return s[:i]
		}
	}
	return s
}

// ---------------------------------------------------------------------------
// Входящие личные сообщения
// ---------------------------------------------------------------------------

// DirectMessage — личное сообщение от контакта.
//
// ВАЖНО: кадр companion-протокола несёт SNR и байт path_len, но НЕ несёт сами
// хэши хопов и не несёт RSSI. Полный маршрут в хэшах и RSSI доступны только из
// сырых пакетов rx_log (см. RxLogEntry). Реальный маршрут, по которому пришёл
// последний advert контакта, даёт ContactAdvertPath().
type DirectMessage struct {
	From         [6]byte
	FromName     string
	HasSNR       bool
	SNR          float64
	Path         PathInfo
	TxtType      byte
	SignerPrefix []byte // только для TxtTypeSignedPlain
	Time         time.Time
	Text         string
}

func parseDirectMessage(f []byte) (*DirectMessage, bool) {
	m := &DirectMessage{}
	i := 1
	if f[0] == respContactMsgV3 {
		// snr(1) + res(2) + key(6) + path(1) + txt_type(1) + ts(4) = 16
		if len(f) < 16 {
			return nil, false
		}
		m.SNR = float64(int8(f[1])) / 4.0
		m.HasSNR = true
		i = 4 // snr + 2 зарезервированных
	} else if len(f) < 13 { // key(6) + path(1) + txt_type(1) + ts(4) = 13
		return nil, false
	}

	copy(m.From[:], f[i:i+6])
	i += 6
	m.Path = decodePathLenByte(f[i])
	i++
	m.TxtType = f[i]
	i++
	ts := binary.LittleEndian.Uint32(f[i:])
	i += 4
	m.Time = time.Unix(int64(ts), 0)

	if m.TxtType == TxtTypeSignedPlain {
		if len(f) < i+4 {
			return nil, false
		}
		m.SignerPrefix = append([]byte(nil), f[i:i+4]...)
		i += 4
	}
	if i > len(f) {
		return nil, false
	}
	m.Text = string(f[i:])
	return m, true
}

// ---------------------------------------------------------------------------
// Входящие групповые сообщения
// ---------------------------------------------------------------------------

// ChannelMessage — сообщение из группового (хэштег) канала.
//
// Поле ScopeCode заполняется только при успешной корреляции с rx_log.
// Имя региона из transport-кода не восстанавливается: это усечённый HMAC,
// а не идентификатор.
type ChannelMessage struct {
	ChannelIndex byte
	ChannelName  string
	HasSNR       bool
	SNR          float64
	Path         PathInfo
	TxtType      byte
	Time         time.Time
	Text         string

	// Заполняется при корреляции с rx_log:
	HasRxLog       bool
	RSSI           float64
	ScopeCode      uint16
	ReplyScopeCode uint16
	RawPathHops    []string
}

func parseChannelMessage(f []byte) (*ChannelMessage, bool) {
	m := &ChannelMessage{}
	i := 1
	if f[0] == respChannelMsgV3 {
		// snr(1) + res(2) + chan(1) + path(1) + txt_type(1) + ts(4) = 11
		if len(f) < 11 {
			return nil, false
		}
		m.SNR = float64(int8(f[1])) / 4.0
		m.HasSNR = true
		i = 4
	} else if len(f) < 8 { // chan(1) + path(1) + txt_type(1) + ts(4) = 8
		return nil, false
	}

	m.ChannelIndex = f[i]
	i++
	m.Path = decodePathLenByte(f[i])
	i++
	m.TxtType = f[i]
	i++
	ts := binary.LittleEndian.Uint32(f[i:])
	i += 4
	m.Time = time.Unix(int64(ts), 0)
	if i > len(f) {
		return nil, false
	}
	m.Text = string(f[i:])
	return m, true
}

// ---------------------------------------------------------------------------
// rx_log: сырые пакеты с маршрутом, SNR и RSSI
// ---------------------------------------------------------------------------

// RxLogEntry — сырой принятый пакет (PUSH_CODE_LOG_RX_DATA, 0x88).
// Единственный источник, дающий одновременно SNR, RSSI, transport-коды
// региона и полный маршрут в хэшах.
type RxLogEntry struct {
	At   time.Time
	SNR  float64
	RSSI float64
	Pkt  *RawPacket
}

func parseRxLog(f []byte) (*RxLogEntry, bool) {
	if len(f) < 4 || f[0] != pushRxLog {
		return nil, false
	}
	e := &RxLogEntry{
		At:   time.Now(),
		SNR:  float64(int8(f[1])) / 4.0,
		RSSI: float64(int8(f[2])),
	}
	pkt, ok := parseRawPacket(f[3:])
	if !ok {
		return nil, false
	}
	e.Pkt = pkt
	return e, true
}

// Echo — факт повторения нашего пакета ретранслятором.
type Echo struct {
	At   time.Time
	SNR  float64
	RSSI float64
	Path PathInfo
	Pkt  *RawPacket
}

// ---------------------------------------------------------------------------
// Телеметрия
// ---------------------------------------------------------------------------

// Telemetry — ответ на запрос телеметрии.
type Telemetry struct {
	From     [6]byte
	FromName string
	Attempts int
	Route    string
	At       time.Time
	LPP      LPPResult
}

func parseTelemetryPush(f []byte) (*Telemetry, bool) {
	if len(f) < 8 || f[0] != pushTelemetry {
		return nil, false
	}
	t := &Telemetry{At: time.Now()}
	copy(t.From[:], f[2:8])
	t.LPP = DecodeLPP(f[8:])
	return t, true
}
