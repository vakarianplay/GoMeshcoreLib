package GoMeshcoreLib

import "time"

// SelfInfo is the APP_START reply.
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

// DeviceInfo is the DEVICE_QUERY reply.
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
	if len(f) < selfInfoMinLen || f[0] != respSelfInfo {
		return s, false
	}
	r := newReader(f[1:])
	s.AdvType = r.u8()
	s.TXPowerDBm = r.i8()
	s.MaxTXPowerDBm = r.i8()
	copy(s.PublicKey[:], r.take(pubKeyLen))
	s.Lat = float64(r.i32()) / latLonScale
	s.Lon = float64(r.i32()) / latLonScale
	s.MultiAcks = r.u8()
	s.AdvertLocPolicy = r.u8()

	modes := r.u8()
	s.TelemetryModeEnv = (modes >> 4) & 0x03
	s.TelemetryModeLoc = (modes >> 2) & 0x03
	s.TelemetryModeBase = modes & 0x03
	s.ManualAddContacts = r.u8() > 0

	s.FreqMHz = float64(r.u32()) / radioFreqScale
	s.BandwidthKHz = float64(r.u32()) / radioFreqScale
	s.SpreadingFactor = int(r.u8())
	s.CodingRate = int(r.u8())
	s.Name = string(r.rest())
	return s, r.ok
}

func parseDeviceInfo(f []byte) (DeviceInfo, bool) {
	var d DeviceInfo
	if len(f) < 2 || f[0] != respDeviceInfo {
		return d, false
	}
	d.FirmwareVerCode = f[1]
	if d.FirmwareVerCode < deviceInfoMinVer || len(f) < deviceInfoMinLen {
		return d, true
	}
	r := newReader(f[2:])
	d.MaxContacts = int(r.u8()) * 2
	d.MaxChannels = int(r.u8())
	d.BLEPin = r.u32()
	d.FirmwareBuild = r.string(deviceInfoFirmwLen)
	d.Model = r.string(deviceInfoModelLen)
	d.Version = r.string(deviceInfoVerLen)
	d.ClientRepeat = r.u8()
	d.PathHashMode = r.u8()
	return d, true
}

// DirectMessage is a direct message from a contact.
//
// The frame carries SNR and the path_len byte only: no hop hashes and no RSSI.
// Those come from rx_log (see RxLogEntry); the route of a contact's latest
// advert is available through ContactAdvertPath.
type DirectMessage struct {
	From         [6]byte
	FromName     string
	HasSNR       bool
	SNR          float64
	Path         PathInfo
	TxtType      byte
	SignerPrefix []byte
	Time         time.Time
	Text         string
}

func parseDirectMessage(f []byte) (*DirectMessage, bool) {
	m := &DirectMessage{}
	body, ok := messageBody(f, textPrefixLen, textPrefixV3Len)
	if !ok {
		return nil, false
	}
	m.HasSNR, m.SNR = body.hasSNR, body.snr

	r := newReader(body.rest)
	copy(m.From[:], r.take(pubKeyPrefixLen))
	m.Path = decodePathLenByte(r.u8())
	m.TxtType = r.u8()
	m.Time = unixTime(r.u32())
	if m.TxtType == TxtTypeSignedPlain {
		m.SignerPrefix = append([]byte(nil), r.take(signerPrefixLen)...)
	}
	if !r.ok {
		return nil, false
	}
	m.Text = string(r.rest())
	return m, true
}

// ChannelMessage is a message from a group (hashtag) channel.
//
// ScopeCode is only filled when the message was correlated with rx_log. A
// region name cannot be recovered from it: the transport code is a truncated
// HMAC, not an identifier.
type ChannelMessage struct {
	ChannelIndex byte
	ChannelName  string
	HasSNR       bool
	SNR          float64
	Path         PathInfo
	TxtType      byte
	Time         time.Time
	Text         string

	HasRxLog       bool
	RSSI           float64
	ScopeCode      uint16
	ReplyScopeCode uint16
	RawPathHops    []string
}

func parseChannelMessage(f []byte) (*ChannelMessage, bool) {
	m := &ChannelMessage{}
	body, ok := messageBody(f, chanTextPrefixLen, chanTextPrefixV3Len)
	if !ok {
		return nil, false
	}
	m.HasSNR, m.SNR = body.hasSNR, body.snr

	r := newReader(body.rest)
	m.ChannelIndex = r.u8()
	m.Path = decodePathLenByte(r.u8())
	m.TxtType = r.u8()
	m.Time = unixTime(r.u32())
	if !r.ok {
		return nil, false
	}
	m.Text = string(r.rest())
	return m, true
}

// msgBody is the part of a message frame after the optional SNR header.
type msgBody struct {
	rest   []byte
	snr    float64
	hasSNR bool
}

// messageBody splits a v2 or v3 message frame into its SNR header and the rest.
func messageBody(f []byte, minV2, minV3 int) (msgBody, bool) {
	var b msgBody
	switch {
	case len(f) == 0:
		return b, false
	case f[0] == respContactMsgV3 || f[0] == respChannelMsgV3:
		if len(f) < minV3 {
			return b, false
		}
		b.snr, b.hasSNR = float64(int8(f[1]))/snrScale, true
		b.rest = f[4:] // snr + 2 reserved bytes
	default:
		if len(f) < minV2 {
			return b, false
		}
		b.rest = f[1:]
	}
	return b, true
}

// RxLogEntry is a raw received packet (PUSH_CODE_LOG_RX_DATA). It is the only
// source carrying SNR, RSSI, transport codes and the hop hashes together.
type RxLogEntry struct {
	At   time.Time
	SNR  float64
	RSSI float64
	Pkt  *RawPacket
}

func parseRxLog(f []byte) (*RxLogEntry, bool) {
	if len(f) < rxLogMinLen || f[0] != pushRxLog {
		return nil, false
	}
	pkt, ok := parseRawPacket(f[rxLogDataIdx:])
	if !ok {
		return nil, false
	}
	return &RxLogEntry{
		At:   time.Now(),
		SNR:  float64(int8(f[1])) / snrScale,
		RSSI: float64(int8(f[2])),
		Pkt:  pkt,
	}, true
}

// Echo records one repeater retransmission of our packet.
type Echo struct {
	At   time.Time
	SNR  float64
	RSSI float64
	Path PathInfo
	Pkt  *RawPacket
}

// Telemetry is a telemetry response.
type Telemetry struct {
	From     [6]byte
	FromName string
	Attempts int
	Route    string
	At       time.Time
	LPP      LPPResult
}

func parseTelemetryPush(f []byte) (*Telemetry, bool) {
	if len(f) < telemetryMinLen || f[0] != pushTelemetry {
		return nil, false
	}
	t := &Telemetry{At: time.Now(), LPP: DecodeLPP(f[telemetryDataIdx:])}
	copy(t.From[:], f[telemetryKeyIdx:telemetryKeyIdx+pubKeyPrefixLen])
	return t, true
}

func unixTime(secs uint32) time.Time { return time.Unix(int64(secs), 0) }
