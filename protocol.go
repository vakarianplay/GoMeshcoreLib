package meshcore

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
)

// --- обрамление кадров (SerialWifiInterface.cpp / ArduinoSerialInterface.cpp)
const (
	frameOutMagic byte = '<' // app -> radio
	frameInMagic  byte = '>' // radio -> app
	maxFrameSize       = 512
)

// --- команды (CMD_*) ---
const (
	cmdAppStart       byte = 1
	cmdSendTxtMsg     byte = 2
	cmdSendChannelTxt byte = 3
	cmdGetContacts    byte = 4
	cmdGetDeviceTime  byte = 5
	cmdSetDeviceTime  byte = 6
	cmdSendSelfAdvert byte = 7
	cmdAddUpdateCtc   byte = 9
	cmdSyncNextMsg    byte = 10
	cmdResetPath      byte = 13
	cmdGetBattStorage byte = 20
	cmdDeviceQuery    byte = 22
	cmdSendLogin      byte = 26
	cmdGetContactKey  byte = 30
	cmdGetChannel     byte = 31
	cmdSetChannel     byte = 32
	cmdSetOtherParams byte = 38
	cmdSendTelemetry  byte = 39 // 0x27
	cmdGetAdvertPath  byte = 42
	cmdSetFloodScope  byte = 54
	cmdGetStats       byte = 56
	cmdSetDefaultScp  byte = 63
	cmdGetDefaultScp  byte = 64
	cmdRunCLI         byte = 66 // v14+

	appTargetVer byte = 3
)

// --- ответы (RESP_CODE_*) ---
const (
	respOK            byte = 0
	respErr           byte = 1
	respContactsStart byte = 2
	respContact       byte = 3
	respContactsEnd   byte = 4
	respSelfInfo      byte = 5
	respSent          byte = 6
	respContactMsg    byte = 7
	respChannelMsg    byte = 8
	respCurrTime      byte = 9
	respNoMoreMsgs    byte = 10
	respBattStorage   byte = 12
	respDeviceInfo    byte = 13
	respDisabled      byte = 15
	respContactMsgV3  byte = 16
	respChannelMsgV3  byte = 17
	respChannelInfo   byte = 18
	respAdvertPath    byte = 22
	respStats         byte = 24
	respChannelData   byte = 27
	respDefaultScope  byte = 28
	respCLIReply      byte = 29
)

// --- пуши (PUSH_CODE_*) ---
const (
	pushAdvert        byte = 0x80
	pushPathUpdated   byte = 0x81
	pushSendConfirmed byte = 0x82
	pushMsgWaiting    byte = 0x83
	pushRawData       byte = 0x84
	pushLoginSuccess  byte = 0x85
	pushLoginFail     byte = 0x86
	pushStatusResp    byte = 0x87
	pushRxLog         byte = 0x88
	pushTraceData     byte = 0x89
	pushNewAdvert     byte = 0x8A
	pushTelemetry     byte = 0x8B
	pushBinaryResp    byte = 0x8C
	pushPathDiscovery byte = 0x8D
	pushControlData   byte = 0x8E
)

// --- типы узлов и текста ---
const (
	AdvTypeNone     byte = 0
	AdvTypeChat     byte = 1
	AdvTypeRepeater byte = 2
	AdvTypeRoom     byte = 3
	AdvTypeSensor   byte = 4
)

const (
	TxtTypePlain       byte = 0
	TxtTypeCLIData     byte = 1
	TxtTypeSignedPlain byte = 2
	TxtTypeCLICmd      byte = 3
)

// --- заголовок пакета (Packet.h) ---
const (
	routeTransportFlood  uint8 = 0x00
	routeFlood           uint8 = 0x01
	routeDirect          uint8 = 0x02
	routeTransportDirect uint8 = 0x03

	payloadReq       uint8 = 0x00
	payloadResponse  uint8 = 0x01
	payloadTxtMsg    uint8 = 0x02
	payloadAck       uint8 = 0x03
	payloadAdvert    uint8 = 0x04
	payloadGrpTxt    uint8 = 0x05
	payloadGrpData   uint8 = 0x06
	payloadAnonReq   uint8 = 0x07
	payloadPath      uint8 = 0x08
	payloadTrace     uint8 = 0x09
	payloadMultipart uint8 = 0x0A
	payloadControl   uint8 = 0x0B
	payloadRawCustom uint8 = 0x0F
)

// MaxTextLen — предел длины текста одного сообщения (спецификация MeshCore).
const MaxTextLen = 133

// OutPathUnknown — маркер «маршрут неизвестен / direct» в поле path_len.
const OutPathUnknown byte = 0xFF

// PublicChannelKey — общеизвестный ключ канала Public.
var PublicChannelKey = [16]byte{
	0x8b, 0x33, 0x87, 0xe9, 0xc5, 0xcd, 0xea, 0x6a,
	0xc9, 0xe5, 0xed, 0xba, 0xa1, 0x15, 0xcd, 0x72,
}

// Ошибки библиотеки.
var (
	ErrTimeout        = errors.New("meshcore: таймаут")
	ErrClosed         = errors.New("meshcore: клиент закрыт")
	ErrNotFound       = errors.New("meshcore: не найдено")
	ErrNoAck          = errors.New("meshcore: ACK не получен")
	ErrNotHeard       = errors.New("meshcore: сообщение не услышал ни один ретранслятор")
	ErrTextTooLong    = errors.New("meshcore: текст длиннее 133 байт")
	ErrNoChannelSlots = errors.New("meshcore: нет свободного слота канала")
	ErrUnsupported    = errors.New("meshcore: команда не поддерживается прошивкой")
)

// ---------------------------------------------------------------------------
// Ключи
// ---------------------------------------------------------------------------

// NormalizeHashtag приводит имя к виду "#name".
func NormalizeHashtag(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return name
	}
	if !strings.HasPrefix(name, "#") {
		return "#" + name
	}
	return name
}

// DeriveKey16 — первые 16 байт SHA-256. Одинаково используется и для ключа
// хэштег-канала, и для ключа региона (TransportKeyStore::getAutoKeyFor).
func DeriveKey16(name string) [16]byte {
	h := sha256.Sum256([]byte(name))
	var k [16]byte
	copy(k[:], h[:16])
	return k
}

// ---------------------------------------------------------------------------
// Маршрут
// ---------------------------------------------------------------------------

// PathInfo — декодированное поле path_len и, если доступны, сами хэши.
type PathInfo struct {
	Encoded   byte
	Valid     bool
	Flood     bool // false = direct (маркер 0xFF)
	HashSize  int  // байт на один хэш: 1..3
	HashCount int  // число хопов
	Bytes     []byte
	Hops      []string // hex каждого хопа
}

// Repeated сообщает, добавил ли хотя бы один ретранслятор свой хэш.
func (p PathInfo) Repeated() bool { return p.HashCount > 0 && len(p.Bytes) > 0 }

// String — человекочитаемое представление.
func (p PathInfo) String() string {
	if !p.Valid {
		return fmt.Sprintf("invalid(0x%02x)", p.Encoded)
	}
	if !p.Flood {
		return "direct"
	}
	if p.HashCount == 0 {
		return "flood/0hop"
	}
	return fmt.Sprintf("flood[%s]", strings.Join(p.Hops, ""))
}

// decodePathLenByte разбирает одиночный байт path_len из кадров
// companion-протокола (сами хэши там не передаются).
func decodePathLenByte(b byte) PathInfo {
	p := PathInfo{Encoded: b}
	if b == OutPathUnknown {
		p.Valid, p.Flood = true, false
		return p
	}
	hashSize := int(b>>6) + 1
	if hashSize == 4 { // зарезервировано
		return p
	}
	p.Valid = true
	p.Flood = true
	p.HashSize = hashSize
	p.HashCount = int(b & 63)
	return p
}

// decodePathBytes разбирает байт path_len вместе с самими хэшами (для rx_log).
// Возвращает также число потреблённых байт маршрута.
func decodePathBytes(b byte, raw []byte) (PathInfo, int) {
	p := decodePathLenByte(b)
	if !p.Flood {
		return p, 0
	}
	n := p.HashCount * p.HashSize
	if n > len(raw) {
		n = len(raw)
	}
	p.Bytes = append([]byte(nil), raw[:n]...)
	for i := 0; i+p.HashSize <= len(p.Bytes); i += p.HashSize {
		p.Hops = append(p.Hops, hex.EncodeToString(p.Bytes[i:i+p.HashSize]))
	}
	return p, n
}

// RawPacket — разобранный сырой пакет из rx_log.
type RawPacket struct {
	Header         byte
	RouteType      uint8
	PayloadType    uint8
	PayloadVer     uint8
	HasTransport   bool
	TransportCodes [2]uint16
	PathLenByte    byte
	Path           PathInfo
	Payload        []byte
}

// IsFlood сообщает, был ли пакет широковещательным.
func (p *RawPacket) IsFlood() bool {
	return p.RouteType == routeFlood || p.RouteType == routeTransportFlood
}

// parseRawPacket разбирает wire-формат пакета (Packet::writeTo).
func parseRawPacket(b []byte) (*RawPacket, bool) {
	if len(b) < 2 {
		return nil, false
	}
	p := &RawPacket{Header: b[0]}
	p.RouteType = b[0] & 0x03
	p.PayloadType = (b[0] >> 2) & 0x0F
	p.PayloadVer = (b[0] >> 6) & 0x03
	i := 1

	switch p.RouteType {
	case routeTransportFlood, routeTransportDirect:
		if len(b) < i+4 {
			return nil, false
		}
		p.HasTransport = true
		p.TransportCodes[0] = binary.LittleEndian.Uint16(b[i:])
		p.TransportCodes[1] = binary.LittleEndian.Uint16(b[i+2:])
		i += 4
	}

	p.PathLenByte = b[i]
	i++
	pi, used := decodePathBytes(p.PathLenByte, b[i:])
	p.Path = pi
	i += used

	if i > len(b) {
		return nil, false
	}
	p.Payload = append([]byte(nil), b[i:]...)
	return p, true
}

// ---------------------------------------------------------------------------
// Кадры
// ---------------------------------------------------------------------------

type frameReader struct{ br *bufio.Reader }

// next читает один кадр, при рассинхронизации ищет следующий '>'.
func (fr *frameReader) next() ([]byte, error) {
	for {
		magic, err := fr.br.ReadByte()
		if err != nil {
			return nil, err
		}
		if magic != frameInMagic {
			continue
		}
		var lb [2]byte
		if _, err := io.ReadFull(fr.br, lb[:]); err != nil {
			return nil, err
		}
		n := int(binary.LittleEndian.Uint16(lb[:]))
		if n == 0 || n > maxFrameSize {
			continue // битая длина — ресинхронизация
		}
		buf := make([]byte, n)
		if _, err := io.ReadFull(fr.br, buf); err != nil {
			return nil, err
		}
		return buf, nil
	}
}

func encodeFrame(payload []byte) []byte {
	buf := make([]byte, 0, 3+len(payload))
	buf = append(buf, frameOutMagic, byte(len(payload)), byte(len(payload)>>8))
	return append(buf, payload...)
}

func frameName(b byte) string {
	switch b {
	case respOK:
		return "OK"
	case respErr:
		return "ERR"
	case respContactsStart:
		return "CONTACTS_START"
	case respContact:
		return "CONTACT"
	case respContactsEnd:
		return "CONTACTS_END"
	case respSelfInfo:
		return "SELF_INFO"
	case respSent:
		return "SENT"
	case respNoMoreMsgs:
		return "NO_MORE_MSGS"
	case respDeviceInfo:
		return "DEVICE_INFO"
	case respChannelInfo:
		return "CHANNEL_INFO"
	case respContactMsg:
		return "CONTACT_MSG"
	case respContactMsgV3:
		return "CONTACT_MSG_V3"
	case respChannelMsg:
		return "CHANNEL_MSG"
	case respChannelMsgV3:
		return "CHANNEL_MSG_V3"
	case respChannelData:
		return "CHANNEL_DATA_RECV"
	case respStats:
		return "STATS"
	case respCLIReply:
		return "CLI_REPLY"
	case respAdvertPath:
		return "ADVERT_PATH"
	case respBattStorage:
		return "BATTERY"
	case pushMsgWaiting:
		return "MSG_WAITING"
	case pushSendConfirmed:
		return "ACK"
	case pushTelemetry:
		return "TELEMETRY"
	case pushRxLog:
		return "RX_LOG"
	case pushStatusResp:
		return "STATUS"
	case pushBinaryResp:
		return "BINARY_RESP"
	case pushAdvert:
		return "ADVERT"
	}
	return fmt.Sprintf("code=%d", b)
}

func errCodeName(c byte) string {
	switch c {
	case 1:
		return "UNSUPPORTED_CMD"
	case 2:
		return "NOT_FOUND"
	case 3:
		return "TABLE_FULL"
	case 4:
		return "BAD_STATE"
	case 5:
		return "FILE_IO_ERROR"
	case 6:
		return "ILLEGAL_ARG"
	}
	return fmt.Sprintf("code=%d", c)
}

// frameError превращает кадр RESP_CODE_ERR в ошибку.
func frameError(f []byte) error {
	code := byte(0)
	if len(f) > 1 {
		code = f[1]
	}
	switch code {
	case 2:
		return fmt.Errorf("%w: %s", ErrNotFound, errCodeName(code))
	case 1:
		return fmt.Errorf("%w: %s", ErrUnsupported, errCodeName(code))
	}
	return fmt.Errorf("meshcore: узел вернул %s", errCodeName(code))
}

func checkText(text string) error {
	if len(text) > MaxTextLen {
		return fmt.Errorf("%w (получено %d)", ErrTextTooLong, len(text))
	}
	return nil
}
