package GoMeshcoreLib

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

// PublicChannelKey is the well-known secret of the built-in Public channel.
var PublicChannelKey = [16]byte{
	0x8b, 0x33, 0x87, 0xe9, 0xc5, 0xcd, 0xea, 0x6a,
	0xc9, 0xe5, 0xed, 0xba, 0xa1, 0x15, 0xcd, 0x72,
}

// OutPathUnknown marks an unknown or direct path in a path_len field.
const OutPathUnknown byte = pathLenDirect

// Sentinel errors.
var (
	ErrTimeout        = errors.New("meshcore: timeout")
	ErrClosed         = errors.New("meshcore: client closed")
	ErrNotFound       = errors.New("meshcore: not found")
	ErrNoAck          = errors.New("meshcore: no ACK received")
	ErrNotHeard       = errors.New("meshcore: no repeater echoed the message")
	ErrTextTooLong    = errors.New("meshcore: text exceeds the 133 byte limit")
	ErrNoChannelSlots = errors.New("meshcore: no free channel slot")
	ErrUnsupported    = errors.New("meshcore: command unsupported by firmware")
)

// NormalizeHashtag returns the name prefixed with '#'.
func NormalizeHashtag(name string) string {
	name = strings.TrimSpace(name)
	if name == "" || strings.HasPrefix(name, "#") {
		return name
	}
	return "#" + name
}

// DeriveKey16 returns the first 16 bytes of SHA-256(name). The firmware uses
// the same derivation for hashtag channel keys and region transport keys.
func DeriveKey16(name string) [16]byte {
	h := sha256.Sum256([]byte(name))
	var k [16]byte
	copy(k[:], h[:16])
	return k
}

// PathInfo is a decoded path_len byte plus the hop hashes when available.
type PathInfo struct {
	Encoded   byte
	Valid     bool
	Flood     bool // false means direct (marker 0xFF)
	HashSize  int  // bytes per hop hash: 1..3
	HashCount int  // number of hops
	Bytes     []byte
	Hops      []string
}

// Repeated reports whether at least one repeater appended its hash.
func (p PathInfo) Repeated() bool { return p.HashCount > 0 && len(p.Bytes) > 0 }

func (p PathInfo) String() string {
	switch {
	case !p.Valid:
		return fmt.Sprintf("invalid(0x%02x)", p.Encoded)
	case !p.Flood:
		return "direct"
	case p.HashCount == 0:
		return "flood/0hop"
	default:
		return fmt.Sprintf("flood[%s]", strings.Join(p.Hops, ""))
	}
}

// decodePathLenByte decodes a lone path_len byte, as carried by message frames.
func decodePathLenByte(b byte) PathInfo {
	p := PathInfo{Encoded: b, Valid: true}
	if b == OutPathUnknown {
		return p // Flood stays false: direct route
	}
	if size := b>>pathHashSizeShift + 1; size >= pathHashSizeMax {
		p.Valid = false // reserved encoding
		return p
	} else {
		p.Flood = true
		p.HashSize = int(size)
		p.HashCount = int(b & pathHashCountMask)
	}
	return p
}

// decodePathBytes decodes a path_len byte together with the hop hashes that
// follow it, and reports how many bytes the path consumed.
func decodePathBytes(b byte, raw []byte) (PathInfo, int) {
	p := decodePathLenByte(b)
	if !p.Flood {
		return p, 0
	}
	n := min(p.HashCount*p.HashSize, len(raw))
	p.Bytes = append([]byte(nil), raw[:n]...)
	for i := 0; i+p.HashSize <= len(p.Bytes); i += p.HashSize {
		p.Hops = append(p.Hops, hex.EncodeToString(p.Bytes[i:i+p.HashSize]))
	}
	return p, n
}

// RawPacket is a packet decoded from an rx_log push.
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

// IsFlood reports whether the packet used a flood route.
func (p *RawPacket) IsFlood() bool {
	return p.RouteType == routeFlood || p.RouteType == routeTransportFlood
}

func routeHasTransportCodes(rt uint8) bool {
	return rt == routeTransportFlood || rt == routeTransportDirect
}

// parseRawPacket decodes the wire format produced by Packet::writeTo.
// parseRawPacket decodes the wire format produced by Packet::writeTo.
func parseRawPacket(b []byte) (*RawPacket, bool) {
	if len(b) < rawPacketMinLen {
		return nil, false
	}
	r := newReader(b)
	p := &RawPacket{Header: r.u8()}
	p.RouteType = p.Header & routeMask
	p.PayloadType = (p.Header >> payloadTypeShift) & payloadTypeMask
	p.PayloadVer = (p.Header >> payloadVerShift) & payloadVerMask

	if routeHasTransportCodes(p.RouteType) {
		if !r.need(len(p.TransportCodes) * 2) {
			return nil, false
		}
		p.HasTransport = true
		p.TransportCodes[0] = r.u16()
		p.TransportCodes[1] = r.u16()
	}

	p.PathLenByte = r.u8()
	if !r.ok {
		return nil, false
	}
	var used int
	p.Path, used = decodePathBytes(p.PathLenByte, r.rest())
	r.skip(used)
	if !r.ok {
		return nil, false
	}
	p.Payload = append([]byte(nil), r.rest()...)
	return p, true
}

// ---------------------------------------------------------------------------
// Little helpers
// ---------------------------------------------------------------------------

// reader walks a frame with bounds checking. After any short read ok is false
// and every accessor returns a zero value, so parsers stay linear.
type reader struct {
	b   []byte
	pos int
	ok  bool
}

func newReader(b []byte) reader { return reader{b: b, ok: true} }

func (r *reader) need(n int) bool {
	if !r.ok || r.pos+n > len(r.b) {
		r.ok = false
		return false
	}
	return true
}

func (r *reader) u8() byte {
	if !r.need(1) {
		return 0
	}
	v := r.b[r.pos]
	r.pos++
	return v
}

func (r *reader) i8() int8 { return int8(r.u8()) }

func (r *reader) u16() uint16 {
	if !r.need(2) {
		return 0
	}
	v := binary.LittleEndian.Uint16(r.b[r.pos:])
	r.pos += 2
	return v
}

func (r *reader) u32() uint32 {
	if !r.need(4) {
		return 0
	}
	v := binary.LittleEndian.Uint32(r.b[r.pos:])
	r.pos += 4
	return v
}

func (r *reader) i32() int32 { return int32(r.u32()) }

func (r *reader) take(n int) []byte {
	if !r.need(n) {
		return nil
	}
	v := r.b[r.pos : r.pos+n]
	r.pos += n
	return v
}

func (r *reader) skip(n int) { _ = r.need(n) }

func (r *reader) rest() []byte {
	if !r.ok || r.pos >= len(r.b) {
		return nil
	}
	return r.b[r.pos:]
}

func (r *reader) string(n int) string { return trimNUL(string(r.take(n))) }

// frame builds a command frame from a code and its parts.
func frame(code byte, parts ...[]byte) []byte {
	n := 1
	for _, p := range parts {
		n += len(p)
	}
	buf := make([]byte, 0, n)
	buf = append(buf, code)
	for _, p := range parts {
		buf = append(buf, p...)
	}
	return buf
}

func u32le(v uint32) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, v)
	return b
}

func zeros(n int) []byte { return make([]byte, n) }

func trimNUL(s string) string {
	if i := strings.IndexByte(s, 0); i >= 0 {
		return s[:i]
	}
	return s
}

func checkText(text string) error {
	if len(text) > MaxTextLen {
		return fmt.Errorf("%w (got %d)", ErrTextTooLong, len(text))
	}
	return nil
}

func frameError(f []byte) error {
	code := byte(0)
	if len(f) > 1 {
		code = f[1]
	}
	name := errCodeName(code)
	switch code {
	case errCodeNotFound:
		return fmt.Errorf("%w: %s", ErrNotFound, name)
	case errCodeUnsupported:
		return fmt.Errorf("%w: %s", ErrUnsupported, name)
	}
	return fmt.Errorf("meshcore: node returned %s", name)
}

// ---------------------------------------------------------------------------
// Framing
// ---------------------------------------------------------------------------

type frameReader struct{ br *bufio.Reader }

// next reads one frame, resynchronising on the next '>' after garbage.
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
			continue
		}
		buf := make([]byte, n)
		if _, err := io.ReadFull(fr.br, buf); err != nil {
			return nil, err
		}
		return buf, nil
	}
}

func encodeFrame(payload []byte) []byte {
	buf := make([]byte, 0, frameHeaderLen+len(payload))
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
