package GoMeshcoreLib

import (
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"

	"go.bug.st/serial"
)

// Transport is the byte channel to a companion radio. TCP and serial use the
// same frame delimiters, so the protocol layer is transport agnostic.
type Transport interface {
	io.Reader
	io.Writer
	io.Closer
}

type tcpTransport struct{ conn net.Conn }

// DialTCP connects to a companion exposing a WiFi (TCP) interface.
func DialTCP(host string, port int) (Transport, error) {
	// JoinHostPort quotes IPv6 literals, so both "192.168.1.10" and
	// "fe80::1" produce a dialable address.
	addr := net.JoinHostPort(host, strconv.Itoa(port))
	conn, err := net.DialTimeout("tcp", addr, dialTimeout)
	if err != nil {
		return nil, fmt.Errorf("meshcore: dial %s: %w", addr, err)
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	return &tcpTransport{conn: conn}, nil
}

func (t *tcpTransport) Read(p []byte) (int, error)  { return t.conn.Read(p) }
func (t *tcpTransport) Write(p []byte) (int, error) { return t.conn.Write(p) }
func (t *tcpTransport) Close() error                { return t.conn.Close() }

type serialTransport struct {
	port serial.Port
	stop chan struct{}
	once sync.Once
}

// DialSerial connects to a companion over USB. baud <= 0 selects 115200.
func DialSerial(name string, baud int) (Transport, error) {
	if baud <= 0 {
		baud = defaultBaud
	}
	mode := &serial.Mode{
		BaudRate: baud,
		DataBits: serialDataBits,
		Parity:   serial.NoParity,
		StopBits: serial.OneStopBit,
	}
	p, err := serial.Open(name, mode)
	if err != nil {
		return nil, fmt.Errorf("meshcore: open %s: %w", name, err)
	}
	// A read timeout lets Read return periodically so Close is noticed.
	if err := p.SetReadTimeout(serialReadTimeout); err != nil {
		_ = p.Close()
		return nil, fmt.Errorf("meshcore: set read timeout: %w", err)
	}
	return &serialTransport{port: p, stop: make(chan struct{})}, nil
}

// Read never returns (0, nil): port timeouts are absorbed here, otherwise
// bufio.Reader would fail with io.ErrNoProgress.
func (t *serialTransport) Read(p []byte) (int, error) {
	for i := 0; ; i++ {
		select {
		case <-t.stop:
			return 0, io.EOF
		default:
		}
		n, err := t.port.Read(p)
		if n > 0 || err != nil {
			return n, err
		}
		if i >= serialReadAttempts {
			return 0, ErrTimeout
		}
	}
}

func (t *serialTransport) Write(p []byte) (int, error) { return t.port.Write(p) }

func (t *serialTransport) Close() error {
	t.once.Do(func() { close(t.stop) })
	return t.port.Close()
}
