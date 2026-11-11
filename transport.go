package meshcore

import (
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"go.bug.st/serial"
)

// Transport — байтовый канал до компаньона. И TCP, и serial используют
// одинаковое обрамление кадров, поэтому различий на уровне протокола нет.
type Transport interface {
	io.Reader
	io.Writer
	io.Closer
}

// ---------------------------------------------------------------------------
// TCP / WiFi
// ---------------------------------------------------------------------------

type tcpTransport struct{ conn net.Conn }

// DialTCP подключается к компаньону с WiFi-интерфейсом.
func DialTCP(host string, port int) (Transport, error) {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", host, port), 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("meshcore: dial %s:%d: %w", host, port, err)
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	return &tcpTransport{conn: conn}, nil
}

func (t *tcpTransport) Read(p []byte) (int, error)  { return t.conn.Read(p) }
func (t *tcpTransport) Write(p []byte) (int, error) { return t.conn.Write(p) }
func (t *tcpTransport) Close() error                { return t.conn.Close() }

// ---------------------------------------------------------------------------
// USB / COM-порт
// ---------------------------------------------------------------------------

type serialTransport struct {
	port serial.Port
	stop chan struct{}
	once sync.Once
}

// DialSerial подключается к компаньону по USB (COM-порт). baud <= 0 -> 115200.
func DialSerial(name string, baud int) (Transport, error) {
	if baud <= 0 {
		baud = 115200
	}
	mode := &serial.Mode{
		BaudRate: baud,
		DataBits: 8,
		Parity:   serial.NoParity,
		StopBits: serial.OneStopBit,
	}
	p, err := serial.Open(name, mode)
	if err != nil {
		return nil, fmt.Errorf("meshcore: open %s: %w", name, err)
	}
	// Таймаут чтения нужен, чтобы Read периодически возвращал управление и
	// можно было заметить закрытие порта.
	if err := p.SetReadTimeout(250 * time.Millisecond); err != nil {
		_ = p.Close()
		return nil, fmt.Errorf("meshcore: set read timeout: %w", err)
	}
	return &serialTransport{port: p, stop: make(chan struct{})}, nil
}

// Read никогда не возвращает (0, nil): таймауты порта проглатываются здесь,
// иначе bufio.Reader ошибся бы io.ErrNoProgress.
func (t *serialTransport) Read(p []byte) (int, error) {
	for {
		select {
		case <-t.stop:
			return 0, io.EOF
		default:
		}
		n, err := t.port.Read(p)
		if n > 0 {
			return n, err
		}
		if err != nil {
			return 0, err
		}
	}
}

func (t *serialTransport) Write(p []byte) (int, error) { return t.port.Write(p) }

func (t *serialTransport) Close() error {
	t.once.Do(func() { close(t.stop) })
	return t.port.Close()
}
