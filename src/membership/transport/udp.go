package transport

import (
	"net"
	"time"
)

type UDP struct {
	conn *net.UDPConn
}

func Listen(addr string) (*UDP, error) {
	a, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	c, err := net.ListenUDP("udp", a)
	if err != nil {
		return nil, err
	}
	_ = c.SetReadBuffer(1 << 20)
	_ = c.SetWriteBuffer(1 << 20)
	return &UDP{conn: c}, nil
}

func (u *UDP) Read(p []byte) (n int, from *net.UDPAddr, err error) {
	n, from, err = u.conn.ReadFromUDP(p)
	return
}

func (u *UDP) WriteTo(p []byte, to *net.UDPAddr) error {
	_, err := u.conn.WriteToUDP(p, to)
	return err
}

func (u *UDP) Close() error                  { return u.conn.Close() }
func (u *UDP) SetDeadline(t time.Time) error { return u.conn.SetDeadline(t) }
