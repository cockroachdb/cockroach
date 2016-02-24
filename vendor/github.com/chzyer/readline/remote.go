package readline

import (
	"io"
	"net"
	"os"
)

type Conn struct {
	Conn     net.Conn
	Terminal *Terminal
	runChan  chan error
}

func NewConn(conn net.Conn, t *Terminal) (*Conn, error) {
	return &Conn{
		Conn:     conn,
		Terminal: t,
		runChan:  make(chan error),
	}, nil
}

func (c *Conn) Run() error {
	c.Terminal.EnterRawMode()
	go func() {
		_, err := io.Copy(c.Conn, os.Stdin)
		c.runChan <- err
	}()
	go func() {
		_, err := io.Copy(os.Stdout, c.Conn)
		c.runChan <- err
	}()
	err := <-c.runChan
	c.Terminal.ExitRawMode()
	return err
}

func Dial(network string, address string) (*Conn, error) {
	conn, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}
	var cfg Config
	t, err := NewTerminal(&cfg)
	if err != nil {
		return nil, err
	}
	return NewConn(conn, t)
}
