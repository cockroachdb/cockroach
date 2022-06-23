package email

import (
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/mail"
	"net/smtp"
	"net/textproto"
	"sync"
	"syscall"
	"time"
)

type Pool struct {
	addr          string
	auth          smtp.Auth
	max           int
	created       int
	clients       chan *client
	rebuild       chan struct{}
	mut           *sync.Mutex
	lastBuildErr  *timestampedErr
	closing       chan struct{}
	tlsConfig     *tls.Config
	helloHostname string
}

type client struct {
	*smtp.Client
	failCount int
}

type timestampedErr struct {
	err error
	ts  time.Time
}

const maxFails = 4

var (
	ErrClosed  = errors.New("pool closed")
	ErrTimeout = errors.New("timed out")
)

func NewPool(address string, count int, auth smtp.Auth, opt_tlsConfig ...*tls.Config) (pool *Pool, err error) {
	pool = &Pool{
		addr:    address,
		auth:    auth,
		max:     count,
		clients: make(chan *client, count),
		rebuild: make(chan struct{}),
		closing: make(chan struct{}),
		mut:     &sync.Mutex{},
	}
	if len(opt_tlsConfig) == 1 {
		pool.tlsConfig = opt_tlsConfig[0]
	} else if host, _, e := net.SplitHostPort(address); e != nil {
		return nil, e
	} else {
		pool.tlsConfig = &tls.Config{ServerName: host}
	}
	return
}

// go1.1 didn't have this method
func (c *client) Close() error {
	return c.Text.Close()
}

// SetHelloHostname optionally sets the hostname that the Go smtp.Client will
// use when doing a HELLO with the upstream SMTP server. By default, Go uses
// "localhost" which may not be accepted by certain SMTP servers that demand
// an FQDN.
func (p *Pool) SetHelloHostname(h string) {
	p.helloHostname = h
}

func (p *Pool) get(timeout time.Duration) *client {
	select {
	case c := <-p.clients:
		return c
	default:
	}

	if p.created < p.max {
		p.makeOne()
	}

	var deadline <-chan time.Time
	if timeout >= 0 {
		deadline = time.After(timeout)
	}

	for {
		select {
		case c := <-p.clients:
			return c
		case <-p.rebuild:
			p.makeOne()
		case <-deadline:
			return nil
		case <-p.closing:
			return nil
		}
	}
}

func shouldReuse(err error) bool {
	// certainly not perfect, but might be close:
	//  - EOF: clearly, the connection went down
	//  - textproto.Errors were valid SMTP over a valid connection,
	//    but resulted from an SMTP error response
	//  - textproto.ProtocolErrors result from connections going down,
	//    invalid SMTP, that sort of thing
	//  - syscall.Errno is probably down connection/bad pipe, but
	//    passed straight through by textproto instead of becoming a
	//    ProtocolError
	//  - if we don't recognize the error, don't reuse the connection
	// A false positive will probably fail on the Reset(), and even if
	// not will eventually hit maxFails.
	// A false negative will knock over (and trigger replacement of) a
	// conn that might have still worked.
	if err == io.EOF {
		return false
	}
	switch err.(type) {
	case *textproto.Error:
		return true
	case *textproto.ProtocolError, textproto.ProtocolError:
		return false
	case syscall.Errno:
		return false
	default:
		return false
	}
}

func (p *Pool) replace(c *client) {
	p.clients <- c
}

func (p *Pool) inc() bool {
	if p.created >= p.max {
		return false
	}

	p.mut.Lock()
	defer p.mut.Unlock()

	if p.created >= p.max {
		return false
	}
	p.created++
	return true
}

func (p *Pool) dec() {
	p.mut.Lock()
	p.created--
	p.mut.Unlock()

	select {
	case p.rebuild <- struct{}{}:
	default:
	}
}

func (p *Pool) makeOne() {
	go func() {
		if p.inc() {
			if c, err := p.build(); err == nil {
				p.clients <- c
			} else {
				p.lastBuildErr = &timestampedErr{err, time.Now()}
				p.dec()
			}
		}
	}()
}

func startTLS(c *client, t *tls.Config) (bool, error) {
	if ok, _ := c.Extension("STARTTLS"); !ok {
		return false, nil
	}

	if err := c.StartTLS(t); err != nil {
		return false, err
	}

	return true, nil
}

func addAuth(c *client, auth smtp.Auth) (bool, error) {
	if ok, _ := c.Extension("AUTH"); !ok {
		return false, nil
	}

	if err := c.Auth(auth); err != nil {
		return false, err
	}

	return true, nil
}

func (p *Pool) build() (*client, error) {
	cl, err := smtp.Dial(p.addr)
	if err != nil {
		return nil, err
	}

	// Is there a custom hostname for doing a HELLO with the SMTP server?
	if p.helloHostname != "" {
		cl.Hello(p.helloHostname)
	}

	c := &client{cl, 0}

	if _, err := startTLS(c, p.tlsConfig); err != nil {
		c.Close()
		return nil, err
	}

	if p.auth != nil {
		if _, err := addAuth(c, p.auth); err != nil {
			c.Close()
			return nil, err
		}
	}

	return c, nil
}

func (p *Pool) maybeReplace(err error, c *client) {
	if err == nil {
		c.failCount = 0
		p.replace(c)
		return
	}

	c.failCount++
	if c.failCount >= maxFails {
		goto shutdown
	}

	if !shouldReuse(err) {
		goto shutdown
	}

	if err := c.Reset(); err != nil {
		goto shutdown
	}

	p.replace(c)
	return

shutdown:
	p.dec()
	c.Close()
}

func (p *Pool) failedToGet(startTime time.Time) error {
	select {
	case <-p.closing:
		return ErrClosed
	default:
	}

	if p.lastBuildErr != nil && startTime.Before(p.lastBuildErr.ts) {
		return p.lastBuildErr.err
	}

	return ErrTimeout
}

// Send sends an email via a connection pulled from the Pool. The timeout may
// be <0 to indicate no timeout. Otherwise reaching the timeout will produce
// and error building a connection that occurred while we were waiting, or
// otherwise ErrTimeout.
func (p *Pool) Send(e *Email, timeout time.Duration) (err error) {
	start := time.Now()
	c := p.get(timeout)
	if c == nil {
		return p.failedToGet(start)
	}

	defer func() {
		p.maybeReplace(err, c)
	}()

	recipients, err := addressLists(e.To, e.Cc, e.Bcc)
	if err != nil {
		return
	}

	msg, err := e.Bytes()
	if err != nil {
		return
	}

	from, err := emailOnly(e.From)
	if err != nil {
		return
	}
	if err = c.Mail(from); err != nil {
		return
	}

	for _, recip := range recipients {
		if err = c.Rcpt(recip); err != nil {
			return
		}
	}

	w, err := c.Data()
	if err != nil {
		return
	}
	if _, err = w.Write(msg); err != nil {
		return
	}

	err = w.Close()

	return
}

func emailOnly(full string) (string, error) {
	addr, err := mail.ParseAddress(full)
	if err != nil {
		return "", err
	}
	return addr.Address, nil
}

func addressLists(lists ...[]string) ([]string, error) {
	length := 0
	for _, lst := range lists {
		length += len(lst)
	}
	combined := make([]string, 0, length)

	for _, lst := range lists {
		for _, full := range lst {
			addr, err := emailOnly(full)
			if err != nil {
				return nil, err
			}
			combined = append(combined, addr)
		}
	}

	return combined, nil
}

// Close immediately changes the pool's state so no new connections will be
// created, then gets and closes the existing ones as they become available.
func (p *Pool) Close() {
	close(p.closing)

	for p.created > 0 {
		c := <-p.clients
		c.Quit()
		p.dec()
	}
}
