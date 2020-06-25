package proxy

import (
	"log"
	"net"
	"time"
)

func Serve(ln net.Listener, opts Options) error {
	doneCh := make(chan error)
	defer close(doneCh)
	proxyErrCh := make(chan error)

	go func() {
		for {
			select {
			case <-doneCh:
				return

			case err := <-proxyErrCh:
				log.Println(err)
			}
		}
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go func() {
			defer conn.Close()
			tBegin := time.Now()
			log.Println("handling client", conn.RemoteAddr())
			err := Proxy(conn, opts)
			log.Printf("client %s disconnected after %.2fs: %v",
				conn.RemoteAddr(), time.Since(tBegin).Seconds(), err)
		}()
	}
	return nil
}
