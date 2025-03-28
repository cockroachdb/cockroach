package roachpb

import (
	"log"
	"net"
	"sync"
	"testing"
)

func TestAccept(t *testing.T) {
	l, _ := net.Listen("tcp", "localhost:8080")
	defer l.Close()

	maxConns := 5
	wg := sync.WaitGroup{}
	wg.Add(maxConns)

	go func() {
		for i := 0; i < maxConns; i++ {
			c, _ := l.Accept()
			log.Printf("Accepted connection from %s", c.RemoteAddr())
			wg.Done()
		}
	}()

	for i := 0; i < maxConns; i++ {
		net.Dial("tcp", "localhost:8080")
	}
	wg.Wait()
}
