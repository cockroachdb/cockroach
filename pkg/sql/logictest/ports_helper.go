package logictest

import "net"

// getFreePort finds a port that is available for a server to listen on.
func getFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}
	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	port := l.Addr().(*net.TCPAddr).Port
	err = l.Close()
	if err != nil {
		return 0, err
	}
	return port, err
}
