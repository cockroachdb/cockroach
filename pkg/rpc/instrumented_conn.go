package rpc

import (
	"net"

	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
	"storj.io/drpc/drpcmigrate"
)

type InstrumentedConn struct {
	net.Conn
	metrics *tcpMetrics
	onClose func() error
}

func getTCPConn(conn net.Conn) *net.TCPConn {
	switch c := conn.(type) {
	case *net.TCPConn:
		return c
	case *drpcmigrate.HeaderConn:
		return getTCPConn(c.Conn)
	default:
		return nil
	}
}

func (c *InstrumentedConn) UpdateMetrics() {
	tcpConn := getTCPConn(c.Conn)
	if tcpConn == nil {
		return
	}
	rttInfo, ok := sysutil.GetRTTInfo(tcpConn)
	if !ok {
		return
	}
	c.metrics.TCPRTT.Update(rttInfo.RTT.Nanoseconds())
	c.metrics.TCPRTTVar.Update(rttInfo.RTTVar.Nanoseconds())
}

func (c *InstrumentedConn) Close() error {
	// we use the underlying connection for idempotency
	if err := c.Conn.Close(); err != nil {
		return err
	}
	return c.onClose()
}
