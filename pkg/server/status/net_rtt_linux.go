//go:build linux

package status

import (
	"time"

	"github.com/shirou/gopsutil/v3/net"
	"golang.org/x/sys/unix"
)

// RTTInfo holds the round-trip time information for a connection.
type RTTInfo struct {
	RTT    time.Duration
	RTTVar time.Duration
}

// getRTTInfo retrieves TCP round-trip time information for a given connection
// using a getsockopt syscall. This is only supported on Linux.
func getRTTInfo(conn net.ConnectionStat) (*RTTInfo, error) {
	// The file descriptor for the socket.
	fd := int(conn.Fd)

	// Retrieve TCP info for the socket.
	info, err := unix.GetsockoptTCPInfo(fd, unix.IPPROTO_TCP, unix.TCP_INFO)
	if err != nil {
		return nil, err
	}

	rttInfo := &RTTInfo{
		// RTT and RTTVar are in microseconds.
		RTT:    time.Duration(info.Rtt) * time.Microsecond,
		RTTVar: time.Duration(info.Rttvar) * time.Microsecond,
	}

	return rttInfo, nil
}
