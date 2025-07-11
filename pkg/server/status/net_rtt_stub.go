//go:build !linux

package status

import (
	"fmt"
	"time"

	"github.com/shirou/gopsutil/v3/net"
)

// RTTInfo holds the round-trip time information for a connection.
type RTTInfo struct {
	RTT    time.Duration
	RTTVar time.Duration
}

// getRTTInfo is a stub implementation for non-Linux platforms.
func getRTTInfo(conn net.ConnectionStat) (*RTTInfo, error) {
	return nil, fmt.Errorf("RTT inspection is only supported on Linux")
}
