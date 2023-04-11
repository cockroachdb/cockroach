// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package histogram

import (
	"context"
	"net"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/codahale/hdrhistogram"
)

type UdpReceiver struct {
	conn *net.UDPConn
	reg  *Registry
}

// CreateReceiver creates a receiver which will receive UDP packets when Observe is called on it.
func CreateUdpReceiver() *UdpReceiver {
	// Set up UDP socket
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
	if err != nil {
		panic(err)
	}
	return &UdpReceiver{conn, NewRegistry(10*time.Second, "receiver")}
}

func (s UdpReceiver) Port() int {
	return s.conn.LocalAddr().(*net.UDPAddr).Port
}

func (s UdpReceiver) Tick() map[string]*hdrhistogram.Histogram {
	return s.reg.swapAll()
}

// Listen listens for UDP packets and calls Observe on them until ctx is done.
func (s UdpReceiver) Listen(ctx context.Context) error {
	histograms := s.reg.GetHandle()
	b := make([]byte, 1024)
	lat := &Latency{}
	for {
		select {
		case <-ctx.Done():
			s.Close()
			return nil
		default:
			n, _, err := s.conn.ReadFrom(b)
			if err != nil {
				if !grpcutil.IsClosedConnection(err) {
					return err
				}
				return nil
			}
			if n == 0 {
				// Spurious empty reads are possible.
				continue
			}
			if err := lat.Unmarshal(b[0:n]); err != nil {
				return err
			}
			histograms.Get(lat.Operation).Record(time.Duration(lat.Duration) * time.Microsecond)
		}
	}
}

func (s UdpReceiver) Close() {
	// Ignore the error, we are closing anyway.
	_ = s.conn.Close()
}
