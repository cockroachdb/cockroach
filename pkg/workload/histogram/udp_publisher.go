// Copyright 2024 The Cockroach Authors.
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
	"net"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

type UdpPublisher struct {
	conn *net.UDPConn
}

var _ Publisher = UdpPublisher{}

// CreateSender creates a sender which will send UDP packets when Send is called on it.
func CreateUdpPublisher(address string) *UdpPublisher {
	// Set up UDP socket
	addr, err := net.ResolveUDPAddr("udp", address)

	if err != nil {
		panic(err)
	}
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		panic(err)
	}

	return &UdpPublisher{conn}
}

// Send the duration in microseconds for this operation to the registered
// address. This is safe to call concurrently as conn.Write is thread-safe.
func (s UdpPublisher) Observe(duration time.Duration, operation string) {
	latency := Latency{Operation: operation, Duration: duration}
	buf, _ := protoutil.Marshal(&latency)

	// Ignore errors while sending. It is best effort, and will not block.
	_, _ = s.conn.Write(buf)
}

// Close the connection. Since UDP is connectionless, this only releases the fd
// associated with the connection.
func (s UdpPublisher) Close() {
	// Ignore the error, we are closing anyway.
	_ = s.conn.Close()
}
