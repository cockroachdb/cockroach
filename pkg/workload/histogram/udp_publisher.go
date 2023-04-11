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
	"encoding/binary"
	"math"
	"net"
	"time"
)

type UdpPublisher struct {
	conn        *net.UDPConn
	nameLookups map[string]uint32
}

var _ Publisher = UdpPublisher{}

// CreateSender creates a sender which will send UDP packets when Send is called on it.
func CreateUdpPublisher(address string, operations []string) *UdpPublisher {
	// Set up UDP socket
	addr, err := net.ResolveUDPAddr("udp", address)

	if err != nil {
		panic(err)
	}
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		panic(err)
	}
	nameLookups := make(map[string]uint32)
	for i, op := range operations {
		nameLookups[op] = uint32(i + 1)
	}

	return &UdpPublisher{conn, nameLookups}
}

// lookupID looks up an id for the operation name.
func (s UdpPublisher) lookupID(name string) uint32 {
	id, ok := s.nameLookups[name]
	if !ok {
		return 0
	}
	return id
}

// Send the duration in microseconds for this operation to the registered
// address. This is safe to call concurrently as conn.Write is thread-safe.
// TODO(baptist): Switch to protobuf for encoding.
func (s UdpPublisher) Observe(duration time.Duration, operation string) {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, s.lookupID(operation))
	dur := duration.Microseconds()
	// Send microseconds as an int32, since 2^32 microseconds = ~71 minutes.
	if dur > math.MaxUint32 {
		dur = math.MaxUint32
	}
	b = binary.LittleEndian.AppendUint32(b, uint32(dur))
	// Ignore errors while sending. It is best effort, and will not block.
	_, _ = s.conn.Write(b)
}

// Close the connection. Since UDP is connectionless, this only releases the fd
// associated with the connection.
func (s UdpPublisher) Close() {
	// Ignore the error, we are closing anyway.
	_ = s.conn.Close()
}
