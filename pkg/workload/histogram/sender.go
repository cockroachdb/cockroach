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
	"net"
	"time"
)

type Sender struct {
	conn *net.UDPConn
}

// CreateSender creates a sender which will send UDP packets when Send is called on it.
func CreateSender(address string) Sender {
	// Set up UDP socket
	addr, err := net.ResolveUDPAddr("udp", address)

	if err != nil {
		panic(err)
	}
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		panic(err)
	}
	return Sender{conn}
}

// Send the duration in microseconds to the address. This is safe to call
// concurrently as conn.Write is thread-safe.
func (s Sender) Send(duration time.Duration, id int) {
	b := make([]byte, 5)
	binary.LittleEndian.PutUint32(b, uint32(duration.Microseconds()))
	// We only store a single byte for the id. The expectation is that there are a
	// small number of unique ids so this won't wrap.
	b[4] = byte(id)
	// Ignore errors on this send. It is best effort anyway.
	_, _ = s.conn.Write(b)
}

// Close the connection. Since UDP is connectionless, this only releases the fd
// associated with the connection.
func (s Sender) Close() error {
	return s.conn.Close()
}
