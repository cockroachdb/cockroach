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

func CreateSender() Sender {
	// Set up UDP socket
	addr, err := net.ResolveUDPAddr("udp", ":12345")

	if err != nil {
		panic(err)
	}
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		panic(err)
	}
	return Sender{conn}
}

// Send the duration in microseconds
func (s Sender) Send(duration time.Duration, t byte) error {
	b := make([]byte, 5)
	binary.LittleEndian.PutUint32(b, uint32(duration.Microseconds()))
	b[4] = t
	if _, err := s.conn.Write(b); err != nil {
		return err
	}
	return nil
}

func (s Sender) Close() error {
	return s.conn.Close()
}
