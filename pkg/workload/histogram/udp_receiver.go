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
	"encoding/binary"
	"net"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/errors"
	"github.com/codahale/hdrhistogram"
)

type UdpReceiver struct {
	conn        *net.UDPConn
	nameLookups map[uint32]string
	reg         *Registry
}

// CreateReceiver creates a receiver which will receive UDP packets when Observe is called on it.
func CreateUdpReceiver(address string, operations []string) *UdpReceiver {
	// Set up UDP socket
	addr, err := net.ResolveUDPAddr("udp", address)

	if err != nil {
		panic(err)
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		panic(err)
	}
	nameLookups := make(map[uint32]string)
	for i, op := range operations {
		nameLookups[uint32(i+1)] = op
	}
	return &UdpReceiver{conn, nameLookups, NewRegistry(10*time.Second, "receiver")}
}

func (s UdpReceiver) lookupName(id uint32) string {
	name, ok := s.nameLookups[id]
	if !ok {
		return ""
	}
	return name
}

func (s UdpReceiver) Tick() map[string]*hdrhistogram.Histogram {
	return s.reg.swapAll()
}

// Listen listens for UDP packets and calls Observe on them until ctx is done.
func (s UdpReceiver) Listen(ctx context.Context) error {
	histograms := s.reg.GetHandle()
	for {
		select {
		case <-ctx.Done():
			s.Close()
			return nil
		default:
			b := make([]byte, 8)
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
			if n != 8 {
				// If we get a packet that is not 8 bytes, panic as our
				// alignment will be off going forward and we don't have a way
				// to correct it.
				return errors.Newf("expected 8 bytes not %d", n)
			}
			operation := s.lookupName(binary.LittleEndian.Uint32(b[:4]))
			duration := time.Duration(binary.LittleEndian.Uint32(b[4:])) * time.Microsecond
			histograms.Get(operation).Record(duration)
		}
	}
}

func (s UdpReceiver) Close() {
	// Ignore the error, we are closing anyway.
	_ = s.conn.Close()
}
