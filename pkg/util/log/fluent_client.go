// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package log

import (
	"fmt"
	"net"

	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/errors"
)

// fluentSink represents a Fluentd-compatible network collector.
type fluentSink struct {
	// The network address of the fluentd collector.
	network string
	addr    string

	// good indicates that the connection can be used.
	good bool
	conn net.Conn
}

func newFluentSink(network, addr string) *fluentSink {
	f := &fluentSink{
		addr:    addr,
		network: network,
	}
	return f
}

func (l *fluentSink) String() string {
	return fmt.Sprintf("fluent:%s://%s", l.network, l.addr)
}

// activeAtSeverity implements the logSink interface.
func (l *fluentSink) active() bool { return true }

// attachHints implements the logSink interface.
func (l *fluentSink) attachHints(stacks []byte) []byte {
	return stacks
}

// exitCode implements the logSink interface.
func (l *fluentSink) exitCode() exit.Code {
	return exit.LoggingNetCollectorUnavailable()
}

// output implements the logSink interface.
func (l *fluentSink) output(extraSync bool, b []byte) error {
	// Try to write and reconnect immediately if the first write fails.
	//
	// TODO(knz): Add some net socket write deadlines here.
	_ = l.tryWrite(b)
	if l.good {
		return nil
	}

	if err := l.ensureConn(b); err != nil {
		return err
	}
	return l.tryWrite(b)
}

// emergencyOutput implements the logSink interface.
func (l *fluentSink) emergencyOutput(b []byte) {
	// TODO(knz): Add some net socket write deadlines here.
	_ = l.tryWrite(b)
	if !l.good {
		_ = l.ensureConn(b)
		_ = l.tryWrite(b)
	}
}

func (l *fluentSink) close() {
	l.good = false
	if l.conn != nil {
		if err := l.conn.Close(); err != nil {
			fmt.Fprintf(OrigStderr, "error closing network logger: %v\n", err)
		}
		l.conn = nil
	}
}

func (l *fluentSink) ensureConn(b []byte) error {
	if l.good {
		return nil
	}
	l.close()
	var err error
	l.conn, err = net.Dial(l.network, l.addr)
	if err != nil {
		fmt.Fprintf(OrigStderr, "%s: error dialing network logger: %v\n%s", l, err, b)
		return err
	}
	fmt.Fprintf(OrigStderr, "%s: connection to network logger resumed\n", l)
	l.good = true
	return nil
}

var errNoConn = errors.New("no connection opened")

func (l *fluentSink) tryWrite(b []byte) error {
	if !l.good {
		return errNoConn
	}
	n, err := l.conn.Write(b)
	if err != nil || n < len(b) {
		fmt.Fprintf(OrigStderr, "%s: logging error: %v or short write (%d/%d)\n%s",
			l, err, n, len(b), b)
		l.good = false
	}
	return err
}
