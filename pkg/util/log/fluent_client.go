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
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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

const fluentDialTimeout = 5 * time.Second
const fluentWriteTimeout = time.Second

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
	l.conn, err = net.DialTimeout(l.network, l.addr, fluentDialTimeout)
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
	if err := l.conn.SetWriteDeadline(timeutil.Now().Add(fluentWriteTimeout)); err != nil {
		// An error here is suggestive of a bug in the Go runtime.
		fmt.Fprintf(OrigStderr, "%s: set write deadline error: %v\n%s",
			l, err, b)
		l.good = false
		return err
	}
	n, err := l.conn.Write(b)
	if err != nil || n < len(b) {
		fmt.Fprintf(OrigStderr, "%s: logging error: %v or short write (%d/%d)\n%s",
			l, err, n, len(b), b)
		l.good = false
	}
	return err
}
