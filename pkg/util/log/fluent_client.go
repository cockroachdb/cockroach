// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package log

import (
	"fmt"
	"net"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// fluentSink represents a Fluentd-compatible network collector.
type fluentSink struct {
	// The network address of the fluentd collector.
	network string
	addr    string

	mu struct {
		syncutil.RWMutex
		// good indicates that the connection can be used.
		good bool
		conn net.Conn
	}
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
func (l *fluentSink) output(b []byte, opts sinkOutputOptions) (err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	logging.metrics.IncrementCounter(FluentSinkWriteAttempt, 1)

	// Try to write and reconnect immediately if the first write fails.
	_ = l.tryWriteLocked(b)
	if l.mu.good {
		return nil
	}

	logging.metrics.IncrementCounter(FluentSinkConnectionAttempt, 1)
	if err := l.ensureConnLocked(b); err != nil {
		logging.metrics.IncrementCounter(FluentSinkConnectionError, 1)
		return err
	}
	if err := l.tryWriteLocked(b); err != nil {
		logging.metrics.IncrementCounter(FluentSinkWriteError, 1)
		return err
	}
	return nil
}

func (l *fluentSink) closeLocked() {
	l.mu.good = false
	if l.mu.conn != nil {
		if err := l.mu.conn.Close(); err != nil {
			fmt.Fprintf(OrigStderr, "error closing network logger: %v\n", err)
		}
		l.mu.conn = nil
	}
}

func (l *fluentSink) ensureConnLocked(b []byte) error {
	if l.mu.good {
		return nil
	}
	l.closeLocked()
	var err error
	l.mu.conn, err = net.DialTimeout(l.network, l.addr, fluentDialTimeout)
	if err != nil {
		fmt.Fprintf(OrigStderr, "%s: error dialing network logger: %v\n%s", l, err, b)
		return err
	}
	fmt.Fprintf(OrigStderr, "%s: connection to network logger resumed\n", l)
	l.mu.good = true
	return nil
}

var errNoConn = errors.New("no connection opened")

func (l *fluentSink) tryWriteLocked(b []byte) error {
	if !l.mu.good {
		return errNoConn
	}
	if err := l.mu.conn.SetWriteDeadline(timeutil.Now().Add(fluentWriteTimeout)); err != nil {
		// An error here is suggestive of a bug in the Go runtime.
		fmt.Fprintf(OrigStderr, "%s: set write deadline error: %v\n%s",
			l, err, b)
		l.mu.good = false
		return err
	}
	n, err := l.mu.conn.Write(b)
	if err != nil || n < len(b) {
		fmt.Fprintf(OrigStderr, "%s: logging error: %v or short write (%d/%d)\n%s",
			l, err, n, len(b), b)
		l.mu.good = false
	}
	return err
}
