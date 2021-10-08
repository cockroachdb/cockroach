// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package idle

import (
	"context"
	"net"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// maxIdleConns limits the number of idle connections that can be found in a
// given pass of the idle monitor.
const maxIdleConns = 1000

// OnIdleFunc is called by the monitor when a connection has been idle for the
// timeout period.
type OnIdleFunc func()

// connMap maps from a wrapped monitorConn to the idle function which should be
// called if the connection goes idle.
type connMap map[*monitorConn]OnIdleFunc

// Monitor detects connections which have had no read or write activity for a
// timeout period. Connections are grouped by their "remote" address (i.e. the
// address to which they connect). Idle checks can be turned off and on for each
// group. Example usage:
//
//   mon := NewMonitor(ctx, 10*time.Second)
//   wrapped := mon.DetectIdle(conn, func() {
//     // Handle idle connection.
//   })
//   defer wrapped.Close()
//   mon.SetIdleChecks(conn.RemoteAddr().String())
//
// NOTE: All methods on Monitor are thread-safe.
type Monitor struct {
	timeout time.Duration

	mu struct {
		syncutil.Mutex

		// conns maps from remote address to a map of connections to that address.
		conns map[string]connMap

		// checks is a set of remote addresses. All connections to those addresses
		// will be monitored for idleness.
		checks map[string]struct{}
	}
}

// NewMonitor constructs a new idle connection monitor. It runs periodically on
// a background thread, until the given context is canceled. Callers must
// always ensure the context is canceled to avoid a goroutine leak.
func NewMonitor(ctx context.Context, timeout time.Duration) *Monitor {
	if timeout == 0 {
		panic("monitor should never be constructed with a zero timeout")
	}

	m := &Monitor{timeout: timeout}
	m.mu.conns = make(map[string]connMap)
	m.mu.checks = make(map[string]struct{})

	go m.start(ctx)

	return m
}

// SetIdleChecks instructs the monitor to detect idleness for any connection
// to the given remote address.
func (m *Monitor) SetIdleChecks(addr string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mu.checks[addr] = struct{}{}
}

// ClearIdleChecks disables idle detection for connections to the given remote
// address.
func (m *Monitor) ClearIdleChecks(addr string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.mu.checks, addr)
}

// DetectIdle adds a new connection to the monitor. The monitor will call the
// given "onIdle" callback function if that connection ever becomes idle.
// DetectIdle returns a "wrapper" connection that intercepts Read/Write calls
// to the given connection in order to detect idleness. Callers must therefore
// redirect all traffic through the returned connection rather than the original
// connection, and be sure to close the wrapper connection (it will remove
// itself from the monitor when closed).
// NOTE: The callback function will be called from a background goroutine. It
// will be called repeatedly for the same connection each time the Monitor runs,
// until the connection is closed or activity on the connection resumes.
func (m *Monitor) DetectIdle(conn net.Conn, onIdle OnIdleFunc) net.Conn {
	m.mu.Lock()
	defer m.mu.Unlock()

	wrapper := newMonitorConn(m, conn)

	addr := conn.RemoteAddr().String()
	existing, ok := m.mu.conns[addr]
	if !ok {
		existing = make(map[*monitorConn]OnIdleFunc)
		m.mu.conns[addr] = existing
	}

	existing[wrapper] = onIdle
	return wrapper
}

// start runs on a background goroutine. It runs 10 times per timeout period, so
// the idle detection granularity error can be as high as ~10%. Each time it
// wakes up, it scans all checked connections for any that have been idle for at
// least the timeout period.
func (m *Monitor) start(ctx context.Context) {
	var checkAddrs []string
	var idleFuncs []OnIdleFunc

	for {
		select {
		case <-ctx.Done():
			return

		case <-time.After(m.timeout / 10):
		}

		// Get the addresses of all pods that need to be monitored for idleness.
		// Copy the addresses into a separate slice in order to avoid holding
		// the mutex for too long (just long enough to copy).
		checkAddrs = m.getAddrsToCheck(checkAddrs)
		for _, addr := range checkAddrs {
			// Get the idle callback functions for all connections to this pod
			// address that have gone idle. Any given pod should have at worst a
			// few thousand connections to it. Benchmarks show that even scanning
			// 10,000 connections for idleness takes only 250us - it's OK to hold
			// the lock for that long.
			idleFuncs = m.findIdleConns(addr, idleFuncs)

			// Callback functions are copied to a separate slice outside the
			// scope of a lock, so there are no concerns with the onIdle callback
			// function causing re-entrancy deadlocks.
			for _, onIdle := range idleFuncs {
				onIdle()
			}
		}
	}
}

// getAddrsToCheck returns the addresses of all pods that require idle timeouts
// to be checked. The list is appended to the given "checkAddrs" slice and the
// slice is returned. Copying into the slice minimizes the amount of time the
// lock needs to be held.
func (m *Monitor) getAddrsToCheck(checkAddrs []string) []string {
	checkAddrs = checkAddrs[:0]

	m.mu.Lock()
	defer m.mu.Unlock()
	for addr := range m.mu.checks {
		checkAddrs = append(checkAddrs, addr)
	}
	return checkAddrs
}

// findIdleConns finds all connections to the given pod address that have been
// idle for longer than the timeout period. It appends the idle callback
// functions associated with those connections to the given "idleFuncs" slice
// and returns that slice. Copying into the slice minimizes the amount of time
// the lock needs to be held.
func (m *Monitor) findIdleConns(addr string, idleFuncs []OnIdleFunc) []OnIdleFunc {
	idleFuncs = idleFuncs[:0]
	now := timeutil.Now().UnixNano()

	// Compute new deadline for connections with recent activity.
	deadline := now + int64(m.timeout)

	m.mu.Lock()
	defer m.mu.Unlock()

	connMap := m.mu.conns[addr]
	for conn, onIdle := range connMap {
		// Get current deadline of the connection.
		// See monitorConn comment for more details on how the deadline field is
		// used by the monitor and connection.
		connDeadline := atomic.LoadInt64(&conn.deadline)
		if connDeadline != 0 {
			// Check if the deadline has passed, in which case the connection is
			// considered idle.
			if now > connDeadline {
				idleFuncs = append(idleFuncs, onIdle)
				if len(idleFuncs) >= maxIdleConns {
					// Limit max size of the slice. Additional idle connections
					// can be found the next time the monitor runs.
					break
				}
			}
		} else {
			// Set a new deadline for the connection.
			atomic.StoreInt64(&conn.deadline, deadline)
		}
	}

	return idleFuncs
}

// removeConn is called by a monitorConn when it is called, in order to remove
// itself from the connection map.
func (m *Monitor) removeConn(conn *monitorConn) {
	m.mu.Lock()
	defer m.mu.Unlock()

	addr := conn.RemoteAddr().String()
	if connMap, ok := m.mu.conns[addr]; ok {
		delete(connMap, conn)
		if len(connMap) == 0 {
			// No more connections to this address, so remove map entries.
			delete(m.mu.conns, addr)
		}
	}
}

func (m *Monitor) countConnsToAddr(addr string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.mu.conns[addr])
}

func (m *Monitor) countAddrsToCheck() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.mu.checks)
}
