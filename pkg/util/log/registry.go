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

import "github.com/cockroachdb/cockroach/pkg/util/syncutil"

type loggerRegistry struct {
	mu struct {
		syncutil.Mutex
		loggers []*loggerT
	}
}

var allLoggers = loggerRegistry{}

// len returns the number of known loggers.
func (r *loggerRegistry) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.mu.loggers)
}

// iterate iterates over all the loggers and stops at the first error
// encountered.
func (r *loggerRegistry) iter(fn func(l *loggerT) error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, l := range r.mu.loggers {
		if err := fn(l); err != nil {
			return err
		}
	}
	return nil
}

// put adds a logger into the registry.
func (r *loggerRegistry) put(l *loggerT) {
	r.mu.Lock()
	r.mu.loggers = append(r.mu.loggers, l)
	r.mu.Unlock()
}

// del removes one logger from the registry.
func (r *loggerRegistry) del(l *loggerT) {
	// Make the registry forget about this logger. This avoids
	// stacking many secondary loggers together when there are
	// subsequent tests starting servers in the same package.
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, thatLogger := range r.mu.loggers {
		if thatLogger != l {
			continue
		}
		r.mu.loggers = append(r.mu.loggers[:i], r.mu.loggers[i+1:]...)
		return
	}
}

type fileSinkRegistry struct {
	mu struct {
		syncutil.Mutex
		sinks []*fileSink
	}
}

var allFileSinks = fileSinkRegistry{}

// iterate iterates over all the file sinks and stops at the first
// error encountered.
func (r *fileSinkRegistry) iter(fn func(l *fileSink) error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, l := range r.mu.sinks {
		if err := fn(l); err != nil {
			return err
		}
	}
	return nil
}

// put adds a logger into the registry.
func (r *fileSinkRegistry) put(l *fileSink) {
	r.mu.Lock()
	r.mu.sinks = append(r.mu.sinks, l)
	r.mu.Unlock()
}

// del removes one logger from the registry.
func (r *fileSinkRegistry) del(l *fileSink) {
	// Make the registry forget about this logger. This avoids
	// stacking many secondary loggers together when there are
	// subsequent tests starting servers in the same package.
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, thatSink := range r.mu.sinks {
		if thatSink != l {
			continue
		}
		r.mu.sinks = append(r.mu.sinks[:i], r.mu.sinks[i+1:]...)
		return
	}
}
