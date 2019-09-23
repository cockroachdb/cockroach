// Copyright 2019 The Cockroach Authors.
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
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

func init() {
	mainLog.pcsPool = sync.Pool{
		New: func() interface{} {
			return [1]uintptr{}
		},
	}
	mainLog.setVState(0, nil, false)
}

// SetVModule alters the vmodule logging level to the passed in value.
func SetVModule(value string) error {
	return mainLog.vmodule.Set(value)
}

// VDepth reports whether verbosity at the call site is at least the requested
// level.
func VDepth(l int32, depth int) bool {
	// This function tries hard to be cheap unless there's work to do.
	// The fast path is three atomic loads and compares.

	// Here is a cheap but safe test to see if V logging is enabled globally.
	if mainLog.verbosity.get() >= level(l) {
		return true
	}

	if f, ok := mainLog.interceptor.Load().(InterceptorFn); ok && f != nil {
		return true
	}

	// It's off globally but vmodule may still be set.
	// Here is another cheap but safe test to see if vmodule is enabled.
	if atomic.LoadInt32(&mainLog.filterLength) > 0 {
		// Grab a buffer to use for reading the program counter. Keeping the
		// interface{} version around to Put back into the pool rather than
		// Put-ting the array saves an interface allocation.
		poolObj := mainLog.pcsPool.Get()
		pcs := poolObj.([1]uintptr)
		// We prefer not to use a defer in this function, which can be used in hot
		// paths, because a defer anywhere in the body of a function causes a call
		// to runtime.deferreturn at the end of that function, which has a
		// measurable performance penalty when in a very hot path.
		// defer mainLog.pcsPool.Put(pcs)
		if runtime.Callers(2+depth, pcs[:]) == 0 {
			mainLog.pcsPool.Put(poolObj)
			return false
		}
		mainLog.mu.Lock()
		v, ok := mainLog.vmap[pcs[0]]
		if !ok {
			v = mainLog.setV(pcs)
		}
		mainLog.mu.Unlock()
		mainLog.pcsPool.Put(poolObj)
		return v >= level(l)
	}
	return false
}

// setVState sets a consistent state for V logging.
// l.mu is held.
func (l *loggingT) setVState(verbosity level, filter []modulePat, setFilter bool) {
	// Turn verbosity off so V will not fire while we are in transition.
	mainLog.verbosity.set(0)
	// Ditto for filter length.
	atomic.StoreInt32(&mainLog.filterLength, 0)

	// Set the new filters and wipe the pc->Level map if the filter has changed.
	if setFilter {
		mainLog.vmodule.filter = filter
		mainLog.vmap = make(map[uintptr]level)
	}

	// Things are consistent now, so enable filtering and verbosity.
	// They are enabled in order opposite to that in V.
	atomic.StoreInt32(&mainLog.filterLength, int32(len(filter)))
	mainLog.verbosity.set(verbosity)
}

// setV computes and remembers the V level for a given PC
// when vmodule is enabled.
// File pattern matching takes the basename of the file, stripped
// of its .go suffix, and uses filepath.Match, which is a little more
// general than the *? matching used in C++.
// l.mu is held.
func (l *loggingT) setV(pc [1]uintptr) level {
	frame, _ := runtime.CallersFrames(pc[:]).Next()
	file := frame.File
	// The file is something like /a/b/c/d.go. We want just the d.
	if strings.HasSuffix(file, ".go") {
		file = file[:len(file)-3]
	}
	if slash := strings.LastIndexByte(file, '/'); slash >= 0 {
		file = file[slash+1:]
	}
	for _, filter := range l.vmodule.filter {
		if filter.match(file) {
			l.vmap[pc[0]] = filter.level
			return filter.level
		}
	}
	l.vmap[pc[0]] = 0
	return 0
}

// moduleSpec represents the setting of the --vmodule flag.
type moduleSpec struct {
	filter []modulePat
}

// modulePat contains a filter for the --vmodule flag.
// It holds a verbosity level and a file pattern to match.
type modulePat struct {
	pattern string
	literal bool // The pattern is a literal string
	level   level
}

// match reports whether the file matches the pattern. It uses a string
// comparison if the pattern contains no metacharacters.
func (m *modulePat) match(file string) bool {
	if m.literal {
		return file == m.pattern
	}
	match, _ := filepath.Match(m.pattern, file)
	return match
}

func (m *moduleSpec) String() string {
	// Lock because the type is not atomic. TODO: clean this up.
	mainLog.mu.Lock()
	defer mainLog.mu.Unlock()
	var b bytes.Buffer
	for i, f := range m.filter {
		if i > 0 {
			b.WriteRune(',')
		}
		fmt.Fprintf(&b, "%s=%d", f.pattern, f.level)
	}
	return b.String()
}

var errVmoduleSyntax = errors.New("syntax error: expect comma-separated list of filename=N")

// Syntax: --vmodule=recordio=2,file=1,gfs*=3
func (m *moduleSpec) Set(value string) error {
	var filter []modulePat
	for _, pat := range strings.Split(value, ",") {
		if len(pat) == 0 {
			// Empty strings such as from a trailing comma can be ignored.
			continue
		}
		patLev := strings.Split(pat, "=")
		if len(patLev) != 2 || len(patLev[0]) == 0 || len(patLev[1]) == 0 {
			return errVmoduleSyntax
		}
		pattern := patLev[0]
		v, err := strconv.Atoi(patLev[1])
		if err != nil {
			return errors.New("syntax error: expect comma-separated list of filename=N")
		}
		if v < 0 {
			return errors.New("negative value for vmodule level")
		}
		if v == 0 {
			continue // Ignore. It's harmless but no point in paying the overhead.
		}
		// TODO: check syntax of filter?
		filter = append(filter, modulePat{pattern, isLiteral(pattern), level(v)})
	}
	mainLog.mu.Lock()
	defer mainLog.mu.Unlock()
	mainLog.setVState(mainLog.verbosity, filter, true)
	return nil
}

// isLiteral reports whether the pattern is a literal string, that is, has no metacharacters
// that require filepath.Match to be called to match the pattern.
func isLiteral(pattern string) bool {
	return !strings.ContainsAny(pattern, `\*?[]`)
}

// Level is exported because it appears in the arguments to V and is
// the type of the v flag, which can be set programmatically.
// It's a distinct type because we want to discriminate it from logType.
// Variables of type level are only changed under mainLog.mu.
// The --verbosity flag is read only with atomic ops, so the state of the logging
// module is consistent.

// Level is treated as a sync/atomic int32.

// Level specifies a level of verbosity for V logs. *Level implements
// flag.Value; the --verbosity flag is of type Level and should be modified
// only through the flag.Value interface.
type level int32

// get returns the value of the Level.
func (l *level) get() level {
	return level(atomic.LoadInt32((*int32)(l)))
}

// set sets the value of the Level.
func (l *level) set(val level) {
	atomic.StoreInt32((*int32)(l), int32(val))
}

// String is part of the flag.Value interface.
func (l *level) String() string {
	return strconv.FormatInt(int64(*l), 10)
}

// Set is part of the flag.Value interface.
func (l *level) Set(value string) error {
	v, err := strconv.Atoi(value)
	if err != nil {
		return err
	}
	mainLog.mu.Lock()
	defer mainLog.mu.Unlock()
	mainLog.setVState(level(v), mainLog.vmodule.filter, false)
	return nil
}
