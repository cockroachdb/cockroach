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
	"fmt"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

type vmoduleConfig struct {
	// pcsPool maintains a set of [1]uintptr buffers to be used in V to avoid
	// allocating every time we compute the caller's PC.
	pcsPool sync.Pool

	// V logging level, the value of the --verbosity flag. Updated with
	// atomics.
	verbosity Level

	mu struct {
		// These flags are modified only under lock.
		syncutil.Mutex

		// vmap is a cache of the V Level for each V() call site, identified by PC.
		// It is wiped whenever the vmodule flag changes state.
		vmap map[uintptr]Level

		// filterLength stores the length of the vmodule filter
		// chain. If greater than zero, it means vmodule is enabled. It is
		// read using atomics but updated under mu.Lock.
		filterLength int32

		// The state of the --vmodule flag.
		vmodule moduleSpec
	}
}

func init() {
	logging.vmoduleConfig.pcsPool = sync.Pool{
		New: func() interface{} {
			return [1]uintptr{}
		},
	}
	logging.vmoduleConfig.setVState(0, nil, false)
}

// SetVModule alters the vmodule logging level to the passed in value.
func SetVModule(value string) error {
	return logging.vmoduleConfig.mu.vmodule.Set(value)
}

// GetVModule returns the current vmodule configuration.
func GetVModule() string {
	return logging.vmoduleConfig.mu.vmodule.String()
}

// VDepth reports whether verbosity at the call site is at least the requested
// level.
func VDepth(l Level, depth int) bool {
	return logging.vmoduleConfig.vDepth(l, depth+1)
}

func (c *vmoduleConfig) vDepth(l Level, depth int) bool {
	// This function tries hard to be cheap unless there's work to do.
	// The fast path is three atomic loads and compares.

	// Here is a cheap but safe test to see if V logging is enabled globally.
	if c.verbosity.get() >= l {
		return true
	}

	// It's off globally but vmodule may still be set.
	// Here is another cheap but safe test to see if vmodule is enabled.
	if atomic.LoadInt32(&c.mu.filterLength) > 0 {
		// Grab a buffer to use for reading the program counter. Keeping the
		// interface{} version around to Put back into the pool rather than
		// Put-ting the array saves an interface allocation.
		poolObj := c.pcsPool.Get()
		pcs := poolObj.([1]uintptr)
		// We prefer not to use a defer in this function, which can be used in hot
		// paths, because a defer anywhere in the body of a function causes a call
		// to runtime.deferreturn at the end of that function, which has a
		// measurable performance penalty when in a very hot path.
		// defer c.pcsPool.Put(pcs)
		if runtime.Callers(2+depth, pcs[:]) == 0 {
			c.pcsPool.Put(poolObj)
			return false
		}
		c.mu.Lock()
		v, ok := c.mu.vmap[pcs[0]]
		if !ok {
			v = c.setV(pcs)
		}
		c.mu.Unlock()
		c.pcsPool.Put(poolObj)
		return v >= l
	}
	return false
}

// setVState sets a consistent state for V logging.
// l.mu is held.
func (c *vmoduleConfig) setVState(verbosity Level, filter []modulePat, setFilter bool) {
	// Turn verbosity off so V will not fire while we are in transition.
	c.verbosity.set(0)
	// Ditto for filter length.
	atomic.StoreInt32(&c.mu.filterLength, 0)

	// Set the new filters and wipe the pc->Level map if the filter has changed.
	if setFilter {
		c.mu.vmodule.filter = filter
		c.mu.vmap = make(map[uintptr]Level)
	}

	// Things are consistent now, so enable filtering and verbosity.
	// They are enabled in order opposite to that in V.
	atomic.StoreInt32(&c.mu.filterLength, int32(len(filter)))
	c.verbosity.set(verbosity)
}

// setV computes and remembers the V level for a given PC
// when vmodule is enabled.
// File pattern matching takes the basename of the file, stripped
// of its .go suffix, and uses filepath.Match, which is a little more
// general than the *? matching used in C++.
//
// c.mu is held.
func (c *vmoduleConfig) setV(pc [1]uintptr) Level {
	frame, _ := runtime.CallersFrames(pc[:]).Next()
	file := frame.File
	// The file is something like /a/b/c/d.go. We want just the d.
	if strings.HasSuffix(file, ".go") {
		file = file[:len(file)-3]
	}
	if slash := strings.LastIndexByte(file, '/'); slash >= 0 {
		file = file[slash+1:]
	}
	for _, filter := range c.mu.vmodule.filter {
		if filter.match(file) {
			c.mu.vmap[pc[0]] = filter.level
			return filter.level
		}
	}
	c.mu.vmap[pc[0]] = 0
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
	level   Level
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
	logging.vmoduleConfig.mu.Lock()
	defer logging.vmoduleConfig.mu.Unlock()
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
		filter = append(filter, modulePat{pattern, isLiteral(pattern), Level(v)})
	}

	logging.vmoduleConfig.mu.Lock()
	defer logging.vmoduleConfig.mu.Unlock()
	logging.vmoduleConfig.setVState(logging.vmoduleConfig.verbosity, filter, true)
	return nil
}

// isLiteral reports whether the pattern is a literal string, that is, has no metacharacters
// that require filepath.Match to be called to match the pattern.
func isLiteral(pattern string) bool {
	return !strings.ContainsAny(pattern, `\*?[]`)
}

// Level specifies a level of verbosity for V logs. *Level implements
// flag.Value; the --verbosity flag is of type Level and should be modified
// only through the flag.Value interface.
//
// Level is exported because it appears in the arguments to V and is
// the type of the v flag, which can be set programmatically.
// It's a distinct type because we want to discriminate it from logType.
// Variables of type level are only changed under loggerT.mu.
// The --verbosity flag is read only with atomic ops, so the state of the logging
// module is consistent.
//
// Level is treated as a sync/atomic int32.
type Level int32

// get returns the value of the Level.
func (l *Level) get() Level {
	return Level(atomic.LoadInt32((*int32)(l)))
}

// set sets the value of the Level.
func (l *Level) set(val Level) {
	atomic.StoreInt32((*int32)(l), int32(val))
}

// String is part of the flag.Value interface.
func (l *Level) String() string {
	return strconv.FormatInt(int64(*l), 10)
}

// Set is part of the flag.Value interface.
func (l *Level) Set(value string) error {
	v, err := strconv.Atoi(value)
	if err != nil {
		return err
	}
	logging.vmoduleConfig.mu.Lock()
	defer logging.vmoduleConfig.mu.Unlock()
	logging.vmoduleConfig.setVState(Level(v), logging.vmoduleConfig.mu.vmodule.filter, false)
	return nil
}
