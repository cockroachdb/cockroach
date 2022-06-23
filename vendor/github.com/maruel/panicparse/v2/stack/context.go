// Copyright 2018 Marc-Antoine Ruel. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

//go:generate go install golang.org/x/tools/cmd/stringer@latest
//go:generate stringer -type state
//go:generate stringer -type Location

package stack

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"unsafe"
)

// Opts represents options to process the snapshot.
type Opts struct {
	// LocalGOROOT is GOROOT with "/" as path separator. No trailing "/". Can be
	// unset.
	LocalGOROOT string
	// LocalGOPATHs is GOPATH with "/" as path separator. No trailing "/". Can be
	// unset.
	LocalGOPATHs []string

	// NameArguments tells panicparse to find the recurring pointer values and
	// give them pseudo 'names'.
	//
	// Since the algorithm is O(n²), this can be worth disabling on live servers.
	NameArguments bool

	// GuessPaths tells panicparse to guess local RemoteGOROOT and GOPATH for
	// what was found in the snapshot.
	//
	// Initializes in Snapshot the following members: RemoteGOROOT,
	// RemoteGOPATHs, LocalGomoduleRoot and GomodImportPath.
	//
	// This is done by scanning the local disk, so be warned of performance
	// impact.
	GuessPaths bool

	// AnalyzeSources tells panicparse to processes source files to improve calls
	// to be more descriptive.
	//
	// Requires GuessPaths to be true.
	AnalyzeSources bool

	// Disallow initialization with unnamed parameters.
	_ struct{}
}

// DefaultOpts returns default options to process the snapshot.
func DefaultOpts() *Opts {
	p := runtime.GOROOT()
	if runtime.GOOS == "windows" {
		p = strings.Replace(p, pathSeparator, "/", -1)
	}
	return &Opts{
		LocalGOROOT:    p,
		LocalGOPATHs:   getGOPATHs(),
		NameArguments:  true,
		GuessPaths:     true,
		AnalyzeSources: true,
	}
}

func (o *Opts) isValid() bool {
	if !o.GuessPaths && o.AnalyzeSources {
		return false
	}
	if strings.Contains(o.LocalGOROOT, "\\") {
		return false
	}
	for _, p := range o.LocalGOPATHs {
		if strings.Contains(p, "\\") {
			return false
		}
	}
	return true
}

// Snapshot is a parsed runtime.Stack() or race detector dump.
type Snapshot struct {
	// Goroutines is the Goroutines found.
	//
	// They are in the order that they were printed.
	Goroutines []*Goroutine

	// LocalGOROOT is copied from Opts.
	LocalGOROOT string
	// LocalGOPATHs is copied from Opts.
	LocalGOPATHs []string

	// The following members are initialized when Opts.GuessPaths is true.

	// RemoteGOROOT is the GOROOT as detected in the traceback, not the on the
	// host.
	//
	// It can be empty if no root was determined, for example the traceback
	// contains only non-stdlib source references.
	RemoteGOROOT string
	// RemoteGOPATHs is the GOPATH as detected in the traceback, with the value
	// being the corresponding path mapped to the host if found.
	//
	// It can be empty if only stdlib code is in the traceback or if no local
	// sources were matched up. In the general case there is only one entry in
	// the map.
	RemoteGOPATHs map[string]string

	// LocalGomods are the root directories containing go.mod or that directly
	// contained source code as detected in the traceback, with the value being
	// the corresponding import path found in the go.mod file.
	//
	// Uses "/" as path separator. No trailing "/".
	//
	// Because of the "replace" statement in go.mod, there can be multiple root
	// directories. A file run by "go run" is also considered a go module to (a
	// certain extent).
	//
	// It is initialized by findRoots().
	//
	// Unlike GOROOT and GOPATH, it only works with stack traces created in the
	// local file system, hence "Local" prefix.
	LocalGomods map[string]string

	// Disallow initialization with unnamed parameters.
	_ struct{}
}

// ScanSnapshot scans the Reader for the output from runtime.Stack() in br.
//
// Returns nil *Snapshot if no stack trace was detected.
//
// If a Snapshot is returned, you can call the function again to find another
// trace, or do io.Copy(br, out) to flush the rest of the stream.
//
// ParseSnapshot processes the output from runtime.Stack() or the race detector.
//
// Returns a nil *Snapshot if no stack trace was detected and SearchSnapshot()
// was a false positive.
//
// Returns io.EOF if all of reader was read.
//
// The suffix of the stack trace is returned as []byte.
//
// It pipes anything not detected as a panic stack trace from r into out. It
// assumes there is junk before the actual stack trace. The junk is streamed to
// out.
func ScanSnapshot(in io.Reader, prefix io.Writer, opts *Opts) (*Snapshot, []byte, error) {
	if opts == nil || !opts.isValid() {
		return nil, nil, errors.New("invalid Opts")
	}
	// TODO(maruel): Validate opts.
	s := scanningState{
		Snapshot: &Snapshot{
			LocalGOROOT:  opts.LocalGOROOT,
			LocalGOPATHs: opts.LocalGOPATHs,
		},
		state: looking,
	}
	r := reader{rd: in}
	var err error
	var suffix []byte
	for err == nil && s.state != done {
		var d []byte
		if d, err = r.readLine(); len(d) != 0 {
			l, err1 := s.scan(d)
			if err1 != nil && (err == nil || err == io.EOF) {
				err = err1
			}
			if !l {
				if s.state != looking {
					suffix = append([]byte{}, d...)
					suffix = append(suffix, r.buffered()...)
					break
				}
				if _, err1 = prefix.Write(d); err1 != nil && (err == nil || err == io.EOF) {
					err = err1
					break
				}
			}
		}
	}
	if s.Goroutines != nil {
		if opts.NameArguments {
			nameArguments(s.Goroutines)
		}
		if opts.GuessPaths {
			_ = s.guessPaths()
		}
		if opts.AnalyzeSources {
			_ = s.augment()
		}
		return s.Snapshot, suffix, err
	}
	return nil, suffix, err
}

// IsRace returns true if a race detector stack trace was found.
//
// Otherwise, it is a normal goroutines snapshot.
//
// When a race condition was detected, it is preferable to not call Aggregate().
func (s *Snapshot) IsRace() bool {
	return s.Goroutines[0].RaceAddr != 0
}

func (s *Snapshot) guessPaths() bool {
	b := s.findRoots() == 0
	for _, r := range s.Goroutines {
		// Note that this is important to call it even if
		// s.RemoteGOROOT == s.LocalGOROOT.
		b = r.updateLocations(s.RemoteGOROOT, s.LocalGOROOT, s.LocalGomods, s.RemoteGOPATHs) && b
	}
	return b
}

// augment processes source files to improve calls to be more descriptive.
//
// It modifies goroutines in place. It requires calling guessPaths() to work
// properly.
//
// Returns the last error that occurred while processing files.
func (s *Snapshot) augment() error {
	c := cacheAST{
		files:  map[string][]byte{},
		parsed: map[string]*parsedFile{},
	}
	var err error
	for _, g := range s.Goroutines {
		if err1 := c.augmentGoroutine(g); err1 != nil {
			err = err1
		}
	}
	return err
}

// Private stuff.

const pathSeparator = string(filepath.Separator)

var (
	lockedToThread = []byte("locked to thread")
	framesElided   = []byte("...additional frames elided...")
	// gotRaceHeader1, done
	raceHeaderFooter = []byte("==================")
	// gotRaceHeader2
	raceHeader             = []byte("WARNING: DATA RACE")
	crlf                   = []byte("\r\n")
	lf                     = []byte("\n")
	commaSpace             = []byte(", ")
	writeCap               = []byte("Write")
	writeLow               = []byte("write")
	threeDots              = []byte("...")
	underscore             = []byte("_")
	inaccurateQuestionMark = []byte("?")
)

// These are effectively constants.
var (
	// gotRoutineHeader
	reRoutineHeader = regexp.MustCompile("^([ \t]*)goroutine (\\d+) \\[([^\\]]+)\\]\\:$")
	reMinutes       = regexp.MustCompile(`^(\d+) minutes$`)

	// gotUnavail
	reUnavail = regexp.MustCompile("^(?:\t| +)goroutine running on other thread; stack unavailable")

	// gotFileFunc, gotRaceOperationFile, gotRaceGoroutineFile
	// See gentraceback() in src/runtime/traceback.go for more information.
	// - Sometimes the source file comes up as "<autogenerated>". It is the
	//   compiler than generated these, not the runtime.
	// - The tab may be replaced with spaces when a user copy-paste it, handle
	//   this transparently.
	// - "runtime.gopanic" is explicitly replaced with "panic" by gentraceback().
	// - The +0x123 byte offset is printed when frame.pc > _func.entry. _func is
	//   generated by the linker.
	// - The +0x123 byte offset is not included with generated code, e.g. unnamed
	//   functions "func·006()" which is generally go func() { ... }()
	//   statements. Since the _func is generated at runtime, it's probably why
	//   _func.entry is not set.
	// - C calls may have fp=0x123 sp=0x123 appended. I think it normally happens
	//   when a signal is not correctly handled. It is printed with m.throwing>0.
	//   These are discarded.
	// - For cgo, the source file may be "??".
	reFile = regexp.MustCompile("^(?:\t| +)(\\?\\?|\\<autogenerated\\>|.+\\.(?:c|go|s))\\:(\\d+)(?:| \\+0x[0-9a-f]+)(?:| fp=0x[0-9a-f]+ sp=0x[0-9a-f]+(?:| pc=0x[0-9a-f]+))$")

	// gotCreated
	// Sadly, it doesn't note the goroutine number so we could cascade them per
	// parenthood.
	reCreated = regexp.MustCompile("^created by (.+)$")

	// gotFunc, gotRaceOperationFunc, gotRaceGoroutineFunc
	reFunc = regexp.MustCompile(`^(.+)\((.*)\)$`)

	// Race:
	// See https://github.com/llvm/llvm-project/blob/HEAD/compiler-rt/lib/tsan/rtl/tsan_report.cpp
	// for the code generating these messages. Please note only the block in
	//   #else  // #if !SANITIZER_GO
	// is used.
	// TODO(maruel): "    [failed to restore the stack]\n\n"
	// TODO(maruel): "Global var %s of size %zu at %p declared at %s:%zu\n"

	// gotRaceOperationHeader
	reRaceOperationHeader = regexp.MustCompile(`^(Read|Write) at (0x[0-9a-f]+) by goroutine (\d+):$`)

	// gotRaceOperationHeader
	reRacePreviousOperationHeader = regexp.MustCompile(`^Previous (read|write) at (0x[0-9a-f]+) by goroutine (\d+):$`)

	// gotRaceGoroutineHeader
	reRaceGoroutine = regexp.MustCompile(`^Goroutine (\d+) \((running|finished)\) created at:$`)

	// TODO(maruel): Use it.
	//reRacePreviousOperationMainHeader = regexp.MustCompile("^Previous (read|write) at (0x[0-9a-f]+) by main goroutine:$")
)

// state is the state of the scan to detect and process a stack trace.
type state int

// Initial state is looking. Other states are when a stack trace is detected.
const (
	// Haven't found a stack trace yet.
	// to: gotRoutineHeader, raceHeader1
	looking state = iota

	// Done processing a stack trace.
	done

	// Panic stack trace:

	// Signature: ""
	// An empty line between goroutines.
	// from: gotFileCreated, gotFileFunc
	// to: gotRoutineHeader, done
	betweenRoutine
	// Regexp: reRoutineHeader
	// Signature: "goroutine 1 [running]:"
	// Goroutine header was found.
	// from: looking
	// to: gotUnavail, gotFunc
	gotRoutineHeader
	// Regexp: reFunc
	// Signature: "main.main()"
	// Function call line was found.
	// from: gotRoutineHeader
	// to: gotFileFunc
	gotFunc
	// Regexp: reCreated
	// Signature: "created by main.glob..func4"
	// Goroutine creation line was found.
	// from: gotFileFunc
	// to: gotFileCreated
	gotCreated
	// Regexp: reFile
	// Signature: "\t/foo/bar/baz.go:116 +0x35"
	// File header was found.
	// from: gotFunc
	// to: gotFunc, gotCreated, betweenRoutine, done
	gotFileFunc
	// Regexp: reFile
	// Signature: "\t/foo/bar/baz.go:116 +0x35"
	// File header was found.
	// from: gotCreated
	// to: betweenRoutine, done
	gotFileCreated
	// Regexp: reUnavail
	// Signature: "goroutine running on other thread; stack unavailable"
	// State when the goroutine stack is instead is reUnavail.
	// from: gotRoutineHeader
	// to: betweenRoutine, gotCreated
	gotUnavail

	// Race detector:

	// Constant: raceHeaderFooter
	// Signature: "=================="
	// from: looking
	// to: done, gotRaceHeader2
	gotRaceHeader1
	// Constant: raceHeader
	// Signature: "WARNING: DATA RACE"
	// from: gotRaceHeader1
	// to: done, gotRaceOperationHeader
	gotRaceHeader2
	// Regexp: reRaceOperationHeader, reRacePreviousOperationHeader
	// Signature: "Read at 0x00c0000e4030 by goroutine 7:"
	// A race operation was found.
	// from: gotRaceHeader2
	// to: done, gotRaceOperationFunc
	gotRaceOperationHeader
	// Regexp: reFunc
	// Signature: "  main.panicRace.func1()"
	// Function that caused the race.
	// from: gotRaceOperationHeader
	// to: done, gotRaceOperationFile
	gotRaceOperationFunc
	// Regexp: reFile
	// Signature: "\t/foo/bar/baz.go:116 +0x35"
	// File header that caused the race.
	// from: gotRaceOperationFunc
	// to: done, betweenRaceOperations, gotRaceOperationFunc
	gotRaceOperationFile
	// Signature: ""
	// Empty line between race operations or just after.
	// from: gotRaceOperationFile
	// to: done, gotRaceOperationHeader, gotRaceGoroutineHeader
	betweenRaceOperations

	// Regexp: reRaceGoroutine
	// Signature: "Goroutine 7 (running) created at:"
	// Goroutine header.
	// from: betweenRaceOperations, betweenRaceGoroutines
	// to: done, gotRaceOperationHeader
	gotRaceGoroutineHeader
	// Regexp: reFunc
	// Signature: "  main.panicRace.func1()"
	// Function that caused the race.
	// from: gotRaceGoroutineHeader
	// to: done, gotRaceGoroutineFile
	gotRaceGoroutineFunc
	// Regexp: reFile
	// Signature: "\t/foo/bar/baz.go:116 +0x35"
	// File header that caused the race.
	// from: gotRaceGoroutineFunc
	// to: done, betweenRaceGoroutines
	gotRaceGoroutineFile
	// Signature: ""
	// Empty line between race stack traces.
	// from: gotRaceGoroutineFile
	// to: done, gotRaceGoroutineHeader
	betweenRaceGoroutines
)

// scanningState is the state of the scan to detect and process a stack trace
// and stores the traces found.
type scanningState struct {
	*Snapshot
	state          state
	prefix         []byte
	goroutineIndex int
}

// scan scans one line, updates goroutines and move to the next state.
//
// Returns true if the line was processed and thus should not be printed out.
//
// TODO(maruel): Handle corrupted stack cases:
// - missed stack barrier
// - found next stack barrier at 0x123; expected
// - runtime: unexpected return pc for FUNC_NAME called from 0x123
func (s *scanningState) scan(line []byte) (bool, error) {
	/* This is very useful to debug issues in the state machine.
	defer func() {
		log.Printf("scan(%q) -> %s", line, s.state)
	}()
	//*/
	var cur *Goroutine
	if len(s.Goroutines) != 0 {
		cur = s.Goroutines[len(s.Goroutines)-1]
	}
	trimmed := line
	if bytes.HasSuffix(line, crlf) {
		trimmed = line[:len(line)-2]
	} else if bytes.HasSuffix(line, lf) {
		trimmed = line[:len(line)-1]
	} else {
		// It's the end of the stream and it's not terminating with EOL character.
		if s.state == looking || s.state == done {
			return false, nil
		}
		// Let it flow. It's possible the last line was trimmed and we still want
		// to parse it.
	}

	if len(trimmed) != 0 && len(s.prefix) != 0 {
		// This can only be the case if s.state != looking | done or the line is
		// empty.
		if !bytes.HasPrefix(trimmed, s.prefix) {
			prefix := s.prefix
			s.state = done
			s.prefix = nil
			return false, fmt.Errorf("inconsistent indentation: %q, expected %q", trimmed, prefix)
		}
		trimmed = trimmed[len(s.prefix):]
	}

	switch s.state {
	case done:
		return false, nil

	case looking:
		// We could look for '^panic:' but this is more risky, there can be a lot
		// of junk between this and the stack dump.
		fallthrough

	case betweenRoutine:
		// Look for a goroutine header.
		if match := reRoutineHeader.FindSubmatch(trimmed); match != nil {
			if id, ok := atou(match[2]); ok {
				// See runtime/traceback.go.
				// "<state>, \d+ minutes, locked to thread"
				items := bytes.Split(match[3], commaSpace)
				sleep := 0
				locked := false
				for i := 1; i < len(items); i++ {
					if bytes.Equal(items[i], lockedToThread) {
						locked = true
						continue
					}
					// Look for duration, if any.
					if match2 := reMinutes.FindSubmatch(items[i]); match2 != nil {
						sleep, _ = atou(match2[1])
					}
				}
				g := &Goroutine{
					Signature: Signature{
						State:    string(items[0]),
						SleepMin: sleep,
						SleepMax: sleep,
						Locked:   locked,
					},
					ID:    id,
					First: len(s.Goroutines) == 0,
				}
				// Increase performance by always allocating 4 goroutines minimally.
				if s.Goroutines == nil {
					s.Goroutines = make([]*Goroutine, 0, 4)
				}
				s.Goroutines = append(s.Goroutines, g)
				s.state = gotRoutineHeader
				s.prefix = append([]byte{}, match[1]...)
				return true, nil
			}
		}
		// Switch to race detection mode.
		if bytes.Equal(trimmed, raceHeaderFooter) {
			// TODO(maruel): We should buffer it in case the next line is not a
			// WARNING so we can output it back.
			s.state = gotRaceHeader1
			return true, nil
		}
		if s.state != looking {
			s.state = done
		}
		return false, nil

	case gotRoutineHeader:
		if reUnavail.Match(trimmed) {
			// Generate a fake stack entry.
			cur.Stack.Calls = []Call{{RemoteSrcPath: "<unavailable>"}}
			// Next line is expected to be an empty line.
			s.state = gotUnavail
			return true, nil
		}
		c := Call{}
		if found, err := parseFunc(&c, trimmed); found {
			// Increase performance by always allocating 4 calls minimally.
			if cur.Stack.Calls == nil {
				cur.Stack.Calls = make([]Call, 0, 4)
			}
			cur.Stack.Calls = append(cur.Stack.Calls, c)
			s.state = gotFunc
			return err == nil, err
		}
		return false, fmt.Errorf("expected a function after a goroutine header, got: %q", bytes.TrimSpace(trimmed))

	case gotFunc:
		// cur.Stack.Calls is guaranteed to have at least one item.
		if found, err := parseFile(&cur.Stack.Calls[len(cur.Stack.Calls)-1], trimmed); err != nil {
			return false, err
		} else if !found {
			return false, fmt.Errorf("expected a file after a function, got: %q", bytes.TrimSpace(trimmed))
		}
		s.state = gotFileFunc
		return true, nil

	case gotCreated:
		if found, err := parseFile(&cur.CreatedBy.Calls[0], trimmed); err != nil {
			return false, err
		} else if !found {
			return false, fmt.Errorf("expected a file after a created line, got: %q", trimmed)
		}
		s.state = gotFileCreated
		return true, nil

	case gotFileFunc:
		if match := reCreated.FindSubmatch(trimmed); match != nil {
			cur.CreatedBy.Calls = make([]Call, 1)
			if err := cur.CreatedBy.Calls[0].Func.Init(string(match[1])); err != nil {
				cur.CreatedBy.Calls = nil
				return false, err
			}
			// This initializes ImportPath.
			cur.CreatedBy.Calls[0].init("", 0)
			s.state = gotCreated
			return true, nil
		}
		if bytes.Equal(trimmed, framesElided) {
			cur.Stack.Elided = true
			// TODO(maruel): New state.
			return true, nil
		}
		c := Call{}
		if found, err := parseFunc(&c, trimmed); found {
			// Increase performance by always allocating 4 calls minimally.
			if cur.Stack.Calls == nil {
				cur.Stack.Calls = make([]Call, 0, 4)
			}
			cur.Stack.Calls = append(cur.Stack.Calls, c)
			s.state = gotFunc
			return err == nil, err
		}
		if len(trimmed) == 0 {
			s.state = betweenRoutine
			return true, nil
		}
		s.state = done
		return false, nil

	case gotFileCreated:
		if len(trimmed) == 0 {
			s.state = betweenRoutine
			return true, nil
		}
		s.state = done
		return false, nil

	case gotUnavail:
		if len(trimmed) == 0 {
			s.state = betweenRoutine
			return true, nil
		}
		if match := reCreated.FindSubmatch(trimmed); match != nil {
			cur.CreatedBy.Calls = make([]Call, 1)
			if err := cur.CreatedBy.Calls[0].Func.Init(string(match[1])); err != nil {
				cur.CreatedBy.Calls = nil
				return false, err
			}
			s.state = gotCreated
			return true, nil
		}
		return false, fmt.Errorf("expected empty line after unavailable stack, got: %q", bytes.TrimSpace(trimmed))

		// Race detector.

	case gotRaceHeader1:
		if bytes.Equal(trimmed, raceHeader) {
			// TODO(maruel): We should buffer it in case the next line is not a
			// WARNING so we can output it back.
			s.state = gotRaceHeader2
			return true, nil
		}
		// TODO(maruel): While this shouldn't error out, it should still force the
		// output of raceHeaderFooter.
		s.state = looking
		s.prefix = nil
		return false, nil

	case gotRaceHeader2:
		if match := reRaceOperationHeader.FindSubmatch(trimmed); match != nil {
			w := bytes.Equal(match[1], writeCap)
			addr, err := strconv.ParseUint(unsafeString(match[2]), 0, 64)
			if err != nil {
				return false, fmt.Errorf("failed to parse address on line: %q", bytes.TrimSpace(trimmed))
			}
			id, ok := atou(match[3])
			if !ok {
				return false, fmt.Errorf("failed to parse goroutine id on line: %q", bytes.TrimSpace(trimmed))
			}
			if s.Goroutines != nil {
				panic("internal failure; expected s.Goroutines to be nil")
			}
			s.Goroutines = append(make([]*Goroutine, 0, 4), &Goroutine{ID: id, First: true, RaceWrite: w, RaceAddr: addr})
			s.goroutineIndex = len(s.Goroutines) - 1
			s.state = gotRaceOperationHeader
			return true, nil
		}
		return false, fmt.Errorf("expected race condition, got: %q", bytes.TrimSpace(trimmed))

	case gotRaceOperationHeader:
		c := Call{}
		if found, err := parseFunc(&c, trimLeftSpace(trimmed)); found {
			// Increase performance by always allocating 4 calls minimally.
			if cur.Stack.Calls == nil {
				cur.Stack.Calls = make([]Call, 0, 4)
			}
			cur.Stack.Calls = append(cur.Stack.Calls, c)
			s.state = gotRaceOperationFunc
			return err == nil, err
		}
		return false, fmt.Errorf("expected a function after a race operation, got: %q", trimmed)

	case gotRaceOperationFunc:
		if found, err := parseFile(&cur.Stack.Calls[len(cur.Stack.Calls)-1], trimmed); err != nil {
			return false, err
		} else if !found {
			return false, fmt.Errorf("expected a file after a race function, got: %q", trimmed)
		}
		s.state = gotRaceOperationFile
		return true, nil

	case gotRaceOperationFile:
		if len(trimmed) == 0 {
			s.state = betweenRaceOperations
			return true, nil
		}
		c := Call{}
		if found, err := parseFunc(&c, trimLeftSpace(trimmed)); found {
			cur.Stack.Calls = append(cur.Stack.Calls, c)
			s.state = gotRaceOperationFunc
			return err == nil, err
		}
		return false, fmt.Errorf("expected an empty line after a race file, got: %q", trimmed)

	case betweenRaceOperations:
		// Look for other previous race data operations.
		if match := reRacePreviousOperationHeader.FindSubmatch(trimmed); match != nil {
			w := bytes.Equal(match[1], writeLow)
			addr, err := strconv.ParseUint(unsafeString(match[2]), 0, 64)
			if err != nil {
				return false, fmt.Errorf("failed to parse address on line: %q", bytes.TrimSpace(trimmed))
			}
			id, ok := atou(match[3])
			if !ok {
				return false, fmt.Errorf("failed to parse goroutine id on line: %q", bytes.TrimSpace(trimmed))
			}
			s.Goroutines = append(s.Goroutines, &Goroutine{ID: id, RaceWrite: w, RaceAddr: addr})
			s.goroutineIndex = len(s.Goroutines) - 1
			s.state = gotRaceOperationHeader
			return true, nil
		}
		fallthrough

	case betweenRaceGoroutines:
		if match := reRaceGoroutine.FindSubmatch(trimmed); match != nil {
			id, ok := atou(match[1])
			if !ok {
				return false, fmt.Errorf("failed to parse goroutine id on line: %q", bytes.TrimSpace(trimmed))
			}
			found := false
			for i, g := range s.Goroutines {
				if g.ID == id {
					g.State = string(match[2])
					s.goroutineIndex = i
					found = true
					break
				}
			}
			if !found {
				return false, fmt.Errorf("unexpected goroutine ID on line: %q", bytes.TrimSpace(trimmed))
			}
			s.state = gotRaceGoroutineHeader
			return true, nil
		}
		return false, fmt.Errorf("expected an operator or goroutine, got: %q", trimmed)

		// Race stack traces

	case gotRaceGoroutineFunc:
		c := s.Goroutines[s.goroutineIndex].CreatedBy.Calls
		if found, err := parseFile(&c[len(c)-1], trimmed); err != nil {
			return false, err
		} else if !found {
			return false, fmt.Errorf("expected a file after a race function, got: %q", trimmed)
		}
		// TODO(maruel): Set s.Goroutines[].CreatedBy.
		s.state = gotRaceGoroutineFile
		return true, nil

	case gotRaceGoroutineFile:
		if len(trimmed) == 0 {
			s.state = betweenRaceGoroutines
			return true, nil
		}
		if bytes.Equal(trimmed, raceHeaderFooter) {
			s.state = done
			return true, nil
		}
		fallthrough

	case gotRaceGoroutineHeader:
		c := Call{}
		if found, err := parseFunc(&c, trimLeftSpace(trimmed)); found {
			s.Goroutines[s.goroutineIndex].CreatedBy.Calls = append(s.Goroutines[s.goroutineIndex].CreatedBy.Calls, c)
			s.state = gotRaceGoroutineFunc
			return err == nil, err
		}
		return false, fmt.Errorf("expected a function after a race operation or a race file, got: %q", trimmed)

	default:
		return false, errors.New("internal error")
	}
}

// parseFunc only return an error if it also returns true.
//
// Uses reFunc.
func parseFunc(c *Call, line []byte) (bool, error) {
	if match := reFunc.FindSubmatch(line); match != nil {
		if err := c.Func.Init(string(match[1])); err != nil {
			return true, err
		}
		// It is also done in c.init() but do it here in case of a corrupted trace
		// for the file section.
		c.ImportPath = c.Func.ImportPath

		args, err := parseArgs(match[2])
		if err != nil {
			return true, fmt.Errorf("%s on line: %q", err, bytes.TrimSpace(line))
		}
		c.Args = args
		return true, nil
	}
	return false, nil
}

// parseArgs parses a collection of comma-separated arguments into an Args
// struct.
func parseArgs(line []byte) (Args, error) {
	const maxDepth = 6 // 5 from traceback.go, +1 for top level
	var stack [maxDepth]*Args
	var args Args
	depth := 0
	stack[depth] = &args
	for _, s := range bytes.Split(line, commaSpace) {
		opened, a, closed := trimCurlyBrackets(s)
		for i := 0; i < opened; i++ {
			cur := stack[depth]
			cur.Values = append(cur.Values, Arg{})
			next := &cur.Values[len(cur.Values)-1]
			next.IsAggregate = true
			depth++
			if depth >= maxDepth {
				return Args{}, fmt.Errorf("nested aggregate-typed arguments exceeded depth limit")
			}
			stack[depth] = &next.Fields
		}
		if len(a) > 0 {
			cur := stack[depth]
			switch {
			case bytes.Equal(a, threeDots):
				cur.Elided = true
			case bytes.Equal(a, underscore):
				arg := Arg{IsOffsetTooLarge: true}
				cur.Values = append(cur.Values, arg)
			default:
				inaccurate := bytes.HasSuffix(a, inaccurateQuestionMark)
				if inaccurate {
					a = a[:len(a)-len(inaccurateQuestionMark)]
				}

				v, err := strconv.ParseUint(unsafeString(a), 0, 64)
				if err != nil {
					return Args{}, errors.New("failed to parse int")
				}
				// Assume the stack was generated with the same bitness (32 vs 64) as
				// the code processing it.
				arg := Arg{Value: v, IsPtr: v > pointerFloor && v < pointerCeiling, IsInaccurate: inaccurate}
				cur.Values = append(cur.Values, arg)
			}
		}
		for i := 0; i < closed; i++ {
			stack[depth] = nil
			depth--
			if depth < 0 {
				return Args{}, errors.New("unmatched closing curly bracket")
			}
		}
	}
	if depth != 0 {
		return Args{}, errors.New("unmatched opening curly bracket")
	}
	return args, nil
}

// parseFile only return an error if also processing a Call.
//
// Uses reFile.
func parseFile(c *Call, line []byte) (bool, error) {
	if match := reFile.FindSubmatch(line); match != nil {
		num, ok := atou(match[2])
		if !ok {
			return true, fmt.Errorf("failed to parse int on line: %q", bytes.TrimSpace(line))
		}
		c.init(string(match[1]), num)
		return true, nil
	}
	return false, nil
}

// hasPrefix returns true if any of s is the prefix of p.
func hasPrefix(p string, s map[string]string) bool {
	lp := len(p)
	for prefix := range s {
		if l := len(prefix); lp > l+1 && p[:l] == prefix && p[l] == '/' {
			return true
		}
	}
	return false
}

// hasSrcPrefix returns true if any of s is the prefix of p with /src/ or
// /pkg/mod/.
func hasSrcPrefix(p string, s map[string]string) bool {
	lp := len(p)
	const src = "/src/"
	const pkgmod = "/pkg/mod/"
	for prefix := range s {
		l := len(prefix)
		if lp > l+len(src) && p[:l] == prefix && p[l:l+len(src)] == src {
			return true
		}
		if lp > l+len(pkgmod) && p[:l] == prefix && p[l:l+len(pkgmod)] == pkgmod {
			return true
		}
	}
	return false
}

// getFiles returns all the source files deduped and ordered.
func getFiles(goroutines []*Goroutine) []string {
	files := map[string]struct{}{}
	for _, g := range goroutines {
		for _, c := range g.Stack.Calls {
			files[c.RemoteSrcPath] = struct{}{}
		}
	}
	if len(files) == 0 {
		return nil
	}
	out := make([]string, 0, len(files))
	for f := range files {
		out = append(out, f)
	}
	sort.Strings(out)
	return out
}

// splitPath splits a path using "/" as separator into its components.
//
// The first item has its initial path separator kept.
func splitPath(p string) []string {
	if p == "" {
		return nil
	}
	var out []string
	s := ""
	for _, c := range p {
		if c != '/' || (len(out) == 0 && strings.Count(s, "/") == len(s)) {
			s += string(c)
		} else if s != "" {
			out = append(out, s)
			s = ""
		}
	}
	if s != "" {
		out = append(out, s)
	}
	return out
}

// isFile returns true if the path is a valid file.
func isFile(p string) bool {
	// TODO(maruel): Is it faster to open the file or to stat it? Worth a perf
	// test on Windows.
	i, err := os.Stat(p)
	return err == nil && !i.IsDir()
}

// isRootedIn returns a root if the file split in parts exists under root.
//
// Uses "/" as path separator.
func isRootedIn(root string, parts []string) string {
	for i := 1; i < len(parts); i++ {
		suffix := pathJoin(parts[i:]...)
		if isFile(pathJoin(root, suffix)) {
			return pathJoin(parts[:i]...)
		}
	}
	return ""
}

// reModule find the module line in a go.mod file. It works even on CRLF file.
var reModule = regexp.MustCompile(`(?m)^module\s+([^\n\r]+)\r?$`)

type gomodCache map[string]struct{}

// isGoModule returns the string to the directory containing a go.mod file, and
// the go import path it represents, if found.
func (g *gomodCache) isGoModule(parts []string) (string, string) {
	for i := len(parts); i > 0; i-- {
		prefix := pathJoin(parts[:i]...)
		// Was already looked up.
		if _, ok := (*g)[prefix]; ok {
			break
		}
		(*g)[prefix] = struct{}{}
		p := pathJoin(prefix, "go.mod")
		if runtime.GOOS == "windows" {
			p = strings.Replace(p, "/", pathSeparator, -1)
		}
		b, err := ioutil.ReadFile(p)
		if err != nil {
			continue
		}
		if match := reModule.FindSubmatch(b); match != nil {
			return prefix, string(match[1])
		}
	}
	return "", ""
}

// findRoots sets member RemoteGOROOT, RemoteGOPATHs and LocalGomods.
//
// This causes disk I/O as it checks for file presence.
//
// Returns the number of missing files.
func (s *Snapshot) findRoots() int {
	// TODO(maruel): Reduce memory allocations in this function.
	s.RemoteGOPATHs = map[string]string{}
	s.LocalGomods = map[string]string{}
	missing := 0
	gmc := gomodCache{}
	for _, f := range getFiles(s.Goroutines) {
		// TODO(maruel): Could a stack dump have mixed cases? I think it's
		// possible, need to confirm and handle.
		//log.Printf("  Analyzing %s", f)

		// First checks skip file I/O.
		if s.RemoteGOROOT != "" && strings.HasPrefix(f, s.RemoteGOROOT+"/src/") {
			// stdlib.
			continue
		}
		if hasSrcPrefix(f, s.RemoteGOPATHs) {
			// $GOPATH/src or go.mod dependency in $GOPATH/pkg/mod.
			continue
		}
		if hasPrefix(f, s.LocalGomods) {
			continue
		}

		// At this point, disk will be looked up.
		parts := splitPath(f)
		// Initializes RemoteGOROOT.
		const src = "/src"
		if s.RemoteGOROOT == "" {
			if r := isRootedIn(s.LocalGOROOT+src, parts); r != "" {
				s.RemoteGOROOT = r[:len(r)-len(src)]
				//log.Printf("Found RemoteGOROOT=%s", s.RemoteGOROOT)
				continue
			}
		}
		// Initializes RemoteGOPATHs.
		found := false
		for _, l := range s.LocalGOPATHs {
			if r := isRootedIn(l+src, parts); r != "" {
				//log.Printf("Found RemoteGOPATHs[%s] = %s", r[:len(r)-len(src)], l)
				s.RemoteGOPATHs[r[:len(r)-len(src)]] = l
				found = true
				break
			}
			const pkgmod = "/pkg/mod"
			if r := isRootedIn(l+pkgmod, parts); r != "" {
				//log.Printf("Found RemoteGOPATHs[%s] = %s", r[:len(r)-len(pkgmod)], l)
				s.RemoteGOPATHs[r[:len(r)-len(pkgmod)]] = l
				found = true
				break
			}
		}
		if found {
			continue
		}
		// Initializes localGomods.
		if len(parts) > 1 {
			// Search upward looking for a go.mod.
			if root, path := gmc.isGoModule(parts[:len(parts)-1]); root != "" {
				s.LocalGomods[root] = path
				continue
			}
		}
		if isFile(f) {
			// Assumes "go run" was used, thus is package main. Still consider it a
			// "go module" but in the weakest sense.
			s.LocalGomods[path.Dir(f)] = "main"
			continue
		}
		// If the source is not found, just too bad.
		//log.Printf("Failed to find locally: %s", f)
		missing++
	}
	return missing
}

// getGOPATHs returns parsed GOPATH or its default, using "/" as path separator.
func getGOPATHs() []string {
	var out []string
	if gp := os.Getenv("GOPATH"); gp != "" {
		for _, v := range filepath.SplitList(gp) {
			// Disallow non-absolute paths?
			if v != "" {
				if runtime.GOOS == "windows" {
					v = strings.Replace(v, pathSeparator, "/", -1)
				}
				// Trim trailing "/".
				if l := len(v); v[l-1] == '/' {
					v = v[:l-1]
				}
				out = append(out, v)
			}
		}
	}
	if len(out) == 0 {
		homeDir := ""
		u, err := user.Current()
		if err != nil {
			homeDir = os.Getenv("HOME")
			if homeDir == "" {
				panic(fmt.Sprintf("Could not get current user or $HOME: %s\n", err.Error()))
			}
		} else {
			homeDir = u.HomeDir
		}
		p := homeDir + "/go"
		if runtime.GOOS == "windows" {
			p = strings.Replace(p, pathSeparator, "/", -1)
		}
		out = []string{p}
	}
	return out
}

// atou is a fast Atoi() function.
//
// It is a very simplified version of strconv.Atoi() that it never go into the
// slow path and it operates on []byte instead of string so it doesn't do
// memory allocation. It will fail on edge cases like prefix of zeros and other
// things that the panic stack trace generator never outputs.
//
// It doesn't handle negative values.
func atou(s []byte) (int, bool) {
	if l := len(s); strconv.IntSize == 32 && (0 < l && l < 10) || strconv.IntSize == 64 && (0 < l && l < 19) {
		n := 0
		for _, ch := range s {
			if ch -= '0'; ch > 9 {
				return 0, false
			}
			n = n*10 + int(ch)
		}
		return n, true
	}
	return 0, false
}

// trimLeftSpace is the faster equivalent of bytes.TrimLeft(s, "\t ").
func trimLeftSpace(s []byte) []byte {
	for i, ch := range s {
		if ch != '\t' && ch != ' ' {
			return s[i:]
		}
	}
	return nil
}

// trimCurlyBrackets is the faster equivalent of
// bytes.TrimRight(bytes.TrimLeft(s, "{"), "}"). The function
// also returns the number of curly brackets trimmed from the
// left and the right.
func trimCurlyBrackets(s []byte) (int, []byte, int) {
	i, j := 0, len(s)
	for ; i < j; i++ {
		if s[i] != '{' {
			break
		}
	}
	for ; i < j; j-- {
		if s[j-1] != '}' {
			break
		}
	}
	return i, s[i:j], len(s) - j
}

// unsafeString performs an unsafe conversion from a []byte to a string. The
// returned string will share the underlying memory with the []byte which thus
// allows the string to be mutable through the []byte. We're careful to use
// this method only in situations in which the []byte will not be modified.
//
// A workaround for the absence of https://github.com/golang/go/issues/2632.
func unsafeString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
