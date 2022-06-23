// Copyright 2015 Marc-Antoine Ruel. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

// Package stack analyzes stack dump of Go processes and simplifies it.
//
// It is mostly useful on servers will large number of identical goroutines,
// making the crash dump harder to read than strictly necessary.
package stack

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Func is a function call in a goroutine stack trace.
type Func struct {
	// Complete is the complete reference. It can be ambiguous in case where a
	// path contains dots.
	Complete string
	// ImportPath is the directory name for this function reference, or "main" if
	// it was in package main. The package name may not match.
	ImportPath string
	// DirName is the directory name containing the package in which the function
	// is. Normally this matches the package name, but sometimes there's smartass
	// folks that use a different directory name than the package name.
	DirName string
	// Name is the function name or fully quality method name.
	Name string
	// IsExported is true if the function is exported.
	IsExported bool
	// IsPkgMain is true if it is in the main package.
	IsPkgMain bool

	// Disallow initialization with unnamed parameters.
	_ struct{}
}

// Init parses the raw function call line from a goroutine stack trace.
//
// Go stack traces print a mangled function call, this wrapper unmangle the
// string before printing and adds other filtering methods.
//
// The main caveat is that for calls in package main, the package import URL is
// left out.
func (f *Func) Init(raw string) error {
	// Format can be:
	//  - gopkg.in/yaml%2ev2.(*Struct).Method  (handling dots is tricky)
	//  - main.func·001  (go statements)
	//  - foo  (C code)
	//
	// The function is optimized to reduce its memory usage.
	endPkg := 0
	if lastSlash := strings.LastIndexByte(raw, '/'); lastSlash != -1 {
		// Cut the path elements.
		r := strings.IndexByte(raw[lastSlash+1:], '.')
		if r == -1 {
			return errors.New("bad function reference: expected to have at least one dot")
		}
		endPkg = lastSlash + r + 1
	} else {
		// It's fine if there's no dot, it happens in C code in go1.4 and lower.
		endPkg = strings.IndexByte(raw, '.')
	}
	// Only the path part is escaped.
	var err error
	if f.Complete, err = url.QueryUnescape(raw); err != nil {
		return fmt.Errorf("bad function reference: "+wrap, err)
	}
	// Update the index in the unescaped string.
	endPkg += len(f.Complete) - len(raw)
	if endPkg != -1 {
		f.ImportPath = f.Complete[:endPkg]
	}
	f.Name = f.Complete[endPkg+1:]
	f.DirName = f.ImportPath
	if i := strings.LastIndexByte(f.DirName, '/'); i != -1 {
		f.DirName = f.DirName[i+1:]
	}
	if f.ImportPath == "main" {
		f.IsPkgMain = true
		// Consider main.main to be exported.
		if f.Name == "main" {
			f.IsExported = true
		}
	} else {
		parts := strings.Split(f.Name, ".")
		r, _ := utf8.DecodeRuneInString(parts[len(parts)-1])
		f.IsExported = unicode.ToUpper(r) == r
	}
	return nil
}

// String returns Complete.
func (f *Func) String() string {
	return f.Complete
}

// Arg is an argument on a Call.
type Arg struct {
	// IsAggregate is true if the argument is an aggregate type. If true, the
	// argument does not contain a value itself, but contains a set of nested
	// argument fields. If false, the argument contains a single scalar value.
	IsAggregate bool

	// The following are set if IsAggregate == false.

	// Name is a pseudo name given to the argument.
	Name string
	// Value is the raw value as found in the stack trace
	Value uint64
	// IsPtr is true if we guess it's a pointer. It's only a guess, it can be
	// easily confused by a bitmask.
	IsPtr bool
	// IsOffsetTooLarge is true if the argument's frame offset was too large,
	// preventing the argument from being printed in the stack trace.
	IsOffsetTooLarge bool

	// IsInaccurate determines if Value is inaccurate. Stacks could have inaccurate values
	// for arguments passed in registers. Go 1.18 prints a ? for these values.
	IsInaccurate bool

	// The following are set if IsAggregate == true.

	// Fields are the fields/elements of aggregate-typed arguments.
	Fields Args

	// Disallow initialization with unnamed parameters.
	_ struct{}
}

const zeroToNine = "0123456789"

// String prints the argument as the name if present, otherwise as the value.
func (a *Arg) String() string {
	if a.Name != "" {
		return a.Name
	}
	if a.IsOffsetTooLarge {
		return "_"
	}
	if a.IsAggregate {
		return "{" + a.Fields.String() + "}"
	}
	if a.Value < uint64(len(zeroToNine)) {
		return zeroToNine[a.Value : a.Value+1]
	}
	return fmt.Sprintf("0x%x", a.Value)
}

const (
	// With go1.15 on Windows, the pointer floor can be below 1MiB (!)
	// Assumes all values are above 512KiB and positive are pointers; assuming
	// that above half the memory is kernel memory.
	//
	// This is not always true but this should be good enough to help
	// implementing AnyPointer.
	pointerFloor = 512 * 1024
	// Assume the stack was generated with the same bitness (32 vs 64) than the
	// code processing it.
	pointerCeiling = uint64((^uint(0)) >> 1)
)

// equal returns true only if both arguments are exactly equal.
func (a *Arg) equal(r *Arg) bool {
	return a.similar(r, ExactFlags)
}

// similar returns true if the two Arg are equal or almost but not quite equal.
func (a *Arg) similar(r *Arg, similar Similarity) bool {
	if a.IsAggregate != r.IsAggregate {
		return false
	}
	if a.IsAggregate {
		return a.Fields.similar(&r.Fields, similar)
	}
	switch similar {
	case ExactFlags, ExactLines:
		if a.Name != r.Name {
			return false
		}
		if a.IsOffsetTooLarge != r.IsOffsetTooLarge {
			return false
		}
		if a.IsPtr != r.IsPtr {
			return false
		}
		return a.Value == r.Value
	case AnyValue:
		return true
	case AnyPointer:
		if a.IsOffsetTooLarge != r.IsOffsetTooLarge {
			return false
		}
		if a.IsPtr != r.IsPtr {
			return false
		}
		return a.IsPtr || a.Value == r.Value
	default:
		return false
	}
}

// Args is a series of function call arguments.
type Args struct {
	// Values is the arguments as shown on the stack trace. They are mangled via
	// simplification.
	Values []Arg
	// Processed is the arguments generated from processing the source files. It
	// can have a length lower than Values.
	Processed []string
	// Elided when set means there was a trailing ", ...".
	Elided bool

	// Disallow initialization with unnamed parameters.
	_ struct{}
}

func (a *Args) String() string {
	var v []string
	if len(a.Processed) != 0 {
		v = a.Processed
	} else {
		v = make([]string, 0, len(a.Values))
		for _, item := range a.Values {
			v = append(v, item.String())
		}
	}
	if a.Elided {
		v = append(v, "...")
	}
	return strings.Join(v, ", ")
}

// equal returns true only if both arguments are exactly equal.
func (a *Args) equal(r *Args) bool {
	if a.Elided != r.Elided || len(a.Values) != len(r.Values) {
		return false
	}
	for i, l := range a.Values {
		if !l.equal(&r.Values[i]) {
			return false
		}
	}
	return true
}

// similar returns true if the two Args are equal or almost but not quite
// equal.
func (a *Args) similar(r *Args, similar Similarity) bool {
	if a.Elided != r.Elided || len(a.Values) != len(r.Values) {
		return false
	}
	for i, l := range a.Values {
		if !l.similar(&r.Values[i], similar) {
			return false
		}
	}
	return true
}

// merge merges two similar Args, zapping out differences.
func (a *Args) merge(r *Args) Args {
	out := Args{
		Values: make([]Arg, len(a.Values)),
		Elided: a.Elided,
	}
	for i, l := range a.Values {
		rv := &r.Values[i]
		if l.IsAggregate {
			out.Values[i].IsAggregate = true
			out.Values[i].Fields = l.Fields.merge(&rv.Fields)
		} else if !l.equal(rv) {
			out.Values[i].Name = "*"
			out.Values[i].Value = l.Value
			out.Values[i].IsPtr = l.IsPtr
		} else {
			out.Values[i] = l
		}
	}
	return out
}

// walk traverses all non-aggregate arguments in the Args struct, calling the
// provided visitor function with each Arg.
func (a *Args) walk(visitor func(arg *Arg)) {
	for i := range a.Values {
		arg := &a.Values[i]
		if arg.IsAggregate {
			arg.Fields.walk(visitor)
		} else {
			visitor(arg)
		}
	}
}

// Location is the source location, if determined.
type Location int

const (
	// LocationUnknown is the default value when Opts.GuessPaths was false.
	LocationUnknown Location = iota
	// GoMod is a go module, it is outside $GOPATH and is inside a directory
	// containing a go.mod file. This is considered a local copy.
	GoMod
	// GOPATH is in $GOPATH/src. This is either a dependency fetched via
	// GO111MODULE=off or intentionally fetched this way. There is no guaranteed
	// that the local copy is pristine.
	GOPATH
	// GoPkg is in $GOPATH/pkg/mod. This is a dependency fetched via go module.
	// It is considered to be an unmodified external dependency.
	GoPkg
	// Stdlib is when it is a Go standard library function. This includes the 'go
	// test' generated main executable.
	Stdlib

	lastLocation
)

// Call is an item in the stack trace.
//
// All paths in this struct are in POSIX format, using "/" as path separator.
type Call struct {
	// The following are initialized on the first line of the call stack.

	// Func is the fully qualified function name (encoded).
	Func Func
	// Args is the call arguments.
	Args Args

	// The following are initialized on the second line of the call stack.

	// RemoteSrcPath is the full path name of the source file as seen in the
	// trace.
	RemoteSrcPath string
	// Line is the line number.
	Line int
	// SrcName is the base file name of the source file.
	SrcName string
	// DirSrc is one directory plus the file name of the source file. It is a
	// subset of RemoteSrcPath.
	DirSrc string

	// The following are only set if Opts.GuessPaths was set.

	// LocalSrcPath is the full path name of the source file as seen in the host,
	// if found.
	LocalSrcPath string
	// RelSrcPath is the relative path to GOROOT, GOPATH or LocalGoMods.
	RelSrcPath string
	// ImportPath is the fully qualified import path as found on disk (when
	// Opts.GuessPaths was set). Defaults to Func.ImportPath otherwise.
	//
	// In the case of package "main", it returns the underlying path to the main
	// package instead of "main" if Opts.GuessPaths was set.
	ImportPath string
	// Location is the source location, if determined.
	Location Location

	// Disallow initialization with unnamed parameters.
	_ struct{}
}

// Init initializes RemoteSrcPath, SrcName, DirName and Line.
//
// For test main, it initializes Location only with Stdlib.
//
// It does its best educated guess for ImportPath.
func (c *Call) init(srcPath string, line int) {
	c.Line = line
	if srcPath != "" {
		c.RemoteSrcPath = srcPath
		if i := strings.LastIndexByte(c.RemoteSrcPath, '/'); i != -1 {
			c.SrcName = c.RemoteSrcPath[i+1:]
			if i = strings.LastIndexByte(c.RemoteSrcPath[:i], '/'); i != -1 {
				c.DirSrc = c.RemoteSrcPath[i+1:]
			}
		}
		if c.DirSrc == testMainSrc {
			// Consider _test/_testmain.go as stdlib since it's injected by "go test".
			c.Location = Stdlib
		}
	}
	c.ImportPath = c.Func.ImportPath
}

const testMainSrc = "_test" + string(os.PathSeparator) + "_testmain.go"

// updateLocations initializes LocalSrcPath, RelSrcPath, Location and ImportPath.
//
// goroot, localgoroot, localgomod, gomodImportPath and gopaths are expected to
// be in "/" format even on Windows. They must not have a trailing "/".
//
// Returns true if a match was found.
func (c *Call) updateLocations(goroot, localgoroot string, localgomods, gopaths map[string]string) bool {
	// TODO(maruel): Reduce memory allocations.
	if c.RemoteSrcPath == "" {
		return false
	}
	// Check GOROOT first.
	if goroot != "" {
		if prefix := goroot + "/src/"; strings.HasPrefix(c.RemoteSrcPath, prefix) {
			// Replace remote GOROOT with local GOROOT.
			c.RelSrcPath = c.RemoteSrcPath[len(prefix):]
			c.LocalSrcPath = pathJoin(localgoroot, "src", c.RelSrcPath)
			if i := strings.LastIndexByte(c.RelSrcPath, '/'); i != -1 {
				c.ImportPath = c.RelSrcPath[:i]
			}
			if c.Location == LocationUnknown {
				c.Location = Stdlib
			}
			return true
		}
	}
	// Check GOPATH.
	// TODO(maruel): Sort for deterministic behavior?
	for prefix, dest := range gopaths {
		if p := prefix + "/src/"; strings.HasPrefix(c.RemoteSrcPath, p) {
			c.RelSrcPath = c.RemoteSrcPath[len(p):]
			c.LocalSrcPath = pathJoin(dest, "src", c.RelSrcPath)
			if i := strings.LastIndexByte(c.RelSrcPath, '/'); i != -1 {
				c.ImportPath = c.RelSrcPath[:i]
			}
			if c.Location == LocationUnknown {
				c.Location = GOPATH
			}
			return true
		}
		// For modules, the path has to be altered, as it contains the version.
		if p := prefix + "/pkg/mod/"; strings.HasPrefix(c.RemoteSrcPath, p) {
			c.RelSrcPath = c.RemoteSrcPath[len(p):]
			c.LocalSrcPath = pathJoin(dest, "pkg/mod", c.RelSrcPath)
			if i := strings.LastIndexByte(c.RelSrcPath, '/'); i != -1 {
				c.ImportPath = c.RelSrcPath[:i]
			}
			if c.Location == LocationUnknown {
				c.Location = GoPkg
			}
			return true
		}
	}
	// Check Go modules.
	// Go module path detection only works with stack traces created on the local
	// file system.
	for prefix, pkg := range localgomods {
		if strings.HasPrefix(c.RemoteSrcPath, prefix+"/") {
			c.RelSrcPath = c.RemoteSrcPath[len(prefix)+1:]
			c.LocalSrcPath = c.RemoteSrcPath
			if i := strings.LastIndexByte(c.RelSrcPath, '/'); i != -1 {
				c.ImportPath = pkg + "/" + c.RelSrcPath[:i]
			} else {
				c.ImportPath = pkg
			}
			if c.Location == LocationUnknown {
				c.Location = GoMod
			}
			return true
		}
	}
	// Maybe the path is just absolute and exists?
	return false
}

// equal returns true only if both calls are exactly equal.
func (c *Call) equal(r *Call) bool {
	return c.Line == r.Line && c.Func.Complete == r.Func.Complete && c.RemoteSrcPath == r.RemoteSrcPath && c.Args.equal(&r.Args)
}

// similar returns true if the two Call are equal or almost but not quite
// equal.
func (c *Call) similar(r *Call, similar Similarity) bool {
	return c.Line == r.Line && c.Func.Complete == r.Func.Complete && c.RemoteSrcPath == r.RemoteSrcPath && c.Args.similar(&r.Args, similar)
}

// merge merges two similar Call, zapping out differences.
func (c *Call) merge(r *Call) Call {
	return Call{
		Func:          c.Func,
		Args:          c.Args.merge(&r.Args),
		RemoteSrcPath: c.RemoteSrcPath,
		Line:          c.Line,
		SrcName:       c.SrcName,
		DirSrc:        c.DirSrc,
		LocalSrcPath:  c.LocalSrcPath,
		RelSrcPath:    c.RelSrcPath,
		ImportPath:    c.ImportPath,
		Location:      c.Location,
	}
}

// Stack is a call stack.
type Stack struct {
	// Calls is the call stack. First is original function, last is leaf
	// function.
	Calls []Call
	// Elided is set when there's >100 items in Stack, currently hardcoded in
	// package runtime.
	Elided bool

	// Disallow initialization with unnamed parameters.
	_ struct{}
}

// equal returns true on if both call stacks are exactly equal.
func (s *Stack) equal(r *Stack) bool {
	if len(s.Calls) != len(r.Calls) || s.Elided != r.Elided {
		return false
	}
	for i := range s.Calls {
		if !s.Calls[i].equal(&r.Calls[i]) {
			return false
		}
	}
	return true
}

// similar returns true if the two Stack are equal or almost but not quite
// equal.
func (s *Stack) similar(r *Stack, similar Similarity) bool {
	if len(s.Calls) != len(r.Calls) || s.Elided != r.Elided {
		return false
	}
	for i := range s.Calls {
		if !s.Calls[i].similar(&r.Calls[i], similar) {
			return false
		}
	}
	return true
}

// merge merges two similar Stack, zapping out differences.
func (s *Stack) merge(r *Stack) *Stack {
	// Assumes similar stacks have the same length.
	out := &Stack{
		Calls:  make([]Call, len(s.Calls)),
		Elided: s.Elided,
	}
	for i := range s.Calls {
		out.Calls[i] = s.Calls[i].merge(&r.Calls[i])
	}
	return out
}

// less compares two Stack, where the ones that are less are more
// important, so they come up front.
//
// A Stack with more private functions is 'less' so it is at the top.
// Inversely, a Stack with only public functions is 'more' so it is at the
// bottom.
func (s *Stack) less(r *Stack) bool {
	lLoc := [lastLocation]int{}
	rLoc := [lastLocation]int{}
	lMain := 0
	rMain := 0
	for _, c := range s.Calls {
		lLoc[c.Location]++
		if c.Func.IsPkgMain {
			lMain++
		}
	}
	for _, s := range r.Calls {
		rLoc[s.Location]++
		if s.Func.IsPkgMain {
			rMain++
		}
	}
	if lMain > rMain {
		return true
	}
	if lMain < rMain {
		return false
	}
	for i := 1; i < int(lastLocation); i++ {
		if lLoc[i] > rLoc[i] {
			return true
		}
		if lLoc[i] < rLoc[i] {
			return false
		}
	}
	// Check unknown code type last.
	if lLoc[LocationUnknown] > rLoc[LocationUnknown] {
		return true
	}
	if lLoc[LocationUnknown] < rLoc[LocationUnknown] {
		return false
	}

	// Stack lengths are the same and they are mostly of the same kind of location.
	for x := range s.Calls {
		if s.Calls[x].Func.Complete < r.Calls[x].Func.Complete {
			return true
		}
		if s.Calls[x].Func.Complete > r.Calls[x].Func.Complete {
			return false
		}
		if s.Calls[x].DirSrc < r.Calls[x].DirSrc {
			return true
		}
		if s.Calls[x].DirSrc > r.Calls[x].DirSrc {
			return false
		}
		if s.Calls[x].Line < r.Calls[x].Line {
			return true
		}
		if s.Calls[x].Line > r.Calls[x].Line {
			return false
		}
	}
	// Stacks are the same.
	return false
}

// updateLocations calls updateLocations on each call frame and returns true if
// they were all resolved.
func (s *Stack) updateLocations(goroot, localgoroot string, localgomods, gopaths map[string]string) bool {
	// If there were none, it was "resolved".
	r := true
	for i := range s.Calls {
		r = s.Calls[i].updateLocations(goroot, localgoroot, localgomods, gopaths) && r
	}
	return r
}

// Signature represents the signature of one or multiple goroutines.
//
// It is effectively the stack trace plus the goroutine internal bits, like
// it's state, if it is thread locked, which call site created this goroutine,
// etc.
type Signature struct {
	// State is the goroutine state at the time of the snapshot.
	//
	// Use git grep 'gopark(|unlock)\(' to find them all plus everything listed
	// in runtime/traceback.go. Valid values includes:
	//     - chan send, chan receive, select
	//     - finalizer wait, mark wait (idle),
	//     - Concurrent GC wait, GC sweep wait, force gc (idle)
	//     - IO wait, panicwait
	//     - semacquire, semarelease
	//     - sleep, timer goroutine (idle)
	//     - trace reader (blocked)
	// Stuck cases:
	//     - chan send (nil chan), chan receive (nil chan), select (no cases)
	// Runnable states:
	//    - idle, runnable, running, syscall, waiting, dead, enqueue, copystack,
	// Scan states:
	//    - scan, scanrunnable, scanrunning, scansyscall, scanwaiting, scandead,
	//      scanenqueue
	//
	// When running under the race detector, the values are 'running' or
	// 'finished'.
	State string
	// CreatedBy is the call stack that created this goroutine, if applicable.
	//
	// Normally, the stack is a single Call.
	//
	// When the race detector is enabled, a full stack snapshot is available.
	CreatedBy Stack
	// SleepMin is the wait time in minutes, if applicable.
	//
	// Not set when running under the race detector.
	SleepMin int
	// SleepMax is the wait time in minutes, if applicable.
	//
	// Not set when running under the race detector.
	SleepMax int
	// Stack is the call stack.
	Stack Stack
	// Locked is set if the goroutine was locked to an OS thread.
	//
	// Not set when running under the race detector.
	Locked bool

	// Disallow initialization with unnamed parameters.
	_ struct{}
}

// equal returns true only if both signatures are exactly equal.
func (s *Signature) equal(r *Signature) bool {
	if s.State != r.State || !s.CreatedBy.equal(&r.CreatedBy) || s.Locked != r.Locked || s.SleepMin != r.SleepMin || s.SleepMax != r.SleepMax {
		return false
	}
	return s.Stack.equal(&r.Stack)
}

// similar returns true if the two Signature are equal or almost but not quite
// equal.
func (s *Signature) similar(r *Signature, similar Similarity) bool {
	if s.State != r.State || !s.CreatedBy.similar(&r.CreatedBy, similar) {
		return false
	}
	if similar == ExactFlags && s.Locked != r.Locked {
		return false
	}
	return s.Stack.similar(&r.Stack, similar)
}

// merge merges two similar Signature, zapping out differences.
func (s *Signature) merge(r *Signature) *Signature {
	min := s.SleepMin
	if r.SleepMin < min {
		min = r.SleepMin
	}
	max := s.SleepMax
	if r.SleepMax > max {
		max = r.SleepMax
	}
	return &Signature{
		State:     s.State,     // Drop right side.
		CreatedBy: s.CreatedBy, // Drop right side.
		SleepMin:  min,
		SleepMax:  max,
		Stack:     *s.Stack.merge(&r.Stack),
		Locked:    s.Locked || r.Locked, // TODO(maruel): This is weirdo.
	}
}

// less compares two Signature, where the ones that are less are more
// important, so they come up front. A Signature with more private functions is
// 'less' so it is at the top. Inversely, a Signature with only public
// functions is 'more' so it is at the bottom.
func (s *Signature) less(r *Signature) bool {
	if s.Stack.less(&r.Stack) {
		return true
	}
	if r.Stack.less(&s.Stack) {
		return false
	}
	if s.Locked && !r.Locked {
		return true
	}
	if r.Locked && !s.Locked {
		return false
	}
	if s.State < r.State {
		return true
	}
	if s.State > r.State {
		return false
	}
	return false
}

// SleepString returns a string "N-M minutes" if the goroutine(s) slept for a
// long time.
//
// Returns an empty string otherwise.
func (s *Signature) SleepString() string {
	if s.SleepMax == 0 {
		return ""
	}
	if s.SleepMin != s.SleepMax {
		return fmt.Sprintf("%d~%d minutes", s.SleepMin, s.SleepMax)
	}
	return fmt.Sprintf("%d minutes", s.SleepMax)
}

// updateLocations calls updateLocations on both CreatedBy and Stack and
// returns true if they were both resolved.
func (s *Signature) updateLocations(goroot, localgoroot string, localgomods, gopaths map[string]string) bool {
	r := s.CreatedBy.updateLocations(goroot, localgoroot, localgomods, gopaths)
	r = s.Stack.updateLocations(goroot, localgoroot, localgomods, gopaths) && r
	return r
}

// Goroutine represents the state of one goroutine, including the stack trace.
type Goroutine struct {
	// Signature is the stack trace, internal bits, state, which call site
	// created it, etc.
	Signature
	// ID is the goroutine id.
	ID int
	// First is the goroutine first printed, normally the one that crashed.
	First bool

	// RaceWrite is true if a race condition was detected, and this goroutine was
	// race on a write operation, otherwise it was a read.
	RaceWrite bool
	// RaceAddr is set to the address when a data race condition was detected.
	// Otherwise it is 0.
	RaceAddr uint64

	// Disallow initialization with unnamed parameters.
	_ struct{}
}

// Private stuff.

// nameArguments is a post-processing step where Args are 'named' with numbers.
func nameArguments(goroutines []*Goroutine) {
	// Set a name for any pointer occurring more than once.
	type object struct {
		args      []*Arg
		inPrimary bool
	}
	objects := map[uint64]object{}
	// Enumerate all the arguments.
	primary := true
	visit := func(arg *Arg) {
		if arg.IsPtr {
			objects[arg.Value] = object{
				args:      append(objects[arg.Value].args, arg),
				inPrimary: objects[arg.Value].inPrimary || primary,
			}
		}
	}
	for i, g := range goroutines {
		primary = i == 0
		for _, c := range g.Stack.Calls {
			c.Args.walk(visit)
		}
		// CreatedBy.Args is never set.
	}
	order := make(uint64Slice, 0, len(objects)/2)
	for k, obj := range objects {
		if len(obj.args) > 1 && obj.inPrimary {
			order = append(order, k)
		}
	}
	sort.Sort(order)
	nextID := 1
	for _, k := range order {
		for _, arg := range objects[k].args {
			arg.Name = fmt.Sprintf("#%d", nextID)
		}
		nextID++
	}

	// Now do the rest. This is done so the output is deterministic.
	order = make(uint64Slice, 0, len(objects))
	for k := range objects {
		order = append(order, k)
	}
	sort.Sort(order)
	for _, k := range order {
		// Process the remaining pointers, they were not referenced by primary
		// thread so will have higher IDs.
		if objects[k].inPrimary {
			continue
		}
		for _, arg := range objects[k].args {
			arg.Name = fmt.Sprintf("#%d", nextID)
		}
		nextID++
	}
}

func pathJoin(s ...string) string {
	return strings.Join(s, "/")
}

type uint64Slice []uint64

func (a uint64Slice) Len() int           { return len(a) }
func (a uint64Slice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a uint64Slice) Less(i, j int) bool { return a[i] < a[j] }
