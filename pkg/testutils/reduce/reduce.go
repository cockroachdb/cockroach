// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package reduce implements a reducer core for reducing the size of test
// failure cases.
//
// See: https://blog.regehr.org/archives/1678.
package reduce

import (
	"errors"
	"fmt"
	"io"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// Pass defines a reduce pass.
type Pass interface {
	New(File) State
	Transform(File, State) (File, Result, error)
	Advance(File, State) State
}

// Result is returned by a Transform func.
type Result int

const (
	// OK indicates there are more transforms in the current Pass.
	OK Result = iota
	// STOP indicates there are no more transforms.
	STOP
)

// State is opaque state for a Pass.
type State interface{}

// File contains the contents of a file.
type File string

// Size returns the size of the file in bytes.
func (f File) Size() int {
	return len(f)
}

// InterestingFn returns true if File triggers the target test failure.
type InterestingFn func(File) bool

// Reduce executes the test case reduction algorithm. logger, if not nil, will
// log progress output.
func Reduce(
	logger io.Writer, originalTestCase File, isInteresting InterestingFn, passList ...Pass,
) (File, error) {
	log := func(format string, args ...interface{}) {
		if logger == nil {
			return
		}
		fmt.Fprintf(logger, format, args...)
	}
	if !isInteresting(originalTestCase) {
		return "", errors.New("original test case not interesting")
	}
	current := originalTestCase
	start := timeutil.Now()
	for {
		sizeAtStart := current.Size()
		log("size: %d\n", current.Size())
		for pi, p := range passList {
			log("\tpass %d of %d\n", pi+1, len(passList))
			state := p.New(current)
			for {
				variant, result, err := p.Transform(current, state)
				if err != nil {
					return "", err
				}
				if result == OK {
					if isInteresting(variant) {
						current = variant
					} else {
						state = p.Advance(current, state)
					}
				} else {
					break
				}
			}
		}
		if current.Size() >= sizeAtStart {
			break
		}
	}
	log("total time: %v\n", timeutil.Since(start))
	log("original size: %v\n", originalTestCase.Size())
	log("final size: %v\n", current.Size())
	log("reduction: %v%%\n", 100-int(100*float64(current.Size())/float64(originalTestCase.Size())))
	return current, nil
}

type intPass func(string, int) (string, bool, error)

// MakeIntPass returns a Pass with a state that starts at 0 and increments
// by 1 each Advance. f is a transformation function that takes the input and
// current state (that is, it should transform the i'th (zero-based) unit. It
// returns the possibly transformed output and a boolean that is false if a
// transformation could not be done because i was exhausted.
func MakeIntPass(f func(s string, i int) (out string, ok bool, err error)) Pass {
	return intPass(f)
}

func (p intPass) New(File) State {
	return 0
}

func (p intPass) Transform(f File, s State) (File, Result, error) {
	i := s.(int)
	data, ok, err := p(string(f), i)
	res := OK
	if !ok {
		res = STOP
	}
	return File(data), res, err
}

func (p intPass) Advance(f File, s State) State {
	return s.(int) + 1
}
