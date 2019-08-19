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
	// New creates a new opaque state object for the input File.
	New(File) State
	// Transform applies this transformation pass to the input File using
	// State to determine which occurrence to transform. It returns the
	// transformed File, a Result indicating whether to proceed or not, and
	// an error if the transformation could not be performed.
	Transform(File, State) (File, Result, error)
	// Advance moves State to the next occurrence of a transformation in
	// the given input File and returns the new State.
	Advance(File, State) State
	// Name returns the name of the Pass.
	Name() string
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
	log("size: %d\n", current.Size())
	for {
		sizeAtStart := current.Size()
		for pi, p := range passList {
			log("\tpass %d of %d: %s...", pi+1, len(passList), p.Name())
			state := p.New(current)
			found, interesting := 0, 0
			for {
				variant, result, err := p.Transform(current, state)
				if err != nil {
					return "", err
				}
				if result == OK {
					found++
					if isInteresting(variant) {
						interesting++
						current = variant
					} else {
						state = p.Advance(current, state)
					}
				} else {
					break
				}
			}
			log(" %d found, %d interesting\n", found, interesting)
			if interesting > 0 {
				log("size: %d\n", current.Size())
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
