// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package opbench

import (
	"bytes"
	"fmt"
	"sort"
)

// Options denotes a single input to a plan, along with the set of legal
// values for that input.
type Options struct {
	Field  string
	Values []float64
}

// Configuration is a particular set of inputs. A Configuration totally defines
// how the parameters for a given plan should be set.
type Configuration map[string]float64

func (c Configuration) String() string {
	// Configurations are stringified as "a=1/b=2/c=3/...".
	keys := make([]string, len(c))
	i := 0
	for k := range c {
		keys[i] = k
		i++
	}

	// Sort the keys so that the stringified form is consistent.
	sort.Strings(keys)

	var buf bytes.Buffer
	for i, k := range keys {
		if i > 0 {
			buf.WriteByte('/')
		}
		buf.WriteString(k)
		buf.WriteByte('=')
		fmt.Fprintf(&buf, "%d", int(c[k]))
	}
	return buf.String()
}

// ConfigIterator takes a list of Options and produces every possible
// combination of them.
// TODO(justin): we should also support some kind of consistent sampling of
// these so we don't necessarily have to run them all.
type ConfigIterator struct {
	options []Options

	// state tracks the current index for each choice. It always has the same
	// length as options.
	state []int
	done  bool
}

// NewConfigIterator returns a ConfigIterator that iterates over all of the
// possible inputs to the given Spec.
func NewConfigIterator(spec *Spec) *ConfigIterator {
	return &ConfigIterator{
		options: spec.Inputs,
		state:   make([]int, len(spec.Inputs)),
	}
}

// Next returns the next Configuration in the iteration process.
func (it *ConfigIterator) Next() (Configuration, bool) {
	if it.done {
		return nil, false
	}
	config := make(Configuration, len(it.options))
	for j := range it.options {
		config[it.options[j].Field] = it.options[j].Values[it.state[j]]
	}
	it.increment()

	return config, true
}

// increment brings the iterator to the next state, given the maximum possible
// value for each "slot". So if the first option has 2 choices and the second
// has 3, the increment process goes like:
//
//   [0 0] => [1 0] => [0 1] => [1 1] => [0 2] => [1 2] => done.
func (it *ConfigIterator) increment() {
	i := 0
	for i < len(it.options) {
		it.state[i]++
		if it.state[i] < len(it.options[i].Values) {
			break
		}
		it.state[i] = 0
		i++
	}
	if i == len(it.options) {
		it.done = true
	}
}
