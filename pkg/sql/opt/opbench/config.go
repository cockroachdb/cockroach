// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package opbench

import (
	"bytes"
	"fmt"
	"sort"
)

// choice denotes a single input value to a plan, along with the set of legal
// values for that input.
type choice struct {
	field   string
	choices []float64
}

// input is a named input to a plan, with a set value.
type input struct {
	key   string
	value float64
}

func (p input) String() string {
	return fmt.Sprintf("%v=%v", p.key, p.value)
}

// Configuration is a particular set of inputs (with a unique set of keys).
// A Configuration totally defines how the parameters for a given plan should
// be set.
type Configuration []input

// Get returns the value that k has in c, panicking if k is not a key in the
// configuration.
func (c Configuration) Get(k string) float64 {
	for _, p := range c {
		if p.key == k {
			return p.value
		}
	}
	panic(fmt.Sprintf("Configuration %q did not contain key %q", c.String(), k))
}

// Set sets a key to a value within the Configuration.
func (c *Configuration) Set(k string, v float64) {
	*c = append(*c, input{k, v})
}

func (c Configuration) String() string {
	// Configurations are stringified as "a=1/b=2/c=3/...".
	sorted := make([]input, len(c))
	copy(sorted, c)

	// Sort the keys so that the stringified form can be used
	// as a key.
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].key < sorted[j].key
	})

	var buf bytes.Buffer
	for i, p := range sorted {
		if i > 0 {
			buf.WriteByte('/')
		}
		buf.WriteString(p.key)
		buf.WriteByte('=')
		fmt.Fprintf(&buf, "%d", int(p.value))
	}
	return buf.String()
}

// configIterator takes a list of choices and produces every possible
// combination of them.
// TODO(justin): we should also support some kind of consistent sampling of
// these so we don't necessarily have to run them all.
type configIterator struct {
	choices []choice
	state   []int
	done    bool
}

// NewConfigIterator returns a ConfigIterator that iterates over all of the
// possible inputs to the given Spec.
func NewConfigIterator(spec *Spec) *configIterator {
	choices := spec.inputs
	return &configIterator{
		choices: choices,
		state:   make([]int, len(choices)),
	}
}

// Next returns the next Configuration in the iteration process.
func (it *configIterator) Next() (Configuration, bool) {
	if it.done {
		return nil, false
	}
	config := make(Configuration, len(it.choices))
	for j := 0; j < len(it.choices); j++ {
		config[j] = input{
			key:   it.choices[j].field,
			value: it.choices[j].choices[it.state[j]],
		}
	}
	it.increment()

	return config, true
}

// increment brings the iterator to the next state, given the maximum possible
// value for each "slot". So if the first option has 2 choices and the second
// has 3, the increment process goes like:
//
//   [0 0] => [1 0] => [0 1] => [1 1] => [0 2] => [1 2] => done.
func (it *configIterator) increment() {
	i := 0
	for i < len(it.choices) {
		it.state[i]++
		if it.state[i] < len(it.choices[i].choices) {
			break
		}
		it.state[i] = 0
		i++
	}
	if i == len(it.choices) {
		it.done = true
	}
}
