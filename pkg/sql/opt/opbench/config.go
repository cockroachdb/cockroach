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

// Choice denotes a single input value to a plan, along with the set of legal
// values for that input.
type Choice struct {
	Field   string
	Choices []float64
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

// configIterator takes a list of Choices and produces every possible
// combination of them.
// TODO(justin): we should also support some kind of consistent sampling of
// these so we don't necessarily have to run them all.
type configIterator struct {
	choices []Choice

	// state tracks the current index for each choice. It always has the same
	// length as choices.
	state []int
	done  bool
}

// NewConfigIterator returns a ConfigIterator that iterates over all of the
// possible inputs to the given Spec.
func NewConfigIterator(spec *Spec) *configIterator {
	return &configIterator{
		choices: spec.Inputs,
		state:   make([]int, len(spec.Inputs)),
	}
}

// Next returns the next Configuration in the iteration process.
func (it *configIterator) Next() (Configuration, bool) {
	if it.done {
		return nil, false
	}
	config := make(Configuration, len(it.choices))
	for j := range it.choices {
		config[it.choices[j].Field] = it.choices[j].Choices[it.state[j]]
	}
	it.increment()

	return config, true
}

// increment brings the iterator to the next state, given the maximum possible
// value for each "slot". So if the first option has 2 Choices and the second
// has 3, the increment process goes like:
//
//   [0 0] => [1 0] => [0 1] => [1 1] => [0 2] => [1 2] => done.
func (it *configIterator) increment() {
	i := 0
	for i < len(it.choices) {
		it.state[i]++
		if it.state[i] < len(it.choices[i].Choices) {
			break
		}
		it.state[i] = 0
		i++
	}
	if i == len(it.choices) {
		it.done = true
	}
}
