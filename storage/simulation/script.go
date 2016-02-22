// Copyright 2015 The Cockroach Authors.
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
//
// Author: Bram Gruneir (bram+code@cockroachlabs.com)

package main

import (
	"bufio"
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/cockroachdb/cockroach/util"
)

// Operation enumerates the possible scripted operations that can occur to a
// cluster.
type Operation string

// These are the possible scripted operations.
const (
	OpSplitRange Operation = "splitrange"
	OpAddNode    Operation = "addnode"
	OpExit       Operation = "exit"
	// TODO(bram): #4566 add optional value to addnode to indicate size
	// TODO(bram): #4566 consider many other operations here.
	//	OpMergeRangeRandom
	//	OpMergeRangeFirst
	//  OpKillNodeRandom
	//  OpKillNodeFirst
	//  OpAddStoreRandom
	//  OpAddStore
	//	OpKillStoreRandom
	//	OpKillStoreFirst
)

// isValidOperation returns true if the passed in string corresponds to a known
// operation.
func isValidOperation(s string) bool {
	switch Operation(s) {
	case OpSplitRange, OpAddNode, OpExit:
		return true
	}
	return false
}

// OperationVariant enumerates the possible options for a scripted operation.
type OperationVariant string

// These are the possible operations options.
const (
	OpVarValue  OperationVariant = ""
	OpVarRandom OperationVariant = "random"
	OpVarFirst  OperationVariant = "first"
	OpVarLast   OperationVariant = "last"
)

// isValidOperationVariant returns true if the passed in string corresponds to
// a known operation variant. If the passed in string does not match any known
// operation variant, an attempt is made to parse the string into the returned
// integer.
func isValidOperationVariant(s string) (bool, int, error) {
	switch OperationVariant(s) {
	case OpVarRandom, OpVarFirst, OpVarLast:
		return true, 0, nil
	}
	value, err := strconv.Atoi(s)
	return false, value, err
}

// Action is a single scripted action. Operation contains the overall operation
// this action will take. Variant contains which version of the operation will
// be executed, such as "random" (OpVarRandom) or "first" (OpVarFirst). There
// are times when an actual number is required and for those cases variant is
// set to OpVarValue to indicate that passed in number is stored in value.
type Action struct {
	operation Operation
	variant   OperationVariant
	value     int
}

// ActionDetails contains an action and all of the metadata surrounding that
// action.
// First is the first epoch in which the action occurs.
// Every is the interval for how often the action re-occurs.
// Last is the final epoch in which the action can occur.
// Repeat is how many times an action occurs at each epoch in which it
// fires.
// Percent is the percentage chance that the action will occur at every epoch
// at which it could occur, a value form 0 to 100.
type ActionDetails struct {
	Action
	first   int
	every   int
	last    int
	repeat  int
	percent int
}

func (ad ActionDetails) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%s", ad.Action.operation)

	if ad.Action.variant == OpVarValue {
		if ad.Action.value > 0 {
			fmt.Fprintf(&buf, "\tValue:%d", ad.Action.value)
		} else {
			fmt.Fprintf(&buf, "\t ")
		}
	} else {
		fmt.Fprintf(&buf, "\tVariant:%s", ad.Action.variant)
	}

	fmt.Fprintf(&buf, "\tFirst:%d", ad.first)
	if ad.last > 0 {
		fmt.Fprintf(&buf, "\tLast:%d", ad.last)
	} else {
		fmt.Fprintf(&buf, "\t")
	}
	if ad.every > 0 {
		fmt.Fprintf(&buf, "\tEvery:%d", ad.every)
	} else {
		fmt.Fprintf(&buf, "\t")
	}
	if ad.repeat > 1 {
		fmt.Fprintf(&buf, "\tRepeat:%d", ad.repeat)
	}
	if ad.percent < 100 {
		fmt.Fprintf(&buf, "\tPercent:%d%%", ad.percent)
	}
	return buf.String()
}

// Script contains a list of all the scripted actions for cluster simulation.
type Script struct {
	maxEpoch int
	actions  map[int][]Action
	rand     *rand.Rand
}

// createScript creates a new Script.
func createScript(maxEpoch int, scriptFile string, rand *rand.Rand) (Script, error) {
	s := Script{
		maxEpoch: maxEpoch,
		actions:  make(map[int][]Action),
		rand:     rand,
	}

	return s, s.parse(scriptFile)
}

// addActionAtEpoch adds an action at a specific epoch to the s.actions map.
func (s *Script) addActionAtEpoch(action Action, repeat, epoch, percent int) {
	for i := 0; i < repeat; i++ {
		if s.rand.Intn(100) < percent {
			s.actions[epoch] = append(s.actions[epoch], action)
		}
	}
}

// addAction adds an action to the script.
func (s *Script) addAction(details ActionDetails) {
	// Always add the action at the first epoch. This way, single actions can
	// just set last and every to 0.
	last := details.last
	if last < details.first {
		last = details.first
	}

	// If every is less than 1, set it to maxEpoch so we never repeat, but do
	// exit the loop.
	every := details.every
	if every < 1 {
		every = s.maxEpoch
	}

	// Always set repeat to 1 if it is below zero.
	repeat := details.repeat
	if repeat < 1 {
		repeat = 1
	}

	// Save all the times this action should occur in s.actions.
	for currentEpoch := details.first; currentEpoch <= last && currentEpoch <= s.maxEpoch; currentEpoch += every {
		s.addActionAtEpoch(details.Action, repeat, currentEpoch, details.percent)
	}
}

// getActions returns the list of actions for a specific epoch.
func (s *Script) getActions(epoch int) []Action {
	if epoch > s.maxEpoch {
		return []Action{{operation: OpExit}}
	}
	return s.actions[epoch]
}

// parse loads all the operations from a script file into the actions map. The
// format is described in the default.script file.
func (s *Script) parse(scriptFile string) error {
	file, err := os.Open(scriptFile)
	if err != nil {
		return err
	}
	defer file.Close()

	tw := tabwriter.NewWriter(os.Stdout, 12, 4, 2, ' ', 0)

	scanner := bufio.NewScanner(file)
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		line := scanner.Text()
		line = strings.ToLower(line)
		line = strings.TrimSpace(line)

		// Ignore empty and commented out lines.
		if len(line) == 0 || strings.HasPrefix(line, "#") {
			continue
		}

		// Split the line by space.
		elements := strings.Split(line, " ")
		if len(elements) < 2 {
			return util.Errorf("line %d has too few elements: %s", lineNumber, line)
		}
		if len(elements) > 6 {
			return util.Errorf("line %d has too many elements: %s", lineNumber, line)
		}

		// Split the first element into operation-variant.
		subElements := strings.Split(elements[0], "-")
		if len(subElements) > 2 {
			return util.Errorf("line %d operation has more than one option: %s", lineNumber, elements[0])
		}
		if !isValidOperation(subElements[0]) {
			return util.Errorf("line %d operation could not be found: %s", lineNumber, subElements[0])
		}

		details := ActionDetails{Action: Action{operation: Operation(subElements[0])}}
		// Do we have a variant or a value?
		if len(subElements) == 2 {
			isValid, value, err := isValidOperationVariant(subElements[1])
			if err != nil {
				return util.Errorf("line %d operation option could not be found or parsed: %s",
					lineNumber, subElements[1])
			}
			if isValid {
				details.Action.variant = OperationVariant(subElements[1])
			} else {
				details.Action.variant, details.Action.value = OpVarValue, value
			}
		}

		// Get the first epoch.
		if details.first, err = strconv.Atoi(elements[1]); err != nil {
			return util.Errorf("line %d FIRST could not be parsed: %s", lineNumber, elements[1])
		}

		// Get the last epoch.
		if len(elements) > 2 {
			if details.last, err = strconv.Atoi(elements[2]); err != nil {
				return util.Errorf("line %d LAST could not be parsed: %s", lineNumber, elements[2])
			}
		}

		// Get the every value.
		if len(elements) > 3 {
			if details.every, err = strconv.Atoi(elements[3]); err != nil {
				return util.Errorf("line %d EVERY could not be parsed: %s", lineNumber, elements[3])
			}
		}

		// Get the repeat value.
		if len(elements) > 4 {
			if details.repeat, err = strconv.Atoi(elements[4]); err != nil {
				return util.Errorf("line %d REPEAT could not be parsed: %s", lineNumber, elements[4])
			}
		}

		// Get the percentage value.
		if len(elements) > 5 {
			if details.percent, err = strconv.Atoi(elements[5]); err != nil {
				return util.Errorf("line %d PERCENT could not be parsed: %s", lineNumber, elements[5])
			}
			if details.percent < 1 {
				return util.Errorf("line %d PERCENT is less than 1: %s", lineNumber, elements[5])
			} else if details.percent > 100 {
				return util.Errorf("line %d PERCENT is greater than 100: %s", lineNumber, elements[5])
			}
		} else {
			details.percent = 100
		}

		s.addAction(details)
		fmt.Fprintf(tw, "%d:\t%s\n", lineNumber, details)
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	_ = tw.Flush()

	return nil
}
