// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package iterutil

// State iterates over the values.
//
// State can be used to create State methods with closures:
//
// 	var s iterutil.State
// 	for _, thing := range myThings {
// 		s.Update(thing)
// 		if err := f(&s); err != nil {
// 			return err
// 		}
// 		if s.Done() {
// 			break
// 		}
// 	}
//
// where `f` is something like:
//
// 	f := func(s *iterutil.State) error {
// 		repl := s.Cur().(*my.Thing)
// 		if something(repl) {
// 			return errors.New("something is not good!")
// 		} else if somethingElse(repl) {
// 			return s.Stop() // that's it, won't be called again, even if Stop returns non-nil error
// 		}
// 		return nil
// 	}
//
type State struct {
	cur  interface{}
	done bool
}

// Cur returns the current element of the iteration.
func (i *State) Cur() interface{} { return i.cur }

// Stop causes the iterator to stop iterating, i.e. the current element
// is the last one to be visited.
func (i *State) Stop() error {
	i.done = true
	return nil
}

// Done returns whether the iteration was stopped or not, i.e., `Stop`
// was called or not.
func (i *State) Done() bool { return i.done }

// Update sets the current element of the iteration.
func (i *State) Update(v interface{}) { i.cur = v }
