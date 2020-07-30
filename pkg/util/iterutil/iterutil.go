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

// Cur is the current element of the iteration.
type Cur struct {
	done  *bool
	elem  interface{}
	index int
}

// Elem returns the element associated with c.
func (c *Cur) Elem() interface{} { return c.elem }

// Index returns the c's index.
func (c *Cur) Index() int { return c.index }

// Stop halts the iteration, i.e., sets the `done` flag to true.
func (c *Cur) Stop() error {
	*c.done = true
	return nil
}

// State iterates over the values.
//
// This can be used to create iterators that use closures, like:
//
// 	s := iterutil.NewState()
// 	for _, thing := range myThings {
// 		s.Update(thing)
// 		if err := f(s.Cur()); err != nil {
// 			return err
// 		}
// 		if s.Done() {
// 			break
// 		}
// 	}
//
// where `f` is something like:
//
// 	f := func(s *iterutil.Cur) error {
// 		repl := s.Elem.(*my.Thing)
// 		if something(repl) {
// 			return errors.New("something is not good!")
// 		} else if somethingElse(repl) {
// 			return s.Stop() // that's it, won't be called again, even if Stop returns non-nil error
// 		}
// 		return nil
// 	}
//
type State struct {
	cur  Cur
	done bool
}

// NewState returns a new iter state to create an iterator.
func NewState() *State {
	s := State{}
	s.cur.done = &s.done
	s.cur.index = -1 // when first update is called, it is set to 0
	return &s
}

// Cur returns the current element of the iteration.
func (s *State) Cur() *Cur { return &s.cur }

// Update sets the current element of the iteration state.
func (s *State) Update(elem interface{}) {
	s.cur.elem = elem
	s.cur.index++
}

// Done tells if the iteration is complete or not.
func (s *State) Done() bool { return s.done }
