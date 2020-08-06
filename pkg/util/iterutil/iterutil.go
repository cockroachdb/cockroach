// Copyright 2020 The Cockroach Authors.
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
	Elem  interface{}
	Index int
	done  *bool
}

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
//	s.Elem = new(my.Thing)
// 	for _, thing := range myThings {
// 		*s.Elem.(*my.Thing) = thing // set the current element
// 		if err := f(s.Current()); err != nil {
// 			return err
// 		}
// 		if s.Done() {
// 			break
// 		}
// 	}
//
// where `f` is something like:
//
// 	f := func(c iterutil.Cur) error {
// 		repl := c.Elem.(*my.Thing)
// 		if something(repl) {
// 			return errors.New("something is not good!")
// 		} else if somethingElse(repl) {
// 			return c.Stop() // that's it, won't be called again, even if Stop returns non-nil error
// 		}
// 		return nil
// 	}
//
type State struct {
	Cur
	done bool
}

// NewState creates a new iterator state.
func NewState() *State {
	s := State{}
	s.Cur.done = &s.done
	s.Cur.Index = -1 // will become 0 when Current is called the first time
	return &s
}

// Current returns the current element of the iteration state.
//
// Once the closure returns, it must not retain or access Elem any more.
// It is preferred to be used over accessing Cur since it increments the index.
func (s *State) Current() Cur {
	s.Cur.Index++
	return s.Cur
}

// Done tells if the iteration is complete or not.
func (s *State) Done() bool { return s.done }
