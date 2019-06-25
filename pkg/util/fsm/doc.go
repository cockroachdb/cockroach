// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

/*
Package fsm provides an interface for defining and working with finite-state
machines.

The package is split into two main types: Transitions and Machine. Transitions
is an immutable State graph with Events acting as the directed edges between
different States. The graph is built by calling Compile on a Pattern, which is
meant to be done at init time. This pattern is a mapping from current States to
Events that may be applied on those states to resulting Transitions. The pattern
supports pattern matching on States and Events using wildcards and variable
bindings. To add new transitions to the graph, simply adjust the Pattern
provided to Compile. Transitions are not used directly after creation, instead,
they're used by Machine instances.

Machine is an instantiation of a finite-state machine. It is given a Transitions
graph when it is created to specify its State graph. Since the Transition graph
is itself state-less, multiple Machines can be powered by the same graph
simultaneously. The Machine has an Apply(Event) method, which applies the
provided event to its current state. This does two things:
1. It may move the current State to a new State, according to the Transitions
   graph.
2. It may apply an Action function on the Machine's ExtendedState, which is
   extra state in a Machine that does not contribute to state transition
   decisions, but that can be affected by a state transition.

See example_test.go for a full working example of a state machine with an
associated set of states and events.

This package encourages the Pattern to be declared as a map literal. When
declaring this literal, be careful to not declare two equal keys: they'll result
in the second overwriting the first with no warning because of how Go deals with
map literals. Note that keys that are not technically equal, but where one is a
superset of the other, will work as intended. E.g. the following is permitted:
 Compile(Pattern{
   stateOpen{retryIntent: Any} {
     eventTxnFinish{}: {...}
   }
   stateOpen{retryIntent: True} {
     eventRetriableErr{}: {...}
   }

Members of this package are accessed frequently when implementing a state
machine. For that reason, it is encouraged to dot-import this package in the
file with the transitions Pattern. The respective file should be kept small and
named <name>_fsm.go; our linter doesn't complain about dot-imports in such
files.

*/
package fsm
