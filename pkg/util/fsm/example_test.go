// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, Any express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package fsm_test

import (
	"bytes"
	"fmt"

	. "github.com/cockroachdb/cockroach/pkg/util/fsm"
)

/// States.

type stateNoTxn struct{}
type stateOpen struct {
	RetryIntent Bool
}
type stateAborted struct {
	RetryIntent Bool
}
type stateRestartWait struct{}

func (stateNoTxn) State()       {}
func (stateOpen) State()        {}
func (stateAborted) State()     {}
func (stateRestartWait) State() {}

/// Events.

type noTopLevelTransitionEvent struct {
	RetryIntent Bool
}
type txnStartEvent struct{}
type txnFinishEvent struct{}
type txnRestartEvent struct{}
type nonRetriableErrEvent struct {
	IsCommit Bool
}
type retriableErrEvent struct {
	CanAutoRetry Bool
	IsCommit     Bool
}

func (noTopLevelTransitionEvent) Event() {}
func (txnStartEvent) Event()             {}
func (txnFinishEvent) Event()            {}
func (txnRestartEvent) Event()           {}
func (nonRetriableErrEvent) Event()      {}
func (retriableErrEvent) Event()         {}

/// Transitions.

var txnStateTransitions = Compile(Pattern{
	stateNoTxn{}: {
		noTopLevelTransitionEvent{False}: {
			Next:   stateNoTxn{},
			Action: func(es ExtendedState) { es.(*executor).write("Identity") },
		},
		txnStartEvent{}: {
			Next:   stateOpen{False},
			Action: func(es ExtendedState) { es.(*executor).write("Open...") },
		},
	},
	stateOpen{Var("x")}: {
		noTopLevelTransitionEvent{False}: {
			Next:   stateOpen{Var("x")},
			Action: func(es ExtendedState) { es.(*executor).write("Identity") },
		},
		txnFinishEvent{}: {
			Next:   stateNoTxn{},
			Action: func(es ExtendedState) { es.(*executor).write("Finish...") },
		},
		nonRetriableErrEvent{True}: {
			Next:   stateNoTxn{},
			Action: func(es ExtendedState) { es.(*executor).write("Error") },
		},
		retriableErrEvent{True, Any}: {
			Next:   stateOpen{Var("x")},
			Action: func(es ExtendedState) { es.(*executor).write("Transition 6") },
		},
		nonRetriableErrEvent{False}: {
			Next:   stateAborted{Var("x")},
			Action: func(es ExtendedState) { es.(*executor).write("Abort") },
		},
	},
	stateOpen{False}: {
		noTopLevelTransitionEvent{True}: {
			Next:   stateOpen{True},
			Action: func(es ExtendedState) { es.(*executor).write("Make Open") },
		},
		retriableErrEvent{False, Any}: {
			Next:   stateAborted{False},
			Action: func(es ExtendedState) { es.(*executor).write("Abort") },
		},
	},
	stateOpen{True}: {
		retriableErrEvent{False, False}: {
			Next:   stateRestartWait{},
			Action: func(es ExtendedState) { es.(*executor).write("Wait for restart") },
		},
		retriableErrEvent{False, True}: {
			Next:   stateNoTxn{},
			Action: func(es ExtendedState) { es.(*executor).write("No more") },
		},
	},
	stateAborted{Var("x")}: {
		noTopLevelTransitionEvent{False}: {
			Next:   stateAborted{Var("x")},
			Action: func(es ExtendedState) { es.(*executor).write("Identity") },
		},
		txnFinishEvent{}: {
			Next:   stateNoTxn{},
			Action: func(es ExtendedState) { es.(*executor).write("Abort finished") },
		},
		txnStartEvent{}: {
			Next:   stateOpen{Var("x")},
			Action: func(es ExtendedState) { es.(*executor).write("Open from abort") },
		},
		nonRetriableErrEvent{Any}: {
			Next:   stateAborted{Var("x")},
			Action: func(es ExtendedState) { es.(*executor).write("Abort") },
		},
	},
	stateRestartWait{}: {
		noTopLevelTransitionEvent{False}: {
			Next:   stateRestartWait{},
			Action: func(es ExtendedState) { es.(*executor).write("Identity") },
		},
		txnFinishEvent{}: {
			Next:   stateNoTxn{},
			Action: func(es ExtendedState) { es.(*executor).write("No more") },
		},
		txnRestartEvent{}: {
			Next:   stateOpen{True},
			Action: func(es ExtendedState) { es.(*executor).write("Restarting") },
		},
		nonRetriableErrEvent{Any}: {
			Next:   stateAborted{True},
			Action: func(es ExtendedState) { es.(*executor).write("Abort") },
		},
	},
})

type executor struct {
	m   Machine
	log bytes.Buffer
}

func (e *executor) write(s string) {
	e.log.WriteString(s)
	e.log.WriteString("\n")
}

func ExampleMachine() {
	var e executor
	e.m = MakeMachine(txnStateTransitions, stateNoTxn{}, &e)
	e.m.Apply(txnStartEvent{})
	e.m.Apply(noTopLevelTransitionEvent{True})
	e.m.Apply(retriableErrEvent{False, False})
	e.m.Apply(txnRestartEvent{})
	e.m.Apply(txnFinishEvent{})
	fmt.Print(e.log.String())

	// Output:
	// Open...
	// Make Open
	// Wait for restart
	// Restarting
	// Finish...
}

func ExampleReport() {
	txnStateTransitions.PrintReport()

	// Output:
	// fsm_test.stateAborted{RetryIntent:false}
	// 	handled events:
	// 		fsm_test.noTopLevelTransitionEvent{RetryIntent:false}
	// 		fsm_test.nonRetriableErrEvent{IsCommit:false}
	// 		fsm_test.nonRetriableErrEvent{IsCommit:true}
	// 		fsm_test.txnFinishEvent{}
	// 		fsm_test.txnStartEvent{}
	// 	missing events:
	// 		fsm_test.noTopLevelTransitionEvent{RetryIntent:true}
	// 		fsm_test.retriableErrEvent{CanAutoRetry:false, IsCommit:false}
	// 		fsm_test.retriableErrEvent{CanAutoRetry:false, IsCommit:true}
	// 		fsm_test.retriableErrEvent{CanAutoRetry:true, IsCommit:false}
	// 		fsm_test.retriableErrEvent{CanAutoRetry:true, IsCommit:true}
	// 		fsm_test.txnRestartEvent{}
	// fsm_test.stateAborted{RetryIntent:true}
	// 	handled events:
	// 		fsm_test.noTopLevelTransitionEvent{RetryIntent:false}
	// 		fsm_test.nonRetriableErrEvent{IsCommit:false}
	// 		fsm_test.nonRetriableErrEvent{IsCommit:true}
	// 		fsm_test.txnFinishEvent{}
	// 		fsm_test.txnStartEvent{}
	// 	missing events:
	// 		fsm_test.noTopLevelTransitionEvent{RetryIntent:true}
	// 		fsm_test.retriableErrEvent{CanAutoRetry:false, IsCommit:false}
	// 		fsm_test.retriableErrEvent{CanAutoRetry:false, IsCommit:true}
	// 		fsm_test.retriableErrEvent{CanAutoRetry:true, IsCommit:false}
	// 		fsm_test.retriableErrEvent{CanAutoRetry:true, IsCommit:true}
	// 		fsm_test.txnRestartEvent{}
	// fsm_test.stateNoTxn{}
	// 	handled events:
	// 		fsm_test.noTopLevelTransitionEvent{RetryIntent:false}
	// 		fsm_test.txnStartEvent{}
	// 	missing events:
	// 		fsm_test.noTopLevelTransitionEvent{RetryIntent:true}
	// 		fsm_test.nonRetriableErrEvent{IsCommit:false}
	// 		fsm_test.nonRetriableErrEvent{IsCommit:true}
	// 		fsm_test.retriableErrEvent{CanAutoRetry:false, IsCommit:false}
	// 		fsm_test.retriableErrEvent{CanAutoRetry:false, IsCommit:true}
	// 		fsm_test.retriableErrEvent{CanAutoRetry:true, IsCommit:false}
	// 		fsm_test.retriableErrEvent{CanAutoRetry:true, IsCommit:true}
	// 		fsm_test.txnFinishEvent{}
	// 		fsm_test.txnRestartEvent{}
	// fsm_test.stateOpen{RetryIntent:false}
	// 	handled events:
	// 		fsm_test.noTopLevelTransitionEvent{RetryIntent:false}
	// 		fsm_test.noTopLevelTransitionEvent{RetryIntent:true}
	// 		fsm_test.nonRetriableErrEvent{IsCommit:false}
	// 		fsm_test.nonRetriableErrEvent{IsCommit:true}
	// 		fsm_test.retriableErrEvent{CanAutoRetry:false, IsCommit:false}
	// 		fsm_test.retriableErrEvent{CanAutoRetry:false, IsCommit:true}
	// 		fsm_test.retriableErrEvent{CanAutoRetry:true, IsCommit:false}
	// 		fsm_test.retriableErrEvent{CanAutoRetry:true, IsCommit:true}
	// 		fsm_test.txnFinishEvent{}
	// 	missing events:
	// 		fsm_test.txnRestartEvent{}
	// 		fsm_test.txnStartEvent{}
	// fsm_test.stateOpen{RetryIntent:true}
	// 	handled events:
	// 		fsm_test.noTopLevelTransitionEvent{RetryIntent:false}
	// 		fsm_test.nonRetriableErrEvent{IsCommit:false}
	// 		fsm_test.nonRetriableErrEvent{IsCommit:true}
	// 		fsm_test.retriableErrEvent{CanAutoRetry:false, IsCommit:false}
	// 		fsm_test.retriableErrEvent{CanAutoRetry:false, IsCommit:true}
	// 		fsm_test.retriableErrEvent{CanAutoRetry:true, IsCommit:false}
	// 		fsm_test.retriableErrEvent{CanAutoRetry:true, IsCommit:true}
	// 		fsm_test.txnFinishEvent{}
	// 	missing events:
	// 		fsm_test.noTopLevelTransitionEvent{RetryIntent:true}
	// 		fsm_test.txnRestartEvent{}
	// 		fsm_test.txnStartEvent{}
	// fsm_test.stateRestartWait{}
	// 	handled events:
	// 		fsm_test.noTopLevelTransitionEvent{RetryIntent:false}
	// 		fsm_test.nonRetriableErrEvent{IsCommit:false}
	// 		fsm_test.nonRetriableErrEvent{IsCommit:true}
	// 		fsm_test.txnFinishEvent{}
	// 		fsm_test.txnRestartEvent{}
	// 	missing events:
	// 		fsm_test.noTopLevelTransitionEvent{RetryIntent:true}
	// 		fsm_test.retriableErrEvent{CanAutoRetry:false, IsCommit:false}
	// 		fsm_test.retriableErrEvent{CanAutoRetry:false, IsCommit:true}
	// 		fsm_test.retriableErrEvent{CanAutoRetry:true, IsCommit:false}
	// 		fsm_test.retriableErrEvent{CanAutoRetry:true, IsCommit:true}
	// 		fsm_test.txnStartEvent{}
}
