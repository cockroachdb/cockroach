// Copyright 2019 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cdctest

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"

	. "github.com/cockroachdb/cockroach/pkg/util/fsm"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

// RunChangefeedNemesis runs a jepsen-style validation of whether a changefeed
// meets our user-facing guarantees.
func RunChangefeedNemesis(f TestFeedFactory, db *sql.DB) (Validator, error) {
	// possible relevant nemeses
	// - crdb chaos
	// - sink chaos
	// - schema changes
	// - job pause/unpause during initial scan
	// - job pause/unpause after initial scan
	// - job pause/unpause during schema change backfill
	// - splits
	// - rebalancing
	// - INSERT/UPDATE/DELETE
	// - txn rollbacks

	ctx := context.Background()
	rng, _ := randutil.NewPseudoRand()

	var e executor
	e.m = MakeMachine(txnStateTransitions, stateInitialScan{False}, &e)

	if _, err := db.Exec(`CREATE TABLE foo (a INT PRIMARY KEY)`); err != nil {
		return nil, err
	}
	foo, err := f.Feed(`CREATE CHANGEFEED FOR foo`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = foo.Close() }()
	v := Validators{
		NewOrderValidator(`foo`),
		NewFingerprintValidator(db, `foo`, `fprint`, foo.Partitions()),
	}

loop:
	for {
		var event Event
		switch s := e.m.CurState().(type) {
		case stateDone:
			break loop
		case stateInitialScan:
			r := rng.Intn(10)
			if s.Paused.Get() {
				event = eventResume{}
			} else {
				if r < 1 {
					event = eventInitialScanFinished{}
				} else if r < 5 {
					event = eventPause{}
				} else {
					event = eventRow{}
					// Get a row from `foo` and put it in `v`.
				}
			}
		case stateSteady:
			r := rng.Intn(10)
			if s.Paused.Get() {
				event = eventResume{}
			} else {
				if r < 1 {
					event = eventFinished{}
				} else if r < 5 {
					event = eventPause{}
				} else {
					// Get a message from `foo` and put it in `v` depending on if it's a
					// row or resolved timestamp.
					if r < 8 {
						event = eventRow{}
					} else {
						event = eventResolvedTimestamp{}
					}
				}
			}
		}
		if err := e.m.Apply(ctx, event); err != nil {
			return nil, err
		}
	}

	fmt.Print(e.log.String())
	// Example output:
	// Initial Scan Row
	// Initial Scan paused
	// Initial Scan resumed
	// Initial Scan Row
	// Initial Scan Row
	// Initial Scan finished
	// Steady paused
	// Steady resumed
	// Steady Row
	// Steady paused
	// Steady resumed
	// Steady Row
	// Steady Row
	// Steady Resolved Timestamp
	// Steady paused
	// Steady resumed
	// Steady Resolved Timestamp
	// Steady Row
	// Finish

	return v, nil
}

type stateInitialScan struct {
	Paused Bool
}
type stateSteady struct {
	Paused Bool
}
type stateDone struct{}

func (stateInitialScan) State() {}
func (stateSteady) State()      {}
func (stateDone) State()        {}

type eventRow struct{}
type eventResolvedTimestamp struct{}
type eventInitialScanFinished struct{}
type eventPause struct{}
type eventResume struct{}
type eventFinished struct{}

func (eventRow) Event()                 {}
func (eventResolvedTimestamp) Event()   {}
func (eventInitialScanFinished) Event() {}
func (eventPause) Event()               {}
func (eventResume) Event()              {}
func (eventFinished) Event()            {}

var txnStateTransitions = Compile(Pattern{
	stateInitialScan{False}: {
		eventRow{}: {
			Next:   stateInitialScan{False},
			Action: writeAction("Initial Scan Row"),
		},
		eventInitialScanFinished{}: {
			Next:   stateSteady{False},
			Action: writeAction("Initial Scan finished"),
		},
		eventPause{}: {
			Next:   stateInitialScan{True},
			Action: writeAction("Initial Scan paused"),
		},
	},
	stateInitialScan{True}: {
		eventResume{}: {
			Next:   stateInitialScan{False},
			Action: writeAction("Initial Scan resumed"),
		},
	},
	stateSteady{False}: {
		eventRow{}: {
			Next:   stateSteady{False},
			Action: writeAction("Steady Row"),
		},
		eventResolvedTimestamp{}: {
			Next:   stateSteady{False},
			Action: writeAction("Steady Resolved Timestamp"),
		},
		eventPause{}: {
			Next:   stateSteady{True},
			Action: writeAction("Steady paused"),
		},
		eventFinished{}: {
			Next:   stateDone{},
			Action: writeAction("Finish"),
		},
	},
	stateSteady{True}: {
		eventResume{}: {
			Next:   stateSteady{False},
			Action: writeAction("Steady resumed"),
		},
	},
})

func writeAction(s string) func(Args) error {
	return func(a Args) error {
		a.Extended.(*executor).write(s)
		return nil
	}
}

type executor struct {
	m   Machine
	log bytes.Buffer
}

func (e *executor) write(s string) {
	e.log.WriteString(s)
	e.log.WriteString("\n")
}
