// Copyright 2019 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cdctest

import (
	"context"
	gosql "database/sql"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/util/fsm"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/pkg/errors"
)

// RunNemesis runs a jepsen-style validation of whether a changefeed meets our
// user-facing guarantees. It's driven by a state machine with various nemeses:
// txn begin/commit/rollback, job pause/unpause.
//
// Changefeeds have a set of user-facing guarantees about ordering and
// duplicates, which the two cdctest.Validator implementations verify for the
// real output of a changefeed. The output rows and resolved timestamps of the
// tested feed are fed into them to check for anomalies.
func RunNemesis(f TestFeedFactory, db *gosql.DB, numInitialRows int) (Validator, error) {
	// possible additional nemeses:
	// - schema changes
	// - splits
	// - rebalancing
	// - INSERT/UPDATE/DELETE
	// mostly redundant with the pause/unpause nemesis, but might be nice to have:
	// - crdb chaos
	// - sink chaos

	ctx := context.Background()
	rng, _ := randutil.NewPseudoRand()

	ns := &nemeses{
		rowCount: 4,
		db:       db,
	}
	// Initialize table rows by repeatedly running the `transact` transition,
	// which randomly starts, commits, and rolls back transactions. This will
	// leave some committed rows and some outstanding intents for the initial
	// scan.
	if _, err := db.Exec(`CREATE TABLE foo (id INT PRIMARY KEY, ts STRING DEFAULT '0')`); err != nil {
		return nil, err
	}
	for i := 0; i < ns.rowCount*5; i++ {
		if err := transact(fsm.Args{Extended: ns}); err != nil {
			return nil, err
		}
	}
	if err := db.QueryRow(`SELECT count(*) FROM foo`).Scan(&ns.availableRows); err != nil {
		return nil, err
	}

	// Set up some initial splits.
	if _, err := db.Exec(`SET CLUSTER SETTING kv.range_merge.queue_enabled = false`); err != nil {
		return nil, err
	}
	if _, err := db.Exec(`ALTER TABLE foo SPLIT AT VALUES ($1)`, ns.rowCount/2); err != nil {
		return nil, err
	}

	foo, err := f.Feed(`CREATE CHANGEFEED FOR foo WITH updated, resolved`)
	if err != nil {
		return nil, err
	}
	ns.f = foo
	defer func() { _ = foo.Close() }()

	if _, err := db.Exec(`CREATE TABLE fprint (id INT PRIMARY KEY, ts STRING)`); err != nil {
		return nil, err
	}
	ns.v = MakeCountValidator(Validators{
		NewOrderValidator(`foo`),
		NewFingerprintValidator(db, `foo`, `fprint`, foo.Partitions()),
	})

	// Run the state machine until it finishes. Exit criteria is in `nextEvent`
	// and is based on the number of rows that have been resolved and the number
	// of resolved timestamp messages.
	m := fsm.MakeMachine(txnStateTransitions, stateRunning{fsm.False}, ns)
	for {
		state := m.CurState()
		if _, ok := state.(stateDone); ok {
			return ns.v, nil
		}
		event, payload, err := ns.nextEvent(rng, state, foo)
		if err != nil {
			return nil, err
		}
		if err := m.ApplyWithPayload(ctx, event, payload); err != nil {
			return nil, err
		}
	}
}

type nemeses struct {
	rowCount int

	v  *CountValidator
	db *gosql.DB
	f  TestFeed

	transactions  []*gosql.Tx
	availableRows int
}

// nextEvent selects the next state transition.
func (ns *nemeses) nextEvent(
	rng *rand.Rand, state fsm.State, f TestFeed,
) (fsm.Event, fsm.EventPayload, error) {
	if ns.v.NumResolvedWithRows > 5 && ns.v.NumResolvedRows > 20 {
		return eventFinished{}, nil, nil
	}

	switch s := state.(type) {
	case stateRunning:
		if s.Paused.Get() {
			return eventResume{}, nil, nil
		}
		if r := rng.Intn(10); r < 1 {
			return eventPause{}, nil, nil
		}
		if r := rng.Intn(2); r < 1 {
			return eventTransact{}, nil, nil
		}

		m, err := f.Next()
		if err != nil {
			return nil, nil, err
		} else if m == nil {
			return nil, nil, errors.Errorf(`expected another message`)
		}
		if len(m.Resolved) > 0 {
			return eventFeedResolved{}, m, nil
		}
		return eventFeedRow{}, m, nil
	default:
		return nil, nil, errors.Errorf(`unknown state: %T %s`, s, s)
	}
}

type stateRunning struct {
	Paused fsm.Bool
}
type stateDone struct{}

func (stateRunning) State() {}
func (stateDone) State()    {}

type eventTransact struct{}
type eventFeedRow struct{}
type eventFeedResolved struct{}
type eventPause struct{}
type eventResume struct{}
type eventFinished struct{}

func (eventTransact) Event()     {}
func (eventFeedRow) Event()      {}
func (eventFeedResolved) Event() {}
func (eventPause) Event()        {}
func (eventResume) Event()       {}
func (eventFinished) Event()     {}

var txnStateTransitions = fsm.Compile(fsm.Pattern{
	stateRunning{fsm.Any}: {
		eventFinished{}: {
			Next:   stateDone{},
			Action: logEvent(noop),
		},
	},
	stateRunning{fsm.False}: {
		eventTransact{}: {
			Next:   stateRunning{fsm.False},
			Action: logEvent(transact),
		},
		eventFeedRow{}: {
			Next:   stateRunning{fsm.False},
			Action: logEvent(noteFeedRow),
		},
		eventFeedResolved{}: {
			Next:   stateRunning{fsm.False},
			Action: logEvent(noteFeedResolved),
		},
		eventPause{}: {
			Next:   stateRunning{fsm.True},
			Action: logEvent(pause),
		},
	},
	stateRunning{fsm.True}: {
		eventResume{}: {
			Next:   stateRunning{fsm.False},
			Action: logEvent(resume),
		},
	},
})

func logEvent(fn func(fsm.Args) error) func(fsm.Args) error {
	return func(a fsm.Args) error {
		if a.Payload == nil {
			log.Infof(a.Ctx, "%#v\n", a.Event)
		} else if m := a.Payload.(*TestFeedMessage); len(m.Resolved) > 0 {
			log.Infof(a.Ctx, `%s`, m.Resolved)
		} else {
			log.Infof(a.Ctx, `%s`, m.Value)
		}
		return fn(a)
	}
}

func noop(a fsm.Args) error {
	return nil
}

func transact(a fsm.Args) error {
	ns := a.Extended.(*nemeses)
	if len(ns.transactions) == 0 || rand.Intn(len(ns.transactions)+ns.rowCount) < ns.rowCount {
		// If there are no transactions, create one. Otherwise create one with a
		// probability that gets smaller as more transactions are in flight.
		txn, err := ns.db.Begin()
		if err != nil {
			return err
		}
		if _, err := txn.Exec(
			`UPSERT INTO foo VALUES ((random() * $1)::int, cluster_logical_timestamp()::string)`,
			ns.rowCount,
		); err != nil {
			return err
		}
		ns.transactions = append(ns.transactions, txn)
		return nil
	}

	// Pop the most recently created transaction.
	txn := ns.transactions[len(ns.transactions)-1]
	ns.transactions = ns.transactions[:len(ns.transactions)-1]

	// Roll it back half and time and commit it the other half.
	if rand.Intn(2) < 1 {
		return txn.Rollback()
	}
	if err := txn.Commit(); err != nil {
		return err
	}
	ns.availableRows++
	return nil
}

func noteFeedRow(a fsm.Args) error {
	ns := a.Extended.(*nemeses)
	m := a.Payload.(*TestFeedMessage)
	ts, _, err := ParseJSONValueTimestamps(m.Value)
	if err != nil {
		return err
	}
	ns.availableRows--
	ns.v.NoteRow(m.Partition, string(m.Key), string(m.Value), ts)
	return nil
}

func noteFeedResolved(a fsm.Args) error {
	ns := a.Extended.(*nemeses)
	m := a.Payload.(*TestFeedMessage)
	_, ts, err := ParseJSONValueTimestamps(m.Resolved)
	if err != nil {
		return err
	}
	return ns.v.NoteResolved(m.Partition, ts)
}

func pause(a fsm.Args) error {
	return a.Extended.(*nemeses).f.Pause()
}

func resume(a fsm.Args) error {
	return a.Extended.(*nemeses).f.Resume()
}
