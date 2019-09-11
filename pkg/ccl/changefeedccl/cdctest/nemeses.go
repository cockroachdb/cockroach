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
	"strings"

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
func RunNemesis(f TestFeedFactory, db *gosql.DB, isSinkless bool) (Validator, error) {
	// possible additional nemeses:
	// - schema changes
	// - merges
	// - rebalancing
	// - lease transfers
	// - receiving snapshots
	// mostly redundant with the pause/unpause nemesis, but might be nice to have:
	// - crdb chaos
	// - sink chaos

	ctx := context.Background()
	rng, _ := randutil.NewPseudoRand()

	eventPauseCount := 10
	if isSinkless {
		// Disable eventPause for sinkless changefeeds because we currently do not
		// have "correct" pause and unpause mechanisms for changefeeds that aren't
		// based on the jobs infrastructure. Enabling it for sinkless might require
		// using "AS OF SYSTEM TIME" for sinkless changefeeds. See #41006 for more
		// details.
		eventPauseCount = 0
	}
	ns := &nemeses{
		rowCount: 4,
		db:       db,
		// eventMix does not have to add to 100
		eventMix: map[fsm.Event]int{
			// eventTransact opens an UPSERT transaction is there is not one open. If
			// there is one open, it either commits it or rolls it back.
			eventTransact{}: 50,

			// eventFeedMessage reads a message from the feed, or if the state machine
			// thinks there will be no message available, it falls back to
			// eventTransact.
			eventFeedMessage{}: 50,

			// eventPause PAUSEs the changefeed. The state machine will handle
			// RESUMEing it.
			eventPause{}: eventPauseCount,

			// eventPush pushes every open transaction by running a high priority
			// SELECT.
			eventPush{}: 5,

			// eventAbort aborts every open transaction by running a high priority
			// DELETE.
			eventAbort{}: 5,

			// eventSplit splits between two random rows (the split is a no-op if it
			// already exists).
			eventSplit{}: 5,
		},
	}

	// Create the table and set up some initial splits.
	if _, err := db.Exec(`CREATE TABLE foo (id INT PRIMARY KEY, ts STRING DEFAULT '0')`); err != nil {
		return nil, err
	}
	if _, err := db.Exec(`SET CLUSTER SETTING kv.range_merge.queue_enabled = false`); err != nil {
		return nil, err
	}
	if _, err := db.Exec(`ALTER TABLE foo SPLIT AT VALUES ($1)`, ns.rowCount/2); err != nil {
		return nil, err
	}

	// Initialize table rows by repeatedly running the `transact` transition,
	// which randomly starts, commits, and rolls back transactions. This will
	// leave some committed rows and maybe an outstanding intent for the initial
	// scan.
	for i := 0; i < ns.rowCount*5; i++ {
		if err := transact(fsm.Args{Ctx: ctx, Extended: ns}); err != nil {
			return nil, err
		}
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
	fprintV, err := NewFingerprintValidator(db, `foo`, `fprint`, foo.Partitions())
	if err != nil {
		return nil, err
	}
	ns.v = MakeCountValidator(Validators{
		NewOrderValidator(`foo`),
		fprintV,
	})

	// Initialize the actual row count, overwriting what `transact` did.
	// `transact` has set this to the number of modified rows, which is correct
	// during changefeed operation, but not for the initial scan, because some of
	// the rows may have had the same primary key.
	if err := db.QueryRow(`SELECT count(*) FROM foo`).Scan(&ns.availableRows); err != nil {
		return nil, err
	}

	// Kick everything off by reading the first message. This accomplishes two
	// things. First, it maximizes the chance that we hit an unresolved intent
	// during the initial scan. Second, it guarantees that the feed is running
	// before anything else commits, which could mess up the availableRows count
	// we just set.
	if err := noteFeedMessage(fsm.Args{Ctx: ctx, Extended: ns}); err != nil {
		return nil, err
	}
	// Now push everything to make sure the initial scan can complete, otherwise
	// we may deadlock.
	if err := push(fsm.Args{Ctx: ctx, Extended: ns}); err != nil {
		return nil, err
	}

	// Run the state machine until it finishes. Exit criteria is in `nextEvent`
	// and is based on the number of rows that have been resolved and the number
	// of resolved timestamp messages.
	m := fsm.MakeMachine(txnStateTransitions, stateRunning{fsm.False}, ns)
	for {
		state := m.CurState()
		if _, ok := state.(stateDone); ok {
			return ns.v, nil
		}
		event, err := ns.nextEvent(rng, state, foo)
		if err != nil {
			return nil, err
		}
		if err := m.Apply(ctx, event); err != nil {
			return nil, err
		}
	}
}

type openTxnType string

const (
	openTxnTypeUpsert openTxnType = `UPSERT`
	openTxnTypeDelete openTxnType = `DELETE`
)

type nemeses struct {
	rowCount int
	eventMix map[fsm.Event]int
	mixTotal int

	v  *CountValidator
	db *gosql.DB
	f  TestFeed

	availableRows int
	txn           *gosql.Tx
	openTxnType   openTxnType
	openTxnID     int
	openTxnTs     string
}

// nextEvent selects the next state transition.
func (ns *nemeses) nextEvent(rng *rand.Rand, state fsm.State, f TestFeed) (fsm.Event, error) {
	if ns.v.NumResolvedWithRows >= 6 && ns.v.NumResolvedRows >= 10 {
		return eventFinished{}, nil
	}

	if ns.mixTotal == 0 {
		for _, weight := range ns.eventMix {
			ns.mixTotal += weight
		}
	}

	switch state {
	case stateRunning{Paused: fsm.True}:
		return eventResume{}, nil
	case stateRunning{Paused: fsm.False}:
		r, t := rng.Intn(ns.mixTotal), 0
		for event, weight := range ns.eventMix {
			t += weight
			if r >= t {
				continue
			}
			if _, ok := event.(eventFeedMessage); ok {
				break
			}
			return event, nil
		}

		// If there are no available rows, transact instead of reading.
		if ns.availableRows < 1 {
			return eventTransact{}, nil
		}
		return eventFeedMessage{}, nil
	default:
		return nil, errors.Errorf(`unknown state: %T %s`, state, state)
	}
}

type stateRunning struct {
	Paused fsm.Bool
}
type stateDone struct{}

func (stateRunning) State() {}
func (stateDone) State()    {}

type eventTransact struct{}
type eventFeedMessage struct{}
type eventPause struct{}
type eventResume struct{}
type eventPush struct{}
type eventAbort struct{}
type eventSplit struct{}
type eventFinished struct{}

func (eventTransact) Event()    {}
func (eventFeedMessage) Event() {}
func (eventPause) Event()       {}
func (eventResume) Event()      {}
func (eventPush) Event()        {}
func (eventAbort) Event()       {}
func (eventSplit) Event()       {}
func (eventFinished) Event()    {}

var txnStateTransitions = fsm.Compile(fsm.Pattern{
	stateRunning{Paused: fsm.Any}: {
		eventFinished{}: {
			Next:   stateDone{},
			Action: logEvent(cleanup),
		},
	},
	stateRunning{Paused: fsm.False}: {
		eventPause{}: {
			Next:   stateRunning{Paused: fsm.True},
			Action: logEvent(pause),
		},
		eventTransact{}: {
			Next:   stateRunning{Paused: fsm.False},
			Action: logEvent(transact),
		},
		eventFeedMessage{}: {
			Next:   stateRunning{Paused: fsm.False},
			Action: logEvent(noteFeedMessage),
		},
		eventPush{}: {
			Next:   stateRunning{Paused: fsm.False},
			Action: logEvent(push),
		},
		eventAbort{}: {
			Next:   stateRunning{Paused: fsm.False},
			Action: logEvent(abort),
		},
		eventSplit{}: {
			Next:   stateRunning{Paused: fsm.False},
			Action: logEvent(split),
		},
	},
	stateRunning{Paused: fsm.True}: {
		eventResume{}: {
			Next:   stateRunning{Paused: fsm.False},
			Action: logEvent(resume),
		},
	},
})

func logEvent(fn func(fsm.Args) error) func(fsm.Args) error {
	return func(a fsm.Args) error {
		log.Infof(a.Ctx, "%#v\n", a.Event)
		return fn(a)
	}
}

func cleanup(a fsm.Args) error {
	if txn := a.Extended.(*nemeses).txn; txn != nil {
		return txn.Rollback()
	}
	return nil
}

func transact(a fsm.Args) error {
	ns := a.Extended.(*nemeses)

	// If there are no transactions, create one.
	if ns.txn == nil {
		const noDeleteSentinel = int(-1)
		// 10% of the time attempt a DELETE.
		deleteID := noDeleteSentinel
		if rand.Intn(10) == 0 {
			rows, err := ns.db.Query(`SELECT id FROM foo ORDER BY random() LIMIT 1`)
			if err != nil {
				return err
			}
			defer func() { _ = rows.Close() }()
			if rows.Next() {
				if err := rows.Scan(&deleteID); err != nil {
					return err
				}
			}
			// If there aren't any rows, skip the DELETE this time.
		}

		txn, err := ns.db.Begin()
		if err != nil {
			return err
		}
		if deleteID == noDeleteSentinel {
			if err := txn.QueryRow(
				`UPSERT INTO foo VALUES ((random() * $1)::int, cluster_logical_timestamp()::string) RETURNING *`,
				ns.rowCount,
			).Scan(&ns.openTxnID, &ns.openTxnTs); err != nil {
				return err
			}
			ns.openTxnType = openTxnTypeUpsert
		} else {
			if err := txn.QueryRow(
				`DELETE FROM foo WHERE id = $1 RETURNING *`, deleteID,
			).Scan(&ns.openTxnID, &ns.openTxnTs); err != nil {
				return err
			}
			ns.openTxnType = openTxnTypeDelete
		}
		ns.txn = txn
		return nil
	}

	// If there is an outstanding transaction, roll it back half the time and
	// commit it the other half.
	txn := ns.txn
	ns.txn = nil

	if rand.Intn(2) < 1 {
		return txn.Rollback()
	}
	if err := txn.Commit(); err != nil {
		// Don't error out if we got pushed, but don't increment availableRows no
		// matter what error was hit.
		if strings.Contains(err.Error(), `restart transaction`) {
			return nil
		}
		return err
	}
	log.Infof(a.Ctx, "%s (%d, %s)", ns.openTxnType, ns.openTxnID, ns.openTxnTs)
	ns.availableRows++
	return nil
}

func noteFeedMessage(a fsm.Args) error {
	ns := a.Extended.(*nemeses)

	m, err := ns.f.Next()
	if err != nil {
		return err
	} else if m == nil {
		return errors.Errorf(`expected another message`)
	}

	if len(m.Resolved) > 0 {
		_, ts, err := ParseJSONValueTimestamps(m.Resolved)
		if err != nil {
			return err
		}
		log.Info(a.Ctx, string(m.Resolved))
		return ns.v.NoteResolved(m.Partition, ts)
	}
	ts, _, err := ParseJSONValueTimestamps(m.Value)
	if err != nil {
		return err
	}

	ns.availableRows--
	log.Infof(a.Ctx, "%s->%s", m.Key, m.Value)
	ns.v.NoteRow(m.Partition, string(m.Key), string(m.Value), ts)
	return nil
}

func pause(a fsm.Args) error {
	return a.Extended.(*nemeses).f.Pause()
}

func resume(a fsm.Args) error {
	return a.Extended.(*nemeses).f.Resume()
}

func push(a fsm.Args) error {
	ns := a.Extended.(*nemeses)
	_, err := ns.db.Exec(`BEGIN TRANSACTION PRIORITY HIGH; SELECT * FROM foo; COMMIT`)
	return err
}

func abort(a fsm.Args) error {
	ns := a.Extended.(*nemeses)
	const delete = `BEGIN TRANSACTION PRIORITY HIGH; ` +
		`SELECT count(*) FROM [DELETE FROM foo RETURNING *]; ` +
		`COMMIT`
	var deletedRows int
	if err := ns.db.QueryRow(delete).Scan(&deletedRows); err != nil {
		return err
	}
	ns.availableRows += deletedRows
	return nil
}

func split(a fsm.Args) error {
	ns := a.Extended.(*nemeses)
	_, err := ns.db.Exec(`ALTER TABLE foo SPLIT AT VALUES ((random() * $1)::int)`, ns.rowCount)
	return err
}
