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
	gosql "database/sql"
	"fmt"
	"math/rand"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/fsm"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
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
		maxTestColumnCount: 10,
		rowCount:           4,
		db:                 db,
		// eventMix does not have to add to 100
		eventMix: map[fsm.Event]int{
			// We don't want `eventFinished` to ever be returned by `nextEvent` so we set
			// its weight to 0.
			eventFinished{}: 0,

			// eventFeedMessage reads a message from the feed, or if the state machine
			// thinks there will be no message available, it falls back to eventOpenTxn or
			// eventCommit (if there is already a txn open).
			eventFeedMessage{}: 50,

			// eventSplit splits between two random rows (the split is a no-op if it
			// already exists).
			eventSplit{}: 5,

			// TRANSACTIONS
			// eventOpenTxn opens an UPSERT or DELETE transaction.
			eventOpenTxn{}: 10,

			// eventCommit commits the outstanding transaction.
			eventCommit{}: 5,

			// eventRollback simply rolls the outstanding transaction back.
			eventRollback{}: 5,

			// eventPush pushes every open transaction by running a high priority SELECT.
			eventPush{}: 5,

			// eventAbort aborts every open transaction by running a high priority DELETE.
			eventAbort{}: 5,

			// PAUSE / RESUME
			// eventPause PAUSEs the changefeed.
			eventPause{}: eventPauseCount,

			// eventResume RESUMEs the changefeed.
			eventResume{}: 50,

			// SCHEMA CHANGES
			// eventAddColumn performs a schema change by adding a new column with a default
			// value in order to trigger a backfill.
			eventAddColumn{
				CanAddColumnAfter: fsm.True,
			}: 5,

			eventAddColumn{
				CanAddColumnAfter: fsm.False,
			}: 5,

			// eventRemoveColumn performs a schema change by removing a column.
			eventRemoveColumn{
				CanRemoveColumnAfter: fsm.True,
			}: 5,

			eventRemoveColumn{
				CanRemoveColumnAfter: fsm.False,
			}: 5,

			// eventCreateEnum creates a new enum type.
			eventCreateEnum{}: 5,
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

	// Initialize table rows by repeatedly running the `openTxn` transition,
	// then randomly either committing or rolling back transactions. This will
	// leave some committed rows.
	for i := 0; i < ns.rowCount*5; i++ {
		payload, err := newOpenTxnPayload(ns)
		if err != nil {
			return nil, err
		}
		if err := openTxn(fsm.Args{Ctx: ctx, Extended: ns, Payload: payload}); err != nil {
			return nil, err
		}
		// Randomly commit or rollback, but commit at least one row to the table.
		if rand.Intn(3) < 2 || i == 0 {
			if err := commit(fsm.Args{Ctx: ctx, Extended: ns}); err != nil {
				return nil, err
			}
		} else {
			if err := rollback(fsm.Args{Ctx: ctx, Extended: ns}); err != nil {
				return nil, err
			}
		}
	}

	foo, err := f.Feed(`CREATE CHANGEFEED FOR foo WITH updated, resolved, diff`)
	if err != nil {
		return nil, err
	}
	ns.f = foo
	defer func() { _ = foo.Close() }()

	// Create scratch table with a pre-specified set of test columns to avoid having to
	// accommodate schema changes on-the-fly.
	scratchTableName := `fprint`
	var createFprintStmtBuf bytes.Buffer
	fmt.Fprintf(&createFprintStmtBuf, `CREATE TABLE %s (id INT PRIMARY KEY, ts STRING)`, scratchTableName)
	if _, err := db.Exec(createFprintStmtBuf.String()); err != nil {
		return nil, err
	}
	baV, err := NewBeforeAfterValidator(db, `foo`)
	if err != nil {
		return nil, err
	}
	fprintV, err := NewFingerprintValidator(db, `foo`, scratchTableName, foo.Partitions(), ns.maxTestColumnCount)
	if err != nil {
		return nil, err
	}
	ns.v = MakeCountValidator(Validators{
		NewOrderValidator(`foo`),
		baV,
		fprintV,
	})

	// Initialize the actual row count, overwriting what the initialization loop did. That
	// loop has set this to the number of modified rows, which is correct during
	// changefeed operation, but not for the initial scan, because some of the rows may
	// have had the same primary key.
	if err := db.QueryRow(`SELECT count(*) FROM foo`).Scan(&ns.availableRows); err != nil {
		return nil, err
	}

	txnOpenBeforeInitialScan := false
	// Maybe open an intent.
	if rand.Intn(2) < 1 {
		txnOpenBeforeInitialScan = true
		payload, err := newOpenTxnPayload(ns)
		if err != nil {
			return nil, err
		}
		if err := openTxn(fsm.Args{Ctx: ctx, Extended: ns, Payload: payload}); err != nil {
			return nil, err
		}
	}

	// Run the state machine until it finishes. Exit criteria is in `nextEvent`
	// and is based on the number of rows that have been resolved and the number
	// of resolved timestamp messages.
	initialState := stateRunning{
		FeedPaused:      fsm.False,
		TxnOpen:         fsm.FromBool(txnOpenBeforeInitialScan),
		CanAddColumn:    fsm.True,
		CanRemoveColumn: fsm.False,
	}
	m := fsm.MakeMachine(compiledStateTransitions, initialState, ns)
	for {
		state := m.CurState()
		if _, ok := state.(stateDone); ok {
			return ns.v, nil
		}
		event, eventPayload, err := ns.nextEvent(rng, state, &m)
		if err != nil {
			return nil, err
		}
		if err := m.ApplyWithPayload(ctx, event, eventPayload); err != nil {
			return nil, err
		}
	}
}

type openTxnType string

const (
	openTxnTypeUpsert openTxnType = `UPSERT`
	openTxnTypeDelete openTxnType = `DELETE`
)

type openTxnPayload struct {
	openTxnType openTxnType

	// rowID is the column ID to operate on.
	rowID int
}

type addColumnType string

const (
	addColumnTypeString addColumnType = "string"
	addColumnTypeEnum   addColumnType = "enum"
)

type addColumnPayload struct {
	columnType addColumnType

	// if columnType is enumColumn, which enum to add
	enum int
}

type nemeses struct {
	rowCount           int
	maxTestColumnCount int
	eventMix           map[fsm.Event]int

	v  *CountValidator
	db *gosql.DB
	f  TestFeed

	availableRows          int
	currentTestColumnCount int
	txn                    *gosql.Tx
	openTxnType            openTxnType
	openTxnID              int
	openTxnTs              string

	enumCount int
}

// nextEvent selects the next state transition.
func (ns *nemeses) nextEvent(
	rng *rand.Rand, state fsm.State, m *fsm.Machine,
) (se fsm.Event, payload fsm.EventPayload, err error) {
	var noPayload interface{}
	if ns.v.NumResolvedWithRows >= 6 && ns.v.NumResolvedRows >= 10 {
		return eventFinished{}, noPayload, nil
	}
	possibleEvents, ok := compiledStateTransitions.GetExpanded()[state]
	if !ok {
		return nil, noPayload, errors.Errorf(`unknown state: %T %s`, state, state)
	}
	mixTotal := 0
	for event := range possibleEvents {
		weight, ok := ns.eventMix[event]
		if !ok {
			return nil, noPayload, errors.Errorf(`unknown event: %T`, event)
		}
		mixTotal += weight
	}
	r, t := rng.Intn(mixTotal), 0
	for event := range possibleEvents {
		t += ns.eventMix[event]
		if r >= t {
			continue
		}
		if _, ok := event.(eventFeedMessage); ok {
			// If there are no available rows, openTxn or commit outstanding txn instead
			// of reading.
			if ns.availableRows < 1 {
				s := state.(stateRunning)
				if s.TxnOpen.Get() {
					return eventCommit{}, noPayload, nil
				}
				payload, err := newOpenTxnPayload(ns)
				if err != nil {
					return eventOpenTxn{}, noPayload, err
				}
				return eventOpenTxn{}, payload, nil
			}
			return eventFeedMessage{}, noPayload, nil
		}
		if _, ok := event.(eventOpenTxn); ok {
			payload, err := newOpenTxnPayload(ns)
			if err != nil {
				return eventOpenTxn{}, noPayload, err
			}
			return eventOpenTxn{}, payload, nil
		}
		if e, ok := event.(eventAddColumn); ok {
			e.CanAddColumnAfter = fsm.FromBool(ns.currentTestColumnCount < ns.maxTestColumnCount-1)
			payload := addColumnPayload{}
			if ns.enumCount > 0 && rng.Intn(4) < 1 {
				payload.columnType = addColumnTypeEnum
				payload.enum = rng.Intn(ns.enumCount)
			} else {
				payload.columnType = addColumnTypeString
			}
			return e, payload, nil
		}
		if e, ok := event.(eventRemoveColumn); ok {
			e.CanRemoveColumnAfter = fsm.FromBool(ns.currentTestColumnCount > 1)
			return e, noPayload, nil
		}
		return event, noPayload, nil
	}

	panic(`unreachable`)
}

func newOpenTxnPayload(ns *nemeses) (openTxnPayload, error) {
	payload := openTxnPayload{}
	if rand.Intn(10) == 0 {
		rows, err := ns.db.Query(`SELECT id FROM foo ORDER BY random() LIMIT 1`)
		if err != nil {
			return payload, err
		}
		defer func() { _ = rows.Close() }()
		if rows.Next() {
			var deleteID int
			if err := rows.Scan(&deleteID); err != nil {
				return payload, err
			}
			payload.rowID = deleteID
			payload.openTxnType = openTxnTypeDelete
			return payload, nil
		}
		if err := rows.Err(); err != nil {
			return payload, err
		}
		// No rows to delete, do an upsert
	}

	payload.rowID = rand.Intn(ns.rowCount)
	payload.openTxnType = openTxnTypeUpsert
	return payload, nil
}

type stateRunning struct {
	FeedPaused      fsm.Bool
	TxnOpen         fsm.Bool
	CanRemoveColumn fsm.Bool
	CanAddColumn    fsm.Bool
}
type stateDone struct{}

func (stateRunning) State() {}
func (stateDone) State()    {}

type eventOpenTxn struct{}
type eventFeedMessage struct{}
type eventPause struct{}
type eventResume struct{}
type eventCommit struct{}
type eventPush struct{}
type eventAbort struct{}
type eventRollback struct{}
type eventSplit struct{}
type eventAddColumn struct {
	CanAddColumnAfter fsm.Bool
}
type eventRemoveColumn struct {
	CanRemoveColumnAfter fsm.Bool
}
type eventCreateEnum struct{}
type eventFinished struct{}

func (eventOpenTxn) Event()      {}
func (eventFeedMessage) Event()  {}
func (eventPause) Event()        {}
func (eventResume) Event()       {}
func (eventCommit) Event()       {}
func (eventPush) Event()         {}
func (eventAbort) Event()        {}
func (eventRollback) Event()     {}
func (eventSplit) Event()        {}
func (eventAddColumn) Event()    {}
func (eventRemoveColumn) Event() {}
func (eventCreateEnum) Event()   {}
func (eventFinished) Event()     {}

var stateTransitions = fsm.Pattern{
	stateRunning{
		FeedPaused:      fsm.Var("FeedPaused"),
		TxnOpen:         fsm.Var("TxnOpen"),
		CanAddColumn:    fsm.Var("CanAddColumn"),
		CanRemoveColumn: fsm.Var("CanRemoveColumn"),
	}: {
		eventSplit{}: {
			Next: stateRunning{
				FeedPaused:      fsm.Var("FeedPaused"),
				TxnOpen:         fsm.Var("TxnOpen"),
				CanAddColumn:    fsm.Var("CanAddColumn"),
				CanRemoveColumn: fsm.Var("CanRemoveColumn")},
			Action: logEvent(split),
		},
		eventFinished{}: {
			Next:   stateDone{},
			Action: logEvent(cleanup),
		},
	},
	stateRunning{
		FeedPaused:      fsm.Var("FeedPaused"),
		TxnOpen:         fsm.False,
		CanAddColumn:    fsm.True,
		CanRemoveColumn: fsm.Any,
	}: {
		eventAddColumn{
			CanAddColumnAfter: fsm.Var("CanAddColumnAfter"),
		}: {
			Next: stateRunning{
				FeedPaused:      fsm.Var("FeedPaused"),
				TxnOpen:         fsm.False,
				CanAddColumn:    fsm.Var("CanAddColumnAfter"),
				CanRemoveColumn: fsm.True},
			Action: logEvent(addColumn),
		},
	},
	stateRunning{
		FeedPaused:      fsm.Var("FeedPaused"),
		TxnOpen:         fsm.False,
		CanAddColumn:    fsm.Any,
		CanRemoveColumn: fsm.True,
	}: {
		eventRemoveColumn{
			CanRemoveColumnAfter: fsm.Var("CanRemoveColumnAfter"),
		}: {
			Next: stateRunning{
				FeedPaused:      fsm.Var("FeedPaused"),
				TxnOpen:         fsm.False,
				CanAddColumn:    fsm.True,
				CanRemoveColumn: fsm.Var("CanRemoveColumnAfter")},
			Action: logEvent(removeColumn),
		},
	},
	stateRunning{
		FeedPaused:      fsm.False,
		TxnOpen:         fsm.False,
		CanAddColumn:    fsm.Var("CanAddColumn"),
		CanRemoveColumn: fsm.Var("CanRemoveColumn"),
	}: {
		eventFeedMessage{}: {
			Next: stateRunning{
				FeedPaused:      fsm.False,
				TxnOpen:         fsm.False,
				CanAddColumn:    fsm.Var("CanAddColumn"),
				CanRemoveColumn: fsm.Var("CanRemoveColumn")},
			Action: logEvent(noteFeedMessage),
		},
	},
	stateRunning{
		FeedPaused:      fsm.Var("FeedPaused"),
		TxnOpen:         fsm.False,
		CanAddColumn:    fsm.Var("CanAddColumn"),
		CanRemoveColumn: fsm.Var("CanRemoveColumn"),
	}: {
		eventOpenTxn{}: {
			Next: stateRunning{
				FeedPaused:      fsm.Var("FeedPaused"),
				TxnOpen:         fsm.True,
				CanAddColumn:    fsm.Var("CanAddColumn"),
				CanRemoveColumn: fsm.Var("CanRemoveColumn")},
			Action: logEvent(openTxn),
		},
		eventCreateEnum{}: {
			Next: stateRunning{
				FeedPaused:      fsm.Var("FeedPaused"),
				TxnOpen:         fsm.False,
				CanAddColumn:    fsm.Var("CanAddColumn"),
				CanRemoveColumn: fsm.Var("CanRemoveColumn"),
			},
			Action: logEvent(createEnum),
		},
	},
	stateRunning{
		FeedPaused:      fsm.Var("FeedPaused"),
		TxnOpen:         fsm.True,
		CanAddColumn:    fsm.Var("CanAddColumn"),
		CanRemoveColumn: fsm.Var("CanRemoveColumn"),
	}: {
		eventCommit{}: {
			Next: stateRunning{
				FeedPaused:      fsm.Var("FeedPaused"),
				TxnOpen:         fsm.False,
				CanAddColumn:    fsm.Var("CanAddColumn"),
				CanRemoveColumn: fsm.Var("CanRemoveColumn")},
			Action: logEvent(commit),
		},
		eventRollback{}: {
			Next: stateRunning{
				FeedPaused:      fsm.Var("FeedPaused"),
				TxnOpen:         fsm.False,
				CanAddColumn:    fsm.Var("CanAddColumn"),
				CanRemoveColumn: fsm.Var("CanRemoveColumn")},
			Action: logEvent(rollback),
		},
		eventAbort{}: {
			Next: stateRunning{
				FeedPaused:      fsm.Var("FeedPaused"),
				TxnOpen:         fsm.True,
				CanAddColumn:    fsm.Var("CanAddColumn"),
				CanRemoveColumn: fsm.Var("CanRemoveColumn")},
			Action: logEvent(abort),
		},
		eventPush{}: {
			Next: stateRunning{
				FeedPaused:      fsm.Var("FeedPaused"),
				TxnOpen:         fsm.True,
				CanAddColumn:    fsm.Var("CanAddColumn"),
				CanRemoveColumn: fsm.Var("CanRemoveColumn")},
			Action: logEvent(push),
		},
	},
	stateRunning{
		FeedPaused:      fsm.False,
		TxnOpen:         fsm.Var("TxnOpen"),
		CanAddColumn:    fsm.Var("CanAddColumn"),
		CanRemoveColumn: fsm.Var("CanRemoveColumn"),
	}: {
		eventPause{}: {
			Next: stateRunning{
				FeedPaused:      fsm.True,
				TxnOpen:         fsm.Var("TxnOpen"),
				CanAddColumn:    fsm.Var("CanAddColumn"),
				CanRemoveColumn: fsm.Var("CanRemoveColumn")},
			Action: logEvent(pause),
		},
	},
	stateRunning{
		FeedPaused:      fsm.True,
		TxnOpen:         fsm.Var("TxnOpen"),
		CanAddColumn:    fsm.Var("CanAddColumn"),
		CanRemoveColumn: fsm.Var("CanRemoveColumn"),
	}: {
		eventResume{}: {
			Next: stateRunning{
				FeedPaused:      fsm.False,
				TxnOpen:         fsm.Var("TxnOpen"),
				CanAddColumn:    fsm.Var("CanAddColumn"),
				CanRemoveColumn: fsm.Var("CanRemoveColumn")},
			Action: logEvent(resume),
		},
	},
}

var compiledStateTransitions = fsm.Compile(stateTransitions)

func logEvent(fn func(fsm.Args) error) func(fsm.Args) error {
	return func(a fsm.Args) error {
		log.Infof(a.Ctx, "Event: %#v, Payload: %#v\n", a.Event, a.Payload)
		return fn(a)
	}
}

func cleanup(a fsm.Args) error {
	if txn := a.Extended.(*nemeses).txn; txn != nil {
		return txn.Rollback()
	}
	return nil
}

func openTxn(a fsm.Args) error {
	ns := a.Extended.(*nemeses)
	payload := a.Payload.(openTxnPayload)

	txn, err := ns.db.Begin()
	if err != nil {
		return err
	}
	switch payload.openTxnType {
	case openTxnTypeUpsert:
		if err := txn.QueryRow(
			`UPSERT INTO foo VALUES ($1, cluster_logical_timestamp()::string) RETURNING id, ts`,
			payload.rowID,
		).Scan(&ns.openTxnID, &ns.openTxnTs); err != nil {
			return err
		}
	case openTxnTypeDelete:
		if err := txn.QueryRow(
			`DELETE FROM foo WHERE id = $1 RETURNING id, ts`,
			payload.rowID,
		).Scan(&ns.openTxnID, &ns.openTxnTs); err != nil {
			return err
		}
	default:
		panic("unreachable")
	}
	ns.openTxnType = payload.openTxnType
	ns.txn = txn
	return nil
}

func commit(a fsm.Args) error {
	ns := a.Extended.(*nemeses)
	defer func() { ns.txn = nil }()
	if err := ns.txn.Commit(); err != nil {
		// Don't error out if we got pushed, but don't increment availableRows no
		// matter what error was hit.
		if strings.Contains(err.Error(), `restart transaction`) {
			return nil
		}
	}
	ns.availableRows++
	return nil
}

func rollback(a fsm.Args) error {
	ns := a.Extended.(*nemeses)
	defer func() { ns.txn = nil }()
	return ns.txn.Rollback()
}

func createEnum(a fsm.Args) error {
	ns := a.Extended.(*nemeses)

	if _, err := ns.db.Exec(fmt.Sprintf(`CREATE TYPE enum%d AS ENUM ('hello')`, ns.enumCount)); err != nil {
		return err
	}
	ns.enumCount++
	return nil
}

func addColumn(a fsm.Args) error {
	ns := a.Extended.(*nemeses)
	payload := a.Payload.(addColumnPayload)

	if ns.currentTestColumnCount >= ns.maxTestColumnCount {
		return errors.AssertionFailedf(`addColumn should be called when`+
			`there are less than %d columns.`, ns.maxTestColumnCount)
	}

	switch payload.columnType {
	case addColumnTypeEnum:
		// Pick a random enum to add.
		enum := payload.enum
		if _, err := ns.db.Exec(fmt.Sprintf(`ALTER TABLE foo ADD COLUMN test%d enum%d DEFAULT 'hello'`,
			ns.currentTestColumnCount, enum)); err != nil {
			return err
		}
	case addColumnTypeString:
		if _, err := ns.db.Exec(fmt.Sprintf(`ALTER TABLE foo ADD COLUMN test%d STRING DEFAULT 'x'`,
			ns.currentTestColumnCount)); err != nil {
			return err
		}
	}

	ns.currentTestColumnCount++
	var rows int
	// Adding a column should trigger a full table scan.
	if err := ns.db.QueryRow(`SELECT count(*) FROM foo`).Scan(&rows); err != nil {
		return err
	}
	// We expect one table scan that corresponds to the schema change backfill, and one
	// scan that corresponds to the changefeed level backfill.
	ns.availableRows += 2 * rows
	return nil
}

func removeColumn(a fsm.Args) error {
	ns := a.Extended.(*nemeses)

	if ns.currentTestColumnCount == 0 {
		return errors.AssertionFailedf(`removeColumn should be called with` +
			`at least one test column.`)
	}
	if _, err := ns.db.Exec(fmt.Sprintf(`ALTER TABLE foo DROP COLUMN test%d`,
		ns.currentTestColumnCount-1)); err != nil {
		return err
	}
	ns.currentTestColumnCount--
	var rows int
	// Dropping a column should trigger a full table scan.
	if err := ns.db.QueryRow(`SELECT count(*) FROM foo`).Scan(&rows); err != nil {
		return err
	}
	// We expect one table scan that corresponds to the schema change backfill, and one
	// scan that corresponds to the changefeed level backfill.
	ns.availableRows += 2 * rows
	return nil
}

func noteFeedMessage(a fsm.Args) error {
	ns := a.Extended.(*nemeses)

	if ns.availableRows <= 0 {
		return errors.AssertionFailedf(`noteFeedMessage should be called with at` +
			`least one available row.`)
	}
	for {
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
			log.Infof(a.Ctx, "%v", string(m.Resolved))
			err = ns.v.NoteResolved(m.Partition, ts)
			if err != nil {
				return err
			}
			// Keep consuming until we hit a row
		} else {
			ts, _, err := ParseJSONValueTimestamps(m.Value)
			if err != nil {
				return err
			}
			ns.availableRows--
			log.Infof(a.Ctx, "%s->%s", m.Key, m.Value)
			return ns.v.NoteRow(m.Partition, string(m.Key), string(m.Value), ts)
		}
	}
}

func pause(a fsm.Args) error {
	return a.Extended.(*nemeses).f.(EnterpriseTestFeed).Pause()
}

func resume(a fsm.Args) error {
	return a.Extended.(*nemeses).f.(EnterpriseTestFeed).Resume()
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

func push(a fsm.Args) error {
	ns := a.Extended.(*nemeses)
	_, err := ns.db.Exec(`BEGIN TRANSACTION PRIORITY HIGH; SELECT * FROM foo; COMMIT`)
	return err
}

func split(a fsm.Args) error {
	ns := a.Extended.(*nemeses)
	_, err := ns.db.Exec(`ALTER TABLE foo SPLIT AT VALUES ((random() * $1)::int)`, ns.rowCount)
	return err
}
