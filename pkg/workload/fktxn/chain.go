// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fktxn

import (
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/util/fsm"
	"github.com/cockroachdb/errors"
)

// chainStateUnknown is the entry state for a fresh chain. We don't yet know
// whether a row at the chain's PK exists in the database (another worker may
// have written one previously), so both Upsert and Delete are valid first
// events.
type chainStateUnknown struct{}

// chainStateNone means the row chain identified by the chain's PK assignment
// definitely does not exist in the database. The only valid next event is
// eventUpsert.
type chainStateNone struct{}

// chainStateExists means the row chain has been written and not deleted.
// Both eventUpsert (re-write the row, mutating its UC and FK columns) and
// eventDelete are valid.
type chainStateExists struct{}

func (chainStateUnknown) State() {}
func (chainStateNone) State()    {}
func (chainStateExists) State()  {}

// eventUpsert writes (or re-writes) the entire row chain in topological
// order. Allowed from any state. From Unknown/None it creates the row chain
// for the first time; from Exists it overwrites the existing chain, which
// re-rolls non-PK FK and UC columns and produces a real replicable diff.
type eventUpsert struct{}

// eventDelete removes the entire row chain (children before parents). The
// walk is a no-op if the row doesn't exist, so Delete is allowed from any
// state — Unknown collapses to None when the row was missing, or Exists →
// None when it was present.
type eventDelete struct{}

func (eventUpsert) Event() {}
func (eventDelete) Event() {}

// chainExtended is the per-chain state passed through fsm.Args.Extended to
// each action function. The sub-DAG, dropped edges, pools, and RNG are
// stable for the lifetime of the chain. pkIdx is also stable — it identifies
// the chain's PK row in pools.PKs[T] for every table T in the sub-DAG. The
// transaction handle is the chain's single tx; FSM actions write through it.
// Typed as dbTx so a recording wrapper can be substituted by tests;
// production sets it to a *sql.Tx.
//
// repointFKs tells runUpsert whether to roll fresh parent indexes for non-PK
// FK columns (true on UPSERT-as-update, when the chain's parent rows already
// exist in the database) or pin them to pkIdx (false on the chain's first
// UPSERT, when the parent rows are being written in the same walk and only
// exist at pkIdx). The worker sets it from the FSM's pre-transition state.
type chainExtended struct {
	tx         dbTx
	rng        *rand.Rand
	sorted     []*Table
	sub        *FKGraph
	dropped    []FKEdge
	pools      *Pools
	pkIdx      int
	repointFKs bool
}

// chainTransitions defines the valid (state, event) → (next state, action)
// table for the per-chain row lifecycle. Transitions match the README's
// Unknown/None/Exists state machine:
//
//	Unknown + Upsert → Exists (insert; if conflict, retry path pivots)
//	Unknown + Delete → None   (delete-if-exists; no-op when row absent)
//	None    + Upsert → Exists
//	Exists  + Upsert → Exists (re-write; mutates UC and non-PK FK columns)
//	Exists  + Delete → None
//
// Failed actions return an error and leave the state unchanged so the
// caller can break the chain cleanly.
var chainTransitions = fsm.Compile(fsm.Pattern{
	chainStateUnknown{}: {
		eventUpsert{}: {
			Next:        chainStateExists{},
			Action:      runUpsert,
			Description: "insert chain",
		},
		eventDelete{}: {
			Next:        chainStateNone{},
			Action:      runDelete,
			Description: "delete chain (no-op if absent)",
		},
	},
	chainStateNone{}: {
		eventUpsert{}: {
			Next:        chainStateExists{},
			Action:      runUpsert,
			Description: "insert chain",
		},
	},
	chainStateExists{}: {
		eventUpsert{}: {
			Next:        chainStateExists{},
			Action:      runUpsert,
			Description: "re-write chain (mutate UC/FK columns)",
		},
		eventDelete{}: {
			Next:        chainStateNone{},
			Action:      runDelete,
			Description: "delete chain",
		},
	},
})

func runUpsert(a fsm.Args) error {
	c, ok := a.Extended.(*chainExtended)
	if !ok {
		return errors.AssertionFailedf("chain action: bad extended state %T", a.Extended)
	}
	_, err := ExecuteUpsert(
		a.Ctx, c.tx, c.rng, c.sorted, c.sub, c.dropped, c.pools, c.pkIdx, c.repointFKs,
	)
	return err
}

func runDelete(a fsm.Args) error {
	c, ok := a.Extended.(*chainExtended)
	if !ok {
		return errors.AssertionFailedf("chain action: bad extended state %T", a.Extended)
	}
	_, err := ExecuteDelete(a.Ctx, c.tx, c.sorted, c.sub, c.pools, c.pkIdx)
	return err
}

// OpMix weights the choice between Upsert and Delete when the chain is in a
// state that accepts both (Unknown and Exists). None forces Upsert (the only
// valid event). Higher Delete weight ends chains sooner; higher Upsert weight
// keeps the chain alive longer, producing more UPSERT-as-update events that
// mutate the row's UC and FK columns.
type OpMix struct {
	Upsert int
	Delete int
}

// pickEvent selects the next event to apply given the current state. None
// always advances via Upsert (no choice). Unknown and Exists sample Upsert
// vs Delete by weight.
func (m OpMix) pickEvent(rng *rand.Rand, state fsm.State) fsm.Event {
	if _, none := state.(chainStateNone); none {
		return eventUpsert{}
	}
	total := m.Upsert + m.Delete
	if total <= 0 {
		// Defensive: caller passed an empty mix. Default to Delete so the
		// chain at least advances out of its current state.
		return eventDelete{}
	}
	if rng.Intn(total) < m.Upsert {
		return eventUpsert{}
	}
	return eventDelete{}
}
