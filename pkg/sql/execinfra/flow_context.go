// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// This file lives here instead of sql/flowinfra to avoid an import cycle.

package execinfra

import (
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// FlowCtx encompasses the configuration parameters needed for various flow
// components.
type FlowCtx struct {
	log.AmbientContext

	Cfg *ServerConfig

	// ID is a unique identifier for a remote flow. It is mainly used as a key
	// into the flowRegistry. Since local flows do not need to exist in the flow
	// registry (no inbound stream connections need to be performed), they are not
	// assigned ids. This is done for performance reasons, as local flows are
	// more likely to be dominated by setup time.
	ID execinfrapb.FlowID

	// EvalCtx is used by all the processors in the flow to evaluate expressions.
	// Processors that intend to evaluate expressions with this EvalCtx should
	// get a copy with NewEvalCtx instead of storing a pointer to this one
	// directly (since some processor mutate the EvalContext they use).
	//
	// TODO(andrei): Get rid of this field and pass a non-shared EvalContext to
	// cores of the processors that need it.
	EvalCtx *tree.EvalContext

	// The transaction in which kv operations performed by processors in the flow
	// must be performed. Processors in the Flow will use this txn concurrently.
	// This field is generally not nil, except for flows that don't run in a
	// higher-level txn (like backfills).
	Txn *kv.Txn

	// nodeID is the ID of the node on which the processors using this FlowCtx
	// run.
	NodeID roachpb.NodeID

	// TraceKV is true if KV tracing was requested by the session.
	TraceKV bool

	// Local is true if this flow is being run as part of a local-only query.
	Local bool
}

// NewEvalCtx returns a modifiable copy of the FlowCtx's EvalContext.
// Processors should use this method any time they need to store a pointer to
// the EvalContext, since processors may mutate the EvalContext. Specifically,
// every processor that runs ProcOutputHelper.Init must pass in a modifiable
// EvalContext, since it stores that EvalContext in its exprHelpers and mutates
// them at runtime to ensure expressions are evaluated with the correct indexed
// var context.
func (ctx *FlowCtx) NewEvalCtx() *tree.EvalContext {
	return ctx.EvalCtx.Copy()
}

// TestingKnobs returns the distsql testing knobs for this flow context.
func (ctx *FlowCtx) TestingKnobs() TestingKnobs {
	return ctx.Cfg.TestingKnobs
}

// Stopper returns the stopper for this flowCtx.
func (ctx *FlowCtx) Stopper() *stop.Stopper {
	return ctx.Cfg.Stopper
}
