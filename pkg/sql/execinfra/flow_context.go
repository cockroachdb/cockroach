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
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
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
	NodeID *base.SQLIDContainer

	// TraceKV is true if KV tracing was requested by the session.
	TraceKV bool

	// CollectStats is true if execution stats collection was requested.
	CollectStats bool

	// Local is true if this flow is being run as part of a local-only query.
	Local bool

	// Gateway is true if this flow is being run on the gateway node.
	Gateway bool

	// TypeResolverFactory is used to construct transaction bound TypeResolvers
	// to resolve type references during flow setup. It is not safe for concurrent
	// use and is intended to be used only during flow setup and initialization.
	// The TypeResolverFactory is initialized when the FlowContext is created
	// on the gateway node using the planner's descs.Collection and is created
	// on remote nodes with a new descs.Collection. After the flow is complete,
	// all descriptors leased from the factory must be released.
	TypeResolverFactory *descs.DistSQLTypeResolverFactory

	// DiskMonitor is this flow's disk monitor. All disk usage for this flow must
	// be registered through this monitor.
	DiskMonitor *mon.BytesMonitor
}

// NewEvalCtx returns a modifiable copy of the FlowCtx's EvalContext.
// Processors should use this method any time they need to store a pointer to
// the EvalContext, since processors may mutate the EvalContext. Specifically,
// every processor that runs ProcOutputHelper.Init must pass in a modifiable
// EvalContext, since it stores that EvalContext in its exprHelpers and mutates
// them at runtime to ensure expressions are evaluated with the correct indexed
// var context.
func (ctx *FlowCtx) NewEvalCtx() *tree.EvalContext {
	evalCopy := ctx.EvalCtx.Copy()
	return evalCopy
}

// TestingKnobs returns the distsql testing knobs for this flow context.
func (ctx *FlowCtx) TestingKnobs() TestingKnobs {
	return ctx.Cfg.TestingKnobs
}

// Stopper returns the stopper for this flowCtx.
func (ctx *FlowCtx) Stopper() *stop.Stopper {
	return ctx.Cfg.Stopper
}

// Codec returns the SQL codec for this flowCtx.
func (ctx *FlowCtx) Codec() keys.SQLCodec {
	return ctx.EvalCtx.Codec
}

// ProcessorComponentID returns a ComponentID for the given processor in this
// flow.
func (ctx *FlowCtx) ProcessorComponentID(procID int32) execinfrapb.ComponentID {
	return execinfrapb.ProcessorComponentID(ctx.NodeID.SQLInstanceID(), ctx.ID, procID)
}

// StreamComponentID returns a ComponentID for the given stream in this flow.
// The stream must originate from the node associated with this FlowCtx.
func (ctx *FlowCtx) StreamComponentID(streamID execinfrapb.StreamID) execinfrapb.ComponentID {
	return execinfrapb.StreamComponentID(ctx.NodeID.SQLInstanceID(), ctx.ID, streamID)
}
