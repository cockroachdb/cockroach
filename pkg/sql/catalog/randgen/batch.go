// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package randgen

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
)

// newDesc queues a freshly created descriptor for (later) write to KV.
// It ensures a namespace entry will also be created.
func (g *testSchemaGenerator) newDesc(ctx context.Context, desc catalog.MutableDescriptor) {
	if g.cfg.DryRun {
		return
	}
	g.newDescs = append(g.newDescs, desc)
	g.queueDescMut(ctx, desc)
}

// queueDesc queues a modified descriptor for (later) write to KV.
func (g *testSchemaGenerator) queueDescMut(ctx context.Context, desc catalog.MutableDescriptor) {
	if g.cfg.DryRun {
		return
	}
	g.queuedDescs = append(g.queuedDescs, desc)
	if len(g.queuedDescs) > g.cfg.BatchSize {
		g.flush(ctx)
	}
}

// flush writes pending descriptors to KV, while respecting the batch
// configuration.
func (g *testSchemaGenerator) flush(ctx context.Context) {
	if g.cfg.DryRun {
		return
	}
	if len(g.queuedDescs) == 0 {
		return
	}
	for _, desc := range g.queuedDescs {
		if err := g.coll.WriteDescToBatch(ctx, g.kvTrace, desc, g.b); err != nil {
			panic(genError{err})
		}
	}
	for _, desc := range g.newDescs {
		if err := g.coll.InsertNamespaceEntryToBatch(ctx, g.kvTrace, desc, g.b); err != nil {
			panic(genError{err})
		}
	}
	if err := g.txn.Run(ctx, g.b); err != nil {
		panic(genError{err})
	}
	g.b = g.txn.NewBatch()
	g.queuedDescs = g.queuedDescs[:0]
	g.newDescs = g.newDescs[:0]
}

// reset starts a new generation round.
func (g *testSchemaGenerator) reset() {
	g.cfg.GeneratedCounts.Databases = 0
	g.cfg.GeneratedCounts.Schemas = 0
	g.cfg.GeneratedCounts.Tables = 0
	g.queuedDescs = nil
	g.newDescs = nil
	g.b = g.txn.NewBatch()
}
