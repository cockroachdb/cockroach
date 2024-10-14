// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	g.output.newDescs = append(g.output.newDescs, desc)
	g.queueDescMut(ctx, desc)
}

// queueDesc queues a modified descriptor for (later) write to KV.
func (g *testSchemaGenerator) queueDescMut(ctx context.Context, desc catalog.MutableDescriptor) {
	if g.cfg.DryRun {
		return
	}
	g.output.queuedDescs = append(g.output.queuedDescs, desc)
	if len(g.output.queuedDescs) > g.cfg.BatchSize {
		g.flush(ctx)
	}
}

// flush writes pending descriptors to KV, while respecting the batch
// configuration.
func (g *testSchemaGenerator) flush(ctx context.Context) {
	if g.cfg.DryRun {
		return
	}
	if len(g.output.queuedDescs) == 0 {
		return
	}
	for _, desc := range g.output.queuedDescs {
		if err := g.ext.coll.WriteDescToBatch(ctx, g.cfg.kvTrace, desc, g.output.b); err != nil {
			panic(genError{err})
		}
	}
	for _, desc := range g.output.newDescs {
		if err := g.ext.coll.InsertNamespaceEntryToBatch(ctx, g.cfg.kvTrace, desc, g.output.b); err != nil {
			panic(genError{err})
		}
	}
	if err := g.ext.txn.Run(ctx, g.output.b); err != nil {
		panic(genError{err})
	}
	g.output.b = g.ext.txn.NewBatch()
	g.output.queuedDescs = g.output.queuedDescs[:0]
	g.output.newDescs = g.output.newDescs[:0]
}

// reset starts a new generation round.
func (g *testSchemaGenerator) reset() {
	g.cfg.GeneratedCounts.Databases = 0
	g.cfg.GeneratedCounts.Schemas = 0
	g.cfg.GeneratedCounts.Tables = 0
	g.output.queuedDescs = nil
	g.output.newDescs = nil
	g.output.b = g.ext.txn.NewBatch()
}
