// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
)

func (p *planner) dropNamespaceEntry(
	ctx context.Context, b *kv.Batch, oldNameKey catalog.NameKey,
) error {
	return p.Descriptors().DeleteNamespaceEntryToBatch(
		ctx, p.ExtendedEvalContext().Tracing.KVTracingEnabled(), oldNameKey, b,
	)
}

func (p *planner) renameNamespaceEntry(
	ctx context.Context, b *kv.Batch, oldNameKey catalog.NameKey, desc catalog.MutableDescriptor,
) error {
	if err := p.dropNamespaceEntry(ctx, b, oldNameKey); err != nil {
		return err
	}
	return p.Descriptors().InsertNamespaceEntryToBatch(
		ctx, p.ExtendedEvalContext().Tracing.KVTracingEnabled(), desc, b,
	)
}
