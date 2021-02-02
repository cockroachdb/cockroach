// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec/scmutationexec"
)

type mutationDescGetter struct {
	descs     *descs.Collection
	txn       *kv.Txn
	retrieved catalog.DescriptorIDSet
}

func (m *mutationDescGetter) GetMutableTableByID(
	ctx context.Context, id descpb.ID,
) (*tabledesc.Mutable, error) {
	table, err := m.descs.GetMutableTableVersionByID(ctx, id, m.txn)
	if err != nil {
		return nil, err
	}
	table.MaybeIncrementVersion()
	m.retrieved.Add(table.GetID())
	return table, nil
}

var _ scmutationexec.MutableDescGetter = (*mutationDescGetter)(nil)
