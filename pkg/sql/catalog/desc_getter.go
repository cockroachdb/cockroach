// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catalog

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
)

// DescGetter is an interface to retrieve descriptors. In general the interface
// is used to look up other descriptors during validation.
type DescGetter interface {
	GetDesc(ctx context.Context, id descpb.ID) (Descriptor, error)
}

// BatchDescGetter is like DescGetter but retrieves batches of descriptors,
// which for some implementations may make more sense performance-wise.
type BatchDescGetter interface {
	DescGetter
	GetDescs(ctx context.Context, reqs []descpb.ID) ([]Descriptor, error)
}

// GetTableDescFromID retrieves the table descriptor for the table
// ID passed in using an existing proto getter. Returns an error if the
// descriptor doesn't exist or if it exists and is not a table.
func GetTableDescFromID(ctx context.Context, dg DescGetter, id descpb.ID) (TableDescriptor, error) {
	desc, err := dg.GetDesc(ctx, id)
	if err != nil {
		return nil, err
	}
	table, ok := desc.(TableDescriptor)
	if !ok {
		return nil, WrapTableDescRefErr(id, ErrDescriptorNotFound)
	}
	return table, nil
}

// MapDescGetter is a protoGetter that has a hard-coded map of keys to proto
// messages.
type MapDescGetter map[descpb.ID]Descriptor

// GetDesc implements the catalog.DescGetter interface.
func (m MapDescGetter) GetDesc(ctx context.Context, id descpb.ID) (Descriptor, error) {
	desc := m[id]
	return desc, nil
}
