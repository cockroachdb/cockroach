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
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
)

// DescGetter is an interface to retrieve descriptors and namespace entries.
// In general the interface is used to look up other descriptors during
// validation.
// Lookups are performed on a best-effort basis. When a descriptor or namespace
// entry cannot be found, the zero-value is returned, with no error.
type DescGetter interface {
	GetDesc(ctx context.Context, id descpb.ID) (Descriptor, error)
	GetNamespaceEntry(ctx context.Context, parentID, parentSchemaID descpb.ID, name string) (descpb.ID, error)
}

// BatchDescGetter is like DescGetter but retrieves batches of descriptors,
// which for some implementations may make more sense performance-wise.
type BatchDescGetter interface {
	DescGetter
	GetDescs(ctx context.Context, reqs []descpb.ID) ([]Descriptor, error)
	GetNamespaceEntries(ctx context.Context, requests []descpb.NameInfo) ([]descpb.ID, error)
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

// MapDescGetter is an in-memory DescGetter implementation.
type MapDescGetter struct {
	Descriptors map[descpb.ID]Descriptor
	Namespace   map[descpb.NameInfo]descpb.ID
}

// MakeMapDescGetter returns an initialized MapDescGetter.
func MakeMapDescGetter() MapDescGetter {
	return MapDescGetter{
		Descriptors: make(map[descpb.ID]Descriptor),
		Namespace:   make(map[descpb.NameInfo]descpb.ID),
	}
}

// OrderedDescriptors returns the descriptors ordered by ID.
func (m MapDescGetter) OrderedDescriptors() []Descriptor {
	ret := make([]Descriptor, 0, len(m.Descriptors))
	for _, d := range m.Descriptors {
		ret = append(ret, d)
	}
	sort.Sort(Descriptors(ret))
	return ret
}

// GetDesc implements the DescGetter interface.
func (m MapDescGetter) GetDesc(_ context.Context, id descpb.ID) (Descriptor, error) {
	desc := m.Descriptors[id]
	return desc, nil
}

// GetNamespaceEntry implements the DescGetter interface.
func (m MapDescGetter) GetNamespaceEntry(
	_ context.Context, parentID, parentSchemaID descpb.ID, name string,
) (descpb.ID, error) {
	id := m.Namespace[descpb.NameInfo{
		ParentID:       parentID,
		ParentSchemaID: parentSchemaID,
		Name:           name,
	}]
	return id, nil
}
