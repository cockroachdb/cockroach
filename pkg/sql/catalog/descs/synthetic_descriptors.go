// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package descs

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
)

type syntheticDescriptors struct {
	descs nstree.NameMap
}

func (sd *syntheticDescriptors) add(desc catalog.Descriptor) {
	if mut, ok := desc.(catalog.MutableDescriptor); ok {
		desc = mut.ImmutableCopy()
	}
	sd.descs.Upsert(desc, desc.SkipNamespace())
}

func (sd *syntheticDescriptors) reset() {
	sd.descs.Clear()
}

func (sd *syntheticDescriptors) getSyntheticByName(
	dbID descpb.ID, schemaID descpb.ID, name string,
) catalog.NameEntry {
	return sd.descs.GetByName(dbID, schemaID, name)
}

func (sd *syntheticDescriptors) getSyntheticByID(id descpb.ID) catalog.Descriptor {
	if entry := sd.descs.GetByID(id); entry != nil {
		return entry.(catalog.Descriptor)
	}
	return nil
}

// iterateSyntheticByID applies fn to the synthetic descriptors in ascending
// sequence of IDs.
func (sd *syntheticDescriptors) iterateSyntheticByID(fn func(desc catalog.Descriptor) error) error {
	return sd.descs.IterateByID(func(entry catalog.NameEntry) error {
		return fn(entry.(catalog.Descriptor))
	})
}
