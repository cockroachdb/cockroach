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
	descs nstree.Map
}

func makeSyntheticDescriptors() syntheticDescriptors {
	return syntheticDescriptors{descs: nstree.MakeMap()}
}

func (sd *syntheticDescriptors) set(descs []catalog.Descriptor) {
	sd.descs.Clear()
	for _, desc := range descs {
		if mut, ok := desc.(catalog.MutableDescriptor); ok {
			desc = mut.ImmutableCopy()
		}
		sd.descs.Upsert(desc)
	}
}

func (sd *syntheticDescriptors) reset() {
	sd.descs.Clear()
}

func (sd *syntheticDescriptors) getByName(
	dbID descpb.ID, schemaID descpb.ID, name string,
) (found bool, desc catalog.Descriptor) {
	if entry := sd.descs.GetByName(dbID, schemaID, name); entry != nil {
		return true, entry.(catalog.Descriptor)
	}
	return false, nil
}

func (sd *syntheticDescriptors) getByID(id descpb.ID) (found bool, desc catalog.Descriptor) {
	if entry := sd.descs.GetByID(id); entry != nil {
		return true, entry.(catalog.Descriptor)
	}
	return false, nil
}
