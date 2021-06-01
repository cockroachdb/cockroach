// Copyright 2021 The Cockroach Authors.
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
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descriptortree"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func makeLeasedDescriptors() leasedDescriptors {
	return leasedDescriptors{
		descs: descriptortree.Make(),
	}
}

// leasedDescriptors holds references to all the descriptors leased in the
// transaction, and supports access by name and by ID.
type leasedDescriptors struct {
	descs descriptortree.Tree
}

func (ld *leasedDescriptors) add(desc lease.LeasedDescriptor) {
	ld.descs.Upsert(desc)
}

func (ld *leasedDescriptors) getDeadline() (deadline hlc.Timestamp, haveDeadline bool) {
	_ = ld.descs.IterateByID(func(descriptor catalog.Descriptor) error {
		expiration := descriptor.(lease.LeasedDescriptor).Expiration()
		if !haveDeadline || expiration.Less(deadline) {
			deadline, haveDeadline = expiration, true
		}
		return nil
	})
	return deadline, haveDeadline
}

func (ld *leasedDescriptors) releaseAll(ctx context.Context) {
	log.VEventf(ctx, 2, "releasing %d descriptors", ld.numDescriptors())
	_ = ld.descs.IterateByID(func(descriptor catalog.Descriptor) error {
		descriptor.(lease.LeasedDescriptor).Release(ctx)
		return nil
	})
	ld.descs.Clear()
}

func (ld *leasedDescriptors) release(ctx context.Context, descs []lease.IDVersion) {
	for _, idv := range descs {
		if removed, ok := ld.descs.Remove(idv.ID); ok {
			removed.(lease.LeasedDescriptor).Release(ctx)
		}
	}
}

func (ld *leasedDescriptors) getByID(id descpb.ID) catalog.Descriptor {
	if got, ok := ld.descs.GetByID(id); ok {
		return got.(lease.LeasedDescriptor).Underlying()
	}
	return nil
}

func (ld *leasedDescriptors) getByName(
	dbID descpb.ID, schemaID descpb.ID, name string,
) catalog.Descriptor {
	if got, ok := ld.descs.GetByName(dbID, schemaID, name); ok {
		return got.(lease.LeasedDescriptor).Underlying()
	}
	return nil
}

func (ld *leasedDescriptors) numDescriptors() int {
	return ld.descs.Len()
}
