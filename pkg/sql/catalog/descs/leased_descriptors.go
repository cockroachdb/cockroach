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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func makeLeasedDescriptors() leasedDescriptors {
	return leasedDescriptors{
		descs: makeDescriptorTree(),
	}
}

// leasedDescriptors holds references to all the descriptors leased in the
// transaction, and supports access by name and by ID.
type leasedDescriptors struct {
	descs descriptorTree
}

func (ld *leasedDescriptors) add(desc lease.LeasedDescriptor) {
	ld.descs.add(desc)
}

func (ld *leasedDescriptors) releaseAll(ctx context.Context) {
	log.VEventf(ctx, 2, "releasing %d descriptors", ld.numDescriptors())
	_ = ld.descs.iterateByID(func(descriptor catalog.Descriptor) error {
		d := descriptor.(lease.LeasedDescriptor)
		d.Release(ctx)
		return nil
	})
	ld.descs.clear()
}

func (ld *leasedDescriptors) getDeadline() (deadline hlc.Timestamp, haveDeadline bool) {
	_ = ld.descs.iterateByID(func(descriptor catalog.Descriptor) error {
		expiration := descriptor.(lease.LeasedDescriptor).Expiration()
		if !haveDeadline || expiration.Less(deadline) {
			deadline, haveDeadline = expiration, true
		}
		return nil
	})
	return deadline, haveDeadline
}

func (ld *leasedDescriptors) release(ctx context.Context, descs []lease.IDVersion) {
	for _, idv := range descs {
		got, ok := ld.descs.getByID(idv.ID)
		if !ok {
			continue
		}
		d := got.(lease.LeasedDescriptor)
		ld.descs.remove(idv.ID)
		d.Release(ctx)
	}
}

func (ld *leasedDescriptors) getByID(id descpb.ID) catalog.Descriptor {
	if got, ok := ld.descs.getByID(id); ok {
		return got.(lease.LeasedDescriptor).Desc()
	}
	return nil
}

func (ld *leasedDescriptors) getByName(
	dbID descpb.ID, schemaID descpb.ID, name string,
) catalog.Descriptor {
	if got, ok := ld.descs.getByName(dbID, schemaID, name); ok {
		return got.(lease.LeasedDescriptor).Desc()
	}
	return nil
}

func (ld *leasedDescriptors) numDescriptors() int {
	return ld.descs.len()
}
