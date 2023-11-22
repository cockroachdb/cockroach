// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package schemafeed

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

type leaseAcquirer interface {
	Acquire(ctx context.Context, timestamp hlc.Timestamp, id descpb.ID) (lease.LeasedDescriptor, error)
	AcquireFreshestFromStore(ctx context.Context, id descpb.ID) error
}
