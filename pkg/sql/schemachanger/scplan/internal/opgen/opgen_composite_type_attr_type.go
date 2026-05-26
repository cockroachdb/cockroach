// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opgen

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
)

func init() {
	opRegistry.register((*scpb.CompositeTypeAttrType)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.CompositeTypeAttrType) *scop.UpdateTypeBackReferencesInTypes {
					if len(this.ClosedTypeIDs) == 0 {
						return nil
					}
					return &scop.UpdateTypeBackReferencesInTypes{
						TypeIDs:              this.ClosedTypeIDs,
						BackReferencedTypeID: this.CompositeTypeID,
					}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.CompositeTypeAttrType) *scop.UpdateTypeBackReferencesInTypes {
					if len(this.ClosedTypeIDs) == 0 {
						return nil
					}
					return &scop.UpdateTypeBackReferencesInTypes{
						TypeIDs:              this.ClosedTypeIDs,
						BackReferencedTypeID: this.CompositeTypeID,
					}
				}),
			),
		),
	)
}
