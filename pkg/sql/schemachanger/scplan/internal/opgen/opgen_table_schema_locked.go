// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opgen

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
)

// TableSchemaLocked transitions between ABSENT and PUBLIC directly.
// ABSENT --> PUBLIC: unimplemented
// PUBLIC --> ABSENT: unimplemented (but we allow it if the table is dropped)
func init() {
	opRegistry.register((*scpb.TableSchemaLocked)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.TableSchemaLocked) *scop.SetTableSchemaLocked {
					return &scop.SetTableSchemaLocked{
						TableID: this.TableID,
						Locked:  true,
					}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			equiv(scpb.Status_TRANSIENT_PUBLIC),
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.TableSchemaLocked) *scop.SetTableSchemaLocked {
					return &scop.SetTableSchemaLocked{
						TableID: this.TableID,
						Locked:  false,
					}
				}),
			),
		),
		toTransientPublic(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.TableSchemaLocked) *scop.SetTableSchemaLocked {
					return &scop.SetTableSchemaLocked{
						TableID: this.TableID,
						Locked:  false,
					}
				})),
			to(scpb.Status_TRANSIENT_PUBLIC,
				emit(func(this *scpb.TableSchemaLocked) *scop.SetTableSchemaLocked {
					return &scop.SetTableSchemaLocked{
						TableID: this.TableID,
						Locked:  true,
					}
				}),
			),
		),
	)
}
