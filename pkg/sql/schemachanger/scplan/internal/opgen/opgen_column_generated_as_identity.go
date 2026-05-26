// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opgen

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

func init() {
	opRegistry.register((*scpb.ColumnGeneratedAsIdentity)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.ColumnGeneratedAsIdentity) *scop.AddColumnGeneratedAsIdentity {
					return &scop.AddColumnGeneratedAsIdentity{
						GeneratedAsIdentity: *protoutil.Clone(this).(*scpb.ColumnGeneratedAsIdentity),
					}
				}),
			),
			// equiv status to handle migration to 26.1 or later during a schema change
			equiv(scpb.Status_WRITE_ONLY),
			equiv(scpb.Status_DELETE_ONLY),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.ColumnGeneratedAsIdentity) *scop.RemoveColumnGeneratedAsIdentity {
					return &scop.RemoveColumnGeneratedAsIdentity{
						TableID:  this.TableID,
						ColumnID: this.ColumnID,
					}
				}),
			),
			// equiv status to handle migration to 26.1 or later during a schema change
			equiv(scpb.Status_WRITE_ONLY),
			equiv(scpb.Status_DELETE_ONLY),
		),
	)
}
