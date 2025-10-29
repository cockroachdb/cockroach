// Copyright 2021 The Cockroach Authors.
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
		),
	)
}
