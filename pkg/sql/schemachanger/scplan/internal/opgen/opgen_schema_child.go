// Copyright 2022 The Cockroach Authors.
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
	opRegistry.register((*scpb.SchemaChild)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.SchemaChild) *scop.SetObjectParentID {
					op := protoutil.Clone(this).(*scpb.SchemaChild)
					return &scop.SetObjectParentID{ObjParent: *op}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT,
				// TODO(postamar): remove revertibility constraint when possible
				revertible(false),
				emit(func(this *scpb.SchemaChild) *scop.RemoveObjectParent {
					return &scop.RemoveObjectParent{
						ObjectID:       this.ChildObjectID,
						ParentSchemaID: this.SchemaID,
					}
				}),
			),
		),
	)
}
