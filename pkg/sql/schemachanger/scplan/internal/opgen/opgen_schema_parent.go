// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opgen

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

func init() {
	opRegistry.register((*scpb.SchemaParent)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.SchemaParent, md *opGenContext) *scop.AddSchemaParent {
					// Skip adding a parent in the parent database for temporary schemas.
					if strings.HasPrefix(md.Name(this.SchemaID), catconstants.PgTempSchemaName) {
						return nil
					}
					return &scop.AddSchemaParent{
						Parent: *protoutil.Clone(this).(*scpb.SchemaParent),
					}
				}),
				emit(func(this *scpb.SchemaParent, md *opGenContext) *scop.InsertTemporarySchemaParent {
					// If its not temporary schema then we don't need to register it in
					// the session data.
					if !strings.HasPrefix(md.Name(this.SchemaID), catconstants.PgTempSchemaName) {
						return nil
					}
					return &scop.InsertTemporarySchemaParent{
						SchemaID:   this.SchemaID,
						DatabaseID: this.ParentDatabaseID,
					}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT,
				// TODO(postamar): remove revertibility constraint when possible
				revertible(false),
				emit(func(this *scpb.SchemaParent) *scop.RemoveSchemaParent {
					return &scop.RemoveSchemaParent{
						Parent: *protoutil.Clone(this).(*scpb.SchemaParent),
					}
				}),
			),
		),
	)
}
