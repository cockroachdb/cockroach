// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opgen

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
)

func init() {
	opRegistry.register((*scpb.ForeignKeyConstraintUnvalidated)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.ForeignKeyConstraintUnvalidated) *scop.AddForeignKeyConstraint {
					return &scop.AddForeignKeyConstraint{
						TableID:                 this.TableID,
						ConstraintID:            this.ConstraintID,
						ColumnIDs:               this.ColumnIDs,
						ReferencedTableID:       this.ReferencedTableID,
						ReferencedColumnIDs:     this.ReferencedColumnIDs,
						OnUpdateAction:          this.OnUpdateAction,
						OnDeleteAction:          this.OnDeleteAction,
						CompositeKeyMatchMethod: this.CompositeKeyMatchMethod,
						Validity:                descpb.ConstraintValidity_Unvalidated,
					}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.ForeignKeyConstraintUnvalidated) *scop.RemoveForeignKeyBackReference {
					return &scop.RemoveForeignKeyBackReference{
						ReferencedTableID:  this.ReferencedTableID,
						OriginTableID:      this.TableID,
						OriginConstraintID: this.ConstraintID,
					}
				}),
				emit(func(this *scpb.ForeignKeyConstraintUnvalidated) *scop.RemoveForeignKeyConstraint {
					return &scop.RemoveForeignKeyConstraint{
						TableID:      this.TableID,
						ConstraintID: this.ConstraintID,
					}
				}),
			),
		),
	)
}
