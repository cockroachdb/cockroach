package scplan

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	. "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/opgen"
)

func init() {
	opGenRegistry.Register(
		(*scpb.Column)(nil),
		scpb.Target_ADD,
		scpb.Status_ABSENT,
		To(scpb.Status_DELETE_ONLY,
			Emit(func(this *scpb.Column) scop.Op {
				return &scop.MakeAddedColumnDeleteOnly{
					TableID:    this.TableID,
					FamilyID:   this.FamilyID,
					FamilyName: this.FamilyName,
					Column:     this.Column,
				}
			})),
		To(scpb.Status_DELETE_AND_WRITE_ONLY,
			Emit(func(this *scpb.Column) scop.Op {
				return &scop.MakeAddedColumnDeleteAndWriteOnly{
					TableID:  this.TableID,
					ColumnID: this.Column.ID,
				}
			})),
		To(scpb.Status_PUBLIC,
			Emit(func(this *scpb.Column) scop.Op {
				return &scop.MakeColumnPublic{
					TableID:  this.TableID,
					ColumnID: this.Column.ID,
				}
			})),
	)

	opGenRegistry.Register(
		(*scpb.Column)(nil),
		scpb.Target_DROP,
		scpb.Status_PUBLIC,
		To(scpb.Status_DELETE_AND_WRITE_ONLY,
			Emit(func(this *scpb.Column) scop.Op {
				return &scop.MakeDroppedColumnDeleteAndWriteOnly{
					TableID:  this.TableID,
					ColumnID: this.Column.ID,
				}
			})),
		To(scpb.Status_DELETE_AND_WRITE_ONLY,
			Emit(func(this *scpb.Column) scop.Op {
				return &scop.MakeDroppedColumnDeleteOnly{
					TableID:  this.TableID,
					ColumnID: this.Column.ID,
				}
			})),
		To(scpb.Status_ABSENT,
			Emit(func(this *scpb.Column) scop.Op {
				return &scop.MakeColumnAbsent{
					TableID:  this.TableID,
					ColumnID: this.Column.ID,
				}
			})),
	)
}
