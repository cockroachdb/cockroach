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
	opRegistry.register((*scpb.IndexColumn)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC, emit(func(column *scpb.IndexColumn) *scop.AddColumnToIndex {
				return &scop.AddColumnToIndex{
					TableID:      column.TableID,
					ColumnID:     column.ColumnID,
					IndexID:      column.IndexID,
					Kind:         column.Kind,
					Direction:    column.Direction,
					Ordinal:      column.OrdinalInKind,
					InvertedKind: column.InvertedKind,
				}
			})),
		),
		toTransientAbsentLikePublic(),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT,
				revertible(false),
				emit(func(column *scpb.IndexColumn) *scop.RemoveColumnFromIndex {
					return &scop.RemoveColumnFromIndex{
						TableID:  column.TableID,
						ColumnID: column.ColumnID,
						IndexID:  column.IndexID,
						Kind:     column.Kind,
						Ordinal:  column.OrdinalInKind,
					}
				})),
		),
	)
}
