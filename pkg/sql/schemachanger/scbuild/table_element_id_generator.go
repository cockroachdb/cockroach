// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scbuild

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild/internal/scbuildstmt"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
)

var _ scbuildstmt.TableElementIDGenerator = buildCtx{}

// NextColumnID implements the scbuildstmt.TableElementIDGenerator interface.
func (b buildCtx) NextColumnID(tbl catalog.TableDescriptor) descpb.ColumnID {
	var maxAddedColID descpb.ColumnID
	b.ForEachNode(func(_ scpb.Status, dir scpb.Target_Direction, elem scpb.Element) {
		if dir != scpb.Target_ADD || screl.GetDescID(elem) != tbl.GetID() {
			return
		}
		if ac, ok := elem.(*scpb.Column); ok && ac.Column.ID > maxAddedColID {
			maxAddedColID = ac.Column.ID
		}
	})
	if maxAddedColID != 0 {
		return maxAddedColID + 1
	}
	return tbl.GetNextColumnID()
}

// NextColumnFamilyID implements the scbuildstmt.TableElementIDGenerator
// interface.
func (b buildCtx) NextColumnFamilyID(tbl catalog.TableDescriptor) descpb.FamilyID {
	nextFamilyID := tbl.GetNextFamilyID()
	b.ForEachNode(func(_ scpb.Status, dir scpb.Target_Direction, elem scpb.Element) {
		if dir != scpb.Target_ADD || screl.GetDescID(elem) != tbl.GetID() {
			return
		}
		if ac, ok := elem.(*scpb.Column); ok && ac.FamilyID > nextFamilyID {
			nextFamilyID = ac.FamilyID
		}
	})
	return nextFamilyID
}

// NextIndexID implements the scbuildstmt.TableElementIDGenerator interface.
func (b buildCtx) NextIndexID(tbl catalog.TableDescriptor) descpb.IndexID {
	var maxAddedIndexID descpb.IndexID
	b.ForEachNode(func(_ scpb.Status, dir scpb.Target_Direction, elem scpb.Element) {
		if dir != scpb.Target_ADD || screl.GetDescID(elem) != tbl.GetID() {
			return
		}
		switch e := elem.(type) {
		case *scpb.SecondaryIndex:
			if e.IndexID > maxAddedIndexID {
				maxAddedIndexID = e.IndexID
			}
		case *scpb.PrimaryIndex:
			if e.IndexID > maxAddedIndexID {
				maxAddedIndexID = e.IndexID
			}
		}
	})
	if maxAddedIndexID != 0 {
		return maxAddedIndexID + 1
	}
	return tbl.GetNextIndexID()
}
