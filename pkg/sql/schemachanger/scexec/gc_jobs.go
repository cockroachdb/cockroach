// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scexec

import (
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// gcJobs is part of the mutationVisitorState which accumulates information
// regarding gc jobs which need to be created.
type gcJobs struct {
	dbs     []gcJobForDB
	tables  []gcJobForTable
	indexes []gcJobForIndex
}

type gcJobForTable struct {
	parentID, id descpb.ID
	statement    scop.StatementForDropJob
}

type gcJobForIndex struct {
	tableID   descpb.ID
	indexID   descpb.IndexID
	statement scop.StatementForDropJob
}

type gcJobForDB struct {
	id        descpb.ID
	statement scop.StatementForDropJob
}

func (gj *gcJobs) AddNewGCJobForTable(stmt scop.StatementForDropJob, dbID, tableID descpb.ID) {
	gj.tables = append(gj.tables, gcJobForTable{
		parentID:  dbID,
		id:        tableID,
		statement: stmt,
	})
}

func (gj *gcJobs) AddNewGCJobForDatabase(stmt scop.StatementForDropJob, dbID descpb.ID) {
	gj.dbs = append(gj.dbs, gcJobForDB{
		id:        dbID,
		statement: stmt,
	})
}

func (gj *gcJobs) AddNewGCJobForIndex(
	stmt scop.StatementForDropJob, tableID descpb.ID, indexID descpb.IndexID,
) {
	gj.indexes = append(gj.indexes, gcJobForIndex{
		tableID:   tableID,
		indexID:   indexID,
		statement: stmt,
	})
}

// makeRecords processes the accumulated state into job records to be created.
// If there are any database to drop which do not have any corresponding
// tables, their IDs will be in dbZoneConfigsToRemove and will not be mentioned
// in any of the returned job records.
func (gj gcJobs) makeRecords(
	mkJobID func() jobspb.JobID,
) (dbZoneConfigsToRemove catalog.DescriptorIDSet, gcJobRecords []jobs.Record) {
	type stmts struct {
		s   []scop.StatementForDropJob
		set intsets.Fast
	}
	addStmt := func(s *stmts, stmt scop.StatementForDropJob) {
		if id := int(stmt.StatementID); !s.set.Contains(id) {
			s.set.Add(id)
			s.s = append(s.s, stmt)
		}
	}
	formatStatements := func(s *stmts) string {
		sort.Slice(s.s, func(i, j int) bool {
			return s.s[i].StatementID < s.s[j].StatementID
		})
		var buf strings.Builder
		if len(s.s) > 0 && s.s[0].Rollback {
			buf.WriteString("ROLLBACK of ")
		}
		for i, s := range s.s {
			if i > 0 {
				buf.WriteString("; ")
			}
			buf.WriteString(s.Statement)
		}
		return buf.String()
	}

	var tablesBeingDropped catalog.DescriptorIDSet
	now := timeutil.Now()
	addTableToJob := func(s *stmts, j *jobspb.SchemaChangeGCDetails, t gcJobForTable) {
		addStmt(s, t.statement)
		tablesBeingDropped.Add(t.id)
		j.Tables = append(j.Tables, jobspb.SchemaChangeGCDetails_DroppedID{
			ID:       t.id,
			DropTime: now.UnixNano(),
		})
	}

	// Create GC jobs for all database which are being dropped.
	// Then create one GC job for all tables being dropped which are
	// not part of a database being dropped.
	// Finally, create drop jobs for each index being dropped which is
	// not part of a table being dropped.
	gj.sort()
	for _, db := range gj.dbs {
		i := sort.Search(len(gj.tables), func(i int) bool {
			return gj.tables[i].parentID >= db.id
		})
		toDropIsID := func(i int) bool {
			return i < len(gj.tables) && gj.tables[i].parentID == db.id
		}
		if !toDropIsID(i) {
			dbZoneConfigsToRemove.Add(db.id)
			continue
		}
		var s stmts
		var j jobspb.SchemaChangeGCDetails
		j.ParentID = db.id
		for ; toDropIsID(i); i++ {
			addTableToJob(&s, &j, gj.tables[i])
		}
		gcJobRecords = append(gcJobRecords,
			createGCJobRecord(
				mkJobID(), formatStatements(&s), username.NodeUserName(), j,
			))
	}
	{
		var j jobspb.SchemaChangeGCDetails
		var s stmts
		for _, t := range gj.tables {
			if tablesBeingDropped.Contains(t.id) {
				continue
			}
			addTableToJob(&s, &j, t)
		}
		if len(j.Tables) > 0 {
			gcJobRecords = append(gcJobRecords, createGCJobRecord(
				mkJobID(), formatStatements(&s), username.NodeUserName(), j,
			))
		}
	}
	indexes := gj.indexes
	for len(indexes) > 0 {
		tableID := indexes[0].tableID
		var s stmts
		var j jobspb.SchemaChangeGCDetails
		j.ParentID = tableID
		for _, idx := range indexes {
			if idx.tableID != tableID {
				break
			}
			indexes = indexes[1:]
			// If we're already dropping the table, no need to create a job to
			// drop any of its indexes.
			if tablesBeingDropped.Contains(tableID) {
				continue
			}
			addStmt(&s, idx.statement)
			j.Indexes = append(j.Indexes, jobspb.SchemaChangeGCDetails_DroppedIndex{
				IndexID:  idx.indexID,
				DropTime: now.UnixNano(),
			})
		}
		if len(j.Indexes) > 0 {
			gcJobRecords = append(gcJobRecords, createGCJobRecord(
				mkJobID(), formatStatements(&s), username.NodeUserName(), j,
			))
		}
	}
	return dbZoneConfigsToRemove, gcJobRecords
}

func (gj gcJobs) sort() {
	sort.Slice(gj.dbs, func(i, j int) bool {
		return gj.dbs[i].id < gj.dbs[j].id
	})
	sort.Slice(gj.tables, func(i, j int) bool {
		dbi, dbj := gj.tables[i].parentID, gj.tables[j].parentID
		if dbi == dbj {
			return gj.tables[i].id < gj.tables[j].id
		}
		return dbi < dbj
	})
	sort.Slice(gj.indexes, func(i, j int) bool {
		dbi, dbj := gj.indexes[i].tableID, gj.indexes[j].tableID
		if dbi == dbj {
			return gj.indexes[i].indexID < gj.indexes[j].indexID
		}
		return dbi < dbj
	})
}

// createGCJobRecord creates the job record for a GC job, setting some
// properties which are common for all GC jobs.
func createGCJobRecord(
	id jobspb.JobID,
	description string,
	userName username.SQLUsername,
	details jobspb.SchemaChangeGCDetails,
) jobs.Record {
	descriptorIDs := make([]descpb.ID, 0)
	if len(details.Indexes) > 0 {
		if len(descriptorIDs) == 0 {
			descriptorIDs = []descpb.ID{details.ParentID}
		}
	} else {
		for _, table := range details.Tables {
			descriptorIDs = append(descriptorIDs, table.ID)
		}
		if details.ParentID != descpb.InvalidID {
			descriptorIDs = append(descriptorIDs, details.ParentID)
		}
	}
	return jobs.Record{
		JobID:         id,
		Description:   "GC for " + description,
		Username:      userName,
		DescriptorIDs: descriptorIDs,
		Details:       details,
		Progress:      jobspb.SchemaChangeGCProgress{},
		NonCancelable: true,
	}
}
