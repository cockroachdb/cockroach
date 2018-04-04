// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"context"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type dropDatabaseNode struct {
	n      *tree.DropDatabase
	dbDesc *sqlbase.DatabaseDescriptor
	td     []toDelete
}

// DropDatabase drops a database.
// Privileges: DROP on database and DROP on all tables in the database.
//   Notes: postgres allows only the database owner to DROP a database.
//          mysql requires the DROP privileges on the database.
// TODO(XisiHuang): our DROP DATABASE is like the postgres DROP SCHEMA
// (cockroach database == postgres schema). the postgres default of not
// dropping the schema if there are dependent objects is more sensible
// (see the RESTRICT and CASCADE options).
func (p *planner) DropDatabase(ctx context.Context, n *tree.DropDatabase) (planNode, error) {
	if n.Name == "" {
		return nil, errEmptyDatabaseName
	}

	if string(n.Name) == p.SessionData().Database && p.SessionData().SafeUpdates {
		return nil, pgerror.NewDangerousStatementErrorf("DROP DATABASE on current database")
	}

	// Check that the database exists.
	var dbDesc *DatabaseDescriptor
	var err error
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		dbDesc, err = ResolveDatabase(ctx, p, string(n.Name), !n.IfExists)
	})
	if err != nil {
		return nil, err
	}
	if dbDesc == nil {
		// IfExists was specified and database was not found.
		return newZeroNode(nil /* columns */), nil
	}

	if err := p.CheckPrivilege(ctx, dbDesc, privilege.DROP); err != nil {
		return nil, err
	}

	tbNames, err := GetObjectNames(ctx, p, dbDesc, tree.PublicSchema, true /*explicitPrefix*/)
	if err != nil {
		return nil, err
	}

	if len(tbNames) > 0 {
		switch n.DropBehavior {
		case tree.DropRestrict:
			return nil, pgerror.NewErrorf(pgerror.CodeDependentObjectsStillExistError,
				"database %q is not empty and RESTRICT was specified",
				tree.ErrNameString(&dbDesc.Name))
		case tree.DropDefault:
			// The default is CASCADE, however be cautious if CASCADE was
			// not specified explicitly.
			if p.SessionData().SafeUpdates {
				return nil, pgerror.NewDangerousStatementErrorf(
					"DROP DATABASE on non-empty database without explicit CASCADE")
			}
		}
	}

	td := make([]toDelete, len(tbNames))
	for i := range tbNames {
		tbDesc, err := p.prepareDrop(ctx, &tbNames[i], true /*required*/, anyDescType)
		if err != nil {
			return nil, err
		}
		if tbDesc == nil {
			// Database claims to have this table, but it does not exist.
			return nil, errors.Errorf("table %q was described by database %q, but does not exist",
				tree.ErrString(&tbNames[i]), n.Name)
		}
		// Recursively check permissions on all dependent views, since some may
		// be in different databases.
		for _, ref := range tbDesc.DependedOnBy {
			if err := p.canRemoveDependentView(ctx, tbDesc, ref, tree.DropCascade); err != nil {
				return nil, err
			}
		}
		td[i] = toDelete{&tbNames[i], tbDesc}
	}

	td, err = p.filterCascadedTables(ctx, td)
	if err != nil {
		return nil, err
	}

	return &dropDatabaseNode{n: n, dbDesc: dbDesc, td: td}, nil
}

func (n *dropDatabaseNode) startExec(params runParams) error {
	ctx := params.ctx
	p := params.p
	tbNameStrings := make([]string, 0, len(n.td))
	for _, toDel := range n.td {
		tbDesc := toDel.desc
		if tbDesc.IsView() {
			cascadedViews, err := p.dropViewImpl(ctx, tbDesc, tree.DropCascade)
			if err != nil {
				return err
			}
			// TODO(knz): dependent dropped views should be qualified here.
			tbNameStrings = append(tbNameStrings, cascadedViews...)
		} else {
			cascadedViews, err := p.dropTableImpl(params, tbDesc)
			if err != nil {
				return err
			}
			// TODO(knz): dependent dropped table names should be qualified here.
			tbNameStrings = append(tbNameStrings, cascadedViews...)
		}
		tbNameStrings = append(tbNameStrings, toDel.tn.FQString())
	}

	_ /* zoneKey */, nameKey, descKey := getKeysForDatabaseDescriptor(n.dbDesc)
	zoneKeyPrefix := config.MakeZoneKeyPrefix(uint32(n.dbDesc.ID))

	b := &client.Batch{}
	if p.ExtendedEvalContext().Tracing.KVTracingEnabled() {
		log.VEventf(ctx, 2, "Del %s", descKey)
		log.VEventf(ctx, 2, "Del %s", nameKey)
		log.VEventf(ctx, 2, "DelRange %s", zoneKeyPrefix)
	}
	b.Del(descKey)
	b.Del(nameKey)
	// Delete the zone config entry for this database.
	b.DelRange(zoneKeyPrefix, zoneKeyPrefix.PrefixEnd(), false /* returnKeys */)

	p.Tables().addUncommittedDatabase(n.dbDesc.Name, n.dbDesc.ID, dbDropped)

	if err := p.txn.Run(ctx, b); err != nil {
		return err
	}

	// Log Drop Database event. This is an auditable log event and is recorded
	// in the same transaction as the table descriptor update.
	return MakeEventLogger(params.extendedEvalCtx.ExecCfg).InsertEventRecord(
		ctx,
		p.txn,
		EventLogDropDatabase,
		int32(n.dbDesc.ID),
		int32(params.extendedEvalCtx.NodeID),
		struct {
			DatabaseName          string
			Statement             string
			User                  string
			DroppedTablesAndViews []string
		}{n.n.Name.String(), n.n.String(), p.SessionData().User, tbNameStrings},
	)
}

func (*dropDatabaseNode) Next(runParams) (bool, error) { return false, nil }
func (*dropDatabaseNode) Close(context.Context)        {}
func (*dropDatabaseNode) Values() tree.Datums          { return tree.Datums{} }

// filterCascadedTables takes a list of table descriptors and removes any
// descriptors from the list that are dependent on other descriptors in the
// list (e.g. if view v1 depends on table t1, then v1 will be filtered from
// the list).
func (p *planner) filterCascadedTables(ctx context.Context, tables []toDelete) ([]toDelete, error) {
	// Accumulate the set of all tables/views that will be deleted by cascade
	// behavior so that we can filter them out of the list.
	cascadedTables := make(map[sqlbase.ID]bool)
	for _, toDel := range tables {
		desc := toDel.desc
		if err := p.accumulateDependentTables(ctx, cascadedTables, desc); err != nil {
			return nil, err
		}
	}
	filteredTableList := make([]toDelete, 0, len(tables))
	for _, toDel := range tables {
		if !cascadedTables[toDel.desc.ID] {
			filteredTableList = append(filteredTableList, toDel)
		}
	}
	return filteredTableList, nil
}

func (p *planner) accumulateDependentTables(
	ctx context.Context, dependentTables map[sqlbase.ID]bool, desc *sqlbase.TableDescriptor,
) error {
	for _, ref := range desc.DependedOnBy {
		dependentTables[ref.ID] = true
		dependentDesc, err := sqlbase.GetTableDescFromID(ctx, p.txn, ref.ID)
		if err != nil {
			return err
		}
		if err := p.accumulateDependentTables(ctx, dependentTables, dependentDesc); err != nil {
			return err
		}
	}
	return nil
}
