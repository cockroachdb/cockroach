// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// Discard implements the DISCARD statement.
// See https://www.postgresql.org/docs/9.6/static/sql-discard.html for details.
func (p *planner) Discard(ctx context.Context, s *tree.Discard) (planNode, error) {
	switch s.Mode {
	case tree.DiscardModeAll:
		if !p.autoCommit {
			return nil, pgerror.New(pgcode.ActiveSQLTransaction,
				"DISCARD ALL cannot run inside a transaction block")
		}

		// RESET ALL
		if err := p.sessionDataMutatorIterator.applyOnEachMutatorError(
			func(m sessionDataMutator) error {
				return resetSessionVars(ctx, m)
			},
		); err != nil {
			return nil, err
		}

		// DEALLOCATE ALL
		p.preparedStatements.DeleteAll(ctx)

		//	DISCARD TEMP
		err := prepareDeleteTempTables(ctx, p)
		if err != nil {
			return nil, err
		}

	case tree.DiscardModeTemp:
		err := prepareDeleteTempTables(ctx, p)
		if err != nil {
			return nil, err
		}

	default:
		return nil, errors.AssertionFailedf("unknown mode for DISCARD: %d", s.Mode)
	}
	return newZeroNode(nil /* columns */), nil
}

func resetSessionVars(ctx context.Context, m sessionDataMutator) error {
	for _, varName := range varNames {
		if err := resetSessionVar(ctx, m, varName); err != nil {
			return err
		}
	}
	return nil
}

func resetSessionVar(ctx context.Context, m sessionDataMutator, varName string) error {
	v := varGen[varName]
	if v.Set != nil {
		hasDefault, defVal := getSessionVarDefaultString(varName, v, m.sessionDataMutatorBase)
		if hasDefault {
			if err := v.Set(ctx, m, defVal); err != nil {
				return err
			}
		}
	}
	return nil
}

func getTempSchema(
	ctx context.Context, p *planner, codec keys.SQLCodec, allDbDescs []catalog.DatabaseDescriptor,
) (map[clusterunique.ID]struct{}, error) {

	sessionIDs := make(map[clusterunique.ID]struct{})
	for _, dbDesc := range allDbDescs {
		var schemaEntries map[descpb.ID]resolver.SchemaEntryForDB
		schemaEntries, err := resolver.GetForDatabase(ctx, p.txn, codec, dbDesc)
		if err != nil {
			return nil, err
		}

		for _, scEntry := range schemaEntries {
			isTempSchema, sessionID, err := temporarySchemaSessionID(scEntry.Name)
			if err != nil {
				// This should not cause an error.
				log.Warningf(ctx, "could not parse %q as temporary schema name", scEntry)
				continue
			}
			if isTempSchema {
				sessionIDs[sessionID] = struct{}{}
			}

		}
	}
	log.Infof(ctx, "found %d temporary schemas", len(sessionIDs))
	return sessionIDs, nil
}

func prepareDeleteTempTables(ctx context.Context, p *planner) error {
	codec := p.execCfg.Codec
	descCol := p.Descriptors()
	allDbDescs, err := descCol.GetAllDatabaseDescriptors(ctx, p.Txn())
	if err != nil {
		return err
	}
	ie := p.execCfg.InternalExecutor
	sessionIDs, err := getTempSchema(ctx, p, codec, allDbDescs)
	if err != nil {
		return err
	}

	for _, dbDesc := range allDbDescs {
		err = removeTempTable(ctx, p, ie, descCol, dbDesc, sessionIDs)
		if err != nil {
			return err
		}
	}
	return nil
}

func removeTempTable(
	ctx context.Context,
	p *planner,
	ie *InternalExecutor,
	descCol *descs.Collection,
	dbDesc catalog.DatabaseDescriptor,
	sessionIDs map[clusterunique.ID]struct{},
) error {
	for sessionID := range sessionIDs {
		schemaName := temporarySchemaName(sessionID)
		tbNames, tbIDs, err := descCol.GetObjectNamesAndIDs(
			ctx,
			p.txn,
			dbDesc,
			schemaName,
			tree.DatabaseListFlags{CommonLookupFlags: tree.CommonLookupFlags{Required: false}},
		)
		if err != nil {
			return err
		}

		databaseIDToTempSchemaID := make(map[uint32]uint32)
		var tables descpb.IDs
		tblDescsByID := make(map[descpb.ID]catalog.TableDescriptor, len(tbNames))
		tblNamesByID := make(map[descpb.ID]tree.TableName, len(tbNames))
		for i, tbName := range tbNames {
			desc, err := descCol.Direct().MustGetTableDescByID(ctx, p.txn, tbIDs[i])
			if err != nil {
				return err
			}

			tblDescsByID[desc.GetID()] = desc
			tblNamesByID[desc.GetID()] = tbName

			databaseIDToTempSchemaID[uint32(desc.GetParentID())] = uint32(desc.GetParentSchemaID())

			if !desc.IsSequence() {
				tables = append(tables, desc.GetID())
			}
		}

		searchPath := sessiondata.DefaultSearchPathForUser(username.RootUserName()).WithTemporarySchemaName(schemaName)
		override := sessiondata.InternalExecutorOverride{
			SearchPath:               &searchPath,
			User:                     username.RootUserName(),
			DatabaseIDToTempSchemaID: databaseIDToTempSchemaID,
		}

		for _, toDelete := range []struct {
			// typeName is the type of table being deleted, e.g. view, table, sequence
			typeName string
			// ids represents which ids we wish to remove.
			ids descpb.IDs
			// preHook is used to perform any operations needed before calling
			// delete on all the given ids.
			preHook func(descpb.ID) error
		}{
			{"TABLE", tables, nil},
		} {
			if len(toDelete.ids) > 0 {
				if toDelete.preHook != nil {
					for _, id := range toDelete.ids {
						if err := toDelete.preHook(id); err != nil {
							return err
						}
					}
				}

				var query strings.Builder
				query.WriteString("DROP ")
				query.WriteString(toDelete.typeName)

				for i, id := range toDelete.ids {
					tbName := tblNamesByID[id]
					if i != 0 {
						query.WriteString(",")
					}
					query.WriteString(" ")
					query.WriteString(tbName.FQString())
				}
				query.WriteString(" CASCADE")
				_, err = ie.ExecEx(ctx, "delete-temp-"+toDelete.typeName, p.txn, override, query.String())
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}
