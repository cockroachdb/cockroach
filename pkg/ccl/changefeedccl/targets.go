// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupresolver"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// verifyChangefeedTargets verifies that the target list consists of supported
// changefeed targets (i.e. table names).
func verifyChangefeedTargets(targets tree.TargetList) error {
	// For now, disallow targeting a database or wildcard table selection.
	// Getting it right as tables enter and leave the set over time is
	// tricky.
	if len(targets.Databases) > 0 {
		return errors.Errorf(`CHANGEFEED cannot target %s`,
			tree.AsString(&targets))
	}
	for _, t := range targets.Tables {
		p, err := t.NormalizeTablePattern()
		if err != nil {
			return err
		}
		if _, ok := p.(*tree.TableName); !ok {
			return errors.Errorf(`CHANGEFEED cannot target %s`, tree.AsString(t))
		}
	}
	return nil
}

func resolveChangefeedTargets(
	ctx context.Context,
	p sql.PlanHookState,
	targetNames tree.TargetList,
	asOf hlc.Timestamp,
	forEachResolved func(desc catalog.TableDescriptor) error,
) error {
	// This grabs table descriptors once to get their ids.
	targetDescs, _, err := backupresolver.ResolveTargetsToDescriptors(
		ctx, p, asOf, &targetNames)
	if err != nil {
		return err
	}

	for _, desc := range targetDescs {
		if table, isTable := desc.(catalog.TableDescriptor); isTable {
			if err := p.CheckPrivilege(ctx, desc, privilege.SELECT); err != nil {
				return err
			}
			if err := changefeedbase.ValidateTableDescriptor(table); err != nil {
				return err
			}
			if err := forEachResolved(table); err != nil {
				return err
			}
		}
	}
	return nil
}

// getQualifiedTableName returns the database-qualified name of the table
// or view represented by the provided descriptor.
func getQualifiedTableName(
	ctx context.Context, execCfg sql.ExecutorConfig, txn *kv.Txn, desc catalog.TableDescriptor,
) (tree.TablePattern, error) {
	dbDesc, err := catalogkv.MustGetDatabaseDescByID(ctx, txn, execCfg.Codec, desc.GetParentID())
	if err != nil {
		return nil, err
	}
	schemaID := desc.GetParentSchemaID()
	schemaName, err := resolver.ResolveSchemaNameByID(ctx, txn, execCfg.Codec, desc.GetParentID(), schemaID)
	if err != nil {
		return nil, err
	}
	tbName := tree.NewTableNameWithSchema(
		tree.Name(dbDesc.GetName()),
		tree.Name(schemaName),
		tree.Name(desc.GetName()),
	)
	return tbName, nil
}
