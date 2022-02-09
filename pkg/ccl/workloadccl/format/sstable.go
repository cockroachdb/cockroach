// Copyright 2019 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package format

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/importccl"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/errors"
)

// ToTableDescriptor returns the corresponding TableDescriptor for a workload
// Table.
func ToTableDescriptor(
	t workload.Table, tableID descpb.ID, ts time.Time,
) (catalog.TableDescriptor, error) {
	ctx := context.Background()
	semaCtx := tree.MakeSemaContext()
	stmt, err := parser.ParseOne(fmt.Sprintf(`CREATE TABLE "%s" %s`, t.Name, t.Schema))
	if err != nil {
		return nil, err
	}
	createTable, ok := stmt.AST.(*tree.CreateTable)
	if !ok {
		return nil, errors.Errorf("expected *tree.CreateTable got %T", stmt)
	}
	// We need to assign a valid parent database ID to the table descriptor, but
	// the value itself doesn't matter, so we arbitrarily pick the system database
	// ID because we know it's valid.
	parentID := descpb.ID(keys.SystemDatabaseID)
	testSettings := cluster.MakeTestingClusterSettings()
	tableDesc, err := importccl.MakeTestingSimpleTableDescriptor(
		ctx, &semaCtx, testSettings, createTable, parentID, keys.PublicSchemaID, tableID, importccl.NoFKs, ts.UnixNano())
	if err != nil {
		return nil, err
	}
	return tableDesc.ImmutableCopy().(catalog.TableDescriptor), nil
}
