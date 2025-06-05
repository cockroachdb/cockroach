// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package replicationtestutils

import (
	"context"
	"fmt"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/errors"
)

func CheckEmptyDLQs(ctx context.Context, db sqlutils.DBHandle, dbName string) error {
	dlqNameQuery := fmt.Sprintf("SELECT table_name FROM [SHOW TABLES FROM %s] where schema_name = 'crdb_replication'", dbName)
	rows, err := db.QueryContext(ctx, dlqNameQuery)
	if err != nil {
		return errors.Wrapf(err, "failed to query dlq table name for database %s", dbName)
	}
	defer rows.Close()

	var dlqTableName string
	var dlqRowCount int
	for rows.Next() {
		if err := rows.Scan(&dlqTableName); err != nil {
			return errors.Wrapf(err, "failed to scan dlq table name for database %s", dbName)
		}
		if err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT count(*) FROM %s.crdb_replication.%s", dbName, dlqTableName)).Scan(&dlqRowCount); err != nil {
			return err
		}
		if dlqRowCount != 0 {
			return fmt.Errorf("expected DLQ to be empty, but found %d rows", dlqRowCount)
		}
	}
	if dlqTableName == "" {
		return errors.Newf("didn't find any any dlq tables in database %s", dbName)
	}
	return nil
}

func GenerateLDRTable(
	ctx context.Context, rng *rand.Rand, tableName string, supportKVWriter bool,
) string {
	columnByName := func(name tree.Name, columnDefs []*tree.ColumnTableDef) *tree.ColumnTableDef {
		for _, col := range columnDefs {
			if col.Name == name {
				return col
			}
		}
		return nil
	}

	tableDef := randgen.RandCreateTableWithName(ctx, rng, tableName, 0, []randgen.TableOption{
		randgen.WithPrimaryIndexRequired(),
		randgen.WithSkipColumnFamilyMutations(),
		randgen.WithPrimaryIndexFilter(func(indexDef *tree.IndexTableDef, columnDefs []*tree.ColumnTableDef) bool {
			for _, col := range indexDef.Columns {
				columnDef := columnByName(col.Column, columnDefs)
				// TODO(127315): types with composite encoding are not supported in the
				// primary key by LDR.
				if colinfo.CanHaveCompositeKeyEncoding(columnDef.Type.(*types.T)) {
					return false
				}
				// Do not allow computed columns in the primary key. Non-virtual computed columns in the primary key is
				// allowed by LDR in general, but its not compatible with the conflict workload.
				// TODO(jeffswenson): support computed columns in the primary key in the conflict workload.
				if columnDef.IsComputed() {
					return false
				}
			}
			if supportKVWriter && indexDef.Sharded != nil {
				// The KV writer does not support hash sharded indexes.
				return false
			}
			return true
		}),
		randgen.WithIndexFilter(func(indexDef tree.TableDef, columnDefs []*tree.ColumnTableDef) bool {
			switch indexDef := indexDef.(type) {
			case *tree.UniqueConstraintTableDef:
				// Do not allow unique indexes. The random data may cause
				// spurious unique constraint violations.
				// TODO(jeffswenson): extend the conflict workload to support unique indexes on fields
				// that can randomly generate exclusively unique values for each row. E.g. UUIDs could be unique, but
				// BOOLs are too limiting.
				return false
			case *tree.IndexTableDef:
				for _, col := range indexDef.Columns {
					if supportKVWriter && col.Expr != nil {
						// Do not allow expression indexes. These cause SQL to generate a hidden computed column, which is not
						// supported by the kv writer.
						if col.Expr != nil {
							return false
						}
					}
					columnDef := columnByName(col.Column, columnDefs)
					if columnDef.IsVirtual() {
						// Virtual computed columns are not supported in indexes by the classic sql writer or the kv writer.
						// TODO(jeffswenson): remove this restriction once the crud writer is the only writer.
						return false
					}
				}
				if supportKVWriter && indexDef.Sharded != nil {
					// The KV writer does not support hash sharded indexes.
					return false
				}
			}
			return true
		}),
	})
	return tree.AsStringWithFlags(tableDef, tree.FmtParsable)
}
