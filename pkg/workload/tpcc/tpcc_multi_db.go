// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tpcc

import (
	"context"
	gosql "database/sql"
	"fmt"
	"os"
	"strings"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/jackc/pgx/v5"
)

type tpccMultiDB struct {
	*tpcc

	// txnPreambleFile queries that will be executed before each operation.
	txnPreambleFile       string
	txnPreambleStatements statements.Statements

	// dbListFile contains the list of databases that tpcc schema will be
	// created on and have the workload executed on.
	dbListFile string
	dbList     []string

	// nextDatabase selects the next database in a round robin manner.
	nextDatabase atomic.Uint64
}

var tpccMultiDBMeta = workload.Meta{
	Name: `tpccmultidb`,
	Description: `TPC-C simulates a transaction processing workload` +
		` using a rich schema of multiple tables. This has been modified ` +
		` to run against multiple instances of the same schema`,
	Version:    `2.2.0`,
	RandomSeed: RandomSeed,
	New: func() workload.Generator {
		g := tpccMultiDB{}
		g.tpcc = tpccMeta.New().(*tpcc)
		g.flags.Meta["txn-preamble-file"] = workload.FlagMeta{RuntimeOnly: true}
		// Support executing a query file before each transaction.
		g.flags.StringVar(&g.txnPreambleFile, "txn-preamble-file", "", "queries that will be injected before each txn")
		g.flags.StringVar(&g.dbListFile, "db-list-file", "", "a file containing a list of databases.")
		return &g
	},
}

// runBeforeEachTxn is executed at the start of each transaction
// inside normal tpcc.
func (t *tpccMultiDB) runBeforeEachTxn(ctx context.Context, tx pgx.Tx) error {
	// If multiple DBs are specified via list, select one
	// in a round robin manner.
	if t.dbList != nil {
		databaseIdx := int(t.nextDatabase.Add(1) % uint64(len(t.dbList)))
		if _, err := tx.Exec(ctx, "USE $1", t.dbList[databaseIdx]); err != nil {
			return err
		}
	}
	// Next execute any statements at the start of the txn.
	for _, stmt := range t.txnPreambleStatements {
		if _, err := tx.Exec(ctx, stmt.AST.String()); err != nil {
			return err
		}
	}
	return nil
}

// Ops implements the Opser interface.
func (t *tpccMultiDB) Ops(
	ctx context.Context, urls []string, reg *histogram.Registry,
) (workload.QueryLoad, error) {
	// Execute extra logic at the start of each txn.
	t.onTxnStart = t.runBeforeEachTxn
	return t.tpcc.Ops(ctx, urls, reg)
}

// Tables implements the Generator interface.
func (t *tpccMultiDB) Tables() []workload.Table {
	existingTables := t.tpcc.Tables()
	if len(t.dbList) == 0 {
		return existingTables
	}
	// Take the normal TPCC tables and make a copy for each
	// database in the list.
	tablesPerDb := make([]workload.Table, 0, len(existingTables)*len(t.dbList))
	for _, db := range t.dbList {
		for _, tbl := range existingTables {
			tbl.ObjectPrefix = &tree.ObjectNamePrefix{
				CatalogName:     tree.Name(db),
				ExplicitCatalog: true,
				SchemaName:      "public",
				ExplicitSchema:  true,
			}
			tablesPerDb = append(tablesPerDb, tbl)
		}
	}
	return tablesPerDb
}

func (*tpccMultiDB) Meta() workload.Meta { return tpccMultiDBMeta }

func (t *tpccMultiDB) Hooks() workload.Hooks {
	hooks := t.tpcc.Hooks()
	oldPrecreate := hooks.PreCreate
	hooks.PreCreate = func(db *gosql.DB) error {
		// If queries were specified before each operation, then lets
		// execute those.
		if t.txnPreambleFile != "" {
			file, err := os.ReadFile(t.txnPreambleFile)
			if err != nil {
				return err
			}
			txnPreambleStatements, err := parser.Parse(string(file))
			if err != nil {
				return err
			}
			t.txnPreambleStatements = txnPreambleStatements
		}
		if t.dbListFile != "" {
			file, err := os.ReadFile(t.dbListFile)
			if err != nil {
				return err
			}
			t.dbList = strings.Split(string(file), "\n")
		}
		if v := len(t.dbList); v > 0 && len(t.dbList[v-1]) == 0 {
			t.dbList = t.dbList[:v-1]
		}
		// Create all of the databases that was specified in the list.
		for _, dbName := range t.dbList {
			_, err := db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbName))
			if err != nil {
				return err
			}
		}
		// Run the usual TPCC precreate logic after.
		if oldPrecreate != nil {
			return oldPrecreate(db)
		}
		return nil
	}

	oldPostLoad := hooks.PostLoad
	// Execute the original post load logic across all the databases.
	hooks.PostLoad = func(ctx context.Context, db *gosql.DB) error {
		for _, dbName := range t.dbList {
			if _, err := db.Exec("USE $1", dbName); err != nil {
				return err
			}
			if err := oldPostLoad(ctx, db); err != nil {
				return err
			}
		}
		return nil
	}

	return hooks
}

func init() {
	workload.Register(tpccMultiDBMeta)
}
