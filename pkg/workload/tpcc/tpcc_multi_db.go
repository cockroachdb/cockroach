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
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/jackc/pgx/v5"
)

type tpccMultiDB struct {
	*tpcc

	// dbListFile contains the list of databases that tpcc schema will be
	// created on and have the workload executed on.
	dbListFile string
	dbList     []*tree.ObjectNamePrefix

	// nextDatabase selects the next database in a round robin manner.
	nextDatabase atomic.Uint64

	// initLogic executes the init logic one time.
	initLogic sync.Once
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
		g.tpcc.workloadName = "tpccmultidb"
		g.flags.Meta["txn-preamble-file"] = workload.FlagMeta{RuntimeOnly: true}
		// Support accessing multiple databases via the client driver.
		g.flags.StringVar(&g.dbListFile, "db-list-file", "", "a file containing a list of databases.")
		// Because this workload can create a large number of objects, the import
		// concurrent may need to be limited.
		g.flags.Int(workload.ImportDataLoaderConcurrencyFlag, 32, workload.ImportDataLoaderConcurrencyFlagDescription)
		return &g
	},
}

// runBeforeEachTxn is executed at the start of each transaction
// inside normal tpcc.
func (t *tpccMultiDB) runBeforeEachTxn(ctx context.Context, tx pgx.Tx) error {
	// If multiple DBs are specified via list, select one
	// in a roundrobin manner.
	if t.dbList != nil {
		databaseIdx := int(t.nextDatabase.Add(1) % uint64(len(t.dbList)))
		if _, err := tx.Exec(ctx, "USE $1", t.dbList[databaseIdx].Catalog()); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, fmt.Sprintf("SET search_path = %s", t.dbList[databaseIdx].Schema())); err != nil {
			return err
		}
	}

	return nil
}

// Ops implements the Opser interface.
func (t *tpccMultiDB) Ops(
	ctx context.Context, urls []string, reg *histogram.Registry,
) (workload.QueryLoad, error) {
	if err := t.runInit(); err != nil {
		return workload.QueryLoad{}, err
	}
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
			tbl.ObjectPrefix = db
			tablesPerDb = append(tablesPerDb, tbl)
		}
	}
	return tablesPerDb
}

func (*tpccMultiDB) Meta() workload.Meta { return tpccMultiDBMeta }

func (t *tpccMultiDB) runInit() error {
	var err error
	t.initLogic.Do(func() {
		if t.dbListFile != "" {
			file, err := os.ReadFile(t.dbListFile)
			if err != nil {
				return
			}
			strDbList := strings.Split(string(file), "\n")
			if v := len(strDbList); v > 0 && len(strDbList[v-1]) == 0 {
				strDbList = strDbList[:v-1]
			}

			for _, dbAndSchema := range strDbList {
				parts := strings.Split(dbAndSchema, ".")
				prefix := &tree.ObjectNamePrefix{
					CatalogName:     tree.Name(parts[0]),
					ExplicitCatalog: true,
					SchemaName:      "public",
					ExplicitSchema:  true,
				}
				if len(parts) > 1 {
					prefix.SchemaName = tree.Name(parts[1])
				}
				t.dbList = append(t.dbList, prefix)
			}
		}
		// Execute extra logic at the start of each txn.
		t.onTxnStartFns = append(t.onTxnStartFns, t.runBeforeEachTxn)

	})
	return err
}

func (t *tpccMultiDB) Hooks() workload.Hooks {
	hooks := t.tpcc.Hooks()
	oldPrecreate := hooks.PreCreate
	hooks.PreCreate = func(db *gosql.DB) error {
		if err := t.runInit(); err != nil {
			return err
		}
		// Create all of the databases that was specified in the list.
		for _, dbName := range t.dbList {
			if _, err := db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbName.Catalog())); err != nil {
				return err
			}
			if _, err := db.Exec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s.%s", dbName.Catalog(), dbName.Schema())); err != nil {
				return err
			}
			if _, err := db.Exec("USE $1", dbName.Catalog()); err != nil {
				return err
			}
			if _, err := db.Exec(fmt.Sprintf("SET search_path = %s", dbName.Schema())); err != nil {
				return err
			}
			// Run the usual TPCC pre-create logic after.
			if oldPrecreate == nil {
				continue
			}
			if err := oldPrecreate(db); err != nil {
				return err
			}
		}
		return nil
	}

	oldPostLoad := hooks.PostLoad
	// Execute the original post load logic across all the databases.
	hooks.PostLoad = func(ctx context.Context, db *gosql.DB) error {
		for _, dbName := range t.dbList {
			if _, err := db.Exec("USE $1", dbName.Catalog()); err != nil {
				return err
			}
			if _, err := db.Exec(fmt.Sprintf("SET search_path = %s", dbName.Schema())); err != nil {
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
