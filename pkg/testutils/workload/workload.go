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

// Package workload provides an abstraction for generators of sql query loads
// (and requisite initial data) as well as tools for working with these
// generators.
package workload

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"

	"github.com/pkg/errors"
)

// Generator represents one or more sql query loads and associated initial data.
//
// A set of options are used to configure this Generator. Any randomness must be
// deterministic from these options so that table data initialization, query
// work, etc can be distributed by serialization only this options map.
//
// TODO(dan): To support persisted test fixtures, we'll need versioning of
// Generators.
type Generator interface {
	// Name returns a unique and descriptive name for this generator.
	Name() string

	// Tables returns the set of tables for this generator, including schemas
	// and initial data.
	Tables() []Table

	// Ops returns the work functions for this generator. The tables are
	// required to have been created and initialized before running these.
	Ops() []Operation
}

// GeneratorFn is used to register a Generator at init time. A map of string to
// string options are used for configuration, see Generator for details.
type GeneratorFn func(opts map[string]string) (Generator, error)

// Table represents a single table in a Generator. Included is a name, schema,
// and initial data.
type Table struct {
	// Name is the unqualified table name, pre-escaped for use directly in SQL.
	Name string
	// Schema is the SQL formatted schema for this table, with the `CREATE TABLE
	// <name>` prefix omitted.
	Schema string
	// InitialRowCount is the initial number of rows that will be present in the
	// table after setup is completed.
	InitialRowCount int
	// InitialRowFn is a function to deterministically compute the datums in a
	// row of the table's initial data given its index. They are returned as
	// strings and must be pre-escaped for use directly in SQL/CSVs.
	InitialRowFn func(int) []string
}

// Operation represents some SQL query workload performable on a database
// initialized with the requisite tables.
//
// TODO(dan): Finish nailing down the invariants of Operation as more workloads
// are ported to this framework. TPCC in particular should be informative.
type Operation struct {
	// Name is a name for the work performed by this Operation.
	Name string
	// Fn returns a function to be called once per unit of work to be done.
	// Various generator tools use this to track progress.
	Fn func(*gosql.DB) (func(context.Context) error, error)
}

var registered = make(map[string]GeneratorFn)

// Register is a hook for init-time registration of Generator implementations.
// This allows only the necessary generators to be compiled into a given binary.
func Register(fn GeneratorFn) {
	g, err := fn(map[string]string{})
	if err != nil {
		panic(errors.Wrapf(err, "could not register: %T", fn))
	}
	registered[g.Name()] = fn
}

// Get returns the registered Generator with the given name, if it exists.
func Get(name string) (GeneratorFn, error) {
	g, ok := registered[name]
	if !ok {
		return nil, errors.Errorf("unknown generator: %s", name)
	}
	return g, nil
}

// Setup creates the given tables and fills them with initial data via batched
// INSERTs.
//
// The size of the loaded data is returned in bytes, suitable for use with
// SetBytes of benchmarks.
func Setup(db *gosql.DB, tables []Table, batchSize int) (int64, error) {
	if batchSize <= 0 {
		batchSize = 1000
	}
	var insertStmtBuf bytes.Buffer

	var size int64
	for _, table := range tables {
		createStmt := fmt.Sprintf(`CREATE TABLE %s %s`, table.Name, table.Schema)
		if _, err := db.Exec(createStmt); err != nil {
			return 0, err
		}

		for {
			insertStmtBuf.Reset()
			fmt.Fprintf(&insertStmtBuf, `INSERT INTO %s VALUES `, table.Name)
			rowIdx := 0
			for ; rowIdx < table.InitialRowCount; rowIdx++ {
				if rowIdx != 0 {
					insertStmtBuf.WriteString(`,`)
				}
				insertStmtBuf.WriteString(`(`)
				for i, datum := range table.InitialRowFn(rowIdx) {
					size += int64(len(datum))
					if i != 0 {
						insertStmtBuf.WriteString(`,`)
					}
					insertStmtBuf.WriteString(datum)
				}
				insertStmtBuf.WriteString(`)`)
			}
			if rowIdx > 0 {
				insertStmt := insertStmtBuf.String()
				if _, err := db.Exec(insertStmt); err != nil {
					return 0, err
				}
			}
			if rowIdx == table.InitialRowCount {
				break
			}
		}
	}
	return size, nil
}
