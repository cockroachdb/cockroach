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
	"sort"
	"strings"

	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

// Generator represents one or more sql query loads and associated initial data.
//
// A slice of string flags are used to configure this Generator. Any randomness
// must be deterministic from these options so that table data initialization,
// query work, etc can be distributed by sending only these flags.
//
// TODO(dan): To support persisted test fixtures, we'll need versioning of
// Generators.
type Generator interface {
	// Meta returns meta information about this generator, including a name,
	// description, and a function to create instances of it.
	Meta() Meta

	// Flags returns the flags this Generator is configured with.
	Flags() *pflag.FlagSet

	// Configure parses the provided flags, which are only allowed to be the
	// ones in Flags(). Configure may only be called once.
	Configure(flags []string) error

	// Tables returns the set of tables for this generator, including schemas
	// and initial data.
	Tables() []Table

	// Ops returns the work functions for this generator. The tables are
	// required to have been created and initialized before running these.
	Ops() []Operation
}

// Meta is used to register a Generator at init time and holds meta information
// about this generator, including a name, description, and a function to create
// instances of it.
type Meta struct {
	// Name is a unique name for this generator.
	Name string
	// Description is a short description of this generator.
	Description string
	// Version is a semantic version for this generator. It should be bumped
	// whenever InitialRowFn or InitialRowCount change for any of the tables.
	Version string
	// New returns an unconfigured instance of this generator.
	New func() Generator
}

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

var registered = make(map[string]Meta)

// Register is a hook for init-time registration of Generator implementations.
// This allows only the necessary generators to be compiled into a given binary.
func Register(m Meta) {
	if _, ok := registered[m.Name]; ok {
		panic(m.Name + " is already registered")
	}
	registered[m.Name] = m
}

// Get returns the registered Generator with the given name, if it exists.
func Get(name string) (Meta, error) {
	m, ok := registered[name]
	if !ok {
		return Meta{}, errors.Errorf("unknown generator: %s", name)
	}
	return m, nil
}

// Registered returns all registered Generators.
func Registered() []Meta {
	gens := make([]Meta, 0, len(registered))
	for _, gen := range registered {
		gens = append(gens, gen)
	}
	sort.Slice(gens, func(i, j int) bool { return strings.Compare(gens[i].Name, gens[j].Name) < 0 })
	return gens
}

// Setup creates the given tables and fills them with initial data via batched
// INSERTs. batchSize will only be used when positive (but INSERTs are batched
// either way).
//
// The size of the loaded data is returned in bytes, suitable for use with
// SetBytes of benchmarks. This size is defined as the sum of lengths of the
// string representations of the sql (e.g. `1` the int is 1 and `'x'` the string
// is three).
//
// TODO(dan): Is there something better we could be doing here for the size of
// the loaded data?
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

		for rowIdx := 0; rowIdx < table.InitialRowCount; {
			insertStmtBuf.Reset()
			fmt.Fprintf(&insertStmtBuf, `INSERT INTO %s VALUES `, table.Name)

			batchIdx := 0
			for ; batchIdx < batchSize && rowIdx < table.InitialRowCount; batchIdx++ {
				if batchIdx != 0 {
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
				rowIdx++
			}
			if batchIdx > 0 {
				insertStmt := insertStmtBuf.String()
				if _, err := db.Exec(insertStmt); err != nil {
					return 0, err
				}
			}
		}
	}
	return size, nil
}
