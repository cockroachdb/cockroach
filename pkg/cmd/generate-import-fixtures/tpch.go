// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
)

// ColumnType describes the logical type of a TPC-H column. Each output format
// maps these to its native type system (e.g., AVRO "long", "double", "string").
type ColumnType int

const (
	Long   ColumnType = iota + 1 // SQL INTEGER / BIGINT
	Double                       // SQL FLOAT / DECIMAL(p,s) approximated as float64
	Text                         // SQL CHAR(n) / VARCHAR(n)
	Date                         // SQL DATE, parsed as time.Time at ingest and converted per-format at write time
)

// String returns a human-readable name for the column type.
func (ct ColumnType) String() string {
	switch ct {
	case Long:
		return "Long"
	case Double:
		return "Double"
	case Text:
		return "Text"
	case Date:
		return "Date"
	default:
		return fmt.Sprintf("ColumnType(%d)", int(ct))
	}
}

// ColumnDef defines a single column in a TPC-H table.
type ColumnDef struct {
	Name  string
	Type  ColumnType
	Parse func(string) (interface{}, error)
}

// TableDef defines the schema for a TPC-H table.
type TableDef struct {
	Name    string
	Columns []ColumnDef
}

func parseLong(s string) (interface{}, error) {
	return strconv.ParseInt(strings.TrimSpace(s), 10, 64)
}

func parseDouble(s string) (interface{}, error) {
	return strconv.ParseFloat(strings.TrimSpace(s), 64)
}

// parseString strips whitespace padding from dbgen's fixed-width field output.
func parseString(s string) (interface{}, error) {
	return strings.TrimSpace(s), nil
}

// parseDate parses a "YYYY-MM-DD" date string into a time.Time. Each output
// format converts the time.Time to its native representation at write time.
func parseDate(s string) (interface{}, error) {
	return time.Parse("2006-01-02", strings.TrimSpace(s))
}

func longCol(name string) ColumnDef {
	return ColumnDef{Name: name, Type: Long, Parse: parseLong}
}

func doubleCol(name string) ColumnDef {
	return ColumnDef{Name: name, Type: Double, Parse: parseDouble}
}

func stringCol(name string) ColumnDef {
	return ColumnDef{Name: name, Type: Text, Parse: parseString}
}

func dateCol(name string) ColumnDef {
	return ColumnDef{Name: name, Type: Date, Parse: parseDate}
}

// tpchTables defines the TPC-H table schemas. Column order matches the dbgen
// pipe-delimited output files.
var tpchTables = map[string]TableDef{
	"customer": {
		Name: "customer",
		Columns: []ColumnDef{
			longCol("c_custkey"),
			stringCol("c_name"),
			stringCol("c_address"),
			longCol("c_nationkey"),
			stringCol("c_phone"),
			doubleCol("c_acctbal"),
			stringCol("c_mktsegment"),
			stringCol("c_comment"),
		},
	},
	"lineitem": {
		Name: "lineitem",
		Columns: []ColumnDef{
			longCol("l_orderkey"),
			longCol("l_partkey"),
			longCol("l_suppkey"),
			longCol("l_linenumber"),
			doubleCol("l_quantity"),
			doubleCol("l_extendedprice"),
			doubleCol("l_discount"),
			doubleCol("l_tax"),
			stringCol("l_returnflag"),
			stringCol("l_linestatus"),
			dateCol("l_shipdate"),
			dateCol("l_commitdate"),
			dateCol("l_receiptdate"),
			stringCol("l_shipinstruct"),
			stringCol("l_shipmode"),
			stringCol("l_comment"),
		},
	},
	"orders": {
		Name: "orders",
		Columns: []ColumnDef{
			longCol("o_orderkey"),
			longCol("o_custkey"),
			stringCol("o_orderstatus"),
			doubleCol("o_totalprice"),
			dateCol("o_orderdate"),
			stringCol("o_orderpriority"),
			stringCol("o_clerk"),
			longCol("o_shippriority"),
			stringCol("o_comment"),
		},
	},
	"part": {
		Name: "part",
		Columns: []ColumnDef{
			longCol("p_partkey"),
			stringCol("p_name"),
			stringCol("p_mfgr"),
			stringCol("p_brand"),
			stringCol("p_type"),
			longCol("p_size"),
			stringCol("p_container"),
			doubleCol("p_retailprice"),
			stringCol("p_comment"),
		},
	},
	"partsupp": {
		Name: "partsupp",
		Columns: []ColumnDef{
			longCol("ps_partkey"),
			longCol("ps_suppkey"),
			longCol("ps_availqty"),
			doubleCol("ps_supplycost"),
			stringCol("ps_comment"),
		},
	},
	"supplier": {
		Name: "supplier",
		Columns: []ColumnDef{
			longCol("s_suppkey"),
			stringCol("s_name"),
			stringCol("s_address"),
			longCol("s_nationkey"),
			stringCol("s_phone"),
			doubleCol("s_acctbal"),
			stringCol("s_comment"),
		},
	},
	"region": {
		Name: "region",
		Columns: []ColumnDef{
			longCol("r_regionkey"),
			stringCol("r_name"),
			stringCol("r_comment"),
		},
	},
	"nation": {
		Name: "nation",
		Columns: []ColumnDef{
			longCol("n_nationkey"),
			stringCol("n_name"),
			longCol("n_regionkey"),
			stringCol("n_comment"),
		},
	},
}

// allTPCHTables returns the names of all defined TPC-H tables in sorted order.
func allTPCHTables() []string {
	names := make([]string, 0, len(tpchTables))
	for name := range tpchTables {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// getTPCHTable returns the TableDef for a given TPC-H table name.
func getTPCHTable(name string) (TableDef, error) {
	td, ok := tpchTables[name]
	if !ok {
		return TableDef{}, fmt.Errorf("unknown TPC-H table: %s", name)
	}
	return td, nil
}
