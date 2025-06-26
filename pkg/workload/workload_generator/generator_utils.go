// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package workload_generator

import (
	"fmt"
	"math"
	"math/rand"
	"regexp"
	"strconv"
	"strings"
)

var (
	// Regular expressions used to interpret SQL column types in the
	// DDL and map them to workload generator types.
	decimalRe = regexp.MustCompile(`(?i)^(?:decimal|numeric)\s*(?:\(\s*(\d+)\s*,\s*(\d+)\s*\))?$`)
	numericRe = regexp.MustCompile(`(?i)^(decimal|numeric|float|double|real)`)
	varcharRe = regexp.MustCompile(`(?i)^(varchar|character varying)\((\d+)\)`)
	charRe    = regexp.MustCompile(`(?i)^char\((\d+)\)$`)
	bitRe     = regexp.MustCompile(`(?i)^(bit|varbit)(?:\((\d+)\))?`)
	byteRe    = regexp.MustCompile(`(?i)^(bytea|blob|bytes)$`)

	simpleNum = regexp.MustCompile(`^[+-]?\d+(?:\.\d+)?$`)
	quotedStr = regexp.MustCompile(`^'.*'$`)
	boolLit   = regexp.MustCompile(`^(?i:true|false)$`)
)

// min returns the smallest integer in a slice, or 1 if empty
func min(vals []int) int {
	if len(vals) == 0 {
		return 1
	}
	m := vals[0]
	for _, v := range vals[1:] {
		if v < m {
			m = v
		}
	}
	return m
}

// parseFK splits "table.col" into its components
func parseFK(fk string) (string, string) {
	parts := strings.SplitN(fk, ".", 2)
	if len(parts) != 2 {
		panic(fmt.Sprintf("invalid FK spec %q", fk))
	}
	return parts[0], parts[1]
}

// fanoutProduct computes the cascaded fanout product following the FK chain
func fanoutProduct(col ColumnMeta, schema Schema) int {
	prod := 1
	curr := col
	for curr.HasForeignKey {
		prod *= curr.Fanout
		rawTbl, parentCol := parseFK(curr.FK)
		// collapse namespaced table to base name
		tblParts := strings.Split(rawTbl, seedKeyDelimiter)
		simpleTbl := tblParts[len(tblParts)-1]
		blocks, ok := schema[simpleTbl]
		if !ok || len(blocks) == 0 {
			break
		}
		curr = blocks[0].Columns[parentCol]
	}
	return prod
}

// isLiteralDefault determines whether a column default expression is a
// simple literal that can be reproduced by the workload generator.
// Complex expressions are ignored.
func isLiteralDefault(expr string) bool {
	txt := strings.TrimSpace(strings.TrimRight(strings.TrimLeft(expr, "("), ")"))
	return simpleNum.MatchString(txt) ||
		quotedStr.MatchString(txt) ||
		boolLit.MatchString(txt)
}

// Helper numeric functions
func atoi(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}

func setArgsRange(args map[string]any, min, max int) {
	args["min"] = min
	args["max"] = max
}

// canonical replaces "." with "__" to match the legacy YAML format.
func canonical(name string) string { return strings.ReplaceAll(name, ".", "__") }

// decanonical undoes canonical() for at most two components.
func decanonical(name string) string {
	parts := strings.SplitN(name, "__", 2)
	if len(parts) == 1 {
		return parts[0]
	}
	return strings.Join(parts, ".")
}

// mapSQLType maps a SQL column type to the workload generator type and
// argument set expected by cockroach workloads. The returned map may
// include bounds, formatting information or other hints used by the
// data generators.
func mapSQLType(sql string, col *Column, rng *rand.Rand) (string, map[string]any) {
	sql = strings.ToLower(sql)
	args := map[string]any{"seed": rng.Intn(100)}

	switch {
	case strings.HasPrefix(sql, "int") ||
		sql == "integer" || sql == "bigint" || sql == "smallint" || sql == "serial":
		return mapIntegerType(sql, col, args)

	case sql == "uuid":
		return "uuid", args

	case bitRe.MatchString(sql):
		return mapBitType(sql, col, args)

	case byteRe.MatchString(sql):
		return mapByteType(sql, col, args)

	case varcharRe.MatchString(sql):
		return mapVarcharType(sql, col, args)

	case charRe.MatchString(sql):
		return mapCharType(sql, col, args)

	case sql == "text" || sql == "clob" || sql == "string":
		return mapPlainStringType(sql, col, args)

	case decimalRe.MatchString(sql):
		return mapDecimalType(sql, col, args)

	case numericRe.MatchString(sql):
		return mapFloatType(sql, col, args)

	case sql == "date":
		return mapDateType(sql, col, args)

	case sql == "timestamp" || sql == "timestamptz":
		return mapTimestampType(sql, col, args)

	case sql == "bool" || sql == "boolean":
		return "bool", args
	}
	setArgsRange(args, 5, 30)
	return "string", args
}

func mapIntegerType(sql string, col *Column, args map[string]any) (string, map[string]any) {
	if col.IsPrimaryKey || col.IsUnique {
		return "sequence", map[string]any{"start": 1, "seed": args["seed"]}
	}
	setArgsRange(args, -(1 << 31), (1<<31)-1)
	return "integer", args
}

func mapBitType(sql string, col *Column, args map[string]any) (string, map[string]any) {
	m := bitRe.FindStringSubmatch(sql)
	size := 1
	if m[2] != "" {
		size = atoi(m[2])
	}
	args["size"] = size
	return "bit", args
}

func mapByteType(sql string, col *Column, args map[string]any) (string, map[string]any) {
	args["size"] = 1
	return "bytes", args
}

func mapVarcharType(sql string, col *Column, args map[string]any) (string, map[string]any) {
	m := varcharRe.FindStringSubmatch(sql)
	length := atoi(m[2])
	setArgsRange(args, 1, length)
	return "string", args
}

func mapCharType(sql string, col *Column, args map[string]any) (string, map[string]any) {
	n := atoi(charRe.FindStringSubmatch(sql)[1])
	setArgsRange(args, n, n)
	return "string", args
}

func mapDecimalType(sql string, col *Column, args map[string]any) (string, map[string]any) {
	m := decimalRe.FindStringSubmatch(sql)
	if m[1] != "" {
		precision := atoi(m[1])
		scale := atoi(m[2])
		intDigits := precision - scale

		// smallest fractional step: 10^(–scale)
		fracUnit := math.Pow10(-scale)

		var minVal, maxVal float64
		if intDigits > 0 {
			// e.g. p=6,s=4 ⇒ intDigits=2 ⇒ base=99
			base := float64(int(math.Pow10(intDigits)) - 1)
			maxVal = base + (1.0 - fracUnit) // 99.9999
			minVal = -maxVal
		} else {
			// p==s ⇒ only fractional digits, e.g. 0.9999
			maxVal = 1.0 - fracUnit
			minVal = -maxVal
		}
		args["min"] = minVal
		args["max"] = maxVal
		args["round"] = scale
	} else {
		// fallback for DECIMAL without precision
		args["min"] = 0.0
		args["max"] = 1.0
		args["round"] = 2
	}
	return "float", args
}

func mapPlainStringType(sql string, col *Column, args map[string]any) (string, map[string]any) {
	setArgsRange(args, 5, 30)
	return "string", args
}

func mapFloatType(sql string, col *Column, args map[string]any) (string, map[string]any) {
	setArgsRange(args, 0, 1)
	args["round"] = 2
	return "float", args
}

func mapDateType(sql string, col *Column, args map[string]any) (string, map[string]any) {
	args["start"] = "2000-01-01"
	args["end"] = "2025-01-01"
	args["format"] = "%Y-%m-%d"
	return "date", args
}

func mapTimestampType(sql string, col *Column, args map[string]any) (string, map[string]any) {
	args["start"] = "2000-01-01"
	args["end"] = "2025-01-01"
	args["format"] = "%Y-%m-%d %H:%M:%S.%f"
	return "timestamp", args
}
