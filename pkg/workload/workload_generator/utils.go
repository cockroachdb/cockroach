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

const (
	// defaultFanout is the default fan-out multiplier for FK child rows
	defaultFanout = 10
	// fkSeedMapSize approximates the expected number of FK seed entries
	fkSeedMapSize = 256
	// seedKeyDelimiter separates namespace parts in FK seed map keys
	seedKeyDelimiter = "__"
	// nullPct is the key for nullability percentage in args maps
	nullPct = "null_pct"
)

// GeneratorType is an enum for all the data generator types.
type GeneratorType string

const (
	GenTypeSequence  GeneratorType = "sequence"
	GenTypeInteger   GeneratorType = "integer"
	GenTypeUUID      GeneratorType = "uuid"
	GenTypeBit       GeneratorType = "bit"
	GenTypeBytes     GeneratorType = "bytes"
	GenTypeString    GeneratorType = "string"
	GenTypeFloat     GeneratorType = "float"
	GenTypeDate      GeneratorType = "date"
	GenTypeTimestamp GeneratorType = "timestamp"
	GenTypeBool      GeneratorType = "bool"
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

	simpleNumberRe   = regexp.MustCompile(`^[+-]?\d+(?:\.\d+)?$`)
	quotedStrRe      = regexp.MustCompile(`^'.*'$`)
	booleanLiteralRe = regexp.MustCompile(`^(?i:true|false)$`)
)

// Schema is the map of TableBlocks, one per table, which is used by all data generators
type Schema map[string][]TableBlock

// buildInitialBlocks creates one TableBlock per table and collects FK seeds.
func buildInitialBlocks(
	allSchemas map[string]*TableSchema, dbName string, rng *rand.Rand, baseRowCount int,
) (Schema, map[[2]string]int) {
	out := make(Schema, len(allSchemas))
	fkSeed := make(map[[2]string]int, fkSeedMapSize)

	for tblName, schema := range allSchemas {
		block := TableBlock{
			Count:         baseRowCount, // This is the initial row count, which will be adjusted later.
			Columns:       make(map[string]ColumnMeta, len(schema.Columns)),
			PK:            schema.PrimaryKeys,
			SortBy:        make([]string, 0),
			Unique:        schema.UniqueConstraints,
			OriginalTable: schema.OriginalTable,
			ColumnOrder:   schema.ColumnOrder,
			TableNumber:   schema.TableNumber,
		}

		// Populate columns and record FK seeds.
		for _, col := range schema.Columns {
			cm, seed, seedable := buildColumnMeta(tblName, dbName, col, rng)
			block.Columns[col.Name] = cm
			if seedable {
				recordFKSeed(tblName, col.Name, seed, dbName, fkSeed)
			}
		}

		out[tblName] = []TableBlock{block}
	}

	return out, fkSeed
}

// makeColumnMeta maps a Column into ColumnMeta, returning any FK-seed if present.
func buildColumnMeta(tblName, dbName string, col *Column, rng *rand.Rand) (ColumnMeta, int, bool) {
	args, colMeta := identifyTypeAndMapArgs(col, rng)

	// Default value probability
	if col.Default != "" && isLiteralDefault(col.Default) {
		colMeta.Default = col.Default
		colMeta.DefaultProb = 0.2
	}

	// Extract any seed from args
	seed, seedable := 0, false
	if s, ok := args["seed"].(int); ok {
		seed, seedable = s, true
	}

	return colMeta, seed, seedable
}

// identifyTypeAndMapArgs determines the SQL type of column and maps it to
// the corresponding workload generator type and arguments. It also sets
// the nullability and primary key properties in the ColumnMeta.
func identifyTypeAndMapArgs(col *Column, rng *rand.Rand) (map[string]any, ColumnMeta) {
	typ, args := mapSQLType(col.ColType, col, rng)

	// Base ColumnMeta
	cm := ColumnMeta{
		Type:          typ,
		Args:          args,
		IsPrimaryKey:  col.IsPrimaryKey,
		IsUnique:      col.IsUnique,
		Default:       "",
		DefaultProb:   0,
		HasForeignKey: false,
	}

	// Nullability
	if col.IsNullable && !col.IsPrimaryKey {
		cm.Args[nullPct] = 0.1
	} else {
		cm.Args[nullPct] = 0.0
	}
	return args, cm
}

// recordFKSeed stores a seed for multiple namespace variants of tblName.colName
func recordFKSeed(tblName, colName string, seed int, dbName string, fkSeed map[[2]string]int) {
	// base table
	fkSeed[[2]string{tblName, colName}] = seed
	// public schema variant
	publicKey := "public" + seedKeyDelimiter + tblName
	fkSeed[[2]string{publicKey, colName}] = seed
	// fully qualified namespace
	fullKey := dbName + seedKeyDelimiter + publicKey
	fkSeed[[2]string{fullKey, colName}] = seed
}

// wireForeignKeys sets FK metadata on each ColumnMeta based on schema.ForeignKeys.
func wireForeignKeys(
	blocks Schema, allSchemas map[string]*TableSchema, fkSeed map[[2]string]int, rng *rand.Rand,
) {
	for tblName, tblSchema := range allSchemas {
		block := &blocks[tblName][0]
		for _, fk := range tblSchema.ForeignKeys {
			locals := fk[0].([]string)
			parentTbl := fk[1].(string)
			parents := fk[2].([]string)

			// Composite ID for multi-col FKs
			compositeID := 0
			if len(locals) > 1 {
				compositeID = rng.Intn(1 << 30)
			}

			for i, lc := range locals {
				cm := block.Columns[lc]
				cm.HasForeignKey = true
				cm.FK = canonical(parentTbl) + "." + parents[i]
				cm.FKMode = "block"
				cm.Fanout = defaultFanout
				cm.CompositeID = compositeID
				seedKey := [2]string{parentTbl, parents[i]}
				cm.ParentSeed = float64(fkSeed[seedKey])
				block.Columns[lc] = cm
			}
		}
	}
}

// adjustFanoutForPureFKPKs drops fanout to 1 if all PKs are foreign keys.
func adjustFanoutForPureFKPKs(blocks Schema) {
	for _, tblBlocks := range blocks {
		blk := &tblBlocks[0]
		allPKsAreFK := true
		for _, pk := range blk.PK {
			if !blk.Columns[pk].HasForeignKey {
				allPKsAreFK = false
				break
			}
		}
		if allPKsAreFK {
			for name, cm := range blk.Columns {
				if cm.HasForeignKey {
					cm.Fanout = 1
					blk.Columns[name] = cm
				}
			}
		}
	}
}

// computeRowCounts adjusts each block.Count by the smallest FK fanout product.
func computeRowCounts(blocks Schema, baseRowCount int) {
	for _, tblBlocks := range blocks {
		blk := &tblBlocks[0]
		// gather products for FK columns
		prods := []int{}
		for _, cm := range blk.Columns {
			if !cm.HasForeignKey {
				continue
			}
			prods = append(prods, fanoutProduct(cm, blocks))
		}
		mult := 1
		if len(prods) > 0 {
			mult = min(prods)
		}
		blk.Count = baseRowCount * mult
	}
}

// min returns the smallest integer in a slice, or 1 if empty.
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

// parseFK splits "table.col" into its components.
func parseFK(fk string) (string, string) {
	parts := strings.SplitN(fk, ".", 2)
	if len(parts) != 2 {
		panic(fmt.Sprintf("invalid FK spec %q", fk))
	}
	return parts[0], parts[1]
}

// fanoutProduct computes the cascaded fanout product following the FK chain.
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
	return simpleNumberRe.MatchString(txt) ||
		quotedStrRe.MatchString(txt) ||
		booleanLiteralRe.MatchString(txt)
}

// atoi is a helper function to convert a string to an integer.
func atoi(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}

// setArgsRange sets the "min" and "max" keys in the args map to the specified range.
func setArgsRange(args map[string]any, min, max int) {
	args["min"] = min
	args["max"] = max
}

// canonical replaces "." with "__" to match the legacy YAML format.
func canonical(name string) string { return strings.ReplaceAll(name, ".", "__") }

// mapSQLType maps a SQL column type to the workload generator type and
// argument set expected by cockroach workloads. The returned map may
// include bounds, formatting information or other hints used by the
// data generators.
func mapSQLType(sql string, col *Column, rng *rand.Rand) (GeneratorType, map[string]any) {
	sql = strings.ToLower(sql)
	args := map[string]any{"seed": rng.Intn(100)}

	switch {
	case strings.HasPrefix(sql, "int") ||
		sql == "integer" || sql == "bigint" || sql == "smallint" || sql == "serial":
		return mapIntegerType(sql, col, args)

	case sql == "uuid":
		return GenTypeUUID, args

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
		return GenTypeBool, args
	}
	setArgsRange(args, 5, 30)
	return GenTypeString, args
}

func mapIntegerType(sql string, col *Column, args map[string]any) (GeneratorType, map[string]any) {
	if col.IsPrimaryKey || col.IsUnique {
		return GenTypeSequence, map[string]any{"start": 1, "seed": args["seed"]}
	}
	setArgsRange(args, -(1 << 31), (1<<31)-1)
	return GenTypeInteger, args
}

func mapBitType(sql string, col *Column, args map[string]any) (GeneratorType, map[string]any) {
	m := bitRe.FindStringSubmatch(sql)
	size := 1
	if m[2] != "" {
		size = atoi(m[2])
	}
	args["size"] = size
	return GenTypeBit, args
}

func mapByteType(sql string, col *Column, args map[string]any) (GeneratorType, map[string]any) {
	args["size"] = 1
	return GenTypeBytes, args
}

func mapVarcharType(sql string, col *Column, args map[string]any) (GeneratorType, map[string]any) {
	m := varcharRe.FindStringSubmatch(sql)
	length := atoi(m[2])
	setArgsRange(args, 1, length)
	return GenTypeString, args
}

func mapCharType(sql string, col *Column, args map[string]any) (GeneratorType, map[string]any) {
	n := atoi(charRe.FindStringSubmatch(sql)[1])
	setArgsRange(args, n, n)
	return GenTypeString, args
}

func mapDecimalType(sql string, col *Column, args map[string]any) (GeneratorType, map[string]any) {
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
	return GenTypeFloat, args
}

func mapPlainStringType(
	sql string, col *Column, args map[string]any,
) (GeneratorType, map[string]any) {
	setArgsRange(args, 5, 30)
	return GenTypeString, args
}

func mapFloatType(sql string, col *Column, args map[string]any) (GeneratorType, map[string]any) {
	setArgsRange(args, 0, 1)
	args["round"] = 2
	return GenTypeFloat, args
}

func mapDateType(sql string, col *Column, args map[string]any) (GeneratorType, map[string]any) {
	args["start"] = "2000-01-01"
	args["end"] = "2025-01-01"
	args["format"] = "%Y-%m-%d"
	return GenTypeDate, args
}

func mapTimestampType(
	sql string, col *Column, args map[string]any,
) (GeneratorType, map[string]any) {
	args["start"] = "2000-01-01"
	args["end"] = "2025-01-01"
	args["format"] = "%Y-%m-%d %H:%M:%S.%f"
	return GenTypeTimestamp, args
}
