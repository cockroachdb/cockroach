// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file

package workload_generator

import (
	"math/rand"
	"time"
)

const (
	// defaultFanout is the default fan-out multiplier for FK child rows
	defaultFanout = 10
	// fkSeedMapSize approximates the expected number of FK seed entries
	fkSeedMapSize = 256
	// seedKeyDelimiter separates namespace parts in FK seed map keys
	seedKeyDelimiter = "__"
)

type Schema map[string][]TableBlock

// buildWorkloadSchema orchestrates the construction of a Schema from parsed TableSchemas.
func buildWorkloadSchema(
	allSchemas map[string]*TableSchema,
	dbName string,
	baseRowCount int,
) Schema {
	// Initialize RNG for seeding and composite IDs
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	// 1) Build initial blocks and capture FK seeds
	blocks, fkSeed := buildInitialBlocks(allSchemas, dbName, rng, baseRowCount)

	// 2) Wire up foreign-key relationships in the blocks
	wireForeignKeys(blocks, allSchemas, fkSeed, rng)

	// 3) If a table's PK cols are all FKs, drop its fanout to 1
	adjustFanoutForPureFKPKs(blocks)

	// 4) Recompute each block's row count based on FK fanouts
	computeRowCounts(blocks, baseRowCount)

	return blocks
}

// buildInitialBlocks creates one TableBlock per table and collects FK seeds.
func buildInitialBlocks(
	allSchemas map[string]*TableSchema,
	dbName string,
	rng *rand.Rand,
	baseRowCount int,
) (Schema, map[[2]string]int) {
	out := make(Schema, len(allSchemas))
	fkSeed := make(map[[2]string]int, fkSeedMapSize)

	for tblName, schema := range allSchemas {
		block := TableBlock{
			Count:         baseRowCount,
			Columns:       make(map[string]ColumnMeta, len(schema.Columns)),
			PK:            schema.PrimaryKeys,
			SortBy:        []string{},
			Unique:        schema.UniqueConstraints,
			OriginalTable: schema.OriginalTable,
			ColumnOrder:   schema.ColumnOrder,
			TableNumber:   schema.TableNumber,
		}

		// Populate columns and record FK seeds
		for _, col := range schema.Columns {
			cm, seed, seedable := makeColumnMeta(tblName, dbName, col, rng)
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
func makeColumnMeta(
	tblName, dbName string,
	col *Column,
	rng *rand.Rand,
) (ColumnMeta, int, bool) {
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
		cm.Args["null_pct"] = 0.1
	} else {
		cm.Args["null_pct"] = 0.0
	}

	// Default value probability
	if col.Default != "" && isLiteralDefault(col.Default) {
		cm.Default = col.Default
		cm.DefaultProb = 0.2
	}

	// Extract any seed from args
	seed, seedable := 0, false
	if s, ok := args["seed"].(int); ok {
		seed, seedable = s, true
	}

	return cm, seed, seedable
}

// recordFKSeed stores a seed for multiple namespace variants of tblName.colName
func recordFKSeed(
	tblName, colName string,
	seed int,
	dbName string,
	fkSeed map[[2]string]int,
) {
	// base table
	fkSeed[[2]string{tblName, colName}] = seed
	// public schema variant
	publicKey := "public" + seedKeyDelimiter + tblName
	fkSeed[[2]string{publicKey, colName}] = seed
	// fully qualified namespace
	fullKey := dbName + seedKeyDelimiter + publicKey
	fkSeed[[2]string{fullKey, colName}] = seed
}

// wireForeignKeys sets FK metadata on each ColumnMeta based on schema.ForeignKeys
func wireForeignKeys(
	blocks Schema,
	allSchemas map[string]*TableSchema,
	fkSeed map[[2]string]int,
	rng *rand.Rand,
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

// adjustFanoutForPureFKPKs drops fanout to 1 if all PKs are foreign keys
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

// computeRowCounts adjusts each block.Count by the smallest FK fanout product
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
