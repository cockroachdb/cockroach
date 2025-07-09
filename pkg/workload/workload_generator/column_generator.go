// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package workload_generator

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// buildGenerator builds a Generator for one column given
//   - col:       metadata for this column
//   - batchIdx:  which batch we’re in (0,1,2…)
//   - baseBatchSize: how many rows per batch (for sequences)
//   - schema:    full YAML schema, so we can recurse on FKs
func buildGenerator(col ColumnMeta, batchIdx, batchSize int, schema Schema) Generator {
	// seed the per-batch RNG
	origSeed := getIntArg(col.Args, "seed", 0)
	seed64 := makeBatchSeed(origSeed, batchIdx)
	rng := rand.New(rand.NewSource(seed64))

	// pick the base generator by type
	var base Generator
	switch col.Type {
	case GenTypeSequence:
		// jump start by batchIdx*batchSize
		base = makeSequenceGenerator(col, batchIdx, batchSize)
	case GenTypeInteger:
		base = makeIntegerGenerator(col, rng)
	case GenTypeFloat:
		base = makeFloatGenerator(col, rng)
	case GenTypeString:
		base = makeStringGenerator(col, rng)
	case GenTypeTimestamp:
		base = makeTimestampGenerator(col, rng)
	case GenTypeUUID:
		base = makeUuidGenerator(col, rng)
	case GenTypeBool:
		base = makeBooleanGenerator(col, rng)
	case GenTypeJson: //missed json type ig, will check
		base = makeJsonGenerator(col, rng)

	default:
		panic("type not supported: " + col.Type)
	}

	// layer on DefaultWrapper if required
	if col.Default != "" && col.DefaultProb > 0 {
		// use a distinct sub-seed so the literal probability RNG
		// doesn’t collide with the main RNG
		wrapperSeed := makeBatchSeed(origSeed, batchIdx^0xdeadbeef)
		base = NewDefaultWrapper(base, col.DefaultProb, col.Default, wrapperSeed)
	}

	// layer on UniqueWrapper if required
	if col.IsUnique && !col.HasForeignKey && col.Type != "sequence" {
		base = NewUniqueWrapper(base, defaultUniqueCap) // or configurable capacity
	}

	// layer on FKWrapper if this column has a foreign key
	if col.HasForeignKey {
		// col.FK might be "tpcc__public__district.d_id"
		parts := strings.SplitN(col.FK, ".", 2)
		if len(parts) != 2 {
			panic(fmt.Sprintf("invalid FK spec %q", col.FK))
		}
		fqTable, childCol := parts[0], parts[1]          // "tpcc__public__district", "d_id"
		pathSegments := strings.Split(fqTable, "__")     // ["tpcc","public","district"]
		parentTable := pathSegments[len(pathSegments)-1] // "district"

		// now look up in your schema map:
		parentMeta := schema[parentTable][0].Columns[childCol]
		parentGen := buildGenerator(parentMeta, batchIdx, batchSize, schema)
		base = NewFkWrapper(parentGen, col.Fanout)
	}

	return base
}

// getIntArg handles int|float64|string in YAML args
func getIntArg(m map[string]interface{}, key string, defaultVal int) int {
	switch v := m[key].(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	case string:
		i, _ := strconv.Atoi(v)
		return i
	default:
		return defaultVal
	}
}

// getFloatArg handles float64|int|string
func getFloatArg(m map[string]interface{}, key string, defaultVal float64) float64 {
	switch v := m[key].(type) {
	case float64:
		return v
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case string:
		f, _ := strconv.ParseFloat(v, 64)
		return f
	default:
		return defaultVal
	}
}

// getStringArg handles string values
func getStringArg(m map[string]interface{}, key string, defaultVal string) string {
	if v, ok := m[key]; ok {
		if s, ok2 := v.(string); ok2 {
			return s
		}
	}
	return defaultVal
}

// ─── Factory ──────────────────────────────────────────────────────────

// splitmix64 scrambles a 32-bit key into a high-quality 64-bit value
func splitmix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

// makeBatchSeed deterministically scrambles the original YAML seed
// with the batch index so each batch uses a distinct RNG stream.
func makeBatchSeed(origSeed, batchIdx int) int64 {
	key := (uint64(origSeed) << 32) | uint64(batchIdx)
	return int64(splitmix64(key))
}

// makeSequenceGenerator creates a SequenceGen that generates sequential integers
func makeSequenceGenerator(col ColumnMeta, batchIdx int, batchSize int) Generator {
	baseStart := getIntArg(col.Args, "start", 0)
	return &SequenceGen{cur: baseStart + batchIdx*batchSize}
}

// makeIntegerGenerator creates an IntegerGen that generates random integers
func makeIntegerGenerator(col ColumnMeta, rng *rand.Rand) Generator {
	min := getIntArg(col.Args, "min", 0)
	max := getIntArg(col.Args, "max", 0)
	nullPct := getFloatArg(col.Args, "null_pct", 0.0)
	return &IntegerGen{r: rng, min: min, max: max, nullPct: nullPct}
}

// makeFloatGenerator creates a FloatGen that generates random floats
func makeFloatGenerator(col ColumnMeta, rng *rand.Rand) Generator {
	min := getFloatArg(col.Args, "min", 0.0)
	max := getFloatArg(col.Args, "max", 0.0)
	round := getIntArg(col.Args, "round", 2)
	nullPct := getFloatArg(col.Args, "null_pct", 0.0)
	return &FloatGen{r: rng, min: min, max: max, round: round, nullPct: nullPct}
}

// makeStringGenerator creates a StringGen that generates random strings
func makeStringGenerator(col ColumnMeta, rng *rand.Rand) Generator {
	min := getIntArg(col.Args, "min", 0)
	max := getIntArg(col.Args, "max", 0)
	nullPct := getFloatArg(col.Args, "null_pct", 0.0)
	return &StringGen{r: rng, min: min, max: max, nullPct: nullPct}
}

// makeTimestampGenerator creates a TimestampGen that generates random timestamps
func makeTimestampGenerator(col ColumnMeta, rng *rand.Rand) Generator {
	// parse Python-style format → Go layout
	startStr := getStringArg(col.Args, "start", "2000-01-01")
	endStr := getStringArg(col.Args, "end", timeutil.Now().Format("2006-01-02"))
	pyFmt := getStringArg(col.Args, "format", "%Y-%m-%d %H:%M:%S.%f")
	var layout string
	switch pyFmt {
	case "%Y-%m-%d %H:%M:%S.%f":
		layout = "2006-01-02 15:04:05.000000"
	case "%Y-%m-%d":
		layout = "2006-01-02"
	default:
		layout = time.RFC3339Nano
	}
	st, err1 := time.Parse(layout, startStr)
	et, err2 := time.Parse(layout, endStr)
	if err1 != nil || err2 != nil {
		st, _ = time.Parse(time.RFC3339, startStr)
		et, _ = time.Parse(time.RFC3339, endStr)
		layout = time.RFC3339
	}
	span := et.UnixNano() - st.UnixNano()
	if span <= 0 {
		span = 1
	}
	nullPct := getFloatArg(col.Args, "null_pct", 0.0)
	return &TimestampGen{r: rng, startNS: st.UnixNano(), spanNS: span, layout: layout, nullPct: nullPct}
}

// makeUuidGenerator creates a UUIDGen that generates random UUIDs
func makeUuidGenerator(col ColumnMeta, rng *rand.Rand) Generator {
	return &UUIDGen{r: rng}
}

// makeBooleanGenerator creates a BoolGen that generates random booleans
func makeBooleanGenerator(col ColumnMeta, rng *rand.Rand) Generator {
	nullPct := getFloatArg(col.Args, "null_pct", 0.0)
	return &BoolGen{r: rng, nullPct: nullPct}
}

// makeJsonGenerator creates a JsonGen that generates random JSON strings
func makeJsonGenerator(col ColumnMeta, rng *rand.Rand) Generator {
	// JSON is just a StringGen plus a wrapper
	min := getIntArg(col.Args, "min", defaultJSONMinLen)
	max := getIntArg(col.Args, "max", defaultJSONMaxLen)
	nullPct := getFloatArg(col.Args, "null_pct", 0.0)
	sg := &StringGen{r: rng, min: min, max: max, nullPct: nullPct}
	return &JsonGen{strGen: sg}
}
