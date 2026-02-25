// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kv

import (
	"context"
	"crypto/sha1"
	gosql "database/sql"
	"encoding/binary"
	"fmt"
	"hash"
	"math"
	"math/big"
	"math/rand"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/dbexec"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/spf13/pflag"
)

// Ensure kv implements the DialectProvider interface.
var _ workload.DialectProvider = (*kv)(nil)
var _ workload.DialectSetter = (*kv)(nil)

var RandomSeed = workload.NewInt64RandomSeed()

type keyRange struct {
	min, max int64
}

func (r keyRange) totalKeys() uint64 {
	return uint64(r.max - r.min)
}

type kv struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags
	timeout   time.Duration

	batchSize                            int
	minBlockSizeBytes, maxBlockSizeBytes int
	cycleLength                          int64
	readPercent                          int
	followerReadPercent                  int
	spanPercent                          int
	delPercent                           int
	spanLimit                            int
	writesUseSelectForUpdate             bool
	writeSeq                             string
	sequential                           bool
	zipfian                              bool
	sfuDelay                             time.Duration
	longRunningTxn                       bool
	longRunningTxnPriority               string
	longRunningTxnNumWrites              int
	splits                               int
	scatter                              bool
	secondaryIndex                       bool
	shards                               int
	targetCompressionRatio               float64
	enum                                 bool
	keySize                              int
	insertCount                          int
	txnQoS                               string
	prepareReadOnly                      bool
	writesUseSelect1                     bool
	alwaysIncKeySeq                      bool
	dialect                              string
}

func init() {
	workload.Register(kvMeta)
}

var kvMeta = workload.Meta{
	Name:        `kv`,
	Description: `KV reads and writes to keys spread randomly across the cluster.`,
	Details: `
	By default, keys are picked uniformly at random across the cluster.
	--concurrency workers alternate between doing selects and upserts (according
	to a --read-percent ratio). Each select/upsert reads/writes a batch of --batch
	rows. The write keys are randomly generated in a deterministic fashion
	(alternatively it could changed to use --sequential or --zipfian distribution).
	Reads select a random batch of ids out of the ones previously written.
	--write-seq can be used to incorporate data produced by a previous run into
	the current run.
	`,
	Version:    `1.0.0`,
	RandomSeed: RandomSeed,
	New: func() workload.Generator {
		g := &kv{}
		g.flags.FlagSet = pflag.NewFlagSet(`kv`, pflag.ContinueOnError)
		g.flags.Meta = map[string]workload.FlagMeta{
			`batch`:                       {RuntimeOnly: true},
			`sfu-wait-delay`:              {RuntimeOnly: true},
			`sfu-writes`:                  {RuntimeOnly: true},
			`long-running-txn`:            {RuntimeOnly: true},
			`long-running-txn-num-writes`: {RuntimeOnly: true},
			`read-percent`:                {RuntimeOnly: true},
			`span-percent`:                {RuntimeOnly: true},
			`span-limit`:                  {RuntimeOnly: true},
			`del-percent`:                 {RuntimeOnly: true},
			`splits`:                      {RuntimeOnly: true},
			`scatter`:                     {RuntimeOnly: true},
			`timeout`:                     {RuntimeOnly: true},
			`prepare-read-only`:           {RuntimeOnly: true},
			`sel1-writes`:                 {RuntimeOnly: true},
		}
		g.flags.IntVar(&g.batchSize, `batch`, 1,
			`Number of blocks to read/insert in a single SQL statement.`)
		g.flags.IntVar(&g.minBlockSizeBytes, `min-block-bytes`, 1,
			`Minimum amount of raw data written with each insertion.`)
		g.flags.IntVar(&g.maxBlockSizeBytes, `max-block-bytes`, 1,
			`Maximum amount of raw data written with each insertion`)
		g.flags.Int64Var(&g.cycleLength, `cycle-length`, math.MaxInt64,
			`Number of keys repeatedly accessed by each writer through upserts.`)
		g.flags.IntVar(&g.readPercent, `read-percent`, 0,
			`Percent (0-100) of operations that are reads of existing keys.`)
		g.flags.IntVar(&g.followerReadPercent, `follower-read-percent`, 0,
			`Percent (0-100) of read operations that are follower reads.`)
		g.flags.IntVar(&g.spanPercent, `span-percent`, 0,
			`Percent (0-100) of operations that are spanning queries of all ranges.`)
		g.flags.IntVar(&g.delPercent, `del-percent`, 0,
			`Percent (0-100) of operations that delete existing keys.`)
		g.flags.IntVar(&g.spanLimit, `span-limit`, 0,
			`LIMIT count for each spanning query, or 0 for no limit`)
		g.flags.BoolVar(&g.writesUseSelectForUpdate, `sfu-writes`, false,
			`Use SFU and transactional writes with a sleep after SFU.`)
		g.flags.BoolVar(&g.zipfian, `zipfian`, false,
			`Pick keys in a zipfian distribution instead of randomly.`)
		g.flags.BoolVar(&g.sequential, `sequential`, false,
			`Pick keys sequentially instead of randomly.`)
		g.flags.StringVar(&g.writeSeq, `write-seq`, "",
			`Initial write sequence value. Can be used to use the data produced by a previous run. `+
				`It has to be of the form (R|S)<number>, where S implies that it was taken from a `+
				`previous --sequential run and R implies a previous random run.`)
		g.flags.IntVar(&g.splits, `splits`, 0,
			`Number of splits to perform before starting normal operations.`)
		g.flags.BoolVar(&g.scatter, `scatter`, false,
			`Scatter ranges before starting normal operations.`)
		g.flags.BoolVar(&g.secondaryIndex, `secondary-index`, false,
			`Add a secondary index to the schema.`)
		g.flags.IntVar(&g.shards, `num-shards`, 0,
			`Number of shards to create on the primary key.`)
		g.flags.Float64Var(&g.targetCompressionRatio, `target-compression-ratio`, 1.0,
			`Target compression ratio for data blocks. Must be >= 1.0.`)
		g.flags.BoolVar(&g.enum, `enum`, false,
			`Inject an enum column and use it.`)
		g.flags.IntVar(&g.insertCount, `insert-count`, 0,
			`Number of rows to insert before beginning the workload. Keys are inserted `+
				`uniformly over the key range.`)
		g.flags.DurationVar(&g.timeout, `timeout`, 0, `Client-side statement timeout.`)
		RandomSeed.AddFlag(&g.flags)
		g.flags.IntVar(&g.keySize, `key-size`, 0,
			`Use string key of appropriate size instead of int`)
		g.flags.DurationVar(&g.sfuDelay, `sfu-wait-delay`, 10*time.Millisecond,
			`Delay after SFU when using --sfu-writes (or after SELECT 1 when using --sel1-writes).`)
		g.flags.StringVar(&g.txnQoS, `txn-qos`, `regular`,
			`Set default_transaction_quality_of_service session variable, accepted`+
				`values are 'background', 'regular' and 'critical'.`)
		g.flags.BoolVar(&g.prepareReadOnly, `prepare-read-only`, false, `Prepare and perform only read statements.`)
		g.flags.BoolVar(&g.writesUseSelect1, `sel1-writes`, false,
			`Use SELECT 1 as the first statement of transactional writes with a sleep after SELECT 1.`)
		g.flags.BoolVar(&g.longRunningTxn, `long-running-txn`, false,
			`Use a long-running transaction for running lock contention scenarios. If run with `+
				`--sfu-writes or --sel1-writes, it will use those writes in the long-running transaction; `+
				`otherwise, it will use regular writes. Each long-running write transaction counts for a`+
				`single write, as measured by --read-percent.`)
		g.flags.StringVar(&g.longRunningTxnPriority, `long-running-txn-priority`, `normal`,
			`Priority of the long-running transaction`)
		g.flags.IntVar(&g.longRunningTxnNumWrites, `long-running-txn-num-writes`, 10,
			`Number of writes in the long-running transaction when using --long-running-txn.`)
		g.flags.BoolVar(&g.alwaysIncKeySeq, `always-inc-key-seq`, false,
			`Increment the random key seq num for all operations, not just writes.`)
		g.connFlags = workload.NewConnFlags(&g.flags)
		return g
	},
}

// Meta implements the Generator interface.
func (*kv) Meta() workload.Meta { return kvMeta }

// Dialect implements the DialectProvider interface.
func (w *kv) Dialect() string { return w.dialect }

// SetDialect implements the DialectSetter interface.
func (w *kv) SetDialect(dialect string) error {
	w.dialect = strings.ToLower(dialect)
	if w.dialect == "" {
		w.dialect = "crdb"
	}
	return nil
}

// Flags implements the Flagser interface.
func (w *kv) Flags() workload.Flags { return w.flags }

// ConnFlags implements the ConnFlagser interface.
func (w *kv) ConnFlags() *workload.ConnFlags { return w.connFlags }

// Hooks implements the Hookser interface.
func (w *kv) Hooks() workload.Hooks {
	return workload.Hooks{
		PostLoad: func(_ context.Context, db *gosql.DB) error {
			sd := w.createSchemaDialect()
			for _, stmt := range sd.PostLoadStmts("kv", w.enum, w.scatter) {
				if _, err := db.Exec(stmt); err != nil {
					return err
				}
			}
			return nil
		},
		Validate: w.validateConfig,
	}
}

func (w *kv) validateConfig() (err error) {
	if w.maxBlockSizeBytes < w.minBlockSizeBytes {
		return errors.Errorf("Value of 'max-block-bytes' (%d) must be greater than or equal to value of 'min-block-bytes' (%d)",
			w.maxBlockSizeBytes, w.minBlockSizeBytes)
	}
	if w.splits < 0 {
		return errors.Errorf("Value of `--splits` (%d) must not be negative",
			w.splits)
	}
	if w.cycleLength < 1 {
		return errors.Errorf("Value of `--cycle-length` (%d) must be greater than 0",
			w.cycleLength)
	}
	if w.sequential && w.cycleLength <= int64(w.splits) {
		return errors.Errorf("Value of `--splits` (%d) must be less than the value of `--cycle-length` (%d) if --sequential is used",
			w.splits, w.cycleLength)
	}
	if w.sequential && w.zipfian {
		return errors.New("'sequential' and 'zipfian' cannot both be enabled")
	}
	if w.shards > 0 && !(w.sequential || w.zipfian) {
		return errors.New("'num-shards' only work with 'sequential' or 'zipfian' key distributions")
	}
	if w.readPercent+w.spanPercent+w.delPercent > 100 {
		return errors.New("'read-percent', 'span-percent' and 'del-precent' combined exceed 100%")
	}
	if w.prepareReadOnly && w.readPercent < 100 {
		return errors.New("'prepare-read-only' can only be used with 'read-percent' set to 100")
	}
	if w.targetCompressionRatio < 1.0 || math.IsNaN(w.targetCompressionRatio) {
		return errors.New("'target-compression-ratio' must be a number >= 1.0")
	}
	if w.keySize != 0 && w.keySize < minStringKeyDigits {
		return errors.Errorf("key size must be >= %d to fit integer part, requested %d",
			minStringKeyDigits, w.keySize)
	}
	if w.writeSeq != "" {
		first := w.writeSeq[0]
		if len(w.writeSeq) < 2 || (first != 'R' && first != 'S' && first != 'Z') {
			return fmt.Errorf("--write-seq has to be of the form '(R|S|Z)<num>'")
		}
		rest := w.writeSeq[1:]
		var err error
		_, err = strconv.Atoi(rest)
		if err != nil {
			return errors.Errorf("--write-seq has to be of the form '(R|S|Z)<num>'")
		}
		if first == 'R' && (w.sequential || w.zipfian) {
			return errors.Errorf("random --write-seq incompatible with a --sequential and --zipfian")
		}
		if first == 'S' && !w.sequential {
			return errors.Errorf(
				"sequential --write-seq is incompatible with a --zipfian and default (random) key sequence")
		}
		if first == 'Z' && !w.zipfian {
			return errors.Errorf(
				"zipfian --write-seq is incompatible with a --sequential or default (random) key sequence")
		}
	}
	if w.txnQoS != "" && w.txnQoS != "background" &&
		w.txnQoS != "regular" && w.txnQoS != "critical" {
		return errors.Errorf(
			"--txn-qos must be one of 'background', 'regular' or 'critical', found %s", w.txnQoS,
		)
	}
	// We create generator and discard it to have a single piece of code that
	// handles generator type which affects target key range.
	kg := w.createKeyGenerator()
	if kg.kr.totalKeys() < uint64(w.insertCount) {
		return errors.Errorf(
			"`--insert-count` (%d) is greater than the number of unique keys that could be possibly generated [%d,%d)",
			w.insertCount, kg.kr.min, kg.kr.max)
	}

	// Validate dialect.
	validDialects := map[string]bool{
		"crdb": true, "": true, "postgres": true, "aurora": true, "dsql": true, "spanner": true,
	}
	if !validDialects[w.dialect] {
		return errors.Errorf("unknown dialect: %s", w.dialect)
	}

	// Validate dialect-specific features.
	if w.dialect != "crdb" && w.dialect != "" {
		var unsupported []string
		if w.shards > 0 {
			unsupported = append(unsupported, "--num-shards")
		}
		if w.txnQoS != "regular" && w.txnQoS != "" {
			unsupported = append(unsupported, "--txn-qos")
		}
		if w.splits > 0 {
			unsupported = append(unsupported, "--splits")
		}
		if w.scatter {
			unsupported = append(unsupported, "--scatter")
		}
		if w.enum {
			unsupported = append(unsupported, "--enum")
		}
		// --sfu-writes and --sel1-writes are handled by validateCapabilities
		// since they are supported by both CRDBExecutor and PGXExecutor.
		if w.longRunningTxn {
			unsupported = append(unsupported, "--long-running-txn")
		}
		if w.prepareReadOnly {
			unsupported = append(unsupported, "--prepare-read-only")
		}
		// For postgres dialects, follower reads are not supported.
		if (w.dialect == "postgres" || w.dialect == "aurora" || w.dialect == "dsql") &&
			w.followerReadPercent > 0 {
			unsupported = append(unsupported, "--follower-read-percent")
		}
		if len(unsupported) > 0 {
			return errors.Errorf(
				"flags %s are not supported for dialect %s",
				strings.Join(unsupported, ", "),
				w.dialect,
			)
		}
	}

	return nil
}

// Note that sequence is only exposed for testing purposes and is used by
// returned keyGenerators.
func (w *kv) createKeyGenerator() keyGeneratorConfig {
	var writeSeq atomic.Int64
	if w.writeSeq != "" {
		writeSeqVal, err := strconv.Atoi(w.writeSeq[1:])
		if err != nil {
			panic("creating generator from unvalidated workload")
		}
		writeSeq.Store(int64(writeSeqVal))
	}

	var kg keyGeneratorConfig
	kg.seq = &sequence{max: w.cycleLength, val: &writeSeq}

	switch {
	case w.zipfian:
		kg.seqPrefix = 'Z'
		kg.newState = func() *keyGeneratorState {
			return &keyGeneratorState{
				rand:   rand.New(rand.NewSource(timeutil.Now().UnixNano())),
				mapKey: &zipfKeyMapper{zipf: newZipf(1.1, 1, uint64(math.MaxInt64))},
			}
		}
		kg.kr = keyRange{
			min: 0,
			max: math.MaxInt64,
		}
	case w.sequential:
		kg.seqPrefix = 'S'
		kg.newState = func() *keyGeneratorState {
			return &keyGeneratorState{
				rand:   rand.New(rand.NewSource(timeutil.Now().UnixNano())),
				mapKey: sequentialKeyMapper{},
			}
		}
		kg.kr = keyRange{
			min: 0,
			max: w.cycleLength,
		}
	default:
		kg.seqPrefix = 'R'
		kg.newState = func() *keyGeneratorState {
			return &keyGeneratorState{
				rand:   rand.New(rand.NewSource(timeutil.Now().UnixNano())),
				mapKey: &hashKeyMapper{hasher: sha1.New()},
			}
		}
		kg.kr = keyRange{
			min: math.MinInt64,
			max: math.MaxInt64,
		}
	}

	if w.keySize == 0 {
		kg.transformer = intKeyTransformer{}
	} else {
		kg.transformer = stringKeyTransformer{
			startOffset: kg.kr.min,
			fillerSize:  w.keySize - minStringKeyDigits,
		}
	}
	return kg
}

// createSchemaDialect returns the appropriate SchemaDialect for the configured
// dialect. Used by Tables() and Hooks().PostLoad for schema generation.
func (w *kv) createSchemaDialect() dbexec.SchemaDialect {
	switch w.dialect {
	case "crdb", "":
		return dbexec.CockroachDialect{}
	case "postgres", "aurora", "dsql":
		return dbexec.PostgresDialect{}
	case "spanner":
		return dbexec.SpannerSchemaDialect{}
	default:
		return dbexec.CockroachDialect{} // validated elsewhere
	}
}

func splitFinder(i, splits int, r keyRange, k keyTransformer) interface{} {
	if splits < 0 || i >= splits {
		panic(fmt.Sprintf("programming error: split index (%d) cannot be less than 0, "+
			"greater than or equal to the total splits (%d)",
			i, splits,
		))
	}

	stride := r.max/int64(splits+1) - r.min/int64(splits+1)
	splitPoint := r.min + int64(i+1)*stride
	return k.getKey(splitPoint)
}

func insertCountKey(idx, count int64, kr keyRange) int64 {
	stride := kr.max/(count+1) - kr.min/(count+1)
	key := kr.min + (idx+1)*stride
	return key
}

// Tables implements the Generator interface.
func (w *kv) Tables() []workload.Table {
	sd := w.createSchemaDialect()
	kg := w.createKeyGenerator()

	table := workload.Table{
		Name:   "kv",
		Schema: sd.SchemaFragment(w.keySize, w.secondaryIndex, w.shards),
	}

	// Add secondary index DDL if requested. For CRDB, the index is inline
	// in the schema; for other dialects, it's a separate CREATE INDEX statement.
	if w.secondaryIndex {
		table.Indexes = sd.SecondaryIndexStmts("kv")
	}

	// Add split points if configured. Only effective for CRDB (other dialects
	// don't support splits, and validateConfig rejects --splits for them).
	if w.splits > 0 {
		table.Splits = workload.Tuples(
			w.splits,
			func(splitIdx int) []interface{} {
				return []interface{}{splitFinder(splitIdx, w.splits, kg.kr, kg.transformer)}
			},
		)
	}

	if w.insertCount > 0 {
		w.addInitialRows(&table, kg)
	}

	return []workload.Table{table}
}

func (w *kv) addInitialRows(table *workload.Table, kg keyGeneratorConfig) {
	if w.insertCount <= 0 {
		return
	}
	const batchSize = 1000
	table.InitialRows = workload.BatchedTuples{
		NumBatches: (w.insertCount + batchSize - 1) / batchSize,
		// If the key sequence is not sequential, duplicates are possible.
		// The zipfian distribution produces duplicates by design, and the
		// hash key mapper can also produce duplicates at larger insert
		// counts (it's at least inevitable at ~1b rows). Marking that the
		// keys may contain duplicates will cause the data loader to use
		// INSERT ... ON CONFLICT DO NOTHING statements.
		MayContainDuplicates: !w.sequential,
		FillBatch: func(batchIdx int, cb coldata.Batch, a *bufalloc.ByteAllocator) {
			rowBegin, rowEnd := batchIdx*batchSize, (batchIdx+1)*batchSize
			if rowEnd > w.insertCount {
				rowEnd = w.insertCount
			}

			var kvtableTypes = []*types.T{
				kg.transformer.getColumnType(),
				types.Bytes,
			}

			cb.Reset(kvtableTypes, rowEnd-rowBegin, coldata.StandardColumnFactory)

			{
				seq := rowBegin
				kg.transformer.fillColumnBatch(cb, a, func() (s int64, ok bool) {
					if seq < rowEnd {
						seq++
						return insertCountKey(int64(seq-1), int64(w.insertCount), kg.kr), true
					}
					return 0, false
				})
			}

			valCol := cb.ColVec(1).Bytes()
			// coldata.Bytes only allows appends so we have to reset it.
			valCol.Reset()
			rndBlock := rand.New(rand.NewSource(RandomSeed.Seed()))

			for rowIdx := rowBegin; rowIdx < rowEnd; rowIdx++ {
				rowOffset := rowIdx - rowBegin
				var payload []byte
				blockSize, uniqueSize := w.randBlockSize(rndBlock)
				*a, payload = a.Alloc(blockSize)
				w.randFillBlock(rndBlock, payload, uniqueSize)
				valCol.Set(rowOffset, payload)
			}
		},
	}
}

// Ops implements the Opser interface.
func (w *kv) Ops(
	ctx context.Context, urls []string, reg *histogram.Registry,
) (workload.QueryLoad, error) {
	exec, err := w.createExecutor(ctx, urls)
	if err != nil {
		return workload.QueryLoad{}, err
	}

	if err := w.validateCapabilities(exec.Capabilities()); err != nil {
		_ = exec.Close()
		return workload.QueryLoad{}, err
	}

	kg := w.createKeyGenerator()
	ql := workload.QueryLoad{}
	var numEmptyResults atomic.Int64

	for i := 0; i < w.connFlags.Concurrency; i++ {
		op := &kvOp{
			config:          w,
			hists:           reg.GetHandle(),
			exec:            exec,
			kg:              kg,
			ks:              kg.newState(),
			numEmptyResults: &numEmptyResults,
		}
		ql.WorkerFns = append(ql.WorkerFns, op.run)
	}

	ql.Close = func(ctx context.Context) error {
		if empty := numEmptyResults.Load(); empty != 0 {
			fmt.Printf("Number of reads that didn't return any results: %d.\n", empty)
		}
		fmt.Printf("Write sequence could be resumed by passing --write-seq=%s to the next run.\n",
			kg.cursor())
		return exec.Close()
	}

	return ql, nil
}

// keyType returns the SQL key type string for the dbexec.Config.
func (w *kv) keyType() string {
	if w.keySize > 0 {
		return fmt.Sprintf("VARCHAR(%d)", w.keySize)
	}
	return "BIGINT"
}

// createExecutor creates and initializes the appropriate Executor based on the
// configured dialect.
func (w *kv) createExecutor(ctx context.Context, urls []string) (dbexec.Executor, error) {
	var exec dbexec.Executor
	switch w.dialect {
	case "crdb", "":
		exec = dbexec.NewCRDBExecutor()
	case "postgres", "aurora", "dsql":
		exec = dbexec.NewPGXExecutor(dbexec.PostgresDialect{})
	case "spanner":
		exec = dbexec.NewSpannerExecutor()
	default:
		return nil, errors.Errorf("unknown dialect: %s", w.dialect)
	}

	cfg := dbexec.Config{
		URLs:           urls,
		Table:          "kv",
		BatchSize:      w.batchSize,
		KeyType:        w.keyType(),
		SecondaryIndex: w.secondaryIndex,
		NumShards:      w.shards,
		Concurrency:    w.connFlags.Concurrency,
		Enum:           w.enum,
		SpanLimit:      w.spanLimit,
		TxnQoS:         w.txnQoS,
	}

	if err := exec.Init(ctx, cfg, w.connFlags); err != nil {
		return nil, err
	}
	return exec, nil
}

// validateCapabilities checks that all requested features are supported by the
// executor. Returns an error listing any unsupported flags.
func (w *kv) validateCapabilities(caps dbexec.Capabilities) error {
	var unsupported []string

	if w.followerReadPercent > 0 && !caps.FollowerReads {
		unsupported = append(unsupported, "--follower-read-percent")
	}
	if w.writesUseSelectForUpdate && !caps.SelectForUpdate {
		unsupported = append(unsupported, "--sfu-writes")
	}
	if w.longRunningTxn && !caps.TransactionPriority {
		unsupported = append(unsupported, "--long-running-txn")
	}
	if w.txnQoS != "regular" && w.txnQoS != "" && !caps.TransactionQoS {
		unsupported = append(unsupported, "--txn-qos")
	}
	if w.shards > 0 && !caps.HashShardedPK {
		unsupported = append(unsupported, "--num-shards")
	}
	if w.splits > 0 && !caps.Splits {
		unsupported = append(unsupported, "--splits")
	}
	if w.scatter && !caps.Scatter {
		unsupported = append(unsupported, "--scatter")
	}
	if w.enum && !caps.EnumColumn {
		unsupported = append(unsupported, "--enum")
	}
	if w.writesUseSelect1 && !caps.Select1 {
		unsupported = append(unsupported, "--sel1-writes")
	}

	if len(unsupported) > 0 {
		return errors.Errorf("flags %s are not supported for dialect %q",
			strings.Join(unsupported, ", "), w.dialect)
	}
	return nil
}

type kvOp struct {
	config          *kv
	hists           *histogram.Histograms
	exec            dbexec.Executor
	kg              keyGeneratorConfig
	ks              *keyGeneratorState
	numEmptyResults *atomic.Int64
}

func (o *kvOp) run(ctx context.Context) (retErr error) {
	if o.config.timeout > 0 {
		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, o.config.timeout)
		defer cancel()
	}

	statementProbability := o.ks.rand.Intn(100) // Determines what statement is executed.

	// Read operation.
	if statementProbability < o.config.readPercent {
		return o.doRead(ctx)
	}
	statementProbability -= o.config.readPercent

	// Delete operation.
	if statementProbability < o.config.delPercent {
		return o.doDelete(ctx)
	}
	statementProbability -= o.config.delPercent

	// Span operation.
	if statementProbability < o.config.spanPercent {
		return o.doSpan(ctx)
	}

	// Write operation (default).
	return o.doWrite(ctx)
}

// doRead executes a read operation using the executor.
func (o *kvOp) doRead(ctx context.Context) error {
	keys := make([]interface{}, o.config.batchSize)
	for i := range keys {
		key := o.kg.readKey(o.ks)
		if o.config.alwaysIncKeySeq {
			key = o.kg.writeKey(o.ks)
		}
		keys[i] = o.kg.transformer.getKey(key)
	}

	start := timeutil.Now()
	var rows dbexec.Rows
	var err error
	opName := "read"

	if o.ks.rand.Intn(100) < o.config.followerReadPercent {
		rows, err = o.exec.FollowerRead(ctx, keys)
		opName = "follower-read"
	} else {
		rows, err = o.exec.Read(ctx, keys)
	}

	if err != nil {
		return err
	}
	defer rows.Close()

	empty := true
	for rows.Next() {
		empty = false
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if empty {
		o.numEmptyResults.Add(1)
	}

	elapsed := timeutil.Since(start)
	o.hists.Get(opName).Record(elapsed)
	return nil
}

// doDelete executes a delete operation using the executor.
func (o *kvOp) doDelete(ctx context.Context) error {
	keys := make([]interface{}, o.config.batchSize)
	for i := range keys {
		key := o.kg.readKey(o.ks)
		if o.config.alwaysIncKeySeq {
			key = o.kg.writeKey(o.ks)
		}
		keys[i] = o.kg.transformer.getKey(key)
	}

	start := timeutil.Now()
	err := o.exec.Delete(ctx, keys)
	elapsed := timeutil.Since(start)
	o.hists.Get("del").Record(elapsed)
	return err
}

// doSpan executes a span operation using the executor.
func (o *kvOp) doSpan(ctx context.Context) error {
	start := timeutil.Now()
	var startKey interface{}
	if o.config.spanLimit > 0 {
		key := o.kg.readKey(o.ks)
		if o.config.alwaysIncKeySeq {
			key = o.kg.writeKey(o.ks)
		}
		startKey = o.kg.transformer.getKey(key)
	}
	_, err := o.exec.Span(ctx, startKey, o.config.spanLimit)
	elapsed := timeutil.Since(start)
	o.hists.Get("span").Record(elapsed)
	return err
}

// doWrite executes a write operation. For simple writes, it uses the executor's
// Write method. For transactional writes (SFU, SELECT 1, long-running txn), it
// uses BeginTx and type-asserts to ExtendedTx for prepared statement access.
func (o *kvOp) doWrite(ctx context.Context) (retErr error) {
	// Helper to generate write rows.
	generateWriteRows := func() []dbexec.Row {
		rows := make([]dbexec.Row, o.config.batchSize)
		for i := range rows {
			rows[i].K = o.kg.transformer.getKey(o.kg.writeKey(o.ks))
			rows[i].V = o.config.randBlock(o.ks.rand)
		}
		return rows
	}

	start := timeutil.Now()

	// Check if we need transactional writes (CRDB-specific features).
	needsTxn := o.config.writesUseSelect1 || o.config.writesUseSelectForUpdate || o.config.longRunningTxn
	if !needsTxn {
		// Simple non-transactional write.
		rows := generateWriteRows()
		err := o.exec.Write(ctx, rows)
		if err != nil {
			return err
		}
		elapsed := timeutil.Since(start)
		o.hists.Get("write").Record(elapsed)
		return nil
	}

	// Transactional write path (CRDB-specific).
	txOpts := dbexec.TxOptions{}
	if o.config.longRunningTxn {
		txOpts.Priority = o.config.longRunningTxnPriority
	}

	tx, err := o.exec.BeginTx(ctx, txOpts)
	if err != nil {
		return err
	}
	defer func() {
		rollbackErr := tx.Rollback(ctx)
		if rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
			retErr = errors.CombineErrors(retErr, rollbackErr)
		}
	}()

	// Type assert to ExtendedTx for transactional operations using prepared
	// statements (SFU, SELECT 1, Write within transaction).
	extTx, ok := tx.(dbexec.ExtendedTx)
	if !ok {
		// This shouldn't happen since validateCapabilities should have rejected
		// these flags for executors that don't support ExtendedTx.
		return errors.New("transactional writes with SFU/SELECT1/long-running-txn require ExtendedTx support")
	}

	iterations := 1
	if o.config.longRunningTxn {
		iterations = o.config.longRunningTxnNumWrites
	}

	for i := 0; i < iterations; i++ {
		rows := generateWriteRows()
		keys := make([]interface{}, len(rows))
		for j, r := range rows {
			keys[j] = r.K
		}

		// SELECT 1 warmup.
		if o.config.writesUseSelect1 {
			if err := extTx.Select1(ctx); err != nil {
				return err
			}
		}

		// SELECT FOR UPDATE.
		if o.config.writesUseSelectForUpdate {
			sfuRows, err := extTx.SelectForUpdate(ctx, keys)
			if err != nil {
				return o.tryHandleWriteErr("write-write-err", start, err)
			}
			sfuRows.Close()
			if err = sfuRows.Err(); err != nil {
				return o.tryHandleWriteErr("write-write-err", start, err)
			}
		}

		// Simulate a transaction that does other work between the
		// sel1 / SFU and write.
		time.Sleep(o.config.sfuDelay)

		// Write within the transaction.
		if err = extTx.Write(ctx, rows); err != nil {
			return o.tryHandleWriteErr("write-write-err", start, err)
		}
	}

	if err = tx.Commit(ctx); err != nil {
		return o.tryHandleWriteErr("write-commit-err", start, err)
	}

	elapsed := timeutil.Since(start)
	o.hists.Get("write").Record(elapsed)
	return nil
}

// tryHandleWriteErr handles write errors by checking if they are serialization
// failures. For CRDB executors that support serialization retry, these errors
// are recorded as metrics rather than returned as failures.
func (o *kvOp) tryHandleWriteErr(name string, start time.Time, err error) error {
	// Only handle serialization failures if the executor supports it.
	if !o.exec.Capabilities().SerializationRetry {
		return err
	}

	// If the error is not an instance of pgconn.PgError, then it is unexpected.
	pgErr := new(pgconn.PgError)
	if !errors.As(err, &pgErr) {
		return err
	}
	// Transaction retry errors are acceptable. Allow the transaction
	// to rollback.
	if pgcode.MakeCode(pgErr.Code) == pgcode.SerializationFailure {
		elapsed := timeutil.Since(start)
		o.hists.Get(name).Record(elapsed)
		return nil
	}
	return err
}

type sequence struct {
	val *atomic.Int64
	max int64
}

func (s *sequence) write() int64 {
	return (s.val.Add(1) - 1) % s.max
}

// read returns the last key index that has been written. Note that the returned
// index might not actually have been written yet, so a read operation cannot
// require that the key is present.
func (s *sequence) read() int64 {
	return s.val.Load() % s.max
}

// Converts int64 based keys into database keys. Workload uses int64 based
// keyspace to allow predictable sharding and splitting. Transformer allows
// mapping integer key into string of arbitrary size for testing keys size.
// First two methods are used in table creation and ops, while last ones are
// only needed for import. If transformer doesn't support import, it should
// be rejected when parsing flags and methods doesn't need to be implemented.
type keyTransformer interface {
	// getKey transforms int keys into table keys for read and write operations.
	getKey(int64) interface{}
	// keySQLType returns an SQL type used by create table for key column.
	keySQLType() string

	// getColumnType returns a key type for inserting initial data.
	getColumnType() *types.T
	// fillColumnBatch needs to populate key column with sequence of keys returned by
	// next.
	fillColumnBatch(cb coldata.Batch, a *bufalloc.ByteAllocator, next func() (seq int64, ok bool))
}

// intKeyTransformer is a noop transformer that passes int keys as is.
type intKeyTransformer struct{}

func (e intKeyTransformer) getKey(key int64) interface{} {
	return key
}

func (e intKeyTransformer) keySQLType() string {
	return "BIGINT"
}

func (e intKeyTransformer) getColumnType() *types.T {
	return types.Int
}

func (e intKeyTransformer) fillColumnBatch(
	cb coldata.Batch, a *bufalloc.ByteAllocator, next func() (seq int64, ok bool),
) {
	keyCol := cb.ColVec(0).Int64()
	var i int
	for key, ok := next(); ok; key, ok = next() {
		keyCol.Set(i, key)
		i++
	}
}

// Minimum size of keys is set to fit base 10 representation of max uint64.
const minStringKeyDigits = 20

// stringKeyTransformer turns int into a zero padded string representation (for
// 64 bit unsigned integers) and appends a random characters up to desired
// length. Note that filler is number of extra bytes on top of 20 digits and
// the the key size parameter as passed to workload.
type stringKeyTransformer struct {
	fillerSize  int
	startOffset int64
}

func (s stringKeyTransformer) getKey(i int64) interface{} {
	return s.getKeyInternal(i)
}

func (s stringKeyTransformer) getKeyInternal(i int64) string {
	filler := randutil.RandString(randutil.NewTestRandWithSeed(i), s.fillerSize, randutil.PrintableKeyAlphabet)
	var bigKey big.Int
	bigKey.Sub(big.NewInt(i), big.NewInt(s.startOffset))
	strKey := bigKey.String()
	prefix := strings.Repeat("0", minStringKeyDigits-len(strKey))
	return fmt.Sprintf("%s%s%s", prefix, strKey, filler)
}

func (s stringKeyTransformer) keySQLType() string {
	return "STRING"
}

func (e stringKeyTransformer) getColumnType() *types.T {
	return types.String
}

func (e stringKeyTransformer) fillColumnBatch(
	cb coldata.Batch, a *bufalloc.ByteAllocator, next func() (seq int64, ok bool),
) {
	keyCol := cb.ColVec(0).Bytes()
	keyCol.Reset()
	var i int
	for intKey, ok := next(); ok; intKey, ok = next() {
		key := e.getKeyInternal(intKey)
		keyCol.Set(i, []byte(key))
		i++
	}
}

// keyGeneratorConfig holds the key generation configuration necessary for
// generating read and write keys. All the fields of a keyGeneratorConfig may be
// accessed concurrently, but each *keyGeneratorState constructed by newState
// must be accessed sequentially (eg, one state per worker).
//
// Read keys may not yet exist and write keys may already exist.
type keyGeneratorConfig struct {
	kr          keyRange
	seq         *sequence
	seqPrefix   rune
	newState    func() *keyGeneratorState
	transformer keyTransformer
}

// keyGeneratorState holds state for a key generator. Any given state must not
// be accessed concurrently. Every worker may be given its own state to avoid
// synchronization.
type keyGeneratorState struct {
	rand   *rand.Rand
	mapKey keyMapper
}

func (g *keyGeneratorConfig) cursor() string {
	return fmt.Sprintf("%s%d", string(g.seqPrefix), g.seq.read())
}

func (g *keyGeneratorConfig) writeKey(state *keyGeneratorState) int64 {
	return state.mapKey.mapKey(g.seq.write())
}

func (g *keyGeneratorConfig) readKey(state *keyGeneratorState) int64 {
	v := g.seq.read()
	if v == 0 {
		return 0
	}
	return state.mapKey.mapKey(state.rand.Int63n(v))
}

type keyMapper interface {
	mapKey(v int64) int64
}

type hashKeyMapper struct {
	hasher hash.Hash
	buf    [sha1.Size]byte
}

func (h *hashKeyMapper) mapKey(v int64) int64 {
	binary.BigEndian.PutUint64(h.buf[:8], uint64(v))
	binary.BigEndian.PutUint64(h.buf[8:16], uint64(RandomSeed.Seed()))
	h.hasher.Reset()
	_, _ = h.hasher.Write(h.buf[:16])
	h.hasher.Sum(h.buf[:0])
	return int64(binary.BigEndian.Uint64(h.buf[:8]))
}

type sequentialKeyMapper struct{}

func (sequentialKeyMapper) mapKey(v int64) int64 { return v }

type zipfKeyMapper struct {
	zipf *zipf
}

func (g *zipfKeyMapper) mapKey(seed int64) int64 {
	randomWithSeed := rand.New(rand.NewSource(seed))
	return int64(g.zipf.Uint64(randomWithSeed))
}

// randBlock returns a sequence of random bytes according to the kv
// configuration.
func (w *kv) randBlock(r *rand.Rand) []byte {
	blockSize, uniqueSize := w.randBlockSize(r)
	block := make([]byte, blockSize)
	w.randFillBlock(r, block, uniqueSize)
	return block
}

// randBlockSize returns two integers, for a random integer to use for inserts
// according to min/max block bytes and the unique bytes given the
// targetCompressionRatio.
func (w *kv) randBlockSize(r *rand.Rand) (block int, unique int) {
	block = r.Intn(w.maxBlockSizeBytes-w.minBlockSizeBytes+1) + w.minBlockSizeBytes
	unique = int(float64(block) / w.targetCompressionRatio)
	if unique < 1 {
		unique = 1
	}
	return block, unique
}

// randFillBlock fills the provided buffer with random bytes. len(buf) -
// uniqueSize entries are repeated.
func (w *kv) randFillBlock(r *rand.Rand, buf []byte, uniqueSize int) {
	for i := range buf {
		if i >= uniqueSize {
			buf[i] = buf[i-uniqueSize]
		} else {
			buf[i] = byte(r.Int() & 0xff)
		}
	}
}
