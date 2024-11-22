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
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/spf13/pflag"
)

const (
	kvSchema = `(
		k %s NOT NULL PRIMARY KEY,
		v BYTES NOT NULL
	)`
	kvSchemaWithIndex = `(
		k %s NOT NULL PRIMARY KEY,
		v BYTES NOT NULL,
		INDEX (v)
	)`
	shardedKvSchema = `(
		k %s NOT NULL PRIMARY KEY USING HASH WITH (bucket_count = %d),
		v BYTES NOT NULL
	)`
	shardedKvSchemaWithIndex = `(
		k %s NOT NULL PRIMARY KEY USING HASH WITH (bucket_count = %d),
		v BYTES NOT NULL,
		INDEX (v)
	)`
)

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
			`batch`:             {RuntimeOnly: true},
			`sfu-wait-delay`:    {RuntimeOnly: true},
			`sfu-writes`:        {RuntimeOnly: true},
			`read-percent`:      {RuntimeOnly: true},
			`span-percent`:      {RuntimeOnly: true},
			`span-limit`:        {RuntimeOnly: true},
			`del-percent`:       {RuntimeOnly: true},
			`splits`:            {RuntimeOnly: true},
			`scatter`:           {RuntimeOnly: true},
			`timeout`:           {RuntimeOnly: true},
			`prepare-read-only`: {RuntimeOnly: true},
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
			`Delay before sfu write transaction commits or aborts`)
		g.flags.StringVar(&g.txnQoS, `txn-qos`, `regular`,
			`Set default_transaction_quality_of_service session variable, accepted`+
				`values are 'background', 'regular' and 'critical'.`)
		g.flags.BoolVar(&g.prepareReadOnly, `prepare-read-only`, false, `Prepare and perform only read statements.`)
		g.connFlags = workload.NewConnFlags(&g.flags)
		return g
	},
}

// Meta implements the Generator interface.
func (*kv) Meta() workload.Meta { return kvMeta }

// Flags implements the Flagser interface.
func (w *kv) Flags() workload.Flags { return w.flags }

// ConnFlags implements the ConnFlagser interface.
func (w *kv) ConnFlags() *workload.ConnFlags { return w.connFlags }

// Hooks implements the Hookser interface.
func (w *kv) Hooks() workload.Hooks {
	return workload.Hooks{
		PostLoad: func(_ context.Context, db *gosql.DB) error {
			if w.enum {
				_, err := db.Exec(`
CREATE TYPE enum_type AS ENUM ('v');
ALTER TABLE kv ADD COLUMN e enum_type NOT NULL AS ('v') STORED;`)
				if err != nil {
					return err
				}
			}
			if w.scatter {
				if _, err := db.Exec(`ALTER TABLE kv SCATTER`); err != nil {
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
	_, _, _, kr := w.createKeyGenerator()
	if kr.totalKeys() < uint64(w.insertCount) {
		return errors.Errorf(
			"`--insert-count` (%d) is greater than the number of unique keys that could be possibly generated [%d,%d)",
			w.insertCount, kr.min, kr.max)
	}
	return nil
}

// Note that sequence is only exposed for testing purposes and is used by
// returned keyGenerators.
func (w *kv) createKeyGenerator() (func() keyGenerator, *sequence, keyTransformer, keyRange) {
	var writeSeq atomic.Int64
	if w.writeSeq != "" {
		writeSeqVal, err := strconv.Atoi(w.writeSeq[1:])
		if err != nil {
			panic("creating generator from unvalidated workload")
		}
		writeSeq.Store(int64(writeSeqVal))
	}

	// Sequence is shared between all generators.
	seq := &sequence{max: w.cycleLength, val: &writeSeq}

	var gen func() keyGenerator
	var kr keyRange
	switch {
	case w.zipfian:
		gen = func() keyGenerator {
			return newZipfianGenerator(seq, rand.New(rand.NewSource(timeutil.Now().UnixNano())))
		}
		kr = keyRange{
			min: 0,
			max: math.MaxInt64,
		}
	case w.sequential:
		gen = func() keyGenerator {
			return newSequentialGenerator(seq, rand.New(rand.NewSource(timeutil.Now().UnixNano())))
		}
		kr = keyRange{
			min: 0,
			max: w.cycleLength,
		}
	default:
		gen = func() keyGenerator {
			return newHashGenerator(seq, rand.New(rand.NewSource(timeutil.Now().UnixNano())))
		}
		kr = keyRange{
			min: math.MinInt64,
			max: math.MaxInt64,
		}
	}

	if w.keySize == 0 {
		return gen, seq, intKeyTransformer{}, kr
	}

	return gen, seq, stringKeyTransformer{
		startOffset: kr.min,
		fillerSize:  w.keySize - minStringKeyDigits,
	}, kr
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
	// Tables should only run on initialized workload, safe to call create without
	// having a panic. We don't need to defer this to the actual table callbacks
	// like Splits or InitialRows.
	_, _, kt, kr := w.createKeyGenerator()

	table := workload.Table{Name: `kv`}
	table.Splits = workload.Tuples(
		w.splits,
		func(splitIdx int) []interface{} {
			return []interface{}{splitFinder(splitIdx, w.splits, kr, kt)}
		},
	)

	if w.shards > 0 {
		schema := shardedKvSchema
		if w.secondaryIndex {
			schema = shardedKvSchemaWithIndex
		}
		table.Schema = fmt.Sprintf(schema, kt.keySQLType(), w.shards)
	} else {
		schema := kvSchema
		if w.secondaryIndex {
			schema = kvSchemaWithIndex
		}
		table.Schema = fmt.Sprintf(schema, kt.keySQLType())
	}

	if w.insertCount > 0 {
		const batchSize = 1000
		table.InitialRows = workload.BatchedTuples{
			NumBatches: (w.insertCount + batchSize - 1) / batchSize,
			FillBatch: func(batchIdx int, cb coldata.Batch, a *bufalloc.ByteAllocator) {
				rowBegin, rowEnd := batchIdx*batchSize, (batchIdx+1)*batchSize
				if rowEnd > w.insertCount {
					rowEnd = w.insertCount
				}

				var kvtableTypes = []*types.T{
					kt.getColumnType(),
					types.Bytes,
				}

				cb.Reset(kvtableTypes, rowEnd-rowBegin, coldata.StandardColumnFactory)

				{
					seq := rowBegin
					kt.fillColumnBatch(cb, a, func() (s int64, ok bool) {
						if seq < rowEnd {
							seq++
							return insertCountKey(int64(seq-1), int64(w.insertCount), kr), true
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
					*a, payload = a.Alloc(blockSize, 0 /* extraCap */)
					w.randFillBlock(rndBlock, payload, uniqueSize)
					valCol.Set(rowOffset, payload)
				}
			},
		}
	}

	return []workload.Table{table}
}

// Ops implements the Opser interface.
func (w *kv) Ops(
	ctx context.Context, urls []string, reg *histogram.Registry,
) (workload.QueryLoad, error) {
	cfg := workload.NewMultiConnPoolCfgFromFlags(w.connFlags)
	cfg.MaxTotalConnections = w.connFlags.Concurrency + 1
	mcp, err := workload.NewMultiConnPool(ctx, cfg, urls...)
	if err != nil {
		return workload.QueryLoad{}, err
	}

	// Read statement
	var buf strings.Builder
	var folBuf strings.Builder
	if w.enum {
		buf.WriteString(`SELECT k, v, e FROM kv WHERE k IN (`)
		folBuf.WriteString(`SELECT k, v, e FROM kv AS OF SYSTEM TIME follower_read_timestamp() WHERE k IN (`)
	} else {
		buf.WriteString(`SELECT k, v FROM kv WHERE k IN (`)
		folBuf.WriteString(`SELECT k, v FROM kv AS OF SYSTEM TIME follower_read_timestamp() WHERE k IN (`)
	}
	for i := 0; i < w.batchSize; i++ {
		if i > 0 {
			buf.WriteString(", ")
			folBuf.WriteString(", ")
		}
		fmt.Fprintf(&buf, `$%d`, i+1)
		fmt.Fprintf(&folBuf, `$%d`, i+1)
	}
	buf.WriteString(`)`)
	folBuf.WriteString(`)`)

	readStmtStr := buf.String()
	followerReadStmtStr := folBuf.String()

	// Write statement
	buf.Reset()
	buf.WriteString(`UPSERT INTO kv (k, v) VALUES`)
	for i := 0; i < w.batchSize; i++ {
		j := i * 2
		if i > 0 {
			buf.WriteString(", ")
		}
		fmt.Fprintf(&buf, ` ($%d, $%d)`, j+1, j+2)
	}
	writeStmtStr := buf.String()

	// Select for update statement
	var sfuStmtStr string
	if w.writesUseSelectForUpdate {
		buf.Reset()
		buf.WriteString(`SELECT k, v FROM kv WHERE k IN (`)
		for i := 0; i < w.batchSize; i++ {
			if i > 0 {
				buf.WriteString(", ")
			}
			fmt.Fprintf(&buf, `$%d`, i+1)
		}
		buf.WriteString(`) FOR UPDATE`)
		sfuStmtStr = buf.String()
	}

	// Span statement
	buf.Reset()
	buf.WriteString(`SELECT count(v) FROM [SELECT v FROM kv`)
	if w.spanLimit > 0 {
		// Span statements without a limit query all ranges. However, if there's
		// a span limit specified, we want to randomly choose the range from which
		// the limited scan starts at. We do this by introducing the k >= $1
		// predicate.
		fmt.Fprintf(&buf, ` WHERE k >= $1 ORDER BY k LIMIT %d`, w.spanLimit)
	}
	buf.WriteString(`]`)
	spanStmtStr := buf.String()

	// Del statement
	buf.Reset()
	buf.WriteString(`DELETE FROM kv WHERE k IN (`)
	for i := 0; i < w.batchSize; i++ {
		if i > 0 {
			buf.WriteString(", ")
		}
		fmt.Fprintf(&buf, `$%d`, i+1)
	}
	buf.WriteString(`)`)
	delStmtStr := buf.String()

	gen, _, kt, _ := w.createKeyGenerator()
	ql := workload.QueryLoad{}
	var numEmptyResults atomic.Int64
	for i := 0; i < w.connFlags.Concurrency; i++ {
		op := &kvOp{
			config:          w,
			hists:           reg.GetHandle(),
			numEmptyResults: &numEmptyResults,
		}
		op.readStmt = op.sr.Define(readStmtStr)
		op.followerReadStmt = op.sr.Define(followerReadStmtStr)
		if !op.config.prepareReadOnly {
			op.writeStmt = op.sr.Define(writeStmtStr)
		}
		if len(sfuStmtStr) > 0 && !op.config.prepareReadOnly {
			op.sfuStmt = op.sr.Define(sfuStmtStr)
		}
		op.spanStmt = op.sr.Define(spanStmtStr)
		if w.txnQoS != `regular` {
			stmt := op.sr.Define(fmt.Sprintf(
				" SET default_transaction_quality_of_service = %s", w.txnQoS))
			op.qosStmt = &stmt
		}
		if !op.config.prepareReadOnly {
			op.delStmt = op.sr.Define(delStmtStr)
		}
		if err := op.sr.Init(ctx, "kv", mcp); err != nil {
			return workload.QueryLoad{}, err
		}
		op.mcp = mcp
		op.g = gen()
		op.t = kt
		ql.WorkerFns = append(ql.WorkerFns, op.run)
		ql.Close = op.close
	}
	return ql, nil
}

type kvOp struct {
	config           *kv
	hists            *histogram.Histograms
	sr               workload.SQLRunner
	mcp              *workload.MultiConnPool
	qosStmt          *workload.StmtHandle
	readStmt         workload.StmtHandle
	followerReadStmt workload.StmtHandle
	writeStmt        workload.StmtHandle
	spanStmt         workload.StmtHandle
	sfuStmt          workload.StmtHandle
	delStmt          workload.StmtHandle
	g                keyGenerator
	t                keyTransformer
	numEmptyResults  *atomic.Int64
}

func (o *kvOp) run(ctx context.Context) (retErr error) {
	if o.config.timeout > 0 {
		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, o.config.timeout)
		defer cancel()
	}

	if o.qosStmt != nil {
		_, err := o.qosStmt.Exec(ctx)
		if err != nil {
			return err
		}
	}
	statementProbability := o.g.rand().Intn(100) // Determines what statement is executed.
	if statementProbability < o.config.readPercent {
		args := make([]interface{}, o.config.batchSize)
		for i := 0; i < o.config.batchSize; i++ {
			args[i] = o.t.getKey(o.g.readKey())
		}
		start := timeutil.Now()
		readStmt := o.readStmt
		opName := `read`

		if o.g.rand().Intn(100) < o.config.followerReadPercent {
			readStmt = o.followerReadStmt
			opName = `follower-read`
		}
		rows, err := readStmt.Query(ctx, args...)
		if err != nil {
			return err
		}
		empty := true
		for rows.Next() {
			empty = false
		}
		if empty {
			o.numEmptyResults.Add(1)
		}
		elapsed := timeutil.Since(start)
		o.hists.Get(opName).Record(elapsed)
		return rows.Err()
	}
	// Since we know the statement is not a read, we recalibrate
	// statementProbability to only consider the other statements.
	statementProbability -= o.config.readPercent
	if statementProbability < o.config.delPercent {
		start := timeutil.Now()
		args := make([]interface{}, o.config.batchSize)
		for i := 0; i < o.config.batchSize; i++ {
			args[i] = o.g.readKey()
		}
		_, err := o.delStmt.Exec(ctx, args...)
		if err != nil {
			return err
		}
		elapsed := timeutil.Since(start)
		o.hists.Get(`del`).Record(elapsed)
		return nil
	}
	statementProbability -= o.config.delPercent
	if statementProbability < o.config.spanPercent {
		start := timeutil.Now()
		var err error
		if o.config.spanLimit > 0 {
			arg := o.g.readKey()
			_, err = o.spanStmt.Exec(ctx, arg)
		} else {
			_, err = o.spanStmt.Exec(ctx)
		}
		if err != nil {
			return err
		}
		elapsed := timeutil.Since(start)
		o.hists.Get(`span`).Record(elapsed)
		return err
	}
	const argCount = 2
	writeArgs := make([]interface{}, argCount*o.config.batchSize)
	var sfuArgs []interface{}
	if o.config.writesUseSelectForUpdate {
		sfuArgs = make([]interface{}, o.config.batchSize)
	}
	for i := 0; i < o.config.batchSize; i++ {
		j := i * argCount
		writeArgs[j+0] = o.t.getKey(o.g.writeKey())
		if sfuArgs != nil {
			sfuArgs[i] = writeArgs[j]
		}
		writeArgs[j+1] = o.config.randBlock(o.g.rand())
	}
	start := timeutil.Now()
	var err error
	if o.config.writesUseSelectForUpdate {
		// We could use crdb.ExecuteTx, but we avoid retries in this workload so
		// that each run call makes 1 attempt, so that rate limiting in workerRun
		// behaves as expected.
		var tx pgx.Tx
		tx, err := o.mcp.Get().BeginTx(ctx, pgx.TxOptions{})
		if err != nil {
			return err
		}

		defer func() {
			rollbackErr := tx.Rollback(ctx)
			if !errors.Is(rollbackErr, pgx.ErrTxClosed) {
				retErr = errors.CombineErrors(retErr, rollbackErr)
			}
		}()
		rows, err := o.sfuStmt.QueryTx(ctx, tx, sfuArgs...)
		if err != nil {
			return err
		}
		rows.Close()
		if err = rows.Err(); err != nil {
			return err
		}
		// Simulate a transaction that does other work between the SFU and write.
		time.Sleep(o.config.sfuDelay)
		if _, err = o.writeStmt.ExecTx(ctx, tx, writeArgs...); err != nil {
			// Multiple write transactions can contend and encounter
			// a serialization failure. We swallow such an error.
			return o.tryHandleWriteErr("write-write-err", start, err)
		}
		if err = tx.Commit(ctx); err != nil {
			return o.tryHandleWriteErr("write-commit-err", start, err)
		}
	} else {
		_, err = o.writeStmt.Exec(ctx, writeArgs...)
	}
	if err != nil {
		return err
	}
	elapsed := timeutil.Since(start)
	o.hists.Get(`write`).Record(elapsed)
	return err
}

func (o *kvOp) tryHandleWriteErr(name string, start time.Time, err error) error {
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

func (o *kvOp) close(context.Context) error {
	if empty := o.numEmptyResults.Load(); empty != 0 {
		fmt.Printf("Number of reads that didn't return any results: %d.\n", empty)
	}
	fmt.Printf("Write sequence could be resumed by passing --write-seq=%s to the next run.\n",
		o.g.state())
	return nil
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

// keyGenerator generates read and write keys. Read keys may not yet exist and
// write keys may already exist.
type keyGenerator interface {
	writeKey() int64
	readKey() int64
	rand() *rand.Rand
	state() string
}

type hashGenerator struct {
	seq    *sequence
	random *rand.Rand
	hasher hash.Hash
	buf    [sha1.Size]byte
}

func newHashGenerator(seq *sequence, rng *rand.Rand) *hashGenerator {
	return &hashGenerator{
		seq:    seq,
		random: rng,
		hasher: sha1.New(),
	}
}

func (g *hashGenerator) hash(v int64) int64 {
	binary.BigEndian.PutUint64(g.buf[:8], uint64(v))
	binary.BigEndian.PutUint64(g.buf[8:16], uint64(RandomSeed.Seed()))
	g.hasher.Reset()
	_, _ = g.hasher.Write(g.buf[:16])
	g.hasher.Sum(g.buf[:0])
	return int64(binary.BigEndian.Uint64(g.buf[:8]))
}

func (g *hashGenerator) writeKey() int64 {
	return g.hash(g.seq.write())
}

func (g *hashGenerator) readKey() int64 {
	v := g.seq.read()
	if v == 0 {
		return 0
	}
	return g.hash(g.random.Int63n(v))
}

func (g *hashGenerator) rand() *rand.Rand {
	return g.random
}

func (g *hashGenerator) state() string {
	return fmt.Sprintf("R%d", g.seq.read())
}

type sequentialGenerator struct {
	seq    *sequence
	random *rand.Rand
}

func newSequentialGenerator(seq *sequence, rng *rand.Rand) *sequentialGenerator {
	return &sequentialGenerator{
		seq:    seq,
		random: rng,
	}
}

func (g *sequentialGenerator) writeKey() int64 {
	return g.seq.write()
}

func (g *sequentialGenerator) readKey() int64 {
	v := g.seq.read()
	if v == 0 {
		return 0
	}
	return g.random.Int63n(v)
}

func (g *sequentialGenerator) rand() *rand.Rand {
	return g.random
}

func (g *sequentialGenerator) state() string {
	return fmt.Sprintf("S%d", g.seq.read())
}

type zipfGenerator struct {
	seq    *sequence
	random *rand.Rand
	zipf   *zipf
}

// Creates a new zipfian generator.
func newZipfianGenerator(seq *sequence, rng *rand.Rand) *zipfGenerator {
	return &zipfGenerator{
		seq:    seq,
		random: rng,
		zipf:   newZipf(1.1, 1, uint64(math.MaxInt64)),
	}
}

// Get a random number seeded by v that follows the
// zipfian distribution.
func (g *zipfGenerator) zipfian(seed int64) int64 {
	randomWithSeed := rand.New(rand.NewSource(seed))
	return int64(g.zipf.Uint64(randomWithSeed))
}

// Get a zipf write key appropriately.
func (g *zipfGenerator) writeKey() int64 {
	return g.zipfian(g.seq.write())
}

// Get a zipf read key appropriately.
func (g *zipfGenerator) readKey() int64 {
	v := g.seq.read()
	if v == 0 {
		return 0
	}
	return g.zipfian(g.random.Int63n(v))
}

func (g *zipfGenerator) rand() *rand.Rand {
	return g.random
}

func (g *zipfGenerator) state() string {
	return fmt.Sprintf("Z%d", g.seq.read())
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
