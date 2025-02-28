// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rowexec

import (
	"context"
	"encoding/binary"
	"math/rand"
	"time"

	hllNew "github.com/axiomhq/hyperloglog"
	hllOld "github.com/axiomhq/hyperloglog/000"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/execversion"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/collatedstring"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// sketchInfo contains the specification and run-time state for each sketch.
type sketchInfo struct {
	spec execinfrapb.SketchSpec
	// Exactly one of sketchOld and sketchNew will be set.
	sketchOld            *hllOld.Sketch
	sketchNew            *hllNew.Sketch
	numNulls             int64
	numRows              int64
	size                 int64
	legacyFingerprinting bool
}

// A sampler processor returns a random sample of rows, as well as "global"
// statistics (including cardinality estimation sketch data). See SamplerSpec
// for more details.
type samplerProcessor struct {
	execinfra.ProcessorBase

	input           execinfra.RowSource
	memAcc          mon.BoundAccount
	sr              stats.SampleReservoir
	sketches        []sketchInfo
	outTypes        []*types.T
	maxFractionIdle float64

	// invSr and invSketch map column indexes to samplers/sketches.
	invSr     map[uint32]*stats.SampleReservoir
	invSketch map[uint32]*sketchInfo

	// Output column indices for special columns.
	rankCol      int
	sketchIdxCol int
	numRowsCol   int
	numNullsCol  int
	sizeCol      int
	sketchCol    int
	invColIdxCol int
	invIdxKeyCol int
}

var _ execinfra.Processor = &samplerProcessor{}

const samplerProcName = "sampler"

// SamplerProgressInterval corresponds to the number of input rows after which
// the sampler will report progress by pushing a metadata record.  It is mutable
// for testing.
var SamplerProgressInterval = 10000

// maxIdleSleepTime is the maximum amount of time we sleep for throttling
// (we sleep once every SamplerProgressInterval rows).
const maxIdleSleepTime = 10 * time.Second

// At 25% average CPU usage we start throttling automatic stats.
const cpuUsageMinThrottle = 0.25

// At 75% average CPU usage we reach maximum throttling of automatic stats.
const cpuUsageMaxThrottle = 0.75

var bytesRowType = []*types.T{types.Bytes}

func newSamplerProcessor(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.SamplerSpec,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
) (*samplerProcessor, error) {
	useNewHLL := execversion.FromContext(ctx) >= execversion.V25_1
	legacyFingerprinting := execversion.FromContext(ctx) < execversion.V25_2

	// Limit the memory use by creating a child monitor with a hard limit.
	// The processor will disable histogram collection if this limit is not
	// enough.
	//
	// sampler doesn't spill to disk, so ensure some reasonable lower bound on
	// the workmem limit.
	var minMemoryLimit int64 = 8 << 20 // 8MiB
	if flowCtx.Cfg.TestingKnobs.MemoryLimitBytes != 0 {
		minMemoryLimit = flowCtx.Cfg.TestingKnobs.MemoryLimitBytes
	}
	memMonitor := execinfra.NewLimitedMonitorWithLowerBound(
		ctx, flowCtx, "sampler-mem", minMemoryLimit,
	)
	s := &samplerProcessor{
		input:           input,
		memAcc:          memMonitor.MakeBoundAccount(),
		sketches:        make([]sketchInfo, len(spec.Sketches)),
		maxFractionIdle: spec.MaxFractionIdle,
		invSr:           make(map[uint32]*stats.SampleReservoir, len(spec.InvertedSketches)),
		invSketch:       make(map[uint32]*sketchInfo, len(spec.InvertedSketches)),
	}

	inTypes := input.OutputTypes()
	var sampleCols intsets.Fast
	for i := range spec.Sketches {
		s.sketches[i] = sketchInfo{
			spec:                 spec.Sketches[i],
			numNulls:             0,
			numRows:              0,
			legacyFingerprinting: legacyFingerprinting,
		}
		if useNewHLL {
			s.sketches[i].sketchNew = hllNew.New14()
		} else {
			s.sketches[i].sketchOld = hllOld.New14()
		}
		if spec.Sketches[i].GenerateHistogram {
			sampleCols.Add(int(spec.Sketches[i].Columns[0]))
		}
	}
	for i := range spec.InvertedSketches {
		var sr stats.SampleReservoir
		// The datums are converted to their inverted index bytes and
		// sent as single DBytes column.
		var srCols intsets.Fast
		srCols.Add(0)
		sr.Init(int(spec.SampleSize), int(spec.MinSampleSize), bytesRowType, &s.memAcc, srCols)
		col := spec.InvertedSketches[i].Columns[0]
		s.invSr[col] = &sr
		sketchSpec := spec.InvertedSketches[i]
		// Rejigger the sketch spec to only refer to a single bytes column.
		sketchSpec.Columns = []uint32{0}
		s.invSketch[col] = &sketchInfo{
			spec:     sketchSpec,
			numNulls: 0,
			numRows:  0,
		}
		if useNewHLL {
			s.invSketch[col].sketchNew = hllNew.New14()
		} else {
			s.invSketch[col].sketchOld = hllOld.New14()
		}
	}

	s.sr.Init(int(spec.SampleSize), int(spec.MinSampleSize), inTypes, &s.memAcc, sampleCols)

	outTypes := make([]*types.T, 0, len(inTypes)+7)

	// First columns are the same as the input.
	outTypes = append(outTypes, inTypes...)

	// An INT column for the rank of each row.
	s.rankCol = len(outTypes)
	outTypes = append(outTypes, types.Int)

	// An INT column indicating the sketch index.
	s.sketchIdxCol = len(outTypes)
	outTypes = append(outTypes, types.Int)

	// An INT column indicating the number of rows processed.
	s.numRowsCol = len(outTypes)
	outTypes = append(outTypes, types.Int)

	// An INT column indicating the number of rows that have a NULL in all sketch
	// columns.
	s.numNullsCol = len(outTypes)
	outTypes = append(outTypes, types.Int)

	// An INT column indicating the size of all rows in the sketch columns.
	s.sizeCol = len(outTypes)
	outTypes = append(outTypes, types.Int)

	// A BYTES column with the sketch data.
	s.sketchCol = len(outTypes)
	outTypes = append(outTypes, types.Bytes)

	// An INT column indicating the column associated with the inverted index key.
	s.invColIdxCol = len(outTypes)
	outTypes = append(outTypes, types.Int)

	// A BYTES column with the inverted index key datum.
	s.invIdxKeyCol = len(outTypes)
	outTypes = append(outTypes, types.Bytes)

	s.outTypes = outTypes

	if err := s.Init(
		ctx, nil, post, outTypes, flowCtx, processorID, memMonitor,
		execinfra.ProcStateOpts{
			TrailingMetaCallback: func() []execinfrapb.ProducerMetadata {
				s.close()
				return nil
			},
		},
	); err != nil {
		return nil, err
	}
	return s, nil
}

// Run is part of the Processor interface.
func (s *samplerProcessor) Run(ctx context.Context, output execinfra.RowReceiver) {
	ctx = s.StartInternal(ctx, samplerProcName)
	s.input.Start(ctx)

	earlyExit, err := s.mainLoop(ctx, output)
	if err != nil {
		execinfra.DrainAndClose(ctx, s.FlowCtx, s.input, output, err)
	} else if !earlyExit {
		execinfra.SendTraceData(ctx, s.FlowCtx, output)
		s.input.ConsumerClosed()
		output.ProducerDone()
	}
	s.MoveToDraining(nil /* err */)
}

func (s *samplerProcessor) mainLoop(
	ctx context.Context, output execinfra.RowReceiver,
) (earlyExit bool, err error) {
	rng, _ := randutil.NewPseudoRand()
	var buf []byte
	rowCount := 0
	lastWakeupTime := timeutil.Now()

	var invKeys [][]byte
	invRow := rowenc.EncDatumRow{rowenc.EncDatum{}}
	var timer timeutil.Timer
	defer timer.Stop()

	for {
		row, meta := s.input.Next()
		if meta != nil {
			if !emitHelper(ctx, s.FlowCtx, s.input, output, &s.OutputHelper, nil /* row */, meta) {
				// No cleanup required; emitHelper() took care of it.
				return true, nil
			}
			continue
		}
		if row == nil {
			break
		}

		rowCount++
		if rowCount%SamplerProgressInterval == 0 {
			// Send a metadata record to check that the consumer is still alive and
			// report number of rows processed since the last update.
			meta := &execinfrapb.ProducerMetadata{SamplerProgress: &execinfrapb.RemoteProducerMetadata_SamplerProgress{
				RowsProcessed: uint64(SamplerProgressInterval),
			}}
			if !emitHelper(ctx, s.FlowCtx, s.input, output, &s.OutputHelper, nil /* row */, meta) {
				return true, nil
			}

			if s.maxFractionIdle > 0 {
				// Look at CRDB's average CPU usage in the last 10 seconds:
				//  - if it is lower than cpuUsageMinThrottle, we do not throttle;
				//  - if it is higher than cpuUsageMaxThrottle, we throttle all the way;
				//  - in-between, we scale the idle time proportionally.
				usage := s.FlowCtx.Cfg.RuntimeStats.GetCPUCombinedPercentNorm()

				if usage > cpuUsageMinThrottle {
					fractionIdle := s.maxFractionIdle
					if usage < cpuUsageMaxThrottle {
						fractionIdle *= (usage - cpuUsageMinThrottle) /
							(cpuUsageMaxThrottle - cpuUsageMinThrottle)
					}
					if log.V(1) {
						log.Infof(
							ctx, "throttling to fraction idle %.2f (based on usage %.2f)", fractionIdle, usage,
						)
					}

					elapsed := timeutil.Since(lastWakeupTime)
					// Throttle the processor according to fractionIdle.
					// Wait time is calculated as follows:
					//
					//       fraction_idle = t_wait / (t_run + t_wait)
					//  ==>  t_wait = t_run * fraction_idle / (1 - fraction_idle)
					//
					wait := time.Duration(float64(elapsed) * fractionIdle / (1 - fractionIdle))
					if wait > maxIdleSleepTime {
						wait = maxIdleSleepTime
					}
					timer.Reset(wait)
					select {
					case <-timer.C:
						timer.Read = true
						break
					case <-s.FlowCtx.Stopper().ShouldQuiesce():
						break
					}
				}
				lastWakeupTime = timeutil.Now()
			}
		}

		for i := range s.sketches {
			if err := s.sketches[i].addRow(ctx, row, s.outTypes, &buf); err != nil {
				return false, err
			}
		}
		if earlyExit, err = s.sampleRow(ctx, output, &s.sr, row, rng); earlyExit || err != nil {
			return earlyExit, err
		}

		for col, invSr := range s.invSr {
			if err := row[col].EnsureDecoded(s.outTypes[col], nil /* da */); err != nil {
				return false, err
			}

			index := s.invSketch[col].spec.Index
			if index == nil {
				// If we don't have an index descriptor don't attempt to generate inverted
				// index entries.
				continue
			}
			switch s.outTypes[col].Family() {
			case types.GeographyFamily, types.GeometryFamily:
				invKeys, err = rowenc.EncodeGeoInvertedIndexTableKeys(ctx, row[col].Datum, nil /* inKey */, index.GeoConfig)
			default:
				invKeys, err = rowenc.EncodeInvertedIndexTableKeys(row[col].Datum, nil /* inKey */, index.Version)
			}
			if err != nil {
				return false, err
			}
			for _, key := range invKeys {
				d := tree.DBytes(key)
				invRow[0].Datum = &d
				if err := s.invSketch[col].addRow(ctx, invRow, bytesRowType, &buf); err != nil {
					return false, err
				}
				if earlyExit, err = s.sampleRow(ctx, output, invSr, invRow, rng); earlyExit || err != nil {
					return earlyExit, err
				}
			}
		}
	}

	outRow := make(rowenc.EncDatumRow, len(s.outTypes))
	// Emit the sampled rows.
	for i := range outRow {
		outRow[i] = rowenc.NullEncDatum()
	}
	// Reuse the numRows column for the capacity of the sample reservoir.
	outRow[s.numRowsCol] = rowenc.EncDatum{Datum: tree.NewDInt(tree.DInt(s.sr.Cap()))}
	for _, sample := range s.sr.Get() {
		copy(outRow, sample.Row)
		outRow[s.rankCol] = rowenc.EncDatum{Datum: tree.NewDInt(tree.DInt(sample.Rank))}
		if !emitHelper(ctx, s.FlowCtx, s.input, output, &s.OutputHelper, outRow, nil /* meta */) {
			return true, nil
		}
	}
	// Emit the inverted sample rows.
	for i := range outRow {
		outRow[i] = rowenc.NullEncDatum()
	}
	for col, invSr := range s.invSr {
		// Reuse the numRows column for the capacity of the sample reservoir.
		outRow[s.numRowsCol] = rowenc.EncDatum{Datum: tree.NewDInt(tree.DInt(invSr.Cap()))}
		outRow[s.invColIdxCol] = rowenc.EncDatum{Datum: tree.NewDInt(tree.DInt(col))}
		for _, sample := range invSr.Get() {
			// Reuse the rank column for inverted index keys.
			outRow[s.rankCol] = rowenc.EncDatum{Datum: tree.NewDInt(tree.DInt(sample.Rank))}
			outRow[s.invIdxKeyCol] = sample.Row[0]
			if !emitHelper(ctx, s.FlowCtx, s.input, output, &s.OutputHelper, outRow, nil /* meta */) {
				return true, nil
			}
		}
	}
	// Release the memory for the sampled rows.
	s.sr = stats.SampleReservoir{}
	s.invSr = nil

	// Emit the sketch rows.
	for i := range outRow {
		outRow[i] = rowenc.NullEncDatum()
	}
	for i, si := range s.sketches {
		outRow[s.sketchIdxCol] = rowenc.EncDatum{Datum: tree.NewDInt(tree.DInt(i))}
		if earlyExit, err := s.emitSketchRow(ctx, output, &si, outRow); earlyExit || err != nil {
			return earlyExit, err
		}
	}

	// Emit the inverted sketch rows.
	for i := range outRow {
		outRow[i] = rowenc.NullEncDatum()
	}
	for col, invSketch := range s.invSketch {
		outRow[s.invColIdxCol] = rowenc.EncDatum{Datum: tree.NewDInt(tree.DInt(col))}
		if earlyExit, err := s.emitSketchRow(ctx, output, invSketch, outRow); earlyExit || err != nil {
			return earlyExit, err
		}
	}

	// Send one last progress update to the consumer.
	meta := &execinfrapb.ProducerMetadata{SamplerProgress: &execinfrapb.RemoteProducerMetadata_SamplerProgress{
		RowsProcessed: uint64(rowCount % SamplerProgressInterval),
	}}
	if !emitHelper(ctx, s.FlowCtx, s.input, output, &s.OutputHelper, nil /* row */, meta) {
		return true, nil
	}

	return false, nil
}

func (s *samplerProcessor) emitSketchRow(
	ctx context.Context, output execinfra.RowReceiver, si *sketchInfo, outRow rowenc.EncDatumRow,
) (earlyExit bool, err error) {
	outRow[s.numRowsCol] = rowenc.EncDatum{Datum: tree.NewDInt(tree.DInt(si.numRows))}
	outRow[s.numNullsCol] = rowenc.EncDatum{Datum: tree.NewDInt(tree.DInt(si.numNulls))}
	outRow[s.sizeCol] = rowenc.EncDatum{Datum: tree.NewDInt(tree.DInt(si.size))}
	var data []byte
	if si.sketchNew != nil {
		data, err = si.sketchNew.MarshalBinary()
	} else {
		data, err = si.sketchOld.MarshalBinary()
	}
	if err != nil {
		return false, err
	}
	outRow[s.sketchCol] = rowenc.EncDatum{Datum: tree.NewDBytes(tree.DBytes(data))}
	if !emitHelper(ctx, s.FlowCtx, s.input, output, &s.OutputHelper, outRow, nil /* meta */) {
		return true, nil
	}
	return false, nil
}

// sampleRow looks at a row and either drops it or adds it to the reservoir.
func (s *samplerProcessor) sampleRow(
	ctx context.Context,
	output execinfra.RowReceiver,
	sr *stats.SampleReservoir,
	row rowenc.EncDatumRow,
	rng *rand.Rand,
) (earlyExit bool, err error) {
	// Use Int63 so we don't have headaches converting to DInt.
	rank := uint64(rng.Int63())
	prevCapacity := sr.Cap()
	if err := sr.SampleRow(ctx, s.FlowCtx.EvalCtx, row, rank); err != nil {
		if !sqlerrors.IsOutOfMemoryError(err) {
			return false, err
		}
		// We hit an out of memory error. Clear the sample reservoir and
		// disable histogram sample collection.
		sr.Disable()
		log.Info(ctx, "disabling histogram collection due to excessive memory utilization")
		telemetry.Inc(sqltelemetry.StatsHistogramOOMCounter)

		// Send a metadata record so the sample aggregator will also disable
		// histogram collection.
		meta := &execinfrapb.ProducerMetadata{SamplerProgress: &execinfrapb.RemoteProducerMetadata_SamplerProgress{
			HistogramDisabled: true,
		}}
		if !emitHelper(ctx, s.FlowCtx, s.input, output, &s.OutputHelper, nil /* row */, meta) {
			return true, nil
		}
	} else if sr.Cap() != prevCapacity {
		log.Infof(
			ctx, "histogram samples reduced from %d to %d due to excessive memory utilization",
			prevCapacity, sr.Cap(),
		)
	}
	return false, nil
}

// Close is part of the execinfra.Processor interface.
func (s *samplerProcessor) Close(context.Context) {
	s.input.ConsumerClosed()
	s.close()
}

func (s *samplerProcessor) close() {
	if s.InternalClose() {
		s.memAcc.Close(s.Ctx())
		s.MemMonitor.Stop(s.Ctx())
	}
}

var _ execinfra.DoesNotUseTxn = &samplerProcessor{}

// DoesNotUseTxn implements the DoesNotUseTxn interface.
func (s *samplerProcessor) DoesNotUseTxn() bool {
	txnUser, ok := s.input.(execinfra.DoesNotUseTxn)
	return ok && txnUser.DoesNotUseTxn()
}

// addRow adds a row to the sketch and updates row counts.
func (s *sketchInfo) addRow(
	ctx context.Context, row rowenc.EncDatumRow, typs []*types.T, buf *[]byte,
) (err error) {
	if s.legacyFingerprinting {
		return s.addRowLegacy(ctx, row, typs, buf)
	}

	s.numRows++
	allNulls := true
	*buf = (*buf)[:0]
	for _, col := range s.spec.Columns {
		// Avoid calling IsNull() if the datum is unset because it will panic.
		// Instead, return an assertion error that might help in debugging.
		if row[col].IsUnset() {
			return errors.AssertionFailedf("unset datum: col=%d row=%s", col, row.String(typs))
		}
		isNull := row[col].IsNull()
		allNulls = allNulls && isNull
		if b := row[col].EncodedBytes(); b != nil && !containsCollatedString(typs[col]) && !isNull {
			// Composite, value encoded datums may have different encodings for
			// semantically equivalent types, so using the encoded bytes can
			// skew the cardinality estimate slightly. This should be rare and
			// hyperloglog cardinality is already an estimate, so it is
			// considered acceptable. For floats, the only values affected are 0
			// and -0, which are semantically equivalent but have different
			// value encodings. For decimals, 0 and -0 are affected, as well as
			// any equal values with different numbers of trailing zeros. JSON
			// and array types containing decimals are also affected similarly.
			//
			// Value-encoded collated strings are more likely than other types
			// to cause cardinality over-estimations because the ratio of
			// physically distinct strings to semantically distinct strings can
			// be much higher. For example, the und-u-ks-level2 locale is
			// case-insensitive, so there are 8 different physical strings all
			// equivalent to "foo": "foo", "Foo", "fOo", "foO", "FOo", "FoO",
			// "fOO", and "FOO". For this reason, we fall-back to using
			// Fingerprint for collated strings.
			//
			// For NULL datums we use the Fingerprint method so that regardless
			// of datum encoding, all of them got the same encoding.
			//
			// TODO(mgartner): We should probably truncate b to some max size to
			// prevent a really wide value from growing buf. Since the distinct
			// count is an estimate anyway, truncating the value shouldn't have
			// any real impact.
			if enc, _ := row[col].Encoding(); enc == catenumpb.DatumEncoding_VALUE {
				// Value encoding includes column ID delta in the prefix which
				// can differ based on the values of other columns within the
				// same row (i.e. whether the previous columns had NULL or
				// non-NULL values). To prevent this detail from artificially
				// increasing the distinct estimate, we'll remove the column ID
				// delta from encoding that we use for the current datum.
				_, dataOffset, _, typ, err := encoding.DecodeValueTag(b)
				if err != nil {
					return err
				}
				// Including the value tag (i.e. the type) allows us to
				// differentiate some non-NULL values (like integer 0) from NULL
				// ones.
				*buf = append(*buf, byte(typ))
				*buf = append(*buf, b[dataOffset:]...)
			} else {
				// Key-encoded datums can be used as is.
				*buf = append(*buf, b...)
			}
		} else {
			// Fallback to using the Fingerprint method to generate bytes to
			// insert into the sketch.
			//
			// We pass nil DatumAlloc so that each datum allocation was
			// independent (to prevent bounded memory leaks like we've seen in
			// #136394). The problem in that issue was that the same backing
			// slice of datums was shared across rows, so if a single row was
			// kept as a sample, it could keep many garbage datums alive. To go
			// around that we simply disabled the batching.
			//
			// We choose to not perform the memory accounting for possibly
			// decoded tree.Datum because we will lose the references to row
			// very soon.
			*buf, err = row[col].Fingerprint(ctx, typs[col], nil /* da */, *buf, nil /* acc */)
			if err != nil {
				return err
			}
		}
		s.size += int64(row[col].DiskSize())
	}

	if allNulls {
		s.numNulls++
	}
	if s.sketchNew != nil {
		s.sketchNew.Insert(*buf)
	} else {
		s.sketchOld.Insert(*buf)
	}
	return nil
}

// containsCollatedString returns true if the type is a collated string type
// or a container type included a collated string type. It does not return
// true with collated string types with a default-equivalent collation.
func containsCollatedString(t *types.T) bool {
	switch t.Family() {
	case types.CollatedStringFamily:
		return !collatedstring.IsDefaultEquivalentCollation(t.Locale())
	case types.ArrayFamily:
		return containsCollatedString(t.ArrayContents())
	case types.TupleFamily:
		for _, t := range t.TupleContents() {
			if containsCollatedString(t) {
				return true
			}
		}
	}
	return false
}

// addRowLegacy adds a row to the sketch and updates row counts. This is the
// legacy implementation from versions prior to 25.2.
//
// TODO(mgartner): Remove this once compatibility with 25.1 is no longer needed.
func (s *sketchInfo) addRowLegacy(
	ctx context.Context, row rowenc.EncDatumRow, typs []*types.T, buf *[]byte,
) error {
	var err error
	s.numRows++

	var col uint32
	var useFastPath bool
	if len(s.spec.Columns) == 1 {
		col = s.spec.Columns[0]
		if row[col].IsUnset() {
			return errors.AssertionFailedf("unset datum: col=%d row=%s", col, row.String(typs))
		}
		isNull := row[col].IsNull()
		useFastPath = typs[col].Family() == types.IntFamily && !isNull
	}

	if useFastPath {
		// Fast path for integers.
		// TODO(radu): make this more general.
		val, err := row[col].GetInt()
		if err != nil {
			return err
		}

		if cap(*buf) < 8 {
			*buf = make([]byte, 8)
		} else {
			*buf = (*buf)[:8]
		}

		s.size += int64(row[col].DiskSize())

		// Note: this encoding is not identical with the one in the general path
		// below, but it achieves the same thing (we want equal integers to
		// encode to equal []bytes). The only caveat is that all samplers must
		// use the same encodings, so changes will require a new SketchType to
		// avoid problems during upgrade.
		//
		// We could use a more efficient hash function and use InsertHash, but
		// it must be a very good hash function (HLL expects the hash values to
		// be uniformly distributed in the 2^64 range). Experiments (on tpcc
		// order_line) with simplistic functions yielded bad results.
		binary.LittleEndian.PutUint64(*buf, uint64(val))
		if s.sketchNew != nil {
			s.sketchNew.Insert(*buf)
		} else {
			s.sketchOld.Insert(*buf)
		}
		return nil
	}
	isNull := true
	*buf = (*buf)[:0]
	for _, col := range s.spec.Columns {
		// We pass nil DatumAlloc so that each datum allocation was independent
		// (to prevent bounded memory leaks like we've seen in #136394).
		// TODO(yuzefovich): the problem in that issue was that the same backing
		// slice of datums was shared across rows, so if a single row was kept
		// as a sample, it could keep many garbage alive. To go around that we
		// simply disabled the batching. We could improve that behavior by using
		// a DatumAlloc in which we set typeAllocSizes in such a way that all
		// columns of the same type in a single row would be backed by a single
		// slice allocation.
		//
		// We choose to not perform the memory accounting for possibly decoded
		// tree.Datum because we will lose the references to row very soon.
		*buf, err = row[col].Fingerprint(ctx, typs[col], nil /* da */, *buf, nil /* acc */)
		if err != nil {
			return err
		}
		if isNull {
			if row[col].IsUnset() {
				return errors.AssertionFailedf("unset datum: col=%d row=%s", col, row.String(typs))
			}
			isNull = row[col].IsNull()
		}
		s.size += int64(row[col].DiskSize())
	}
	if isNull {
		s.numNulls++
	}
	if s.sketchNew != nil {
		s.sketchNew.Insert(*buf)
	} else {
		s.sketchOld.Insert(*buf)
	}
	return nil
}
