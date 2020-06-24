// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowexec

import (
	"context"
	"encoding/binary"
	"math/rand"
	"time"

	"github.com/axiomhq/hyperloglog"
	"github.com/cockroachdb/cockroach/pkg/geo/geoindex"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// sketchInfo contains the specification and run-time state for each sketch.
type sketchInfo struct {
	spec     execinfrapb.SketchSpec
	sketch   *hyperloglog.Sketch
	numNulls int64
	numRows  int64
}

// A sampler processor returns a random sample of rows, as well as "global"
// statistics (including cardinality estimation sketch data). See SamplerSpec
// for more details.
type samplerProcessor struct {
	execinfra.ProcessorBase

	flowCtx         *execinfra.FlowCtx
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

var supportedSketchTypes = map[execinfrapb.SketchType]struct{}{
	// The code currently hardcodes the use of this single type of sketch
	// (which avoids the extra complexity until we actually have multiple types).
	execinfrapb.SketchType_HLL_PLUS_PLUS_V1: {},
}

// maxIdleSleepTime is the maximum amount of time we sleep for throttling
// (we sleep once every SamplerProgressInterval rows).
const maxIdleSleepTime = 10 * time.Second

// At 25% average CPU usage we start throttling automatic stats.
const cpuUsageMinThrottle = 0.25

// At 75% average CPU usage we reach maximum throttling of automatic stats.
const cpuUsageMaxThrottle = 0.75

var bytesRowType = []*types.T{types.Bytes}

func newSamplerProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.SamplerSpec,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (*samplerProcessor, error) {
	for _, s := range spec.Sketches {
		if _, ok := supportedSketchTypes[s.SketchType]; !ok {
			return nil, errors.Errorf("unsupported sketch type %s", s.SketchType)
		}
	}

	ctx := flowCtx.EvalCtx.Ctx()
	// Limit the memory use by creating a child monitor with a hard limit.
	// The processor will disable histogram collection if this limit is not
	// enough.
	memMonitor := execinfra.NewLimitedMonitor(ctx, flowCtx.EvalCtx.Mon, flowCtx.Cfg, "sampler-mem")
	s := &samplerProcessor{
		flowCtx:         flowCtx,
		input:           input,
		memAcc:          memMonitor.MakeBoundAccount(),
		sketches:        make([]sketchInfo, len(spec.Sketches)),
		maxFractionIdle: spec.MaxFractionIdle,
		invSr:           make(map[uint32]*stats.SampleReservoir, len(spec.InvertedSketches)),
		invSketch:       make(map[uint32]*sketchInfo, len(spec.InvertedSketches)),
	}

	inTypes := input.OutputTypes()
	var sampleCols util.FastIntSet
	for i := range spec.Sketches {
		s.sketches[i] = sketchInfo{
			spec:     spec.Sketches[i],
			sketch:   hyperloglog.New14(),
			numNulls: 0,
			numRows:  0,
		}
		if spec.Sketches[i].GenerateHistogram {
			sampleCols.Add(int(spec.Sketches[i].Columns[0]))
		}
	}
	for i := range spec.InvertedSketches {
		var sr stats.SampleReservoir
		// The datums are converted to their inverted index bytes and
		// sent as single DBytes column.
		var srCols util.FastIntSet
		srCols.Add(0)
		sr.Init(int(spec.SampleSize), bytesRowType, &s.memAcc, srCols)
		col := spec.InvertedSketches[i].Columns[0]
		s.invSr[col] = &sr
		sketchSpec := spec.InvertedSketches[i]
		// Rejigger the sketch spec to only refer to a single bytes column.
		sketchSpec.Columns = []uint32{0}
		s.invSketch[col] = &sketchInfo{
			spec:     sketchSpec,
			sketch:   hyperloglog.New14(),
			numNulls: 0,
			numRows:  0,
		}
	}

	s.sr.Init(int(spec.SampleSize), inTypes, &s.memAcc, sampleCols)

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
		nil, post, outTypes, flowCtx, processorID, output, memMonitor,
		execinfra.ProcStateOpts{
			TrailingMetaCallback: func(context.Context) []execinfrapb.ProducerMetadata {
				s.close()
				return nil
			},
		},
	); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *samplerProcessor) pushTrailingMeta(ctx context.Context) {
	execinfra.SendTraceData(ctx, s.Out.Output())
}

// Run is part of the Processor interface.
func (s *samplerProcessor) Run(ctx context.Context) {
	s.input.Start(ctx)
	s.StartInternal(ctx, samplerProcName)

	earlyExit, err := s.mainLoop(s.Ctx)
	if err != nil {
		execinfra.DrainAndClose(s.Ctx, s.Out.Output(), err, s.pushTrailingMeta, s.input)
	} else if !earlyExit {
		s.pushTrailingMeta(s.Ctx)
		s.input.ConsumerClosed()
		s.Out.Close()
	}
	s.MoveToDraining(nil /* err */)
}

// TestingSamplerSleep introduces a sleep inside the sampler, every
// <samplerProgressInterval>. Used to simulate a heavily throttled
// run for testing.
var TestingSamplerSleep time.Duration

func (s *samplerProcessor) mainLoop(ctx context.Context) (earlyExit bool, err error) {
	rng, _ := randutil.NewPseudoRand()
	var da sqlbase.DatumAlloc
	var buf []byte
	rowCount := 0
	lastWakeupTime := timeutil.Now()

	// Inverted index variables for EncodeInvertedIndexKeys.
	// TODO(mjibson): Once we support configuring geospatial indexes
	// via SQL, we will need to pass down the actual index descriptors
	// or something to here so that we can generate the correct inverted
	// index keys. At this point we'll also need to worry about users who
	// have multiple indexes with different configurations over the same
	// column. For now, since we don't even allow users to change the
	// defaults, we can just not worry about it.
	invIndexDesc := &sqlbase.IndexDescriptor{
		GeoConfig: geoindex.Config{
			S2Geography: geoindex.DefaultGeographyIndexConfig().S2Geography,
			S2Geometry:  geoindex.DefaultGeometryIndexConfig().S2Geometry,
		},
	}
	var invKeys [][]byte
	invRow := sqlbase.EncDatumRow{sqlbase.EncDatum{}}

	for {
		row, meta := s.input.Next()
		if meta != nil {
			if !emitHelper(ctx, &s.Out, nil /* row */, meta, s.pushTrailingMeta, s.input) {
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
			if !emitHelper(ctx, &s.Out, nil /* row */, meta, s.pushTrailingMeta, s.input) {
				return true, nil
			}

			if s.maxFractionIdle > 0 {
				// Look at CRDB's average CPU usage in the last 10 seconds:
				//  - if it is lower than cpuUsageMinThrottle, we do not throttle;
				//  - if it is higher than cpuUsageMaxThrottle, we throttle all the way;
				//  - in-between, we scale the idle time proportionally.
				usage := s.flowCtx.Cfg.RuntimeStats.GetCPUCombinedPercentNorm()

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

					elapsed := timeutil.Now().Sub(lastWakeupTime)
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
					timer := time.NewTimer(wait)
					defer timer.Stop()
					select {
					case <-timer.C:
						break
					case <-s.flowCtx.Stopper().ShouldStop():
						break
					}
				}
				lastWakeupTime = timeutil.Now()
			}

			if TestingSamplerSleep != 0 {
				time.Sleep(TestingSamplerSleep)
			}
		}

		for i := range s.sketches {
			if err := s.sketches[i].addRow(row, s.outTypes, &buf, &da); err != nil {
				return false, err
			}
		}
		if earlyExit, err = s.sampleRow(ctx, &s.sr, row, rng); earlyExit || err != nil {
			return earlyExit, err
		}

		for col, invSr := range s.invSr {
			if err := row[col].EnsureDecoded(s.outTypes[col], &da); err != nil {
				return false, err
			}
			switch s.outTypes[col].Family() {
			case types.GeographyFamily, types.GeometryFamily:
				invKeys, err = sqlbase.EncodeGeoInvertedIndexTableKeys(row[col].Datum, nil /* inKey */, invIndexDesc)
			default:
				invKeys, err = sqlbase.EncodeInvertedIndexTableKeys(row[col].Datum, nil /* inKey */)
			}
			if err != nil {
				return false, err
			}
			for _, key := range invKeys {
				invRow[0].Datum = da.NewDBytes(tree.DBytes(key))
				if err := s.invSketch[col].addRow(invRow, bytesRowType, &buf, &da); err != nil {
					return false, err
				}
				if earlyExit, err = s.sampleRow(ctx, invSr, invRow, rng); earlyExit || err != nil {
					return earlyExit, err
				}
			}
		}
	}

	outRow := make(sqlbase.EncDatumRow, len(s.outTypes))
	// Emit the sampled rows.
	for i := range outRow {
		outRow[i] = sqlbase.DatumToEncDatum(s.outTypes[i], tree.DNull)
	}
	for _, sample := range s.sr.Get() {
		copy(outRow, sample.Row)
		outRow[s.rankCol] = sqlbase.EncDatum{Datum: tree.NewDInt(tree.DInt(sample.Rank))}
		if !emitHelper(ctx, &s.Out, outRow, nil /* meta */, s.pushTrailingMeta, s.input) {
			return true, nil
		}
	}
	// Emit the inverted sample rows.
	for i := range outRow {
		outRow[i] = sqlbase.DatumToEncDatum(s.outTypes[i], tree.DNull)
	}
	for col, invSr := range s.invSr {
		outRow[s.invColIdxCol] = sqlbase.EncDatum{Datum: tree.NewDInt(tree.DInt(col))}
		for _, sample := range invSr.Get() {
			// Reuse the rank column for inverted index keys.
			outRow[s.rankCol] = sqlbase.EncDatum{Datum: tree.NewDInt(tree.DInt(sample.Rank))}
			outRow[s.invIdxKeyCol] = sample.Row[0]
			if !emitHelper(ctx, &s.Out, outRow, nil /* meta */, s.pushTrailingMeta, s.input) {
				return true, nil
			}
		}
	}
	// Release the memory for the sampled rows.
	s.sr = stats.SampleReservoir{}
	s.invSr = nil

	// Emit the sketch rows.
	for i := range outRow {
		outRow[i] = sqlbase.DatumToEncDatum(s.outTypes[i], tree.DNull)
	}
	for i, si := range s.sketches {
		outRow[s.sketchIdxCol] = sqlbase.EncDatum{Datum: tree.NewDInt(tree.DInt(i))}
		if earlyExit, err := s.emitSketchRow(ctx, &si, outRow); earlyExit || err != nil {
			return earlyExit, err
		}
	}

	// Emit the inverted sketch rows.
	for i := range outRow {
		outRow[i] = sqlbase.DatumToEncDatum(s.outTypes[i], tree.DNull)
	}
	for col, invSketch := range s.invSketch {
		outRow[s.invColIdxCol] = sqlbase.EncDatum{Datum: tree.NewDInt(tree.DInt(col))}
		if earlyExit, err := s.emitSketchRow(ctx, invSketch, outRow); earlyExit || err != nil {
			return earlyExit, err
		}
	}

	// Send one last progress update to the consumer.
	meta := &execinfrapb.ProducerMetadata{SamplerProgress: &execinfrapb.RemoteProducerMetadata_SamplerProgress{
		RowsProcessed: uint64(rowCount % SamplerProgressInterval),
	}}
	if !emitHelper(ctx, &s.Out, nil /* row */, meta, s.pushTrailingMeta, s.input) {
		return true, nil
	}

	return false, nil
}

func (s *samplerProcessor) emitSketchRow(
	ctx context.Context, si *sketchInfo, outRow sqlbase.EncDatumRow,
) (earlyExit bool, err error) {
	outRow[s.numRowsCol] = sqlbase.EncDatum{Datum: tree.NewDInt(tree.DInt(si.numRows))}
	outRow[s.numNullsCol] = sqlbase.EncDatum{Datum: tree.NewDInt(tree.DInt(si.numNulls))}
	data, err := si.sketch.MarshalBinary()
	if err != nil {
		return false, err
	}
	outRow[s.sketchCol] = sqlbase.EncDatum{Datum: tree.NewDBytes(tree.DBytes(data))}
	if !emitHelper(ctx, &s.Out, outRow, nil /* meta */, s.pushTrailingMeta, s.input) {
		return true, nil
	}
	return false, nil
}

// sampleRow looks at a row and either drops it or adds it to the reservoir.
func (s *samplerProcessor) sampleRow(
	ctx context.Context, sr *stats.SampleReservoir, row sqlbase.EncDatumRow, rng *rand.Rand,
) (earlyExit bool, err error) {
	// Use Int63 so we don't have headaches converting to DInt.
	rank := uint64(rng.Int63())
	if err := sr.SampleRow(ctx, s.EvalCtx, row, rank); err != nil {
		if code := pgerror.GetPGCode(err); code != pgcode.OutOfMemory {
			return false, err
		}
		// We hit an out of memory error. Clear the sample reservoir and
		// disable histogram sample collection.
		sr.Disable()
		log.Info(ctx, "disabling histogram collection due to excessive memory utilization")

		// Send a metadata record so the sample aggregator will also disable
		// histogram collection.
		meta := &execinfrapb.ProducerMetadata{SamplerProgress: &execinfrapb.RemoteProducerMetadata_SamplerProgress{
			HistogramDisabled: true,
		}}
		if !emitHelper(ctx, &s.Out, nil /* row */, meta, s.pushTrailingMeta, s.input) {
			return true, nil
		}
	}
	return false, nil
}

func (s *samplerProcessor) close() {
	if s.InternalClose() {
		s.memAcc.Close(s.Ctx)
		s.MemMonitor.Stop(s.Ctx)
	}
}

// addRow adds a row to the sketch and updates row counts.
func (s *sketchInfo) addRow(
	row sqlbase.EncDatumRow, typs []*types.T, buf *[]byte, da *sqlbase.DatumAlloc,
) error {
	var err error
	var intbuf [8]byte
	s.numRows++

	var col uint32
	var useFastPath bool
	if len(s.spec.Columns) == 1 {
		col = s.spec.Columns[0]
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
		binary.LittleEndian.PutUint64(intbuf[:], uint64(val))
		s.sketch.Insert(intbuf[:])
	} else {
		isNull := true
		*buf = (*buf)[:0]
		for _, col := range s.spec.Columns {
			*buf, err = row[col].Fingerprint(typs[col], da, *buf)
			isNull = isNull && row[col].IsNull()
			if err != nil {
				return err
			}
		}
		if isNull {
			s.numNulls++
		}
		s.sketch.Insert(*buf)
	}
	return nil
}
