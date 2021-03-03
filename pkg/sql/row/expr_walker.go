// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package row

import (
	"context"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/sequence"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// reseedRandEveryN is the number of calls before reseeding happens.
// TODO (anzoteh96): setting reseedRandEveryN presents the tradeoff
// between the frequency of re-seeding and the number of calls to
// Float64() needed upon every resume. Therefore it will be useful to
// tune this parameter.
const reseedRandEveryN = 1000

// chunkSizeIncrementRate is the factor by which the size of the chunk of
// sequence values we allocate during an import increases.
const chunkSizeIncrementRate = 10
const initialChunkSize = 10
const maxChunkSize = 100000

// importRandPosition uniquely identifies an instance to a call to a random
// function during an import.
type importRandPosition int64

func (pos importRandPosition) distance(o importRandPosition) int64 {
	diff := int64(pos) - int64(o)
	if diff < 0 {
		return -diff
	}
	return diff
}

// getPosForRandImport gives the importRandPosition for the first instance of a
// call to a random function when generating a given row from a given source.
// numInstances refers to the number of random function invocations per row.
func getPosForRandImport(rowID int64, sourceID int32, numInstances int) importRandPosition {
	// We expect r.pos to increment by numInstances for each row.
	// Therefore, assuming that rowID increments by 1 for every row,
	// we will initialize the position as rowID * numInstances + sourceID << rowIDBits.
	rowIDWithMultiplier := int64(numInstances) * rowID
	pos := (int64(sourceID) << rowIDBits) ^ rowIDWithMultiplier
	return importRandPosition(pos)
}

// randomSource is only exposed through an interface to ensure that caller's
// don't access underlying field.
type randomSource interface {
	// Float64 returns, as a float64, a pseudo-random number in [0.0,1.0).
	Float64(c *CellInfoAnnotation) float64
	// Int63 returns a non-negative pseudo-random 63-bit integer as an int64.
	Int63(c *CellInfoAnnotation) int64
}

var _ randomSource = (*importRand)(nil)

type importRand struct {
	*rand.Rand
	pos importRandPosition
}

func (r *importRand) reseed(pos importRandPosition) {
	adjPos := (pos / reseedRandEveryN) * reseedRandEveryN
	rnd := rand.New(rand.NewSource(int64(adjPos)))
	for i := int(pos % reseedRandEveryN); i > 0; i-- {
		_ = rnd.Float64()
	}

	r.Rand = rnd
	r.pos = pos
}

func (r *importRand) maybeReseed(c *CellInfoAnnotation) {
	// newRowPos is the position of the first random function invocation of the
	// row we're currently processing. If this is more than c.randInstancePerRow
	// away, that means that we've skipped a batch of rows. importRand assumes
	// that it operates on a contiguous set of rows when it increments its
	// position, so if we skip some rows we need to reseed.
	// We may skip rows because a single row converter may be responsible for
	// converting several non-contiguous batches of KVs.
	newRowPos := getPosForRandImport(c.rowID, c.sourceID, c.randInstancePerRow)
	rowsSkipped := newRowPos.distance(r.pos) > int64(c.randInstancePerRow)
	if rowsSkipped {
		// Reseed at the new position, since our internally tracked r.pos is now out
		// of sync.
		r.reseed(newRowPos)
	}
	if r.pos%reseedRandEveryN == 0 {
		r.reseed(r.pos)
	}
}

// Float64 implements the randomSource interface.
func (r *importRand) Float64(c *CellInfoAnnotation) float64 {
	r.maybeReseed(c)
	randNum := r.Rand.Float64()
	r.pos++
	return randNum
}

// Int63 implements the randomSource interface.
func (r *importRand) Int63(c *CellInfoAnnotation) int64 {
	r.maybeReseed(c)
	randNum := r.Rand.Int63()
	r.pos++
	return randNum
}

// For some functions (specifically the volatile ones), we do
// not want to use the provided builtin. Instead, we opt for
// our own function definition, which produces deterministic results.
func makeBuiltinOverride(
	builtin *tree.FunctionDefinition, overloads ...tree.Overload,
) *tree.FunctionDefinition {
	props := builtin.FunctionProperties
	return tree.NewFunctionDefinition(
		"import."+builtin.Name, &props, overloads)
}

// SequenceMetadata contains information used when processing columns with
// default expressions which use sequences.
type SequenceMetadata struct {
	id              descpb.ID
	seqDesc         catalog.TableDescriptor
	instancesPerRow int64
	curChunk        *jobspb.SequenceValChunk
	curVal          int64
}

type overrideVolatility bool

const (
	// The following constants are the override volatility constants to
	// decide whether a default expression can be evaluated at the new
	// datum converter stage. Note that overrideErrorTerm is a placeholder
	// to be returned when an error is returned at sanitizeExprForImport.
	overrideErrorTerm overrideVolatility = false
	overrideImmutable overrideVolatility = false
	overrideVolatile  overrideVolatility = true
)

// cellInfoAddr is the address used to store relevant information
// in the Annotation field of evalCtx when evaluating expressions.
const cellInfoAddr tree.AnnotationIdx = iota + 1

// CellInfoAnnotation encapsulates the AST annotation for the various supported
// default expressions for import.
type CellInfoAnnotation struct {
	sourceID int32
	rowID    int64

	// Annotations for unique_rowid().
	uniqueRowIDInstance int
	uniqueRowIDTotal    int

	// Annotations for rand() and gen_random_uuid().
	// randSource should not be used directly, but through getImportRand() instead.
	randSource         randomSource
	randInstancePerRow int

	// Annotations for next_val().
	seqNameToMetadata map[string]*SequenceMetadata
	seqIDToMetadata   map[descpb.ID]*SequenceMetadata
	seqChunkProvider  *SeqChunkProvider
}

func getCellInfoAnnotation(t *tree.Annotations) *CellInfoAnnotation {
	return t.Get(cellInfoAddr).(*CellInfoAnnotation)
}

func (c *CellInfoAnnotation) reset(sourceID int32, rowID int64) {
	c.sourceID = sourceID
	c.rowID = rowID
	c.uniqueRowIDInstance = 0
}

func makeImportRand(c *CellInfoAnnotation) randomSource {
	pos := getPosForRandImport(c.rowID, c.sourceID, c.randInstancePerRow)
	randSource := &importRand{}
	randSource.reseed(pos)
	return randSource
}

// We don't want to call unique_rowid() for columns with such default expressions
// because it is not idempotent and has unfortunate overlapping of output
// spans since it puts the uniqueness-ensuring per-generator part (nodeID)
// in the low-bits. Instead, make our own IDs that attempt to keep each
// generator (sourceID) writing to its own key-space with sequential
// rowIndexes mapping to sequential unique IDs. This is done by putting the
// following as the lower bits, in order to handle the case where there are
// multiple columns with default as `unique_rowid`:
//
// #default_rowid_cols * rowIndex + colPosition (among those with default unique_rowid)
//
// To avoid collisions with the SQL-genenerated IDs (at least for a
// very long time) we also flip the top bit to 1.
//
// Producing sequential keys in non-overlapping spans for each source yields
// observed improvements in ingestion performance of ~2-3x and even more
// significant reductions in required compactions during IMPORT.
//
// TODO(dt): Note that currently some callers (e.g. CSV IMPORT, which can be
// used on a table more than once) offset their rowIndex by a wall-time at
// which their overall job is run, so that subsequent ingestion jobs pick
// different row IDs for the i'th row and don't collide. However such
// time-offset rowIDs mean each row imported consumes some unit of time that
// must then elapse before the next IMPORT could run without colliding e.g.
// a 100m row file would use 10µs/row or ~17min worth of IDs. For now it is
// likely that IMPORT's write-rate is still the limiting factor, but this
// scheme means rowIndexes are very large (1 yr in 10s of µs is about 2^42).
// Finding an alternative scheme for avoiding collisions (like sourceID *
// fileIndex*desc.Version) could improve on this. For now, if this
// best-effort collision avoidance scheme doesn't work in some cases we can
// just recommend an explicit PK as a workaround.
//
// TODO(anzoteh96): As per the issue in #51004, having too many columns with
// default expression unique_rowid() could cause collisions when IMPORTs are run
// too close to each other. It will therefore be nice to fix this problem.
func importUniqueRowID(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
	c := getCellInfoAnnotation(evalCtx.Annotations)
	avoidCollisionsWithSQLsIDs := uint64(1 << 63)
	shiftedIndex := int64(c.uniqueRowIDTotal)*c.rowID + int64(c.uniqueRowIDInstance)
	returnIndex := (uint64(c.sourceID) << rowIDBits) ^ uint64(shiftedIndex)
	c.uniqueRowIDInstance++
	evalCtx.Annotations.Set(cellInfoAddr, c)
	return tree.NewDInt(tree.DInt(avoidCollisionsWithSQLsIDs | returnIndex)), nil
}

func importRandom(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
	c := getCellInfoAnnotation(evalCtx.Annotations)
	if c.randSource == nil {
		c.randSource = makeImportRand(c)
	}
	return tree.NewDFloat(tree.DFloat(c.randSource.Float64(c))), nil
}

func importGenUUID(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
	c := getCellInfoAnnotation(evalCtx.Annotations)
	if c.randSource == nil {
		c.randSource = makeImportRand(c)
	}
	gen := c.randSource.Int63(c)
	id := uuid.MakeV4()
	id.DeterministicV4(uint64(gen), uint64(1<<63))
	return tree.NewDUuid(tree.DUuid{UUID: id}), nil
}

// SeqChunkProvider uses the import job progress to read and write its sequence
// value chunks.
type SeqChunkProvider struct {
	JobID    jobspb.JobID
	Registry *jobs.Registry
}

// RequestChunk updates seqMetadata with information about the chunk of sequence
// values pertaining to the row being processed during an import. The method
// first checks if there is a previously allocated chunk associated with the
// row, and if not goes on to allocate a new chunk.
func (j *SeqChunkProvider) RequestChunk(
	evalCtx *tree.EvalContext, c *CellInfoAnnotation, seqMetadata *SequenceMetadata,
) error {
	var hasAllocatedChunk bool
	return evalCtx.DB.Txn(evalCtx.Context, func(ctx context.Context, txn *kv.Txn) error {
		var foundFromPreviouslyAllocatedChunk bool
		resolveChunkFunc := func(txn *kv.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
			progress := md.Progress

			// Check if we have already reserved a chunk corresponding to this row in a
			// previous run of the import job. If we have, we must reuse the value of
			// the sequence which was designated on this particular invocation of
			// nextval().
			var err error
			if foundFromPreviouslyAllocatedChunk, err = j.checkForPreviouslyAllocatedChunks(
				seqMetadata, c, progress); err != nil {
				return err
			} else if foundFromPreviouslyAllocatedChunk {
				return nil
			}

			// Reserve a new sequence value chunk at the KV level.
			if !hasAllocatedChunk {
				if err := reserveChunkOfSeqVals(evalCtx, c, seqMetadata); err != nil {
					return err
				}
				hasAllocatedChunk = true
			}

			// Update job progress with the newly reserved chunk before it can be used by the import.
			// It is important that this information is persisted before it is used to
			// ensure correct behavior on job resumption.
			// We never want to end up in a situation where row x is assigned a different
			// sequence value on subsequent import job resumptions.
			fileProgress := progress.GetImport().SequenceDetails[c.sourceID]
			if fileProgress.SeqIdToChunks == nil {
				fileProgress.SeqIdToChunks = make(map[int32]*jobspb.SequenceDetails_SequenceChunks)
			}
			seqID := seqMetadata.id
			if _, ok := fileProgress.SeqIdToChunks[int32(seqID)]; !ok {
				fileProgress.SeqIdToChunks[int32(seqID)] = &jobspb.SequenceDetails_SequenceChunks{
					Chunks: make([]*jobspb.SequenceValChunk, 0),
				}
			}
			// We can cleanup some of the older chunks which correspond to rows
			// below the resume pos as we are never going to reprocess those
			// check pointed rows on job resume.
			resumePos := progress.GetImport().ResumePos[c.sourceID]
			trim, chunks := 0, fileProgress.SeqIdToChunks[int32(seqID)].Chunks
			// If the resumePos is below the max bound of the current chunk we need
			// to keep this chunk in case the job is re-resumed.
			for ; trim < len(chunks) && chunks[trim].NextChunkStartRow <= resumePos; trim++ {
			}
			fileProgress.SeqIdToChunks[int32(seqID)].Chunks =
				fileProgress.SeqIdToChunks[int32(seqID)].Chunks[trim:]

			fileProgress.SeqIdToChunks[int32(seqID)].Chunks = append(
				fileProgress.SeqIdToChunks[int32(seqID)].Chunks, seqMetadata.curChunk)
			ju.UpdateProgress(progress)
			return nil
		}
		err := j.Registry.UpdateJobWithTxn(ctx, j.JobID, txn, resolveChunkFunc)
		if err != nil {
			return err
		}

		// Now that the job progress has been written to, we can use the newly
		// allocated chunk.
		if !foundFromPreviouslyAllocatedChunk {
			seqMetadata.curVal = seqMetadata.curChunk.ChunkStartVal
		}
		return nil
	})
}

func incrementSequenceByVal(
	ctx context.Context,
	descriptor catalog.TableDescriptor,
	db *kv.DB,
	codec keys.SQLCodec,
	incrementBy int64,
) (int64, error) {
	seqOpts := descriptor.GetSequenceOpts()
	var val int64
	var err error
	// TODO(adityamaru): Think about virtual sequences.
	if seqOpts.Virtual {
		return 0, errors.New("virtual sequences are not supported by IMPORT INTO")
	}
	seqValueKey := codec.SequenceKey(uint32(descriptor.GetID()))
	val, err = kv.IncrementValRetryable(ctx, db, seqValueKey, incrementBy)
	if err != nil {
		if errors.HasType(err, (*roachpb.IntegerOverflowError)(nil)) {
			return 0, boundsExceededError(descriptor)
		}
		return 0, err
	}
	if val > seqOpts.MaxValue || val < seqOpts.MinValue {
		return 0, boundsExceededError(descriptor)
	}

	return val, nil
}

func boundsExceededError(descriptor catalog.TableDescriptor) error {
	seqOpts := descriptor.GetSequenceOpts()
	isAscending := seqOpts.Increment > 0

	var word string
	var value int64
	if isAscending {
		word = "maximum"
		value = seqOpts.MaxValue
	} else {
		word = "minimum"
		value = seqOpts.MinValue
	}
	name := descriptor.GetName()
	return pgerror.Newf(
		pgcode.SequenceGeneratorLimitExceeded,
		`reached %s value of sequence %q (%d)`, word,
		tree.ErrString((*tree.Name)(&name)), value)
}

// checkForPreviouslyAllocatedChunks checks if a sequence value has already been
// generated for a the current row being imported. If such a value is found, the
// seqMetadata is updated to reflect this.
// This would be true if the IMPORT job has been re-resumed and there were some
// rows which had not been marked as imported.
func (j *SeqChunkProvider) checkForPreviouslyAllocatedChunks(
	seqMetadata *SequenceMetadata, c *CellInfoAnnotation, progress *jobspb.Progress,
) (bool, error) {
	var found bool
	fileProgress := progress.GetImport().SequenceDetails[c.sourceID]
	if fileProgress.SeqIdToChunks == nil {
		return found, nil
	}
	var allocatedSeqChunks *jobspb.SequenceDetails_SequenceChunks
	var ok bool
	if allocatedSeqChunks, ok = fileProgress.SeqIdToChunks[int32(seqMetadata.id)]; !ok {
		return found, nil
	}

	for _, chunk := range allocatedSeqChunks.Chunks {
		// We have found the chunk of sequence values that was assigned to the
		// swath of rows encompassing rowID.
		if chunk.ChunkStartRow <= c.rowID && chunk.NextChunkStartRow > c.rowID {
			relativeRowIndex := c.rowID - chunk.ChunkStartRow
			seqMetadata.curVal = chunk.ChunkStartVal +
				seqMetadata.seqDesc.GetSequenceOpts().Increment*(seqMetadata.instancesPerRow*relativeRowIndex)
			found = true
			return found, nil
		}
	}
	return found, nil
}

// reserveChunkOfSeqVals ascertains the size of the next chunk, and reserves it
// at the KV level. The seqMetadata is updated to reflect this.
func reserveChunkOfSeqVals(
	evalCtx *tree.EvalContext, c *CellInfoAnnotation, seqMetadata *SequenceMetadata,
) error {
	newChunkSize := int64(initialChunkSize)
	// If we are allocating a subsequent chunk of sequence values, we attempt
	// to reserve a factor of 10 more than reserved the last time so as to
	// prevent clobbering the chunk reservation logic which involves writing
	// to job progress.
	if seqMetadata.curChunk != nil {
		newChunkSize = chunkSizeIncrementRate * seqMetadata.curChunk.ChunkSize
		if newChunkSize > maxChunkSize {
			newChunkSize = maxChunkSize
		}
	}

	// We want to encompass at least one complete row with our chunk
	// allocation.
	if newChunkSize < seqMetadata.instancesPerRow {
		newChunkSize = seqMetadata.instancesPerRow
	}

	incrementValBy := newChunkSize * seqMetadata.seqDesc.GetSequenceOpts().Increment
	// incrementSequenceByVal keeps retrying until it is able to find a slot
	// of incrementValBy.
	seqVal, err := incrementSequenceByVal(evalCtx.Context, seqMetadata.seqDesc, evalCtx.DB,
		evalCtx.Codec, incrementValBy)
	if err != nil {
		return err
	}

	// Update the sequence metadata to reflect the newly reserved chunk.
	seqMetadata.curChunk = &jobspb.SequenceValChunk{
		ChunkStartVal:     seqVal - incrementValBy + seqMetadata.seqDesc.GetSequenceOpts().Increment,
		ChunkSize:         newChunkSize,
		ChunkStartRow:     c.rowID,
		NextChunkStartRow: c.rowID + (newChunkSize / seqMetadata.instancesPerRow),
	}
	return nil
}

func importNextVal(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
	c := getCellInfoAnnotation(evalCtx.Annotations)
	seqName := tree.MustBeDString(args[0])
	seqMetadata, ok := c.seqNameToMetadata[string(seqName)]
	if !ok {
		return nil, errors.Newf("sequence %s not found in annotation", seqName)
	}
	return importNextValHelper(evalCtx, c, seqMetadata)
}

func importNextValByID(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
	c := getCellInfoAnnotation(evalCtx.Annotations)
	oid := tree.MustBeDOid(args[0])
	seqMetadata, ok := c.seqIDToMetadata[descpb.ID(oid.DInt)]
	if !ok {
		return nil, errors.Newf("sequence with ID %v not found in annotation", oid)
	}
	return importNextValHelper(evalCtx, c, seqMetadata)
}

func importNextValHelper(
	evalCtx *tree.EvalContext, c *CellInfoAnnotation, seqMetadata *SequenceMetadata,
) (tree.Datum, error) {
	if c.seqChunkProvider == nil {
		return nil, errors.New("no sequence chunk provider configured for the import job")
	}

	// If the current importWorker does not have an active chunk for the sequence
	// seqName, or the row we are processing is outside the range of rows covered
	// by the active chunk, we need to request a chunk.
	if seqMetadata.curChunk == nil || c.rowID == seqMetadata.curChunk.NextChunkStartRow {
		if err := c.seqChunkProvider.RequestChunk(evalCtx, c, seqMetadata); err != nil {
			return nil, err
		}
	} else {
		// The current chunk of sequence values can be used for the row being
		// processed.
		seqMetadata.curVal += seqMetadata.seqDesc.GetSequenceOpts().Increment
	}
	return tree.NewDInt(tree.DInt(seqMetadata.curVal)), nil
}

// Besides overriding, there are also counters that we want to keep track of as
// we walk through the expressions in a row (at datumRowConverter creation
// time). This will be handled by the visitorSideEffect field: it will be called
// with an annotation, and a FuncExpr. The annotation changes the counter, while
// the FuncExpr is used to extract information from the function.
//
// Egs: In the case of unique_rowid, we want to keep track of the total number
// of unique_rowid occurrences in a row.
type customFunc struct {
	visitorSideEffect func(annotations *tree.Annotations, fn *tree.FuncExpr) error
	override          *tree.FunctionDefinition
}

var useDefaultBuiltin *customFunc

// Given that imports can be retried and resumed, we want to
// ensure that the default functions return the same value given
// the same arguments, even on retries. Therfore we decide to support
// only a limited subset of non-immutable functions, which are
// all listed here.
var supportedImportFuncOverrides = map[string]*customFunc{
	// These methods can be supported given that we set the statement
	// and transaction timestamp to be equal, i.e. the write timestamp.
	"current_date":          useDefaultBuiltin,
	"current_timestamp":     useDefaultBuiltin,
	"localtimestamp":        useDefaultBuiltin,
	"now":                   useDefaultBuiltin,
	"statement_timestamp":   useDefaultBuiltin,
	"timeofday":             useDefaultBuiltin,
	"transaction_timestamp": useDefaultBuiltin,
	"unique_rowid": {
		visitorSideEffect: func(annot *tree.Annotations, _ *tree.FuncExpr) error {
			getCellInfoAnnotation(annot).uniqueRowIDTotal++
			return nil
		},
		override: makeBuiltinOverride(
			tree.FunDefs["unique_rowid"],
			tree.Overload{
				Types:      tree.ArgTypes{},
				ReturnType: tree.FixedReturnType(types.Int),
				Fn:         importUniqueRowID,
				Info:       "Returns a unique rowid based on row position and time",
				Volatility: tree.VolatilityVolatile,
			},
		),
	},
	"random": {
		visitorSideEffect: func(annot *tree.Annotations, _ *tree.FuncExpr) error {
			getCellInfoAnnotation(annot).randInstancePerRow++
			return nil
		},
		override: makeBuiltinOverride(
			tree.FunDefs["random"],
			tree.Overload{
				Types:      tree.ArgTypes{},
				ReturnType: tree.FixedReturnType(types.Float),
				Fn:         importRandom,
				Info:       "Returns a random number between 0 and 1 based on row position and time.",
				Volatility: tree.VolatilityVolatile,
			},
		),
	},
	"gen_random_uuid": {
		visitorSideEffect: func(annot *tree.Annotations, _ *tree.FuncExpr) error {
			getCellInfoAnnotation(annot).randInstancePerRow++
			return nil
		},
		override: makeBuiltinOverride(
			tree.FunDefs["gen_random_uuid"],
			tree.Overload{
				Types:      tree.ArgTypes{},
				ReturnType: tree.FixedReturnType(types.Uuid),
				Fn:         importGenUUID,
				Info: "Generates a random UUID based on row position and time, " +
					"and returns it as a value of UUID type.",
				Volatility: tree.VolatilityVolatile,
			},
		),
	},
	"nextval": {
		visitorSideEffect: func(annot *tree.Annotations, fn *tree.FuncExpr) error {
			// Get sequence name so that we can update the annotation with the number
			// of nextval calls to this sequence in a row.
			seqIdentifier, err := sequence.GetSequenceFromFunc(fn)
			if err != nil {
				return err
			}

			var sequenceMetadata *SequenceMetadata
			var ok bool
			if seqIdentifier.IsByID() {
				if sequenceMetadata, ok = getCellInfoAnnotation(annot).seqIDToMetadata[descpb.ID(seqIdentifier.SeqID)]; !ok {
					return errors.Newf("sequence with ID %s not found in annotation", seqIdentifier.SeqID)
				}
			} else {
				if sequenceMetadata, ok = getCellInfoAnnotation(annot).seqNameToMetadata[seqIdentifier.SeqName]; !ok {
					return errors.Newf("sequence %s not found in annotation", seqIdentifier.SeqName)
				}
			}
			sequenceMetadata.instancesPerRow++
			return nil
		},
		override: makeBuiltinOverride(
			tree.FunDefs["nextval"],
			tree.Overload{
				Types:      tree.ArgTypes{{builtins.SequenceNameArg, types.String}},
				ReturnType: tree.FixedReturnType(types.Int),
				Info:       "Advances the value of the sequence and returns the final value.",
				Fn:         importNextVal,
			},
			tree.Overload{
				Types:      tree.ArgTypes{{builtins.SequenceNameArg, types.RegClass}},
				ReturnType: tree.FixedReturnType(types.Int),
				Info:       "Advances the value of the sequence and returns the final value.",
				Fn:         importNextValByID,
			},
		),
	},
}

func unsafeExpressionError(err error, msg string, expr string) error {
	return errors.Wrapf(err, "default expression %q is unsafe for import: %s", expr, msg)
}

// unsafeErrExpr is a wrapper for errors arising from unsafe default
// expression created at row converter stage so that the appropriate
// error can be returned at the Row() stage.
type unsafeErrExpr struct {
	tree.TypedExpr
	err error
}

var _ tree.TypedExpr = &unsafeErrExpr{}

// Eval implements the TypedExpr interface.
func (e *unsafeErrExpr) Eval(_ *tree.EvalContext) (tree.Datum, error) {
	return nil, e.err
}

// importDefaultExprVisitor must be invoked on a typed expression. This
// visitor walks the tree and ensures that any expression in the tree
// that's not immutable is what we explicitly support.
type importDefaultExprVisitor struct {
	err         error
	ctx         context.Context
	annotations *tree.Annotations
	semaCtx     *tree.SemaContext
	// The volatility flag will be set if there's at least one volatile
	// function appearing in the default expression.
	volatility overrideVolatility
}

// VisitPre implements tree.Visitor interface.
func (v *importDefaultExprVisitor) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	return v.err == nil, expr
}

// VisitPost implements tree.Visitor interface.
func (v *importDefaultExprVisitor) VisitPost(expr tree.Expr) (newExpr tree.Expr) {
	if v.err != nil {
		return expr
	}
	fn, ok := expr.(*tree.FuncExpr)
	if !ok || fn.ResolvedOverload().Volatility <= tree.VolatilityImmutable {
		// If an expression is not a function, or is an immutable function, then
		// we can use it as it is.
		return expr
	}
	resolvedFnName := fn.Func.FunctionReference.(*tree.FunctionDefinition).Name
	custom, isSafe := supportedImportFuncOverrides[resolvedFnName]
	if !isSafe {
		v.err = errors.Newf(`function %s unsupported by IMPORT INTO`, resolvedFnName)
		return expr
	}
	if custom == useDefaultBuiltin {
		// No override exists, means it's okay to use the definitions given in
		// builtin.go.
		return expr
	}
	// Override exists, so we turn the volatility flag of the visitor to true.
	// In addition, the visitorSideEffect function needs to be called to update
	// any relevant counter (e.g. the total number of occurrences of the
	// unique_rowid function in an expression).
	v.volatility = overrideVolatile
	if custom.visitorSideEffect != nil {
		err := custom.visitorSideEffect(v.annotations, fn)
		if err != nil {
			v.err = errors.Wrapf(err, "function %s failed when invoking side effect", resolvedFnName)
			return expr
		}
	}
	funcExpr := &tree.FuncExpr{
		Func:  tree.ResolvableFunctionReference{FunctionReference: custom.override},
		Type:  fn.Type,
		Exprs: fn.Exprs,
	}
	// The override must have appropriate overload defined.
	overrideExpr, err := funcExpr.TypeCheck(v.ctx, v.semaCtx, fn.ResolvedType())
	if err != nil {
		v.err = errors.Wrapf(err, "error overloading function")
	}
	return overrideExpr
}

// sanitizeExprsForImport checks whether default expressions are supported
// for import.
func sanitizeExprsForImport(
	ctx context.Context, evalCtx *tree.EvalContext, expr tree.Expr, targetType *types.T,
) (tree.TypedExpr, overrideVolatility, error) {
	semaCtx := tree.MakeSemaContext()

	// If we have immutable expressions, then we can just return it right away.
	typedExpr, err := schemaexpr.SanitizeVarFreeExpr(
		ctx, expr, targetType, "import_default", &semaCtx, tree.VolatilityImmutable)
	if err == nil {
		return typedExpr, overrideImmutable, nil
	}
	// Now that the expressions are not immutable, we first check that they
	// are of the correct type before checking for any unsupported functions
	// for import.
	typedExpr, err = tree.TypeCheck(ctx, expr, &semaCtx, targetType)
	if err != nil {
		return nil, overrideErrorTerm,
			unsafeExpressionError(err, "type checking error", expr.String())
	}
	v := &importDefaultExprVisitor{annotations: evalCtx.Annotations}
	newExpr, _ := tree.WalkExpr(v, typedExpr)
	if v.err != nil {
		return nil, overrideErrorTerm,
			unsafeExpressionError(v.err, "expr walking error", expr.String())
	}
	return newExpr.(tree.TypedExpr), v.volatility, nil
}
