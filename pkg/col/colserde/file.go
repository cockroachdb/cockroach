// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colserde

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/colserde/arrowserde"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	mmap "github.com/edsrzf/mmap-go"
	flatbuffers "github.com/google/flatbuffers/go"
)

const fileMagic = `ARROW1`

var fileMagicPadding [8 - len(fileMagic)]byte

type fileBlock struct {
	offset      int64
	metadataLen int32
	bodyLen     int64
}

// FileSerializer converts our in-mem columnar batch representation into the
// arrow specification's file format. All batches serialized to a file must have
// the same schema.
type FileSerializer struct {
	scratch [4]byte

	w    *countingWriter
	typs []*types.T
	fb   *flatbuffers.Builder
	a    *ArrowBatchConverter
	rb   *RecordBatchSerializer

	recordBatches []fileBlock
}

// NewFileSerializer creates a FileSerializer for the given types. The caller is
// responsible for closing the given writer.
func NewFileSerializer(w io.Writer, typs []*types.T) (*FileSerializer, error) {
	a, err := NewArrowBatchConverter(typs)
	if err != nil {
		return nil, err
	}
	rb, err := NewRecordBatchSerializer(typs)
	if err != nil {
		return nil, err
	}
	s := &FileSerializer{
		typs: typs,
		fb:   flatbuffers.NewBuilder(flatbufferBuilderInitialCapacity),
		a:    a,
		rb:   rb,
	}
	return s, s.Reset(w)
}

// Reset can be called to reuse this FileSerializer with a new io.Writer after
// calling Finish. The types will remain the ones passed to the constructor. The
// caller is responsible for closing the given writer.
func (s *FileSerializer) Reset(w io.Writer) error {
	if s.w != nil {
		return errors.New(`Finish must be called before Reset`)
	}
	s.w = &countingWriter{wrapped: w}
	s.recordBatches = s.recordBatches[:0]
	if _, err := io.WriteString(s.w, fileMagic); err != nil {
		return err
	}
	// Pad to 8 byte boundary.
	if _, err := s.w.Write(fileMagicPadding[:]); err != nil {
		return err
	}

	// The file format is a wrapper around the streaming format and the streaming
	// format starts with a Schema message.
	s.fb.Reset()
	messageOffset := schemaMessage(s.fb, s.typs)
	s.fb.Finish(messageOffset)
	schemaBytes := s.fb.FinishedBytes()
	if _, err := s.w.Write(schemaBytes); err != nil {
		return err
	}
	_, err := s.w.Write(make([]byte, calculatePadding(len(schemaBytes))))
	return err
}

// AppendBatch adds one batch of columnar data to the file.
func (s *FileSerializer) AppendBatch(batch coldata.Batch) error {
	offset := int64(s.w.written)

	arrow, err := s.a.BatchToArrow(batch)
	if err != nil {
		return err
	}
	metadataLen, bodyLen, err := s.rb.Serialize(s.w, arrow, batch.Length())
	if err != nil {
		return err
	}

	s.recordBatches = append(s.recordBatches, fileBlock{
		offset:      offset,
		metadataLen: int32(metadataLen),
		bodyLen:     int64(bodyLen),
	})
	return nil
}

// Finish writes the footer metadata described by the arrow spec. Nothing can be
// called after Finish except Reset.
func (s *FileSerializer) Finish() error {
	defer func() {
		s.w = nil
	}()

	// Write the footer flatbuffer, which has byte offsets of all the record
	// batch messages in the file.
	s.fb.Reset()
	footerOffset := fileFooter(s.fb, s.typs, s.recordBatches)
	s.fb.Finish(footerOffset)
	footerBytes := s.fb.FinishedBytes()
	if _, err := s.w.Write(footerBytes); err != nil {
		return err
	}
	// For the footer, and only the footer, the spec requires the length _after_
	// the footer so that it can be read by starting at the back of the file and
	// working forward.
	binary.LittleEndian.PutUint32(s.scratch[:], uint32(len(footerBytes)))
	if _, err := s.w.Write(s.scratch[:]); err != nil {
		return err
	}
	// Spec wants the magic again here.
	_, err := io.WriteString(s.w, fileMagic)
	return err
}

// FileDeserializer decodes columnar data batches from files encoded according
// to the arrow spec.
type FileDeserializer struct {
	buf        []byte
	bufCloseFn func() error

	recordBatches []fileBlock

	idx  int
	end  int
	typs []*types.T
	a    *ArrowBatchConverter
	rb   *RecordBatchSerializer

	arrowScratch []*array.Data
}

// NewFileDeserializerFromBytes constructs a FileDeserializer for an in-memory
// buffer.
func NewFileDeserializerFromBytes(typs []*types.T, buf []byte) (*FileDeserializer, error) {
	return newFileDeserializer(typs, buf, func() error { return nil })
}

// NewFileDeserializerFromPath constructs a FileDeserializer by reading it from
// a file.
func NewFileDeserializerFromPath(typs []*types.T, path string) (*FileDeserializer, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, pgerror.Wrapf(err, pgcode.Io, `opening %s`, path)
	}
	// TODO(dan): This is currently using copy on write semantics because we store
	// the nulls differently in-mem than arrow does and there's an in-place
	// conversion. If we used the same format that arrow does, this could be
	// switched to mmap.RDONLY (it's easy to check, the test fails with a SIGBUS
	// right now with mmap.RDONLY).
	buf, err := mmap.Map(f, mmap.COPY, 0 /* flags */)
	if err != nil {
		return nil, pgerror.Wrapf(err, pgcode.Io, `mmaping %s`, path)
	}
	return newFileDeserializer(typs, buf, buf.Unmap)
}

func newFileDeserializer(
	typs []*types.T, buf []byte, bufCloseFn func() error,
) (*FileDeserializer, error) {
	d := &FileDeserializer{
		buf:        buf,
		bufCloseFn: bufCloseFn,
		end:        len(buf),
	}
	var err error
	if err = d.init(); err != nil {
		return nil, err
	}
	d.typs = typs

	if d.a, err = NewArrowBatchConverter(typs); err != nil {
		return nil, err
	}
	if d.rb, err = NewRecordBatchSerializer(typs); err != nil {
		return nil, err
	}
	d.arrowScratch = make([]*array.Data, 0, len(typs))

	return d, nil
}

// Close releases any resources held by this deserializer.
func (d *FileDeserializer) Close() error {
	return d.bufCloseFn()
}

// Typs returns the in-memory types for the data stored in this file.
func (d *FileDeserializer) Typs() []*types.T {
	return d.typs
}

// NumBatches returns the number of record batches stored in this file.
func (d *FileDeserializer) NumBatches() int {
	return len(d.recordBatches)
}

// GetBatch fills in the given in-mem batch with the requested on-disk data.
func (d *FileDeserializer) GetBatch(batchIdx int, b coldata.Batch) error {
	rb := d.recordBatches[batchIdx]
	d.idx = int(rb.offset)
	buf, err := d.read(metadataLengthNumBytes + int(rb.metadataLen) + int(rb.bodyLen))
	if err != nil {
		return err
	}
	d.arrowScratch = d.arrowScratch[:0]
	batchLength, err := d.rb.Deserialize(&d.arrowScratch, buf)
	if err != nil {
		return err
	}
	return d.a.ArrowToBatch(d.arrowScratch, batchLength, b)
}

// read gets the next `n` bytes from the start of the buffer, consuming them.
func (d *FileDeserializer) read(n int) ([]byte, error) {
	if d.idx+n > d.end {
		return nil, io.EOF
	}
	start := d.idx
	d.idx += n
	return d.buf[start:d.idx], nil
}

// readBackward gets the `n` bytes from the end of the buffer, consuming them.
func (d *FileDeserializer) readBackward(n int) ([]byte, error) {
	if d.idx+n > d.end {
		return nil, io.EOF
	}
	end := d.end
	d.end -= n
	return d.buf[d.end:end], nil
}

// init verifies the file magic and headers. After init, the `idx` and `end`
// fields are set to the range of record batches and dictionary batches
// described by the arrow spec's streaming format.
func (d *FileDeserializer) init() error {
	// Check the header magic
	if magic, err := d.read(8); err != nil {
		return pgerror.Wrap(err, pgcode.DataException, `verifying arrow file header magic`)
	} else if !bytes.Equal([]byte(fileMagic), magic[:len(fileMagic)]) {
		return errors.New(`arrow file header magic mismatch`)
	}
	if magic, err := d.readBackward(len(fileMagic)); err != nil {
		return pgerror.Wrap(err, pgcode.DataException, `verifying arrow file footer magic`)
	} else if !bytes.Equal([]byte(fileMagic), magic) {
		return errors.New(`arrow file magic footer mismatch`)
	}

	footerSize, err := d.readBackward(4)
	if err != nil {
		return pgerror.Wrap(err, pgcode.DataException, `reading arrow file footer`)
	}
	footerBytes, err := d.readBackward(int(binary.LittleEndian.Uint32(footerSize)))
	if err != nil {
		return pgerror.Wrap(err, pgcode.DataException, `reading arrow file footer`)
	}
	footer := arrowserde.GetRootAsFooter(footerBytes, 0)
	if footer.Version() != arrowserde.MetadataVersionV1 {
		return errors.Errorf(`only arrow V1 is supported got %d`, footer.Version())
	}

	var block arrowserde.Block
	d.recordBatches = d.recordBatches[:0]
	for blockIdx := 0; blockIdx < footer.RecordBatchesLength(); blockIdx++ {
		footer.RecordBatches(&block, blockIdx)
		d.recordBatches = append(d.recordBatches, fileBlock{
			offset:      block.Offset(),
			metadataLen: block.MetaDataLength(),
			bodyLen:     block.BodyLength(),
		})
	}

	return nil
}

type countingWriter struct {
	wrapped io.Writer
	written int
}

func (w *countingWriter) Write(buf []byte) (int, error) {
	n, err := w.wrapped.Write(buf)
	w.written += n
	return n, err
}

func schema(fb *flatbuffers.Builder, typs []*types.T) flatbuffers.UOffsetT {
	fieldOffsets := make([]flatbuffers.UOffsetT, len(typs))
	for idx, typ := range typs {
		var fbTyp byte
		var fbTypOffset flatbuffers.UOffsetT
		switch typeconv.TypeFamilyToCanonicalTypeFamily(typ.Family()) {
		case types.BoolFamily:
			arrowserde.BoolStart(fb)
			fbTypOffset = arrowserde.BoolEnd(fb)
			fbTyp = arrowserde.TypeBool
		case types.BytesFamily, types.JsonFamily:
			arrowserde.BinaryStart(fb)
			fbTypOffset = arrowserde.BinaryEnd(fb)
			fbTyp = arrowserde.TypeBinary
		case types.IntFamily:
			switch typ.Width() {
			case 16:
				arrowserde.IntStart(fb)
				arrowserde.IntAddBitWidth(fb, 16)
				arrowserde.IntAddIsSigned(fb, 1)
				fbTypOffset = arrowserde.IntEnd(fb)
				fbTyp = arrowserde.TypeInt
			case 32:
				arrowserde.IntStart(fb)
				arrowserde.IntAddBitWidth(fb, 32)
				arrowserde.IntAddIsSigned(fb, 1)
				fbTypOffset = arrowserde.IntEnd(fb)
				fbTyp = arrowserde.TypeInt
			case 0, 64:
				arrowserde.IntStart(fb)
				arrowserde.IntAddBitWidth(fb, 64)
				arrowserde.IntAddIsSigned(fb, 1)
				fbTypOffset = arrowserde.IntEnd(fb)
				fbTyp = arrowserde.TypeInt
			default:
				panic(errors.Errorf(`unexpected int width %d`, typ.Width()))
			}
		case types.FloatFamily:
			arrowserde.FloatingPointStart(fb)
			arrowserde.FloatingPointAddPrecision(fb, arrowserde.PrecisionDOUBLE)
			fbTypOffset = arrowserde.FloatingPointEnd(fb)
			fbTyp = arrowserde.TypeFloatingPoint
		case types.DecimalFamily:
			// Decimals are marshaled into bytes, so we use binary headers.
			arrowserde.BinaryStart(fb)
			fbTypOffset = arrowserde.BinaryEnd(fb)
			fbTyp = arrowserde.TypeDecimal
		case types.TimestampTZFamily:
			// Timestamps are marshaled into bytes, so we use binary headers.
			arrowserde.BinaryStart(fb)
			fbTypOffset = arrowserde.BinaryEnd(fb)
			fbTyp = arrowserde.TypeTimestamp
		case types.IntervalFamily:
			// Intervals are marshaled into bytes, so we use binary headers.
			arrowserde.BinaryStart(fb)
			fbTypOffset = arrowserde.BinaryEnd(fb)
			fbTyp = arrowserde.TypeInterval
		case typeconv.DatumVecCanonicalTypeFamily:
			// Datums are marshaled into bytes, so we use binary headers.
			arrowserde.BinaryStart(fb)
			fbTypOffset = arrowserde.BinaryEnd(fb)
			fbTyp = arrowserde.TypeUtf8
		default:
			panic(errors.Errorf(`don't know how to map %s`, typ))
		}
		arrowserde.FieldStart(fb)
		arrowserde.FieldAddTypeType(fb, fbTyp)
		arrowserde.FieldAddType(fb, fbTypOffset)
		fieldOffsets[idx] = arrowserde.FieldEnd(fb)
	}

	arrowserde.SchemaStartFieldsVector(fb, len(typs))
	// flatbuffers adds everything back to front. Reverse iterate so they're in
	// the right order when they come out.
	for i := len(fieldOffsets) - 1; i >= 0; i-- {
		fb.PrependUOffsetT(fieldOffsets[i])
	}
	fields := fb.EndVector(len(typs))

	arrowserde.SchemaStart(fb)
	arrowserde.SchemaAddFields(fb, fields)
	return arrowserde.SchemaEnd(fb)
}

func schemaMessage(fb *flatbuffers.Builder, typs []*types.T) flatbuffers.UOffsetT {
	schemaOffset := schema(fb, typs)
	arrowserde.MessageStart(fb)
	arrowserde.MessageAddVersion(fb, arrowserde.MetadataVersionV1)
	arrowserde.MessageAddHeaderType(fb, arrowserde.MessageHeaderSchema)
	arrowserde.MessageAddHeader(fb, schemaOffset)
	return arrowserde.MessageEnd(fb)
}

func fileFooter(
	fb *flatbuffers.Builder, typs []*types.T, recordBatches []fileBlock,
) flatbuffers.UOffsetT {
	schemaOffset := schema(fb, typs)
	arrowserde.FooterStartRecordBatchesVector(fb, len(recordBatches))
	// flatbuffers adds everything back to front. Reverse iterate so they're in
	// the right order when they come out.
	for i := len(recordBatches) - 1; i >= 0; i-- {
		rb := recordBatches[i]
		arrowserde.CreateBlock(fb, rb.offset, rb.metadataLen, rb.bodyLen)
	}
	recordBatchesOffset := fb.EndVector(len(recordBatches))
	arrowserde.FooterStart(fb)
	arrowserde.FooterAddVersion(fb, arrowserde.MetadataVersionV1)
	arrowserde.FooterAddSchema(fb, schemaOffset)
	arrowserde.FooterAddRecordBatches(fb, recordBatchesOffset)
	return arrowserde.FooterEnd(fb)
}
