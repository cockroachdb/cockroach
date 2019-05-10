// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package colserde

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"os"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/colserde/arrowserde"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/pkg/errors"
	"golang.org/x/sys/unix"
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
	typs []types.T
	fb   *flatbuffers.Builder
	a    *ArrowBatchConverter
	rb   *RecordBatchSerializer

	recordBatches []fileBlock
}

// NewFileSerializer creates a FileSerializer for the given types. The caller is
// responsible for closing the given writer.
func NewFileSerializer(w io.Writer, typs []types.T) (*FileSerializer, error) {
	rb, err := NewRecordBatchSerializer(typs)
	if err != nil {
		return nil, err
	}
	s := &FileSerializer{
		typs: typs,
		fb:   flatbuffers.NewBuilder(flatbufferBuilderInitialCapacity),
		a:    NewArrowBatchConverter(typs),
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
	// Pad to 8 byte boundary
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
	metadataLen, bodyLen, err := s.rb.Serialize(s.w, arrow)
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

	idx int
	end int
	a   *ArrowBatchConverter
	rb  *RecordBatchSerializer

	arrowScratch []*array.Data
}

// FileDeserializerFromBytes constructs a FileDeserializer for an in-memory
// buffer.
func FileDeserializerFromBytes(typs []types.T, buf []byte) (*FileDeserializer, error) {
	return newFileDeserializer(typs, buf, func() error { return nil })
}

// FileDeserializerFromPath constructs a FileDeserializer by mmap-ing (but not
// mlock-ing) the given path. Behavior is undefined if some other process
// concurrently modifies the file at this path.
func FileDeserializerFromPath(typs []types.T, path string) (*FileDeserializer, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return nil, pgerror.Wrapf(err, pgerror.CodeIoError, `opening %s`, path)
	}
	defer func() { _ = f.Close() }()

	fi, err := f.Stat()
	if err != nil {
		return nil, pgerror.Wrapf(err, pgerror.CodeIoError, `opening %s`, path)
	}

	// TODO(dan): This only works on *nix. If we care about windows, we should
	// switch to github.com/edsrzf/mmap-go.

	// It's not clear whether read-only mmap should be used with MAP_SHARED or
	// MAP_PRIVATE. The OS X man page doesn't say much either way. The linux man
	// page says modifications by another process will show up under MAP_SHARED
	// and the behavior is undefined under MAP_PRIVATE, but concurrent
	// modifications will almost certainly mess with the metadata and byte offsets
	// anyway. Intuitively, it seems like there may be some extra accounting for
	// MAP_PRIVATE, unless it's special cased for read-only, so we go with
	// MAP_SHARED for now.
	//
	// Note: The one data point I have for the other way is that the linux man
	// page has an example where a read-only file is mapped with MAP_PRIVATE, but
	// I'm wary of reading too much into that.
	buf, err := unix.Mmap(int(f.Fd()), 0, int(fi.Size()), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return nil, pgerror.Wrapf(err, pgerror.CodeIoError, `mmaping %s`, path)
	}

	// Don't need the file to be open anymore after mmap.
	if err := f.Close(); err != nil {
		if mErr := unix.Munmap(buf); mErr != nil {
			log.Warningf(context.Background(), `failed to un-mmap %s: %v`, path, mErr)
		}
		return nil, err
	}

	return newFileDeserializer(typs, buf, func() error { return unix.Munmap(buf) })
}

func newFileDeserializer(
	typs []types.T, buf []byte, bufCloseFn func() error,
) (*FileDeserializer, error) {
	rb, err := NewRecordBatchSerializer(typs)
	if err != nil {
		return nil, err
	}
	d := &FileDeserializer{
		buf:        buf,
		bufCloseFn: bufCloseFn,

		end: len(buf),
		a:   NewArrowBatchConverter(typs),
		rb:  rb,

		arrowScratch: make([]*array.Data, 0, len(typs)),
	}

	return d, d.init()
}

// Close releases any resources held by this deserializer.
func (d *FileDeserializer) Close() error {
	return d.bufCloseFn()
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
	if err := d.rb.Deserialize(&d.arrowScratch, buf); err != nil {
		return err
	}
	return d.a.ArrowToBatch(d.arrowScratch, b)
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

// read gets the `n` bytes from the end of the buffer, consuming them.
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
		return pgerror.Wrap(err, pgerror.CodeDataExceptionError, `verifying arrow file header magic`)
	} else if !bytes.Equal([]byte(fileMagic), magic[:len(fileMagic)]) {
		return errors.New(`arrow file header magic mismatch`)
	}
	if magic, err := d.readBackward(len(fileMagic)); err != nil {
		return pgerror.Wrap(err, pgerror.CodeDataExceptionError, `verifying arrow file footer magic`)
	} else if !bytes.Equal([]byte(fileMagic), magic) {
		return errors.New(`arrow file magic footer mismatch`)
	}

	footerSize, err := d.readBackward(4)
	if err != nil {
		return pgerror.Wrap(err, pgerror.CodeDataExceptionError, `reading arrow file footer`)
	}
	footerBytes, err := d.readBackward(int(binary.LittleEndian.Uint32(footerSize)))
	if err != nil {
		return pgerror.Wrap(err, pgerror.CodeDataExceptionError, `reading arrow file footer`)
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

func schemaMessage(fb *flatbuffers.Builder, typs []types.T) flatbuffers.UOffsetT {
	fieldOffsets := make([]flatbuffers.UOffsetT, len(typs))
	for idx, typ := range typs {
		var fbTyp byte
		var fbTypOffset flatbuffers.UOffsetT
		switch typ {
		case types.Bool:
			arrowserde.BoolStart(fb)
			fbTypOffset = arrowserde.BoolEnd(fb)
			fbTyp = arrowserde.TypeBool
		case types.Bytes:
			arrowserde.BinaryStart(fb)
			fbTypOffset = arrowserde.BinaryEnd(fb)
			fbTyp = arrowserde.TypeBinary
		case types.Int8:
			arrowserde.IntStart(fb)
			arrowserde.IntAddBitWidth(fb, 8)
			arrowserde.IntAddIsSigned(fb, 1)
			fbTypOffset = arrowserde.IntEnd(fb)
			fbTyp = arrowserde.TypeInt
		case types.Int16:
			arrowserde.IntStart(fb)
			arrowserde.IntAddBitWidth(fb, 16)
			arrowserde.IntAddIsSigned(fb, 1)
			fbTypOffset = arrowserde.IntEnd(fb)
			fbTyp = arrowserde.TypeInt
		case types.Int32:
			arrowserde.IntStart(fb)
			arrowserde.IntAddBitWidth(fb, 32)
			arrowserde.IntAddIsSigned(fb, 1)
			fbTypOffset = arrowserde.IntEnd(fb)
			fbTyp = arrowserde.TypeInt
		case types.Int64:
			arrowserde.IntStart(fb)
			arrowserde.IntAddBitWidth(fb, 64)
			arrowserde.IntAddIsSigned(fb, 1)
			fbTypOffset = arrowserde.IntEnd(fb)
			fbTyp = arrowserde.TypeInt
		case types.Float32:
			arrowserde.FloatingPointStart(fb)
			arrowserde.FloatingPointAddPrecision(fb, arrowserde.PrecisionSINGLE)
			fbTypOffset = arrowserde.FloatingPointEnd(fb)
			fbTyp = arrowserde.TypeFloatingPoint
		case types.Float64:
			arrowserde.FloatingPointStart(fb)
			arrowserde.FloatingPointAddPrecision(fb, arrowserde.PrecisionDOUBLE)
			fbTypOffset = arrowserde.FloatingPointEnd(fb)
			fbTyp = arrowserde.TypeFloatingPoint
		default:
			panic(errors.Errorf(`don't know how to map %s`, typ))
		}
		arrowserde.FieldStart(fb)
		arrowserde.FieldAddTypeType(fb, fbTyp)
		arrowserde.FieldAddType(fb, fbTypOffset)
		fieldOffsets[idx] = arrowserde.FieldEnd(fb)
	}
	arrowserde.SchemaStartFieldsVector(fb, len(typs))
	for _, fieldOffset := range fieldOffsets {
		fb.PrependUOffsetT(fieldOffset)
	}
	fields := fb.EndVector(len(typs))

	arrowserde.SchemaStart(fb)
	arrowserde.SchemaAddFields(fb, fields)
	header := arrowserde.SchemaEnd(fb)

	arrowserde.MessageStart(fb)
	arrowserde.MessageAddVersion(fb, arrowserde.MetadataVersionV1)
	arrowserde.MessageAddHeaderType(fb, arrowserde.MessageHeaderSchema)
	arrowserde.MessageAddHeader(fb, header)
	return arrowserde.MessageEnd(fb)
}

func fileFooter(
	fb *flatbuffers.Builder, typs []types.T, recordBatches []fileBlock,
) flatbuffers.UOffsetT {
	schemaOffset := schemaMessage(fb, typs)
	arrowserde.FooterStartRecordBatchesVector(fb, len(recordBatches))
	// Add the record batch entries in reverse order because flatbuffer adds them
	// back to front.
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
