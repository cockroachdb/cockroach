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
	"encoding/binary"
	"io"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/cockroachdb/cockroach/pkg/col/colserde/arrowserde"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	flatbuffers "github.com/google/flatbuffers/go"
)

const (
	// metadataLengthNumBytes is the number of bytes used to encode the length of
	// the metadata in bytes. These are the first bytes of any arrow IPC message.
	metadataLengthNumBytes           = 4
	flatbufferBuilderInitialCapacity = 1024
)

// numBuffersForType returns how many buffers are used to represent an array of
// the given type.
func numBuffersForType(t *types.T) int {
	// Most types are represented by 3 memory.Buffers (because most types are
	// serialized into flat bytes representation). One buffer for the null
	// bitmap, one for the values, and one for the offsets.
	numBuffers := 3
	switch typeconv.TypeFamilyToCanonicalTypeFamily(t.Family()) {
	case types.BoolFamily, types.FloatFamily, types.IntFamily:
		// This type doesn't have an offsets buffer.
		numBuffers = 2
	}
	return numBuffers
}

// RecordBatchSerializer serializes RecordBatches in the standard Apache Arrow
// IPC format using flatbuffers. Note that only RecordBatch messages are
// supported. This is because the full spec would be too much to support
// (support for DictionaryBatches, Tensors, SparseTensors, and Schema
// messages would be needed) and we only need the part of the spec that allows
// us to send data.
// The IPC format is described here:
// https://arrow.apache.org/docs/format/IPC.html
type RecordBatchSerializer struct {
	// numBuffers holds the number of buffers needed to represent an arrow array
	// of the type at the corresponding index of typs passed in in
	// NewRecordBatchSerializer.
	numBuffers []int

	builder *flatbuffers.Builder
	scratch struct {
		bufferLens     []int
		metadataLength [metadataLengthNumBytes]byte
		padding        []byte
	}
}

// NewRecordBatchSerializer creates a new RecordBatchSerializer according to
// typs. Note that Serializing or Deserializing data that does not follow the
// passed in schema results in undefined behavior.
func NewRecordBatchSerializer(typs []*types.T) (*RecordBatchSerializer, error) {
	s := &RecordBatchSerializer{
		numBuffers: make([]int, len(typs)),
		builder:    flatbuffers.NewBuilder(flatbufferBuilderInitialCapacity),
	}
	for i, t := range typs {
		s.numBuffers[i] = numBuffersForType(t)
	}
	// s.scratch.padding is used to align metadata to an 8 byte boundary, so
	// doesn't need to be larger than 7 bytes.
	s.scratch.padding = make([]byte, 7)
	return s, nil
}

// calculatePadding calculates how many bytes must be added to numBytes to round
// it up to the nearest multiple of 8.
func calculatePadding(numBytes int) int {
	return (8 - (numBytes & 7)) & 7
}

// Serialize serializes data as an arrow RecordBatch message and writes it to w.
// Serializing a schema that does not match the schema given in
// NewRecordBatchSerializer results in undefined behavior.
// Each element of the input data array is consumed to minimize memory waste,
// so users who wish to retain references to individual array.Data elements must
// do so by making a copy elsewhere.
func (s *RecordBatchSerializer) Serialize(
	w io.Writer, data []*array.Data, headerLength int,
) (metadataLen uint32, dataLen uint64, _ error) {
	if len(data) != len(s.numBuffers) {
		return 0, 0, errors.Errorf("mismatched schema length and number of columns: %d != %d", len(s.numBuffers), len(data))
	}
	for i := range data {
		if data[i].Len() != headerLength {
			return 0, 0, errors.Errorf("mismatched data lengths at column %d: %d != %d", i, headerLength, data[i].Len())
		}
		if len(data[i].Buffers()) != s.numBuffers[i] {
			return 0, 0, errors.Errorf(
				"mismatched number of buffers at column %d: %d != %d", i, len(data[i].Buffers()), s.numBuffers[i],
			)
		}
	}

	// The following is a good tutorial to understand flatbuffers (i.e. what is
	// going on here) better:
	// https://google.github.io/flatbuffers/flatbuffers_guide_tutorial.html

	s.builder.Reset()
	s.scratch.bufferLens = s.scratch.bufferLens[:0]
	totalBufferLen := 0

	// Encode the nodes. These are structs that represent each element in data,
	// including the length and null count.
	// When constructing flatbuffers, we start at the leaves of whatever data
	// structure we are serializing so that we can deserialize from the beginning
	// of that buffer while taking advantage of cache prefetching. Vectors are
	// serialized backwards (hence the backwards iteration) because flatbuffer
	// builders can only be prepended to and it simplifies the spec if vectors
	// follow the same back to front approach as other data.
	arrowserde.RecordBatchStartNodesVector(s.builder, len(data))
	for i := len(data) - 1; i >= 0; i-- {
		col := data[i]
		arrowserde.CreateFieldNode(s.builder, int64(col.Len()), int64(col.NullN()))
		buffers := col.Buffers()
		for j := len(buffers) - 1; j >= 0; j-- {
			bufferLen := 0
			// Some value buffers can be nil if the data are all zero values.
			if buffers[j] != nil {
				bufferLen = buffers[j].Len()
			}
			s.scratch.bufferLens = append(s.scratch.bufferLens, bufferLen)
			totalBufferLen += bufferLen
		}
	}
	nodes := s.builder.EndVector(len(data))

	// Encode the buffers vector. There are many buffers for each element in data
	// and the actual bytes will be added to the message body later. Here we
	// encode structs that hold the offset (relative to the start of the body)
	// and the length of each buffer so that the deserializer can seek to the
	// actual bytes in the body. Note that we iterate over s.scratch.bufferLens
	// forwards due to adding lengths in the order that we want to prepend when
	// creating the nodes vector.
	arrowserde.RecordBatchStartBuffersVector(s.builder, len(s.scratch.bufferLens))
	for i, offset := 0, totalBufferLen; i < len(s.scratch.bufferLens); i++ {
		bufferLen := s.scratch.bufferLens[i]
		offset -= bufferLen
		arrowserde.CreateBuffer(s.builder, int64(offset), int64(bufferLen))
	}
	buffers := s.builder.EndVector(len(s.scratch.bufferLens))

	// Encode the RecordBatch. This is a table that holds both the nodes and
	// buffer information.
	arrowserde.RecordBatchStart(s.builder)
	arrowserde.RecordBatchAddLength(s.builder, int64(headerLength))
	arrowserde.RecordBatchAddNodes(s.builder, nodes)
	arrowserde.RecordBatchAddBuffers(s.builder, buffers)
	header := arrowserde.RecordBatchEnd(s.builder)

	// Finally, encode the Message table. This will include the RecordBatch above
	// as well as some metadata.
	arrowserde.MessageStart(s.builder)
	arrowserde.MessageAddVersion(s.builder, arrowserde.MetadataVersionV1)
	arrowserde.MessageAddHeaderType(s.builder, arrowserde.MessageHeaderRecordBatch)
	arrowserde.MessageAddHeader(s.builder, header)
	arrowserde.MessageAddBodyLength(s.builder, int64(totalBufferLen))
	s.builder.Finish(arrowserde.MessageEnd(s.builder))

	metadataBytes := s.builder.FinishedBytes()

	// Use s.scratch.padding to align metadata to 8-byte boundary.
	s.scratch.padding = s.scratch.padding[:calculatePadding(metadataLengthNumBytes+len(metadataBytes))]

	// Write metadata + padding length as the first metadataLengthNumBytes.
	metadataLength := uint32(len(metadataBytes) + len(s.scratch.padding))
	binary.LittleEndian.PutUint32(s.scratch.metadataLength[:], metadataLength)
	if _, err := w.Write(s.scratch.metadataLength[:]); err != nil {
		return 0, 0, err
	}

	// Write metadata.
	if _, err := w.Write(metadataBytes); err != nil {
		return 0, 0, err
	}

	// Add metadata padding.
	if _, err := w.Write(s.scratch.padding); err != nil {
		return 0, 0, err
	}

	// Add message body. The metadata holds the offsets and lengths of these
	// buffers.
	bodyLength := 0
	for i := 0; i < len(data); i++ {
		buffers := data[i].Buffers()
		for j := 0; j < len(buffers); j++ {
			var bufferBytes []byte
			if buffers[j] != nil {
				// Some value buffers can be nil if the data are all zero values.
				bufferBytes = buffers[j].Bytes()
			}
			bodyLength += len(bufferBytes)
			if _, err := w.Write(bufferBytes); err != nil {
				return 0, 0, err
			}
		}
		// Eagerly discard the buffer; we have no use for it any longer.
		data[i] = nil
	}

	// Add body padding. The body also needs to be a multiple of 8 bytes.
	s.scratch.padding = s.scratch.padding[:calculatePadding(bodyLength)]
	_, err := w.Write(s.scratch.padding)
	bodyLength += len(s.scratch.padding)
	return metadataLength, uint64(bodyLength), err
}

// Deserialize deserializes an arrow IPC RecordBatch message contained in bytes
// into data and returns the length of the batch. Deserializing a schema that
// does not match the schema given in NewRecordBatchSerializer results in
// undefined behavior.
func (s *RecordBatchSerializer) Deserialize(data *[]*array.Data, bytes []byte) (int, error) {
	// Read the metadata by first reading its length.
	metadataLen := int(binary.LittleEndian.Uint32(bytes[:metadataLengthNumBytes]))
	metadata := arrowserde.GetRootAsMessage(
		bytes[metadataLengthNumBytes:metadataLengthNumBytes+metadataLen], 0,
	)

	bodyBytes := bytes[metadataLengthNumBytes+metadataLen : metadataLengthNumBytes+metadataLen+int(metadata.BodyLength())]

	// We don't check the version because we don't fully support arrow
	// serialization/deserialization so it's not useful. Refer to the
	// RecordBatchSerializer struct comment for more information.
	_ = metadata.Version()

	if metadata.HeaderType() != arrowserde.MessageHeaderRecordBatch {
		return 0, errors.Errorf(
			`cannot decode RecordBatch from %s message`,
			arrowserde.EnumNamesMessageHeader[metadata.HeaderType()],
		)
	}

	var (
		headerTab flatbuffers.Table
		header    arrowserde.RecordBatch
	)

	if !metadata.Header(&headerTab) {
		return 0, errors.New(`unable to decode metadata table`)
	}

	header.Init(headerTab.Bytes, headerTab.Pos)
	if len(s.numBuffers) != header.NodesLength() {
		return 0, errors.Errorf(
			`mismatched schema and header lengths: %d != %d`, len(s.numBuffers), header.NodesLength(),
		)
	}

	var (
		node arrowserde.FieldNode
		buf  arrowserde.Buffer
	)
	for fieldIdx, bufferIdx := 0, 0; fieldIdx < len(s.numBuffers); fieldIdx++ {
		header.Nodes(&node, fieldIdx)

		// Make sure that this node (i.e. column buffer) is the same length as the
		// length in the header, which specifies how many rows there are in the
		// message body.
		if node.Length() != header.Length() {
			return 0, errors.Errorf(
				`mismatched field and header lengths: %d != %d`, node.Length(), header.Length(),
			)
		}

		// Decode the message body by using the offset and length information in the
		// message header.
		buffers := make([]*memory.Buffer, s.numBuffers[fieldIdx])
		for i := 0; i < s.numBuffers[fieldIdx]; i++ {
			header.Buffers(&buf, bufferIdx)
			bufData := bodyBytes[buf.Offset() : buf.Offset()+buf.Length()]
			if i < len(buffers)-1 {
				// We need to cap the slice so that bufData's capacity doesn't
				// extend into the data of the next buffer if this buffer is not
				// the last one (meaning there is that next buffer).
				bufData = bufData[:buf.Length():buf.Length()]
			}
			buffers[i] = memory.NewBufferBytes(bufData)
			bufferIdx++
		}

		*data = append(
			*data,
			array.NewData(
				nil, /* dType */
				int(header.Length()),
				buffers,
				nil, /* childData. Note that we do not support types with childData */
				int(node.NullCount()),
				0, /* offset */
			),
		)
	}

	return int(header.Length()), nil
}
