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
	"encoding/binary"
	"io"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/cockroachdb/cockroach/pkg/util/colserde/arrowserde"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/pkg/errors"
)

var (
	// supportedTypes represents which types are supported for serialization. Used
	// for checking for type support.
	supportedTypes = map[arrow.Type]arrow.DataType{
		arrow.BOOL:              arrow.FixedWidthTypes.Boolean,
		arrow.INT8:              arrow.PrimitiveTypes.Int8,
		arrow.INT16:             arrow.PrimitiveTypes.Int16,
		arrow.INT32:             arrow.PrimitiveTypes.Int32,
		arrow.INT64:             arrow.PrimitiveTypes.Int64,
		arrow.FLOAT32:           arrow.PrimitiveTypes.Float32,
		arrow.FLOAT64:           arrow.PrimitiveTypes.Float64,
		arrow.STRING:            arrow.BinaryTypes.String,
		arrow.BINARY:            arrow.BinaryTypes.Binary,
		arrow.FIXED_SIZE_BINARY: &arrow.FixedSizeBinaryType{},
	}
	// supportedTypesSlice is a slice of the supportedTypes keys, returned to
	// callers of SupportedTypes.
	supportedTypesSlice []arrow.DataType
)

const (
	// metadataLengthNumBytes is the number of bytes used to encode the length of
	// the metadata in bytes. These are the first bytes of any arrow IPC message.
	metadataLengthNumBytes           = 4
	flatbufferBuilderInitialCapacity = 1024
)

func init() {
	for _, v := range supportedTypes {
		supportedTypesSlice = append(supportedTypesSlice, v)
	}
}

// SupportedTypes returns a slice of types that are supported for serialization.
// A call to this results in the creation of a new slice each time.
func SupportedTypes() []arrow.DataType {
	return supportedTypesSlice
}

// numBuffersForType returns how many buffers are used to represent an array of
// the given type.
func numBuffersForType(dataType arrow.DataType) int {
	// Nearly all types are represented by 2 memory.Buffers. One buffer for the
	// null bitmap and one for the values.
	numBuffers := 2
	switch dataType.(type) {
	case arrow.BinaryDataType, *arrow.FixedSizeBinaryType:
		// These types have an extra offsets buffer.
		numBuffers = 3
	}
	return numBuffers
}

// RecordBatchSerializer serializes RecordBatches in the standard Apache Arrow
// IPC format using flatbuffers. Note that only RecordBatch messages are
// supported and the type support is limited to those returned by
// SupportedTypes. This is because the full spec would be too much to support
// (support for DictionaryBatches, Tensors, SparseTensors, and Schema
// messages would be needed) and we only need the part of the spec that allows
// us to send data of the types we care about.
// The IPC format is described here:
// https://arrow.apache.org/docs/format/IPC.html
type RecordBatchSerializer struct {
	typs []arrow.DataType
	// numBuffers holds the number of buffers needed to represent an arrow array
	// of the type at the corresponding index of typs.
	numBuffers []int

	builder *flatbuffers.Builder
	scratch struct {
		bufferLens     []int
		metadataLength [metadataLengthNumBytes]byte
		padding        []byte
	}
}

// NewRecordBatchSerializer creates a new RecordBatchSerializer according to
// typs. Calling Serialize with a schema other than what is specified in typs
// results in an error, but Deserializing data that does not follow this schema
// results in undefined behavior.
func NewRecordBatchSerializer(typs []arrow.DataType) (*RecordBatchSerializer, error) {
	if len(typs) == 0 {
		return nil, errors.Errorf("zero length schema unsupported")
	}
	s := &RecordBatchSerializer{
		typs:       typs,
		numBuffers: make([]int, len(typs)),
		builder:    flatbuffers.NewBuilder(flatbufferBuilderInitialCapacity),
	}
	for i := range typs {
		if _, ok := supportedTypes[typs[i].ID()]; !ok {
			return nil, errors.Errorf("unsupported arrow type %s for serialization", typs[i].Name())
		}
		s.numBuffers[i] = numBuffersForType(typs[i])
	}
	// s.scratch.padding is used to align metadata to an 8 byte boundary, so
	// doesn't need to be larger than 7 bytes.
	s.scratch.padding = make([]byte, 7)
	return s, nil
}

// calculatePadding calculates how many bytes must be added to numBytes to round
// it up to the nearest multiple of 8.
func (s *RecordBatchSerializer) calculatePadding(numBytes int) int {
	return (8 - (numBytes & 7)) & 7
}

// Serialize serializes data as an arrow RecordBatch message and writes it to w.
// data must conform to the schema provided in NewRecordBatchSerializer,
// otherwise an error is returned.
func (s *RecordBatchSerializer) Serialize(w io.Writer, data []*array.Data) error {
	if len(data) != len(s.typs) {
		return errors.Errorf("mismatched schema length and number of columns: %d != %d", len(s.typs), len(data))
	}
	// Check data types and ensure equal data length. We don't support zero-length
	// schemas, so data[0] is in bounds at this point.
	headerLength := data[0].Len()
	for i := range data {
		typ := data[i].DataType()
		if s.typs[i].ID() != typ.ID() {
			return errors.Errorf(
				"cannot serialize unsupported schema: found %s, expected %s", typ.Name(), s.typs[i].Name(),
			)
		}
		if data[i].Len() != headerLength {
			return errors.Errorf("mismatched data lengths: %d != %d", headerLength, data[i].Len())
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
	s.scratch.padding = s.scratch.padding[:s.calculatePadding(metadataLengthNumBytes+len(metadataBytes))]

	// Write metadata + padding length as the first metadataLengthNumBytes.
	binary.LittleEndian.PutUint32(s.scratch.metadataLength[:], uint32(len(metadataBytes)+len(s.scratch.padding)))
	if _, err := w.Write(s.scratch.metadataLength[:]); err != nil {
		return err
	}

	// Write metadata.
	if _, err := w.Write(metadataBytes); err != nil {
		return err
	}

	// Add metadata padding.
	if _, err := w.Write(s.scratch.padding); err != nil {
		return err
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
				return err
			}
		}
	}

	// Add body padding. The body also needs to be a multiple of 8 bytes.
	s.scratch.padding = s.scratch.padding[:s.calculatePadding(bodyLength)]
	_, err := w.Write(s.scratch.padding)
	return err
}

// Deserialize deserializes an arrow IPC RecordBatch message contained in bytes
// into data. Deserializing a schema that does not match the schema given in
// NewRecordBatchSerializer results in undefined behavior.
func (s *RecordBatchSerializer) Deserialize(data *[]*array.Data, bytes []byte) error {
	// Read the metadata by first reading its length.
	metadataLen := int(binary.LittleEndian.Uint32(bytes[:metadataLengthNumBytes]))
	metadata := arrowserde.GetRootAsMessage(
		bytes[metadataLengthNumBytes:metadataLengthNumBytes+metadataLen], 0,
	)

	bodyBytes := bytes[metadataLengthNumBytes+metadataLen : metadataLengthNumBytes+metadataLen+int(metadata.BodyLength())]

	// We don't check the version because we don't fully support arrow
	// serialization/deserialization so it's not useful. Refer to the
	// RecordBathSerializer struct comment for more information.
	_ = metadata.Version()

	if metadata.HeaderType() != arrowserde.MessageHeaderRecordBatch {
		return errors.Errorf(
			`cannot decode RecordBatch from %s message`,
			arrowserde.EnumNamesMessageHeader[metadata.HeaderType()],
		)
	}

	var (
		headerTab flatbuffers.Table
		header    arrowserde.RecordBatch
	)

	if !metadata.Header(&headerTab) {
		return errors.New(`unable to decode metadata table`)
	}

	header.Init(headerTab.Bytes, headerTab.Pos)
	if len(s.typs) != header.NodesLength() {
		return errors.Errorf(
			`mismatched schema and header lengths: %d != %d`, len(s.typs), header.NodesLength(),
		)
	}

	var (
		node arrowserde.FieldNode
		buf  arrowserde.Buffer
	)
	for fieldIdx, bufferIdx := 0, 0; fieldIdx < len(s.typs); fieldIdx++ {
		header.Nodes(&node, fieldIdx)

		// Make sure that this node (i.e. column buffer) is the same length as the
		// length in the header, which specifies how many rows there are in the
		// message body.
		if node.Length() != header.Length() {
			return errors.Errorf(
				`mismatched field and header lengths: %d != %d`, node.Length(), header.Length(),
			)
		}

		// Decode the message body by using the offset and length information in the
		// message header.
		buffers := make([]*memory.Buffer, s.numBuffers[fieldIdx])
		for i := 0; i < s.numBuffers[fieldIdx]; i++ {
			header.Buffers(&buf, bufferIdx)
			bufData := bodyBytes[int(buf.Offset()):int(buf.Offset()+buf.Length())]
			buffers[i] = memory.NewBufferBytes(bufData)
			bufferIdx++
		}

		*data = append(
			*data,
			array.NewData(
				s.typs[fieldIdx],
				int(header.Length()),
				buffers,
				nil, /* childData. Note that we do not support types with childData */
				int(node.NullCount()),
				0, /* offset */
			),
		)
	}

	return nil
}
