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

package arrow

import (
	"encoding/binary"
	"io"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/cockroachdb/cockroach/pkg/util/arrow/arrowserde"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/pkg/errors"
)

// RecordBatch is a batch of columnar data. The columns are internally stored in
// a format suitable for inter-process communication, so they can be serialized
// and deserialized quickly.
type RecordBatch struct {
	Schema *arrowserde.Schema
	rows   int64
	Data   []*array.Data
}

// MakeBatch constructs a RecordBatch.
func MakeBatch(schema *arrowserde.Schema, rows int64, data []*array.Data) *RecordBatch {
	return &RecordBatch{
		Schema: schema,
		rows:   rows,
		Data:   data,
	}
}

// ApproxSize returns a rough guess at the RecordBatch's size.
func (r *RecordBatch) ApproxSize() int64 {
	var bytes int64
	for _, col := range r.Data {
		bytes += int64(col.Len())
	}
	return bytes
}

// RecordBatchEncoder serializes RecordBatches in the standard Apache Arrow IPC
// format.
type RecordBatchEncoder struct {
	builder        *flatbuffers.Builder
	bufferLens     []int
	totalBufferLen int
}

// Encode writes the given RecordBatch to the Writer. Encode is not threadsafe.
func (e *RecordBatchEncoder) Encode(w io.Writer, b *RecordBatch) error {
	// Warning to the reader: flatbuffers are subtle and the go library doesn't do
	// much to keep you from making mistakes. Read the tutorial VERY carefully, I
	// lost hours to debugging why my list of structs weren't serialized correctly
	// to finally find that you serialize them differently than a list of tables.
	// https://google.github.io/flatbuffers/flatbuffers_guide_tutorial.html

	if e.builder == nil {
		e.builder = flatbuffers.NewBuilder(1024)
	} else {
		e.builder.Reset()
	}
	e.bufferLens = e.bufferLens[:0]
	e.totalBufferLen = 0

	arrowserde.RecordBatchStartNodesVector(e.builder, len(b.Data))
	for i := len(b.Data) - 1; i >= 0; i-- {
		col := b.Data[i]
		// TODO: This probably only works for simple cases.
		arrowserde.CreateFieldNode(e.builder, b.rows, int64(col.NullN()))
		for _, buffer := range col.Buffers() {
			bufferLen := buffer.Len()
			e.bufferLens = append(e.bufferLens, bufferLen)
			e.totalBufferLen += bufferLen
		}
	}
	nodes := e.builder.EndVector(len(b.Data))

	arrowserde.RecordBatchStartBuffersVector(e.builder, len(e.bufferLens))
	for i, offset := len(e.bufferLens)-1, e.totalBufferLen; i >= 0; i-- {
		bufferLen := e.bufferLens[i]
		offset -= bufferLen
		arrowserde.CreateBuffer(e.builder, int64(offset), int64(bufferLen))
	}
	buffers := e.builder.EndVector(len(e.bufferLens))

	arrowserde.RecordBatchStart(e.builder)
	arrowserde.RecordBatchAddLength(e.builder, b.rows)
	arrowserde.RecordBatchAddNodes(e.builder, nodes)
	arrowserde.RecordBatchAddBuffers(e.builder, buffers)
	header := arrowserde.RecordBatchEnd(e.builder)

	// TODO: There's a lot more to set here.
	arrowserde.MessageStart(e.builder)
	arrowserde.MessageAddVersion(e.builder, arrowserde.MetadataVersionV1)
	arrowserde.MessageAddHeaderType(e.builder, arrowserde.MessageHeaderRecordBatch)
	arrowserde.MessageAddHeader(e.builder, header)
	arrowserde.MessageAddBodyLength(e.builder, int64(e.totalBufferLen))
	message := arrowserde.MessageEnd(e.builder)

	e.builder.Finish(message)
	metadataBytes := e.builder.FinishedBytes()

	// TODO: Padding.
	var padding []byte

	var metadataLength [4]byte
	binary.LittleEndian.PutUint32(metadataLength[0:4], uint32(len(metadataBytes)+len(padding)))
	if _, err := w.Write(metadataLength[0:4]); err != nil {
		return err
	}
	if _, err := w.Write(metadataBytes); err != nil {
		return err
	}
	if _, err := w.Write(padding); err != nil {
		return err
	}
	for _, col := range b.Data {
		// TODO: This probably only works for simple cases.
		for _, buffer := range col.Buffers() {
			if _, err := w.Write(buffer.Bytes()); err != nil {
				return err
			}
		}
	}
	return nil
}

// Decode decodes a RecordBatch with the given schema from the bytes.
func (b *RecordBatch) Decode(schema *arrowserde.Schema, bytes []byte) error {
	metadataLen := int(binary.LittleEndian.Uint32(bytes[0:4]))
	metadataBytes := bytes[4 : 4+metadataLen]
	metadata := arrowserde.GetRootAsMessage(metadataBytes, 0)
	bodyBytes := bytes[4+metadataLen : 4+metadataLen+int(metadata.BodyLength())]

	// TODO: Check version.
	if arrowserde.MessageHeader(metadata.HeaderType()) != arrowserde.MessageHeaderRecordBatch {
		return errors.Errorf(`cannot decode RecordBatch from %s message`,
			arrowserde.EnumNamesMessageHeader[metadata.HeaderType()])
	}
	var headerTab flatbuffers.Table
	metadata.Header(&headerTab)
	var header arrowserde.RecordBatch
	header.Init(headerTab.Bytes, headerTab.Pos)
	if schema.FieldsLength() != header.NodesLength() {
		return errors.Errorf(`schema doesn't match header: %d vs %d fields`,
			schema.FieldsLength(), header.NodesLength())
	}

	b.Schema = schema
	b.rows = header.Length()
	b.Data = make([]*array.Data, 0, int(schema.FieldsLength()))

	var field arrowserde.Field
	var node arrowserde.FieldNode
	var buf arrowserde.Buffer
	for fieldIdx, bufferIdx := 0, 0; fieldIdx < schema.FieldsLength(); fieldIdx++ {
		schema.Fields(&field, fieldIdx)
		header.Nodes(&node, fieldIdx)

		if node.Length() != b.rows {
			return errors.Errorf(`field length %d doesn't match header length %d`,
				node.Length(), b.rows)
		}

		dataType, err := field.ToDataType()
		if err != nil {
			return err
		}

		// TODO: This probably only works for simple cases.
		var numBuffers int
		switch arrowserde.Type(field.TypeType()) {
		case arrowserde.TypeInt:
			numBuffers = 2
		case arrowserde.TypeBinary:
			numBuffers = 3
		}

		var buffers []*memory.Buffer
		for i := 0; i < numBuffers; i++ {
			header.Buffers(&buf, bufferIdx)
			bufferIdx++
			bufData := bodyBytes[int(buf.Offset()):int(buf.Offset()+buf.Length())]
			buffers = append(buffers, memory.NewBufferBytes(bufData))
		}

		var childData []*array.Data
		colData := array.NewData(
			dataType, int(header.Length()), buffers, childData, int(node.NullCount()), 0)
		b.Data = append(b.Data, colData)
	}

	return nil
}
