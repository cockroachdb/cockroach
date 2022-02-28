// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scpb

import (
	"bytes"

	"github.com/gogo/protobuf/jsonpb"
)

// Override the marshaling and unmarshaling of PrimaryIndex and SecondaryIndex
// to suppress the noisy nesting to reveal their underlying Index.

var _ jsonpb.JSONPBMarshaler = (*PrimaryIndex)(nil)
var _ jsonpb.JSONPBUnmarshaler = (*PrimaryIndex)(nil)

func (m *PrimaryIndex) MarshalJSONPB(json *jsonpb.Marshaler) ([]byte, error) {
	return marshalIndex(json, &m.Index)
}

func (m *PrimaryIndex) UnmarshalJSONPB(json *jsonpb.Unmarshaler, data []byte) error {
	return json.Unmarshal(bytes.NewReader(data), &m.Index)
}

var _ jsonpb.JSONPBMarshaler = (*SecondaryIndex)(nil)
var _ jsonpb.JSONPBUnmarshaler = (*SecondaryIndex)(nil)

func (m *SecondaryIndex) MarshalJSONPB(json *jsonpb.Marshaler) ([]byte, error) {
	return marshalIndex(json, &m.Index)
}

func (m *SecondaryIndex) UnmarshalJSONPB(json *jsonpb.Unmarshaler, data []byte) error {
	return json.Unmarshal(bytes.NewReader(data), &m.Index)
}

func marshalIndex(json *jsonpb.Marshaler, idx *Index) ([]byte, error) {
	var buf bytes.Buffer
	if err := json.Marshal(&buf, idx); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
