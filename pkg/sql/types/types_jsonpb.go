// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package types

import (
	"bytes"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/lib/pq/oid"
)

// This file contains logic to allow the *types.T to properly marshal to json.
// It is a separate file to make it straightforward to defeat the linter that
// refuses to allow one to call a method Marshal unless it's protoutil.Marshal.

// MarshalJSONPB marshals the T to json. This is necessary as otherwise
// this field will be lost to the crdb_internal.pb_to_json and the likes.
func (t *T) MarshalJSONPB(marshaler *jsonpb.Marshaler) ([]byte, error) {
	// Map empty locale to nil so empty string does not appear in the JSON result.
	// TODO(rafi): When we upgrade to go1.24, we can modify the proto definition
	//  of the locale field to use `[(gogoproto.jsontag) = ",omitzero"]` instead of
	//  this workaround.
	temp := *t
	if temp.InternalType.Locale != nil && len(*temp.InternalType.Locale) == 0 {
		temp.InternalType.Locale = nil
	}
	// VisibleType is only for compatibility with 25.3 and earlier, so we don't
	// need to ever show it in JSON.
	temp.InternalType.VisibleType = 0
	var buf bytes.Buffer
	if err := marshaler.Marshal(&buf, &temp.InternalType); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// UnmarshalJSONPB unmarshals the T to json. This is necessary as otherwise
// this field will be lost to the crdb_internal.json_to_pb and the likes.
func (t *T) UnmarshalJSONPB(unmarshaler *jsonpb.Unmarshaler, data []byte) error {
	if err := unmarshaler.Unmarshal(bytes.NewReader(data), &t.InternalType); err != nil {
		return err
	}
	// In order for descriptors to roundtrip the conversion to JSON and back, add
	// the VisibleType field back for types that need it.
	switch t.InternalType.Oid {
	case oid.T_int2:
		t.InternalType.VisibleType = visibleSMALLINT
	case oid.T_int4:
		t.InternalType.VisibleType = visibleINTEGER
	case oid.T_int8:
		t.InternalType.VisibleType = visibleBIGINT
	case oid.T_float4:
		t.InternalType.VisibleType = visibleREAL
	case oid.T_float8:
		t.InternalType.VisibleType = visibleDOUBLE
	case oid.T_varchar:
		t.InternalType.VisibleType = visibleVARCHAR
	case oid.T_bpchar:
		t.InternalType.VisibleType = visibleCHAR
	case oid.T_char:
		t.InternalType.VisibleType = visibleQCHAR
	case oid.T_varbit:
		t.InternalType.VisibleType = visibleVARBIT
	}
	return t.upgradeType()
}
