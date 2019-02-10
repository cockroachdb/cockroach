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

package arrowserde

import (
	"github.com/apache/arrow/go/arrow"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/pkg/errors"
)

// ToDataType returns the corresponding arrow.DataType used to encode this field.
func (f *Field) ToDataType() (arrow.DataType, error) {
	var tab flatbuffers.Table

	switch Type(f.TypeType()) {
	case TypeInt:
		f.Type(&tab)
		var intType Int
		intType.Init(tab.Bytes, tab.Pos)
		if intType.IsSigned() > 0 {
			switch intType.BitWidth() {
			case 8:
				return arrow.PrimitiveTypes.Int8, nil
			case 16:
				return arrow.PrimitiveTypes.Int16, nil
			case 32:
				return arrow.PrimitiveTypes.Int32, nil
			case 64:
				return arrow.PrimitiveTypes.Int64, nil
			}
		} else {
			switch intType.BitWidth() {
			case 8:
				return arrow.PrimitiveTypes.Uint8, nil
			case 16:
				return arrow.PrimitiveTypes.Uint16, nil
			case 32:
				return arrow.PrimitiveTypes.Uint32, nil
			case 64:
				return arrow.PrimitiveTypes.Uint64, nil
			}
		}
	case TypeBinary:
		var kv KeyValue
		for i := 0; i < f.CustomMetadataLength(); i++ {
			f.CustomMetadata(&kv, i)
			if string(kv.Key()) != `sqltype` {
				continue
			}
			id := sqlbase.ColumnType_SemanticType_value[string(kv.Value())]
			switch sqlbase.ColumnType_SemanticType(id) {
			case sqlbase.ColumnType_STRING:
				return arrow.BinaryTypes.String, nil
			case sqlbase.ColumnType_DECIMAL:
				return arrow.BinaryTypes.Binary, nil
			}
		}
	}
	// TODO: This doesn't contain all the relevant info.
	return nil, errors.Errorf(`unsupported type %s`, EnumNamesType[f.TypeType()])
}
