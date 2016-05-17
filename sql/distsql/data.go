// Copyright 2016 The Cockroach Authors.
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
//
// Author: Radu Berinde (radu@cockroachlabs.com)

package distsql

import (
	"fmt"

	"github.com/cockroachdb/cockroach/sql/sqlbase"
)

func (de DatumInfo_EncodingType) toDatumEncoding() sqlbase.DatumEncoding {
	switch de {
	case DatumInfo_ASCENDING_KEY_ENCODING:
		return sqlbase.AscendingKeyEncoding
	case DatumInfo_DESCENDING_KEY_ENCODING:
		return sqlbase.DescendingKeyEncoding
	default:
		panic(fmt.Sprintf("unknown DatumInfo_EncodingType %s", de))
	}
}

func datumEncodingToDatumInfoEncoding(enc sqlbase.DatumEncoding) DatumInfo_EncodingType {
	switch enc {
	case sqlbase.AscendingKeyEncoding:
		return DatumInfo_ASCENDING_KEY_ENCODING
	case sqlbase.DescendingKeyEncoding:
		return DatumInfo_DESCENDING_KEY_ENCODING
	default:
		panic(fmt.Sprintf("unknown DatumEncoding %d", enc))
	}
}
