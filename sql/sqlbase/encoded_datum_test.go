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
//

package sqlbase

import (
	"testing"

	"github.com/cockroachdb/cockroach/sql/parser"
)

func TestEncDatum(t *testing.T) {
	a := &DatumAlloc{}
	x := &EncDatum{}
	if !x.IsUnset() {
		t.Errorf("empty EncDatum should be unset")
	}
	if x.Encoding() != NoEncoding {
		t.Errorf("invalid encoding %d", x.Encoding())
	}

	x.SetDatum(ColumnType_INT, parser.NewDInt(5))
	if x.IsUnset() {
		t.Errorf("unset after SetDatum()")
	}

	encoded, err := x.Encode(a, AscendingKeyEncoding, nil)
	if err != nil {
		t.Fatal(err)
	}

	y := &EncDatum{}
	y.SetEncoded(ColumnType_INT, AscendingKeyEncoding, encoded)

	if y.IsUnset() {
		t.Errorf("unset after SetEncoded()")
	}
	if y.Encoding() != AscendingKeyEncoding {
		t.Errorf("invalid encoding %d", x.Encoding())
	}

	err = y.Decode(a)
	if err != nil {
		t.Fatal(err)
	}
	if cmp := y.Datum.Compare(x.Datum); cmp != 0 {
		t.Errorf("Datums should be equal, cmp = %d", cmp)
	}

	enc2, err := y.Encode(a, DescendingKeyEncoding, nil)
	if err != nil {
		t.Fatal(err)
	}
	// y's encoding should not change.
	if y.Encoding() != AscendingKeyEncoding {
		t.Errorf("invalid encoding %d", x.Encoding())
	}
	x.SetEncoded(ColumnType_INT, DescendingKeyEncoding, enc2)
	if x.Encoding() != DescendingKeyEncoding {
		t.Errorf("invalid encoding %d", x.Encoding())
	}
	err = x.Decode(a)
	if err != nil {
		t.Fatal(err)
	}
	if cmp := y.Datum.Compare(x.Datum); cmp != 0 {
		t.Errorf("Datums should be equal, cmp = %d", cmp)
	}
}
