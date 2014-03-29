// Copyright 2014 The Cockroach Authors.
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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package schema

import (
	"fmt"
	"log"
	"time"
)

// A struct with every structured schema data type.
type KitchenSink struct {
	ID       int64     `roach:"id,pk"`
	Bool     bool      `roach:"bo"`
	Int      int       `roach:"i"`
	Int8     int8      `roach:"i8"`
	Int16    int16     `roach:"i16"`
	Int32    int32     `roach:"i32"`
	Int64    int64     `roach:"i64"`
	String   string    `roach:"str"`
	Blob     []byte    `roach:"bl"`
	Time     time.Time `roach:"ti"`
	Location LatLong   `roach:"lo"`
	NS       NumberSet `roach:"ns"`
	SS       StringSet `roach:"ss"`
	NM       NumberMap `roach:"nm"`
	SM       StringMap `roach:"sm"`
}

func ExampleKitchenSink() {
	sm := map[string]interface{}{
		"ks": KitchenSink{},
	}
	s, err := NewGoSchema("Test", "t", sm)
	if err != nil {
		log.Fatalf("failed building schema: %s", err)
	}

	yaml, err := s.ToYAML()
	if err != nil {
		log.Fatalf("failed converting to yaml: %s", err)
	}
	fmt.Println(string(yaml))
	// Output:
	// db: Test
	// db_key: t
	// tables:
	// - table: KitchenSink
	//   table_key: ks
	//   columns:
	//   - column: ID
	//     column_key: id
	//     type: integer
	//   - column: Bool
	//     column_key: bo
	//     type: integer
	//   - column: Int
	//     column_key: i
	//     type: integer
	//   - column: Int8
	//     column_key: i8
	//     type: integer
	//   - column: Int16
	//     column_key: i16
	//     type: integer
	//   - column: Int32
	//     column_key: i32
	//     type: integer
	//   - column: Int64
	//     column_key: i64
	//     type: integer
	//   - column: String
	//     column_key: str
	//     type: string
	//   - column: Blob
	//     column_key: bl
	//     type: blob
	//   - column: Time
	//     column_key: ti
	//     type: time
	//   - column: Location
	//     column_key: lo
	//     type: latlong
	//   - column: NS
	//     column_key: ns
	//     type: numberset
	//   - column: SS
	//     column_key: ss
	//     type: stringset
	//   - column: NM
	//     column_key: nm
	//     type: numbermap
	//   - column: SM
	//     column_key: sm
	//     type: stringmap
}
