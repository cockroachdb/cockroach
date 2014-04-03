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
	"reflect"
	"testing"
	"time"
)

// A struct with every structured schema data type.
type KitchenSink struct {
	ID       int64      `roach:"id,pk"`
	Bool     bool       `roach:"bo"`
	Int      int        `roach:"i"`
	Int8     int8       `roach:"i8"`
	Int16    int16      `roach:"i16"`
	Int32    int32      `roach:"i32"`
	Int64    int64      `roach:"i64"`
	String   string     `roach:"str"`
	Blob     []byte     `roach:"bl"`
	Time     time.Time  `roach:"ti"`
	Location LatLong    `roach:"lo"`
	IS       IntegerSet `roach:"is"`
	SS       StringSet  `roach:"ss"`
	IM       IntegerMap `roach:"im"`
	SM       StringMap  `roach:"sm"`
}

func ExampleToYAML() {
	sm := map[string]interface{}{
		"ks": KitchenSink{},
	}
	s, err := NewGoSchema("Test", "t", sm)
	if err != nil {
		log.Fatalf("failed building schema: %v", err)
	}

	yaml, err := s.ToYAML()
	if err != nil {
		log.Fatalf("failed converting to yaml: %v", err)
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
	//     primary_key: true
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
	//   - column: IS
	//     column_key: is
	//     type: integerset
	//   - column: SS
	//     column_key: ss
	//     type: stringset
	//   - column: IM
	//     column_key: im
	//     type: integermap
	//   - column: SM
	//     column_key: sm
	//     type: stringmap
}

// TestYAMLRoundTrip converts from YAML directly back into a schema
// and do a deep-equality comparison.
func TestYAMLRoundTrip(t *testing.T) {
	sm := map[string]interface{}{
		"ks": KitchenSink{},
	}
	s, err := NewGoSchema("Test", "t", sm)
	if err != nil {
		log.Fatalf("failed building schema: %v", err)
	}

	yaml, err := s.ToYAML()
	if err != nil {
		log.Fatalf("failed converting to yaml: %v", err)
	}
	s2, err := NewYAMLSchema([]byte(yaml))
	if err != nil {
		log.Fatalf("failed to convert from yaml to a schema: %v", err)
	}
	if !reflect.DeepEqual(s, s2) {
		log.Fatal("yaml round trip schemas differ")
	}
}

// TestDuplicateTables verifies that duplicate table names and table
// keys are disallowed within a single schema.
func TestDuplicateTables(t *testing.T) {
	badYAML := []string{
		`db: Test
db_key: t
tables:
- table: A
  table_key: a
- table: A
  table_key: b`,

		`db: Test
db_key: t
tables:
- table: A
  table_key: a
- table: B
  table_key: a`,
	}

	for i, yaml := range badYAML {
		if _, err := NewYAMLSchema([]byte(yaml)); err == nil {
			t.Errorf("%d: expected failure on duplicate table names", i)
		}
	}
}

// TestDuplicateColumns verifies that duplicate column names and column
// keys are disallowed within a single schema.
func TestDuplicateColumns(t *testing.T) {
	badYAML := []string{
		`db: Test
db_key: t
tables:
- table: A
  table_key: a
  - column: A
    column_key: a
  - column: A
    column_key: b`,

		`db: Test
db_key: t
tables:
- table: A
  table_key: a
  - column: A
    column_key: a
  - column: B
    column_key: a`,
	}

	for i, yaml := range badYAML {
		if _, err := NewYAMLSchema([]byte(yaml)); err == nil {
			t.Errorf("%d: expected failure on duplicate column names", i)
		}
	}
}
