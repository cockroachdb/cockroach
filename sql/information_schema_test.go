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

package sql

import (
	"testing"

	"github.com/davecgh/go-spew/spew"

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/gogo/protobuf/proto"
)

func TestVirtualTableLiterals(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// test the tables with specific permissions
	for _, schema := range virtualSchemas {
		for _, table := range schema.tables {
			gen, err := CreateTestTableDescriptor(0, keys.VirtualDescriptorID, table.schema, emptyPrivileges)
			if err != nil {
				t.Fatal(err)
			}
			gen.Families = nil
			gen.NextFamilyID = 0
			if !proto.Equal(&table.desc, &gen) {
				s1 := spew.Sdump(table.desc)
				s2 := spew.Sdump(gen)
				for i := range s1 {
					if s1[i] != s2[i] {
						t.Fatalf(
							"mismatch between %q:\npkg:\n\t%s\npartial-gen\n\t%s\ngen\n\t%#v",
							gen.Name, s1[:i+3], s2[:i+3], gen)
					}
				}
				panic("did not locate mismatch between re-generated version and pkg version")
			}
		}
	}
}
