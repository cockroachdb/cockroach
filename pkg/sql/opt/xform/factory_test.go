// Copyright 2018 The Cockroach Authors.
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

package xform

// testFactory adds additional metadata construction methods to the factory,
// to make testing easier.
type testFactory struct {
	Factory
	cat *testCatalog
}

func newTestFactory() *testFactory {
	cat := &testCatalog{}
	mem := newMemo(cat)
	f := &testFactory{cat: cat}
	f.Factory = *newFactory(mem, 0 /* maxSteps */)
	return f
}

// CREATE TABLE a (x INT PRIMARY KEY, y INT)
func (f *testFactory) constructTableA() TableIndex {
	a := &testTable{tabName: "a"}
	a.columns = append(
		a.columns,
		&testColumn{colName: "x"},
		&testColumn{colName: "y", isNullable: true},
	)

	f.cat.addTable(a)
	return f.Metadata().AddTable(a)
}

// CREATE TABLE b (x INT, z INT NOT NULL, FOREIGN KEY (x) REFERENCES a (x))
func (f *testFactory) constructTableB() TableIndex {
	b := &testTable{tabName: "b"}
	b.columns = append(
		b.columns,
		&testColumn{colName: "x"},
		&testColumn{colName: "z"},
	)

	f.cat.addTable(b)
	return f.Metadata().AddTable(b)
}
