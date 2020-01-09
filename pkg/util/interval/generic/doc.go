// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

/*
Package generic provides an implementation of a generic immutable interval
B-Tree.

The package uses code generation to create type-safe, zero-allocation
specializations of the ordered tree structure.

Usage

Users of the package should follow these steps:

  1. Define a type that will be used to parameterize the generic tree structure.
  2. Ensure that the parameter type fulfills the type contract defined in
     internal/contract.go.
  3. Include a go generate declaration that invokes the gen.sh script with the
     type name as the first argument and the package name as the second argument.
  4. Invoke go generate.

Example

1. The latch type is defined:

  type latch struct {
      id         uint64
      span       roachpb.Span
      ts         hlc.Timestamp
      done       *signal
      next, prev *latch // readSet linked-list.
  }

2. Methods are defined to fulfill the type contract.

  func (la *latch) ID() uint64         { return la.id }
  func (la *latch) Key() []byte        { return la.span.Key }
  func (la *latch) EndKey() []byte     { return la.span.EndKey }
  func (la *latch) String() string     { return fmt.Sprintf("%s@%s", la.span, la.ts) }
  func (la *latch) SetID(v uint64)     { la.id = v }
  func (la *latch) SetKey(v []byte)    { la.span.Key = v }
  func (la *latch) SetEndKey(v []byte) { la.span.EndKey = v }

3. The following comment is added near the declaration of the latch type:

  //go:generate ../../util/interval/generic/gen.sh *latch spanlatch

4. Invoking go generate results in the creation of the following files:

  * latch_interval_btree.go
  * latch_interval_btree_test.go

Working Example

See example_t.go for a working example. Running go generate on this package
generates:
  * example_interval_btree.go
  * example_interval_btree_test.go
*/
package generic
