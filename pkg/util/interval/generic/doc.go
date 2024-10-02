// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

/*
Package generic provides an implementation of a generic immutable interval
B-Tree.

The package uses code generation to create type-safe, zero-allocation
specializations of the ordered tree structure.

# Usage

Users of the package should follow these steps:

 1. Define a type that will be used to parameterize the generic tree structure.
 2. Ensure that the parameter type fulfills the type contract defined in
    internal/contract.go.
 3. Add an entry to their BUILD.bazel to generate the file by using the
    gen_interval_btree macro in //pkg/util/interval/generic:gen.bzl.
 4. Add the generated files to the BUILD.bazel file targets.
 5. Run ./dev gen to generate the files.

# Example

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

3. Add the following entry to the BUILD.bazel file:

	load("//pkg/util/interval/generic:gen.bzl", "gen_interval_btree")

	gen_interval_btree(
	  name = "latch_interval_btree",
	  package = "spanlatch",
	  type = "*latch",
	)

4. Add the generated files to the BUILD.bazel file targets:

	go_library(
	    # ...
	    srcs = [
	        # ...
	        ":latch_interval_btree.go",  # keep
	    ],
	    # ...
	)

	go_test(
	    # ...
	    srcs = [
	      # ...
	      ":latch_interval_btree_test.go", # keep
	    ],
	    # ...
	)

5. Run ./dev gen which should result in the following files:

  - latch_interval_btree.go
  - latch_interval_btree_test.go

# Working Example

See example_t.go for a working example. Running ./dev gen generates:

  - example_interval_btree.go
  - example_interval_btree_test.go
*/
package generic
