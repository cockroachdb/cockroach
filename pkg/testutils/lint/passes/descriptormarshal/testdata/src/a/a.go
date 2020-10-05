// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package a

import "github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"

func F() {
	var d descpb.Descriptor
	d.GetTable() // want `Illegal call to Descriptor.GetTable\(\), see descpb.TableFromDescriptor\(\)`

	//nolint:descriptormarshal
	d.GetTable()

	// nolint:descriptormarshal
	d.GetTable()

	// nolint:descriptormarshal
	if t := d.GetTable(); t != nil {
		panic("foo")
	}

	if t := d.
		// nolint:descriptormarshal
		GetTable(); t != nil {
		panic("foo")
	}

	if t :=
		// nolint:descriptormarshal
		d.GetTable(); t != nil {
		panic("foo")
	}

	if t := d.GetTable(); t != // want `Illegal call to Descriptor.GetTable\(\), see descpb.TableFromDescriptor\(\)`
		// nolint:descriptormarshal
		nil {
		panic("foo")
	}

	// It does not work to put the comment as an inline with the preamble to an
	// if statement.
	if t := d.GetTable(); t != nil { // nolint:descriptormarshal // want `Illegal call to Descriptor.GetTable\(\), see descpb.TableFromDescriptor\(\)`
		panic("foo")
	}

	if t := d.GetTable(); t != nil { // want `Illegal call to Descriptor.GetTable\(\), see descpb.TableFromDescriptor\(\)`
		panic("foo")
	}
}
