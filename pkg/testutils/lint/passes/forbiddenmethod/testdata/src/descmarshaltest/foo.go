// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package descmarshaltest

import "github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"

func F() {
	var d descpb.Descriptor
	d.GetDatabase() // want `Illegal call to Descriptor.GetDatabase\(\), see descpb.FromDescriptorWithMVCCTimestamp\(\)`

	//nolint:descriptormarshal
	d.GetDatabase()

	//nolint:descriptormarshal
	d.GetDatabase()

	d.GetTable() // want `Illegal call to Descriptor.GetTable\(\), see descpb.FromDescriptorWithMVCCTimestamp\(\)`

	//nolint:descriptormarshal
	d.GetTable()

	//nolint:descriptormarshal
	d.GetTable()

	d.GetType() // want `Illegal call to Descriptor.GetType\(\), see descpb.FromDescriptorWithMVCCTimestamp\(\)`

	//nolint:descriptormarshal
	d.GetType()

	//nolint:descriptormarshal
	d.GetType()

	d.GetSchema() // want `Illegal call to Descriptor.GetSchema\(\), see descpb.FromDescriptorWithMVCCTimestamp\(\)`

	//nolint:descriptormarshal
	d.GetSchema()

	//nolint:descriptormarshal
	d.GetSchema()

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

	if t := d.GetTable(); t != // want `Illegal call to Descriptor.GetTable\(\), see descpb.FromDescriptorWithMVCCTimestamp\(\)`
		// nolint:descriptormarshal
		nil {
		panic("foo")
	}

	// It does not work to put the comment as an inline with the preamble to an
	// if statement.
	if t := d.GetTable(); t != nil { // nolint:descriptormarshal // want `Illegal call to Descriptor.GetTable\(\), see descpb.FromDescriptorWithMVCCTimestamp\(\)`
		panic("foo")
	}

	if t := d.GetTable(); t != nil { // want `Illegal call to Descriptor.GetTable\(\), see descpb.FromDescriptorWithMVCCTimestamp\(\)`
		panic("foo")
	}
}
