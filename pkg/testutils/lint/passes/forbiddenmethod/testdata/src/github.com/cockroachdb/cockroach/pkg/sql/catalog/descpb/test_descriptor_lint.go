// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package descpb

type Descriptor struct{}

type TableDescriptor struct{}

type DatabaseDescriptor struct{}

type TypeDescriptor struct{}

type SchemaDescriptor struct{}

func (m *Descriptor) GetTable() *TableDescriptor {
	return nil
}

func (m *Descriptor) GetDatabase() *DatabaseDescriptor {
	return nil
}

func (m *Descriptor) GetType() *TypeDescriptor {
	return nil
}

func (m *Descriptor) GetSchema() *SchemaDescriptor {
	return nil
}
