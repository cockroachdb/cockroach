// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
