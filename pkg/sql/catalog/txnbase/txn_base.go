// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
package txnbase

// Package txnbase is to store interfaces for information related to a
// sql txn. They are created just to avoid cyclic dependency, and to actually
// use them, we must do the type assertion to the actual type mentioned in
// their corresponding comments.

// DescsCollection is an interface for descs.Collection. It is created to
// avoid cyclic dependency.
type DescsCollection interface {
	ImplementsDescsCollection()
}

// JobsCollection is an interface for sql.jobsCollection. It is created to
// avoid cyclic dependency.
type JobsCollection interface {
}

// DescpbID is an interface for descpb.ID.
// It is created to avoid cyclic dependency.
type DescpbID interface {
	ImplementsDescpbID()
}

// JobRecords is an interface for jobs.Record.
// It is created to avoid cyclic dependency.
type JobRecords interface {
	ImplementsJobRecords()
}
