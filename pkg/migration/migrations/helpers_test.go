// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migrations

import "github.com/cockroachdb/cockroach/pkg/sql/catalog"

func HasBackoffCols(jobsTable catalog.TableDescriptor, col string) (bool, error) {
	return hasColumn(jobsTable, col)
}

func HasBackoffIndex(jobsTable catalog.TableDescriptor, index string) (bool, error) {
	return hasIndex(jobsTable, index)
}

const AddColsQuery = addColsQuery
const AddIndexQuery = addIndexQuery
