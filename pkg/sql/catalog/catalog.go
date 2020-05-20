// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catalog

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// ObjectDescriptor provides table information for results from a name lookup.
type ObjectDescriptor interface {
	tree.NameResolutionResult

	// TableDesc returns the underlying table descriptor, or nil if the
	// descriptor is not a table backed object.
	TableDesc() *sqlbase.TableDescriptor

	// TypeDesc returns the underlying type descriptor, or nil if the
	// descriptor is not a type backed object.
	TypeDesc() *sqlbase.TypeDescriptor
}
