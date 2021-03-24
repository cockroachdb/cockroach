// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// planDependencyInfo collects the dependencies related to a single
// table -- which index and columns are being depended upon.
type planDependencyInfo struct {
	// desc is a reference to the descriptor for the table being
	// depended on.
	desc catalog.TableDescriptor
	// deps is the list of ways in which the current plan depends on
	// that table. There can be more than one entries when the same
	// table is used in different places. The entries can also be
	// different because some may reference an index and others may
	// reference only a subset of the table's columns.
	// Note: the "ID" field of TableDescriptor_Reference is not
	// (and cannot be) filled during plan construction / dependency
	// analysis because the descriptor that is using this dependency
	// has not been constructed yet.
	deps []descpb.TableDescriptor_Reference
}

// planDependencies maps the ID of a table depended upon to a list of
// detailed dependencies on that table.
type planDependencies map[descpb.ID]planDependencyInfo

// String implements the fmt.Stringer interface.
func (d planDependencies) String() string {
	var buf bytes.Buffer
	for id, deps := range d {
		name := deps.desc.GetName()
		fmt.Fprintf(&buf, "%d (%q):", id, tree.ErrNameStringP(&name))
		for _, dep := range deps.deps {
			buf.WriteString(" [")
			if dep.IndexID != 0 {
				fmt.Fprintf(&buf, "idx: %d ", dep.IndexID)
			}
			fmt.Fprintf(&buf, "cols: %v]", dep.ColumnIDs)
		}
		buf.WriteByte('\n')
	}
	return buf.String()
}

// typeDependencies contains a set of the IDs of types that
// this view depends on.
type typeDependencies map[descpb.ID]struct{}

// checkViewMatchesMaterialized ensures that if a view is required, then the view
// is materialized or not as desired.
func checkViewMatchesMaterialized(
	desc catalog.TableDescriptor, requireView, wantMaterialized bool,
) error {
	if !requireView {
		return nil
	}
	if !desc.IsView() {
		return nil
	}
	isMaterialized := desc.MaterializedView()
	if isMaterialized && !wantMaterialized {
		err := pgerror.Newf(pgcode.WrongObjectType, "%q is a materialized view", desc.GetName())
		return errors.WithHint(err, "use the corresponding MATERIALIZED VIEW command")
	}
	if !isMaterialized && wantMaterialized {
		return pgerror.Newf(pgcode.WrongObjectType, "%q is not a materialized view", desc.GetName())
	}
	return nil
}
