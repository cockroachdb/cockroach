// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package doctor provides utilities for checking the consistency of cockroach
// internal persisted metadata.
package doctor

import (
	"context"
	"fmt"
	"io"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// DescriptorTableRow represents a descriptor from table system.descriptor.
type DescriptorTableRow struct {
	ID        int64
	DescBytes []byte
	ModTime   hlc.Timestamp
}

// NewDescGetter creates a sqlbase.MapProtoGetter from a descriptor table.
func NewDescGetter(rows []DescriptorTableRow) (catalog.MapDescGetter, error) {
	pg := catalog.MapDescGetter{}
	for _, r := range rows {
		var d descpb.Descriptor
		if err := protoutil.Unmarshal(r.DescBytes, &d); err != nil {
			return nil, errors.Errorf("failed to unmarshal descriptor %d: %v", r.ID, err)
		}
		descpb.MaybeSetDescriptorModificationTimeFromMVCCTimestamp(context.Background(), &d, r.ModTime)
		pg[descpb.ID(r.ID)] = catalogkv.UnwrapDescriptorRaw(context.TODO(), &d)
	}
	return pg, nil
}

// Examine runs a suite of consistency checks over the descriptor table.
func Examine(descTable []DescriptorTableRow, verbose bool, stdout io.Writer) (ok bool, err error) {
	fmt.Fprintf(stdout, "Examining %d descriptors...\n", len(descTable))
	descGetter, err := NewDescGetter(descTable)
	if err != nil {
		return false, err
	}
	var problemsFound bool
	for _, row := range descTable {
		table, isTable := descGetter[descpb.ID(row.ID)].(catalog.TableDescriptor)
		if !isTable {
			// So far we only examine table descriptors. We may add checks for other
			// descriptors in later versions.
			continue
		}
		if int64(table.GetID()) != row.ID {
			fmt.Fprintf(stdout, "Table %3d: different id in the descriptor: %d", row.ID, table.GetID())
			problemsFound = true
			continue
		}
		if err := table.Validate(context.Background(), descGetter); err != nil {
			problemsFound = true
			fmt.Fprintf(stdout, "Table %3d: %s\n", table.GetID(), err)
		} else if verbose {
			fmt.Fprintf(stdout, "Table %3d: validated\n", table.GetID())
		}
	}
	return !problemsFound, nil
}
