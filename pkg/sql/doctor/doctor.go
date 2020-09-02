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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// DescriptorTableRow represents a descriptor from table system.descriptor.
type DescriptorTableRow struct {
	ID        int64
	DescBytes []byte
	ModTime   hlc.Timestamp
}

// DescriptorTable represents data read from `system.descriptor`.
type DescriptorTable []DescriptorTableRow

// NamespaceTableRow represents a namespace entry from table system.namespace.
type NamespaceTableRow struct {
	descpb.NameInfo
	ID int64
}

// NamespaceTable represents data read from `system.namespace2`.
type NamespaceTable []NamespaceTableRow

// namespaceReverseMap is the inverse of the namespace map stored in table
// `system.namespace`.
type namespaceReverseMap map[int64][]descpb.NameInfo

func newDescGetter(ctx context.Context, rows []DescriptorTableRow) (catalog.MapDescGetter, error) {
	pg := catalog.MapDescGetter{}
	for _, r := range rows {
		var d descpb.Descriptor
		if err := protoutil.Unmarshal(r.DescBytes, &d); err != nil {
			return nil, errors.Errorf("failed to unmarshal descriptor %d: %v", r.ID, err)
		}
		descpb.MaybeSetDescriptorModificationTimeFromMVCCTimestamp(ctx, &d, r.ModTime)
		pg[descpb.ID(r.ID)] = catalogkv.UnwrapDescriptorRaw(ctx, &d)
	}
	return pg, nil
}

func newNamespaceMap(rows []NamespaceTableRow) namespaceReverseMap {
	res := make(namespaceReverseMap)
	for _, r := range rows {
		l, ok := res[r.ID]
		if !ok {
			res[r.ID] = []descpb.NameInfo{r.NameInfo}
		} else {
			res[r.ID] = append(l, r.NameInfo)
		}
	}
	return res
}

// Examine runs a suite of consistency checks over the descriptor table.
func Examine(
	ctx context.Context,
	descTable DescriptorTable,
	namespaceTable NamespaceTable,
	verbose bool,
	stdout io.Writer,
) (ok bool, err error) {
	fmt.Fprintf(
		stdout, "Examining %d descriptors and %d namespace entries...\n",
		len(descTable), len(namespaceTable))
	descGetter, err := newDescGetter(ctx, descTable)
	if err != nil {
		return false, err
	}
	nMap := newNamespaceMap(namespaceTable)

	var problemsFound bool
	for _, row := range descTable {
		desc, ok := descGetter[descpb.ID(row.ID)]
		if !ok {
			// This should never happen as ids are parsed and inserted from descTable.
			log.Fatalf(ctx, "Descriptor id %d not found", row.ID)
		}

		if int64(desc.GetID()) != row.ID {
			fmt.Fprint(stdout, reportMsg(desc, "different id in descriptor table: %d", row.ID))
			problemsFound = true
			continue
		}

		_, parentExists := descGetter[desc.GetParentID()]
		_, parentSchemaExists := descGetter[desc.GetParentSchemaID()]
		switch d := desc.(type) {
		case catalog.TableDescriptor:
			if err := d.Validate(ctx, descGetter); err != nil {
				problemsFound = true
				fmt.Fprint(stdout, reportMsg(desc, "%s", err))
			}
			// Table has been already validated.
			parentExists = true
			parentSchemaExists = true
		case catalog.TypeDescriptor:
			typ := typedesc.NewImmutable(*d.TypeDesc())
			if err := typ.Validate(ctx, descGetter); err != nil {
				problemsFound = true
				fmt.Fprint(stdout, reportMsg(desc, "%s", err))
			}
		case catalog.SchemaDescriptor:
			// parent schema id is always 0.
			parentSchemaExists = true
		}
		if desc.GetParentID() != descpb.InvalidID && !parentExists {
			problemsFound = true
			fmt.Fprint(stdout, reportMsg(desc, "invalid parent id %d", desc.GetParentID()))
		}
		if desc.GetParentSchemaID() != descpb.InvalidID &&
			desc.GetParentSchemaID() != keys.PublicSchemaID &&
			!parentSchemaExists {
			problemsFound = true
			fmt.Fprint(stdout, reportMsg(desc, "invalid parent schema id %d", desc.GetParentSchemaID()))
		}

		// Process namespace entries pointing to this descriptor.
		names, ok := nMap[row.ID]
		if !ok {
			if !desc.Dropped() {
				fmt.Fprint(stdout, reportMsg(desc, "not being dropped but no namespace entry found"))
				problemsFound = true
			}
			continue
		}
		// We delete all pointed descriptors to leave what is missing in the
		// descriptor table.
		delete(nMap, row.ID)

		var found bool
		for _, n := range names {
			if n.Name == desc.GetName() &&
				n.ParentSchemaID == desc.GetParentSchemaID() &&
				n.ParentID == desc.GetParentID() {
				found = true
				break
			}
		}
		if !found {
			fmt.Fprint(stdout, reportMsg(desc, "could not find name in namespace table"))
			problemsFound = true
			continue
		}
		if verbose {
			fmt.Fprint(stdout, reportMsg(desc, "processed"))
		}
	}

	// Now go over all namespace entries that don't point to descriptors in the
	// descriptor table.
	for id, ni := range nMap {
		if id == keys.PublicSchemaID {
			continue
		}
		if descpb.ID(id) == descpb.InvalidID {
			fmt.Fprintf(stdout, "Row(s) %+v: NULL value found\n", ni)
			problemsFound = true
			continue
		}
		if strings.HasPrefix(ni[0].Name, "pg_temp_") {
			// Temporary schemas have namespace entries but not descriptors.
			continue
		}
		fmt.Fprintf(
			stdout, "Descriptor %d: has namespace row(s) %+v but no descriptor\n", id, ni)
		problemsFound = true
	}
	return !problemsFound, nil
}

func reportMsg(desc catalog.Descriptor, format string, args ...interface{}) string {
	var header string
	switch desc.(type) {
	case catalog.TypeDescriptor:
		header = "    Type"
	case catalog.TableDescriptor:
		header = "   Table"
	case catalog.SchemaDescriptor:
		header = "  Schema"
	case catalog.DatabaseDescriptor:
		header = "Database"
	}
	return fmt.Sprintf("%s %3d: ParentID %3d, ParentSchemaID %2d, Name '%s': ",
		header, desc.GetID(), desc.GetParentID(), desc.GetParentSchemaID(), desc.GetName()) +
		fmt.Sprintf(format, args...) + "\n"
}
