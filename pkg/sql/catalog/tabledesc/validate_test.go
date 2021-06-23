// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tabledesc

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

type validateStatus int

const (
	// statusUnvalidated means that a field hasn't been added to any Validate
	// methods and will fail the test.
	statusUnvalidated validateStatus = iota
	// thisFieldReferencesNoObjects means that a field doesn't have any meaningful
	// references to other objects, and therefore isn't validated.
	thisFieldReferencesNoObjects
	// todoIAmKnowinglyAddingTechDebt means that a field wasn't added to
	// any validation methods, and that you're knowingly adding tech debt! You
	// must add a justification for this in the map.
	todoIAmKnowinglyAddingTechDebt
	// iSolemnlySwearThisFieldIsValidated means that a field was added to a
	//validate method.
	iSolemnlySwearThisFieldIsValidated
)

// validationMap is a structure that contains a "validation status" for every
// field in all listed descriptors.
//
// The purpose of this map is to force people who are adding new descriptor
// fields to remember to add those fields to the appropriate Validate methods.
// If you're here because you failed the test at the bottom of this file, you
// should add an entry for your new field(s) to the map below. Please think
// carefully when adding your entry, and be truthful - nothing checks these
// validation statuses but you and your code reviewers.
//
// Adding information to Validate is extremely important. Writing corrupted
// descriptors to disk is a real risk and has real consequences. Please do your
// part to ensure your features don't have this risk.
//
// The validation statuses are descriptively named so that reviewers will take
// note of what you add to the map :)
var validationMap = []struct {
	obj      interface{}
	fieldMap map[string]validationStatusInfo
}{
	{
		obj: descpb.TableDescriptor{},
		fieldMap: map[string]validationStatusInfo{
			"Name":             {status: thisFieldReferencesNoObjects},
			"ID":               {status: thisFieldReferencesNoObjects},
			"Version":          {status: thisFieldReferencesNoObjects},
			"ModificationTime": {status: thisFieldReferencesNoObjects},
			"DrainingNames":    {status: thisFieldReferencesNoObjects},
			"ParentID":         {status: iSolemnlySwearThisFieldIsValidated},
			"UnexposedParentSchemaID": {
				status: todoIAmKnowinglyAddingTechDebt,
				reason: "initial import: TODO(features): add validation"},
			"Columns":      {status: iSolemnlySwearThisFieldIsValidated},
			"NextColumnID": {status: iSolemnlySwearThisFieldIsValidated},
			"Families":     {status: iSolemnlySwearThisFieldIsValidated},
			"NextFamilyID": {status: thisFieldReferencesNoObjects},
			"PrimaryIndex": {status: iSolemnlySwearThisFieldIsValidated},
			"Indexes":      {status: iSolemnlySwearThisFieldIsValidated},
			"NextIndexID":  {status: iSolemnlySwearThisFieldIsValidated},
			"Privileges":   {status: iSolemnlySwearThisFieldIsValidated},
			"Mutations":    {status: iSolemnlySwearThisFieldIsValidated},
			"Lease":        {status: thisFieldReferencesNoObjects},
			"NextMutationID": {
				status: todoIAmKnowinglyAddingTechDebt,
				reason: "initial import: TODO(schema): add validation"},
			"FormatVersion": {status: thisFieldReferencesNoObjects},
			"State":         {status: thisFieldReferencesNoObjects},
			"OfflineReason": {status: thisFieldReferencesNoObjects},
			"Checks":        {status: iSolemnlySwearThisFieldIsValidated},
			"ViewQuery": {
				status: todoIAmKnowinglyAddingTechDebt,
				reason: "initial import: TODO(features): add validation"},
			"IsMaterializedView": {status: thisFieldReferencesNoObjects},
			"DependsOn": {
				status: todoIAmKnowinglyAddingTechDebt,
				reason: "initial import: TODO(features): add validation"},
			"DependsOnTypes": {status: iSolemnlySwearThisFieldIsValidated},
			"DependedOnBy": {
				status: todoIAmKnowinglyAddingTechDebt,
				reason: "initial import: TODO(features): add validation"},
			"MutationJobs": {status: thisFieldReferencesNoObjects},
			"SequenceOpts": {status: todoIAmKnowinglyAddingTechDebt,
				reason: "initial import: TODO(features): add validation"},
			"DropTime": {status: thisFieldReferencesNoObjects},
			"ReplacementOf": {status: todoIAmKnowinglyAddingTechDebt,
				reason: "initial import: TODO(bulkio): add validation"},
			"AuditMode": {status: thisFieldReferencesNoObjects},
			"DropJobID": {status: thisFieldReferencesNoObjects},
			"GCMutations": {status: todoIAmKnowinglyAddingTechDebt,
				reason: "initial import: TODO(schema): add validation"},
			"CreateQuery":                   {status: thisFieldReferencesNoObjects},
			"CreateAsOfTime":                {status: thisFieldReferencesNoObjects},
			"OutboundFKs":                   {status: iSolemnlySwearThisFieldIsValidated},
			"InboundFKs":                    {status: iSolemnlySwearThisFieldIsValidated},
			"UniqueWithoutIndexConstraints": {status: iSolemnlySwearThisFieldIsValidated},
			"Temporary":                     {status: thisFieldReferencesNoObjects},
			"LocalityConfig":                {status: iSolemnlySwearThisFieldIsValidated},
			"PartitionAllBy":                {status: iSolemnlySwearThisFieldIsValidated},
			"NewSchemaChangeJobID":          {status: iSolemnlySwearThisFieldIsValidated},
		},
	},
	{
		obj: descpb.IndexDescriptor{},
		fieldMap: map[string]validationStatusInfo{
			"Name":                {status: thisFieldReferencesNoObjects},
			"ID":                  {status: thisFieldReferencesNoObjects},
			"Unique":              {status: thisFieldReferencesNoObjects},
			"Version":             {status: thisFieldReferencesNoObjects},
			"KeyColumnNames":      {status: iSolemnlySwearThisFieldIsValidated},
			"KeyColumnDirections": {status: iSolemnlySwearThisFieldIsValidated},
			"StoreColumnNames": {
				status: todoIAmKnowinglyAddingTechDebt,
				reason: "initial import: TODO(features): add validation"},
			"KeyColumnIDs": {status: iSolemnlySwearThisFieldIsValidated},
			"KeySuffixColumnIDs": {
				status: todoIAmKnowinglyAddingTechDebt,
				reason: "initial import: TODO(features): add validation"},
			"StoreColumnIDs": {
				status: todoIAmKnowinglyAddingTechDebt,
				reason: "initial import: TODO(features): add validation"},
			"CompositeColumnIDs": {
				status: todoIAmKnowinglyAddingTechDebt,
				reason: "initial import: TODO(features): add validation"},
			// These next 2 are deprecated and not used anymore.
			"ForeignKey":   {status: thisFieldReferencesNoObjects},
			"ReferencedBy": {status: thisFieldReferencesNoObjects},

			"Interleave":        {status: iSolemnlySwearThisFieldIsValidated},
			"InterleavedBy":     {status: iSolemnlySwearThisFieldIsValidated},
			"Partitioning":      {status: iSolemnlySwearThisFieldIsValidated},
			"Type":              {status: thisFieldReferencesNoObjects},
			"CreatedExplicitly": {status: thisFieldReferencesNoObjects},
			"EncodingType":      {status: thisFieldReferencesNoObjects},
			"Sharded":           {status: iSolemnlySwearThisFieldIsValidated},
			"Disabled":          {status: thisFieldReferencesNoObjects},
			"GeoConfig":         {status: thisFieldReferencesNoObjects},
			"Predicate":         {status: iSolemnlySwearThisFieldIsValidated},
		},
	},
	{
		obj: descpb.ColumnDescriptor{},
		fieldMap: map[string]validationStatusInfo{
			"Name":     {status: thisFieldReferencesNoObjects},
			"ID":       {status: thisFieldReferencesNoObjects},
			"Type":     {status: iSolemnlySwearThisFieldIsValidated},
			"Nullable": {status: thisFieldReferencesNoObjects},
			"DefaultExpr": {
				status: todoIAmKnowinglyAddingTechDebt,
				reason: "initial import: TODO(features): add validation"},
			"Hidden":       {status: iSolemnlySwearThisFieldIsValidated},
			"Inaccessible": {status: iSolemnlySwearThisFieldIsValidated},
			"UsesSequenceIds": {
				status: todoIAmKnowinglyAddingTechDebt,
				reason: "initial import: TODO(features): add validation"},
			"OwnsSequenceIds": {
				status: todoIAmKnowinglyAddingTechDebt,
				reason: "initial import: TODO(features): add validation"},
			"ComputeExpr": {status: iSolemnlySwearThisFieldIsValidated},
			"Virtual":     {status: iSolemnlySwearThisFieldIsValidated},
			"PGAttributeNum": {
				status: todoIAmKnowinglyAddingTechDebt,
				reason: "initial import: TODO(features): add validation"},
			"AlterColumnTypeInProgress": {status: thisFieldReferencesNoObjects},
			"SystemColumnKind":          {status: thisFieldReferencesNoObjects},
		},
	},
	{
		obj: descpb.ForeignKeyConstraint{},
		fieldMap: map[string]validationStatusInfo{
			"OriginTableID": {status: iSolemnlySwearThisFieldIsValidated},
			"OriginColumnIDs": {
				status: todoIAmKnowinglyAddingTechDebt,
				reason: "initial import: TODO(schema): add validation"},
			"ReferencedColumnIDs": {
				status: todoIAmKnowinglyAddingTechDebt,
				reason: "initial import: TODO(schema): add validation"},
			"ReferencedTableID": {status: iSolemnlySwearThisFieldIsValidated},
			"Name":              {status: thisFieldReferencesNoObjects},
			"Validity":          {status: thisFieldReferencesNoObjects},
			"OnDelete":          {status: thisFieldReferencesNoObjects},
			"OnUpdate":          {status: thisFieldReferencesNoObjects},
			"Match":             {status: thisFieldReferencesNoObjects},
		},
	},
	{
		obj: descpb.UniqueWithoutIndexConstraint{},
		fieldMap: map[string]validationStatusInfo{
			"TableID":   {status: iSolemnlySwearThisFieldIsValidated},
			"ColumnIDs": {status: iSolemnlySwearThisFieldIsValidated},
			"Name":      {status: thisFieldReferencesNoObjects},
			"Validity":  {status: thisFieldReferencesNoObjects},
			"Predicate": {status: iSolemnlySwearThisFieldIsValidated},
		},
	},
	{
		obj: descpb.TypeDescriptor{},
		fieldMap: map[string]validationStatusInfo{
			"Name":                     {status: iSolemnlySwearThisFieldIsValidated},
			"ID":                       {status: iSolemnlySwearThisFieldIsValidated},
			"Version":                  {status: thisFieldReferencesNoObjects},
			"ModificationTime":         {status: thisFieldReferencesNoObjects},
			"DrainingNames":            {status: thisFieldReferencesNoObjects},
			"ParentID":                 {status: iSolemnlySwearThisFieldIsValidated},
			"ParentSchemaID":           {status: iSolemnlySwearThisFieldIsValidated},
			"Kind":                     {status: thisFieldReferencesNoObjects},
			"ArrayTypeID":              {status: iSolemnlySwearThisFieldIsValidated},
			"EnumMembers":              {status: iSolemnlySwearThisFieldIsValidated},
			"Alias":                    {status: iSolemnlySwearThisFieldIsValidated},
			"State":                    {status: thisFieldReferencesNoObjects},
			"ReferencingDescriptorIDs": {status: iSolemnlySwearThisFieldIsValidated},
			"Privileges":               {status: iSolemnlySwearThisFieldIsValidated},
			"OfflineReason":            {status: thisFieldReferencesNoObjects},
			"RegionConfig":             {status: iSolemnlySwearThisFieldIsValidated},
		},
	},
	{
		obj: descpb.DatabaseDescriptor{},
		fieldMap: map[string]validationStatusInfo{
			"Name":             {status: iSolemnlySwearThisFieldIsValidated},
			"ID":               {status: iSolemnlySwearThisFieldIsValidated},
			"Version":          {status: thisFieldReferencesNoObjects},
			"ModificationTime": {status: thisFieldReferencesNoObjects},
			"DrainingNames":    {status: thisFieldReferencesNoObjects},
			"Privileges":       {status: iSolemnlySwearThisFieldIsValidated},
			"Schemas":          {status: iSolemnlySwearThisFieldIsValidated},
			"State":            {status: thisFieldReferencesNoObjects},
			"OfflineReason":    {status: thisFieldReferencesNoObjects},
			"RegionConfig":     {status: iSolemnlySwearThisFieldIsValidated},
		},
	},
	{
		obj: descpb.SchemaDescriptor{},
		fieldMap: map[string]validationStatusInfo{
			"Name":             {status: iSolemnlySwearThisFieldIsValidated},
			"ID":               {status: iSolemnlySwearThisFieldIsValidated},
			"State":            {status: thisFieldReferencesNoObjects},
			"OfflineReason":    {status: thisFieldReferencesNoObjects},
			"ModificationTime": {status: thisFieldReferencesNoObjects},
			"Version":          {status: thisFieldReferencesNoObjects},
			"DrainingNames":    {status: thisFieldReferencesNoObjects},
			"ParentID":         {status: iSolemnlySwearThisFieldIsValidated},
			"Privileges":       {status: iSolemnlySwearThisFieldIsValidated},
		},
	},
}

type validationStatusInfo struct {
	status validateStatus
	reason string
}

// Hello! If you're seeing this test fail, you probably just added a new
// protobuf field to a descriptor. Please read the documentation at the top of
// this file.
func TestValidateCoversAllDescriptorFields(t *testing.T) {
	for _, structInfo := range validationMap {
		o := structInfo.obj
		for field, info := range structInfo.fieldMap {
			switch info.status {
			case statusUnvalidated:
				t.Errorf("field %T.%s marked as unvalidated", o, field)
			case todoIAmKnowinglyAddingTechDebt:
				if info.reason == "" {
					t.Errorf("field %T.%s marked as TODO with no reason", o, field)
				}
			}
		}

		typ := reflect.ValueOf(o).Type()
		for i := 0; i < typ.NumField(); i++ {
			fieldName := typ.Field(i).Name
			info := structInfo.fieldMap[fieldName]
			switch info.status {
			case statusUnvalidated:
				t.Errorf("field %T.%s not marked as validated", o, fieldName)
			case todoIAmKnowinglyAddingTechDebt:
				t.Logf("TODO: field %T.%s isn't validated: %s", o, fieldName, info.reason)
			}
		}
	}
}

func TestValidateTableDesc(t *testing.T) {
	defer leaktest.AfterTest(t)()

	computedExpr := "1 + 1"

	testData := []struct {
		err  string
		desc descpb.TableDescriptor
	}{
		{`empty table name`,
			descpb.TableDescriptor{}},
		{`invalid table ID 0`,
			descpb.TableDescriptor{ID: 0, Name: "foo"}},
		{`invalid parent ID 0`,
			descpb.TableDescriptor{ID: 2, Name: "foo"}},
		{`table must contain at least 1 column`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
			}},
		{`empty column name`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 0},
				},
				NextColumnID: 2,
			}},
		{`table is encoded using using version 0, but this client only supports version 3`,
			descpb.TableDescriptor{
				ID:       2,
				ParentID: 1,
				Name:     "foo",
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				NextColumnID: 2,
			}},
		{`virtual column "virt" is not computed`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
					{ID: 2, Name: "virt", Virtual: true},
				},
				NextColumnID: 3,
			}},
		{`invalid column ID 0`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 0, Name: "bar"},
				},
				NextColumnID: 2,
			}},
		{`table must contain a primary key`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
			}},
		{`duplicate column name: "bar"`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
					{ID: 1, Name: "bar"},
				},
				NextColumnID: 2,
			}},
		{`duplicate column name: "bar"`,
			descpb.TableDescriptor{
				ID:            catconstants.CrdbInternalBackwardDependenciesTableID,
				ParentID:      0,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
					{ID: 1, Name: "bar"},
				},
				NextColumnID: 2,
			}},
		{`column "blah" duplicate ID of column "bar": 1`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
					{ID: 1, Name: "blah"},
				},
				NextColumnID: 2,
			}},
		{`at least 1 column family must be specified`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				NextColumnID: 2,
			}},
		{`column "bar" cannot be hidden and inaccessible`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar", Hidden: true, Inaccessible: true},
				},
				NextColumnID: 2,
			}},
		{`the 0th family must have ID 0`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 1},
				},
				NextColumnID: 2,
			}},
		{`duplicate family name: "baz"`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "baz"},
					{ID: 1, Name: "baz"},
				},
				NextColumnID: 2,
				NextFamilyID: 2,
			}},
		{`family "qux" duplicate ID of family "baz": 0`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "baz"},
					{ID: 0, Name: "qux"},
				},
				NextColumnID: 2,
				NextFamilyID: 2,
			}},
		{`duplicate family name: "baz"`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "baz"},
					{ID: 3, Name: "baz"},
				},
				NextColumnID: 2,
				NextFamilyID: 2,
			}},
		{`mismatched column ID size (1) and name size (0)`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "baz", ColumnIDs: []descpb.ColumnID{1}},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
			}},
		{`family "baz" contains unknown column "2"`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "baz", ColumnIDs: []descpb.ColumnID{2}, ColumnNames: []string{"bar"}},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
			}},
		{`family "baz" column 1 should have name "bar", but found name "qux"`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "baz", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"qux"}},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
			}},
		{`column "bar" is not in any column family`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "baz"},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
			}},
		{`column 1 is in both family 0 and 1`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "baz", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar"}},
					{ID: 1, Name: "qux", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				NextColumnID: 2,
				NextFamilyID: 2,
			}},
		{`virtual computed column "virt" cannot be part of a family`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
					{ID: 2, Name: "virt", ComputeExpr: &computedExpr, Virtual: true},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "fam1", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar"}},
					{ID: 1, Name: "fam2", ColumnIDs: []descpb.ColumnID{2}, ColumnNames: []string{"virt"}},
				},
				NextColumnID: 3,
				NextFamilyID: 2,
			}},
		{`table must contain a primary key`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: descpb.IndexDescriptor{
					ID:                  0,
					KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC}},
				NextColumnID: 2,
				NextFamilyID: 1,
			}},
		{`invalid index ID 0`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: descpb.IndexDescriptor{ID: 0, Name: "bar",
					KeyColumnIDs:        []descpb.ColumnID{0},
					KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC}},
				NextColumnID: 2,
				NextFamilyID: 1,
			}},
		{`index "bar" must contain at least 1 column`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: descpb.IndexDescriptor{
					ID: 1, Name: "primary", KeyColumnIDs: []descpb.ColumnID{1}, KeyColumnNames: []string{"bar"},
					KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
					EncodingType:        descpb.PrimaryIndexEncoding,
					Version:             descpb.PrimaryIndexWithStoredColumnsVersion,
				},
				Indexes: []descpb.IndexDescriptor{
					{ID: 2, Name: "bar"},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
				NextIndexID:  3,
			}},
		{`mismatched column IDs (1) and names (0)`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: descpb.IndexDescriptor{ID: 1, Name: "bar", KeyColumnIDs: []descpb.ColumnID{1}},
				NextColumnID: 2,
				NextFamilyID: 1,
				NextIndexID:  2,
			}},
		{`mismatched column IDs (1) and names (2)`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
					{ID: 2, Name: "blah"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []descpb.ColumnID{1, 2}, ColumnNames: []string{"bar", "blah"}},
				},
				PrimaryIndex: descpb.IndexDescriptor{ID: 1, Name: "bar",
					KeyColumnIDs: []descpb.ColumnID{1}, KeyColumnNames: []string{"bar", "blah"},
					KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
				},
				NextColumnID: 3,
				NextFamilyID: 1,
				NextIndexID:  2,
			}},
		{`duplicate index name: "bar"`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: descpb.IndexDescriptor{ID: 1, Name: "bar",
					KeyColumnIDs: []descpb.ColumnID{1}, KeyColumnNames: []string{"bar"},
					KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
					EncodingType:        descpb.PrimaryIndexEncoding,
					Version:             descpb.PrimaryIndexWithStoredColumnsVersion,
				},
				Indexes: []descpb.IndexDescriptor{
					{ID: 2, Name: "bar", KeyColumnIDs: []descpb.ColumnID{1},
						KeyColumnNames:      []string{"bar"},
						KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
					},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
				NextIndexID:  3,
			}},
		{`index "blah" duplicate ID of index "bar": 1`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: descpb.IndexDescriptor{ID: 1, Name: "bar", KeyColumnIDs: []descpb.ColumnID{1},
					KeyColumnNames:      []string{"bar"},
					KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
					EncodingType:        descpb.PrimaryIndexEncoding,
					Version:             descpb.PrimaryIndexWithStoredColumnsVersion,
				},
				Indexes: []descpb.IndexDescriptor{
					{ID: 1, Name: "blah", KeyColumnIDs: []descpb.ColumnID{1},
						KeyColumnNames:      []string{"bar"},
						KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
					},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
				NextIndexID:  2,
			}},
		{`index "bar" column "bar" should have ID 1, but found ID 2`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: descpb.IndexDescriptor{ID: 1, Name: "bar", KeyColumnIDs: []descpb.ColumnID{2},
					KeyColumnNames:      []string{"bar"},
					KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
				NextIndexID:  2,
			}},
		{`index "bar" contains unknown column "blah"`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: descpb.IndexDescriptor{ID: 1, Name: "bar", KeyColumnIDs: []descpb.ColumnID{1},
					KeyColumnNames:      []string{"blah"},
					KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
				NextIndexID:  2,
			}},
		{`mismatched column IDs (1) and directions (0)`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: descpb.IndexDescriptor{ID: 1, Name: "bar", KeyColumnIDs: []descpb.ColumnID{1},
					KeyColumnNames: []string{"blah"},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
				NextIndexID:  2,
			}},
		{`mismatched STORING column IDs (1) and names (0)`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "c1"},
					{ID: 2, Name: "c2"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{
						ID:          0,
						Name:        "fam",
						ColumnIDs:   []descpb.ColumnID{1, 2},
						ColumnNames: []string{"c1", "c2"},
					},
				},
				PrimaryIndex: descpb.IndexDescriptor{
					ID: 1, Name: "primary",
					KeyColumnIDs:        []descpb.ColumnID{1},
					KeyColumnNames:      []string{"c1"},
					KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
					StoreColumnIDs:      []descpb.ColumnID{2},
				},
				NextColumnID: 3,
				NextFamilyID: 1,
				NextIndexID:  2,
			}},
		{`at least one of LIST or RANGE partitioning must be used`,
			// Verify that validatePartitioning is hooked up. The rest of these
			// tests are in TestValidatePartitionion.
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: descpb.IndexDescriptor{
					ID: 1, Name: "primary", KeyColumnIDs: []descpb.ColumnID{1}, KeyColumnNames: []string{"bar"},
					KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
					Partitioning: descpb.PartitioningDescriptor{
						NumColumns: 1,
					},
					EncodingType: descpb.PrimaryIndexEncoding,
					Version:      descpb.PrimaryIndexWithStoredColumnsVersion,
				},
				NextColumnID: 2,
				NextFamilyID: 1,
				NextIndexID:  3,
			}},
		{`index "foo_crdb_internal_bar_shard_5_bar_idx" refers to non-existent shard column "does not exist"`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
					{ID: 2, Name: "crdb_internal_bar_shard_5"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary",
						ColumnIDs:   []descpb.ColumnID{1, 2},
						ColumnNames: []string{"bar", "crdb_internal_bar_shard_5"},
					},
				},
				PrimaryIndex: descpb.IndexDescriptor{
					ID: 1, Name: "primary",
					Unique:              true,
					KeyColumnIDs:        []descpb.ColumnID{1},
					KeyColumnNames:      []string{"bar"},
					KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
					StoreColumnNames:    []string{"crdb_internal_bar_shard_5"},
					StoreColumnIDs:      []descpb.ColumnID{2},
					EncodingType:        descpb.PrimaryIndexEncoding,
					Version:             descpb.PrimaryIndexWithStoredColumnsVersion,
				},
				Indexes: []descpb.IndexDescriptor{
					{ID: 2, Name: "foo_crdb_internal_bar_shard_5_bar_idx",
						KeyColumnIDs:        []descpb.ColumnID{2, 1},
						KeyColumnNames:      []string{"crdb_internal_bar_shard_5", "bar"},
						KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC, descpb.IndexDescriptor_ASC},
						Sharded: descpb.ShardedDescriptor{
							IsSharded:    true,
							Name:         "does not exist",
							ShardBuckets: 5,
						},
					},
				},
				NextColumnID: 3,
				NextFamilyID: 1,
				NextIndexID:  3,
			}},
		{`TableID mismatch for unique without index constraint "bar_unique": "1" doesn't match descriptor: "2"`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary",
						ColumnIDs:   []descpb.ColumnID{1},
						ColumnNames: []string{"bar"},
					},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
				UniqueWithoutIndexConstraints: []descpb.UniqueWithoutIndexConstraint{
					{
						TableID:   1,
						ColumnIDs: []descpb.ColumnID{1},
						Name:      "bar_unique",
					},
				},
			}},
		{`column-id "2" does not exist`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary",
						ColumnIDs:   []descpb.ColumnID{1},
						ColumnNames: []string{"bar"},
					},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
				UniqueWithoutIndexConstraints: []descpb.UniqueWithoutIndexConstraint{
					{
						TableID:   2,
						ColumnIDs: []descpb.ColumnID{1, 2},
						Name:      "bar_unique",
					},
				},
			}},
		{`unique without index constraint "bar_unique" contains duplicate column "1"`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary",
						ColumnIDs:   []descpb.ColumnID{1},
						ColumnNames: []string{"bar"},
					},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
				UniqueWithoutIndexConstraints: []descpb.UniqueWithoutIndexConstraint{
					{
						TableID:   2,
						ColumnIDs: []descpb.ColumnID{1, 1},
						Name:      "bar_unique",
					},
				},
			}},
		{`empty unique without index constraint name`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary",
						ColumnIDs:   []descpb.ColumnID{1},
						ColumnNames: []string{"bar"},
					},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
				UniqueWithoutIndexConstraints: []descpb.UniqueWithoutIndexConstraint{
					{
						TableID:   2,
						ColumnIDs: []descpb.ColumnID{1},
					},
				},
			}},
		{`primary index column "v" cannot be virtual`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
					{ID: 2, Name: "v", ComputeExpr: &computedExpr, Virtual: true},
				},
				PrimaryIndex: descpb.IndexDescriptor{
					ID:             1,
					Name:           "primary",
					Unique:         true,
					KeyColumnIDs:   []descpb.ColumnID{1, 2},
					KeyColumnNames: []string{"bar", "v"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary",
						ColumnIDs:   []descpb.ColumnID{1},
						ColumnNames: []string{"bar"},
					},
				},
				NextColumnID: 3,
				NextFamilyID: 1,
			}},
		{`index "sec" cannot store virtual column "v"`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "c1"},
					{ID: 2, Name: "c2"},
					{ID: 3, Name: "v", ComputeExpr: &computedExpr, Virtual: true},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []descpb.ColumnID{1, 2}, ColumnNames: []string{"c1", "c2"}},
				},
				PrimaryIndex: descpb.IndexDescriptor{
					ID: 1, Name: "pri", KeyColumnIDs: []descpb.ColumnID{1},
					KeyColumnNames:      []string{"c1"},
					KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
					EncodingType:        descpb.PrimaryIndexEncoding,
					Version:             descpb.PrimaryIndexWithStoredColumnsVersion,
				},
				Indexes: []descpb.IndexDescriptor{
					{ID: 2, Name: "sec", KeyColumnIDs: []descpb.ColumnID{2},
						KeyColumnNames:      []string{"c2"},
						KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
						StoreColumnNames:    []string{"v"},
						StoreColumnIDs:      []descpb.ColumnID{3},
					},
				},
				NextColumnID: 4,
				NextFamilyID: 1,
				NextIndexID:  3,
			}},
		{`index "sec" has column ID 2 present in: [KeyColumnIDs StoreColumnIDs]`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "c1"},
					{ID: 2, Name: "c2"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []descpb.ColumnID{1, 2}, ColumnNames: []string{"c1", "c2"}},
				},
				PrimaryIndex: descpb.IndexDescriptor{
					ID: 1, Name: "pri", KeyColumnIDs: []descpb.ColumnID{1},
					KeyColumnNames:      []string{"c1"},
					KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
					EncodingType:        descpb.PrimaryIndexEncoding,
					Version:             descpb.PrimaryIndexWithStoredColumnsVersion,
				},
				Indexes: []descpb.IndexDescriptor{
					{ID: 2, Name: "sec", KeyColumnIDs: []descpb.ColumnID{2},
						KeyColumnNames:      []string{"c2"},
						KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
						StoreColumnNames:    []string{"c2"},
						StoreColumnIDs:      []descpb.ColumnID{2},
						Version:             descpb.StrictIndexColumnIDGuaranteesVersion,
					},
				},
				NextColumnID: 3,
				NextFamilyID: 1,
				NextIndexID:  3,
			}},
	}
	for i, d := range testData {
		t.Run(d.err, func(t *testing.T) {
			desc := NewBuilder(&d.desc).BuildImmutableTable()
			expectedErr := fmt.Sprintf("%s %q (%d): %s", desc.DescriptorType(), desc.GetName(), desc.GetID(), d.err)
			if err := catalog.ValidateSelf(desc); err == nil {
				t.Errorf("%d: expected \"%s\", but found success: %+v", i, expectedErr, d.desc)
			} else if expectedErr != err.Error() {
				t.Errorf("%d: expected \"%s\", but found \"%+v\"", i, expectedErr, err)
			}
		})
	}
}

func TestValidateCrossTableReferences(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	pointer := func(s string) *string {
		return &s
	}

	tests := []struct {
		err        string
		desc       descpb.TableDescriptor
		otherDescs []descpb.TableDescriptor
	}{
		// Foreign keys
		{ // 0
			err: `invalid foreign key: missing table=52: referenced table ID 52: descriptor not found`,
			desc: descpb.TableDescriptor{
				Name:                    "foo",
				ID:                      51,
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				FormatVersion:           descpb.InterleavedFormatVersion,
				OutboundFKs: []descpb.ForeignKeyConstraint{
					{
						Name:                "fk",
						ReferencedTableID:   52,
						ReferencedColumnIDs: []descpb.ColumnID{1},
						OriginTableID:       51,
						OriginColumnIDs:     []descpb.ColumnID{1},
					},
				},
			},
			otherDescs: nil,
		},
		{ // 1
			err: `missing fk back reference "fk" to "foo" from "baz"`,
			desc: descpb.TableDescriptor{
				ID:                      51,
				Name:                    "foo",
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				FormatVersion:           descpb.InterleavedFormatVersion,
				OutboundFKs: []descpb.ForeignKeyConstraint{
					{
						Name:                "fk",
						ReferencedTableID:   52,
						ReferencedColumnIDs: []descpb.ColumnID{1},
						OriginTableID:       51,
						OriginColumnIDs:     []descpb.ColumnID{1},
					},
				},
			},
			otherDescs: []descpb.TableDescriptor{{
				ID:                      52,
				Name:                    "baz",
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				FormatVersion:           descpb.InterleavedFormatVersion,
			}},
		},
		{ // 2
			err: `invalid foreign key backreference: missing table=52: referenced table ID 52: descriptor not found`,
			desc: descpb.TableDescriptor{
				Name:                    "foo",
				ID:                      51,
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				FormatVersion:           descpb.InterleavedFormatVersion,
				InboundFKs: []descpb.ForeignKeyConstraint{
					{
						Name:                "fk",
						ReferencedTableID:   51,
						ReferencedColumnIDs: []descpb.ColumnID{1},
						OriginTableID:       52,
						OriginColumnIDs:     []descpb.ColumnID{1},
					},
				},
			},
		},
		{ // 3
			err: `missing fk forward reference "fk" to "foo" from "baz"`,
			desc: descpb.TableDescriptor{
				ID:                      51,
				Name:                    "foo",
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				FormatVersion:           descpb.InterleavedFormatVersion,
				PrimaryIndex: descpb.IndexDescriptor{
					ID:   1,
					Name: "bar",
				},
				InboundFKs: []descpb.ForeignKeyConstraint{
					{
						Name:                "fk",
						ReferencedTableID:   51,
						ReferencedColumnIDs: []descpb.ColumnID{1},
						OriginTableID:       52,
						OriginColumnIDs:     []descpb.ColumnID{1},
					},
				},
			},
			otherDescs: []descpb.TableDescriptor{{
				ID:                      52,
				Name:                    "baz",
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				FormatVersion:           descpb.InterleavedFormatVersion,
			}},
		},
		{ // 4
			// Regression test for #57066: We can handle one of the referenced tables
			// having a pre-19.2 foreign key reference.
			err: "",
			desc: descpb.TableDescriptor{
				ID:                      51,
				Name:                    "foo",
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				FormatVersion:           descpb.InterleavedFormatVersion,
				Indexes: []descpb.IndexDescriptor{
					{
						ID:           2,
						KeyColumnIDs: []descpb.ColumnID{1, 2},
					},
				},
				OutboundFKs: []descpb.ForeignKeyConstraint{
					{
						Name:                "fk",
						ReferencedTableID:   52,
						ReferencedColumnIDs: []descpb.ColumnID{1},
						OriginTableID:       51,
						OriginColumnIDs:     []descpb.ColumnID{1},
					},
				},
			},
			otherDescs: []descpb.TableDescriptor{{
				ID:                      52,
				Name:                    "baz",
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				FormatVersion:           descpb.InterleavedFormatVersion,
				Indexes: []descpb.IndexDescriptor{
					{
						Unique:       true,
						KeyColumnIDs: []descpb.ColumnID{1},
						ReferencedBy: []descpb.ForeignKeyReference{{Table: 51, Index: 2}},
					},
				},
			}},
		},
		{ // 5
			// Regression test for #57066: We can handle one of the referenced tables
			// having a pre-19.2 foreign key reference.
			err: "",
			desc: descpb.TableDescriptor{
				ID:                      51,
				Name:                    "foo",
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				FormatVersion:           descpb.InterleavedFormatVersion,
				Indexes: []descpb.IndexDescriptor{
					{
						ID:           2,
						KeyColumnIDs: []descpb.ColumnID{7},
						Unique:       true,
					},
				},
				InboundFKs: []descpb.ForeignKeyConstraint{
					{
						Name:                "fk",
						ReferencedTableID:   51,
						ReferencedColumnIDs: []descpb.ColumnID{7},
						OriginTableID:       52,
						OriginColumnIDs:     []descpb.ColumnID{1},
					},
				},
			},
			otherDescs: []descpb.TableDescriptor{{
				ID:                      52,
				Name:                    "baz",
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				FormatVersion:           descpb.InterleavedFormatVersion,
				Indexes: []descpb.IndexDescriptor{
					{
						ID:           2,
						Unique:       true,
						KeyColumnIDs: []descpb.ColumnID{1},
						ForeignKey:   descpb.ForeignKeyReference{Table: 51, Index: 2},
					},
				},
			}},
		},

		// Interleaves
		{ // 6
			err: `invalid interleave: missing table=52 index=2: referenced table ID 52: descriptor not found`,
			desc: descpb.TableDescriptor{
				Name:                    "foo",
				ID:                      51,
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				FormatVersion:           descpb.InterleavedFormatVersion,
				PrimaryIndex: descpb.IndexDescriptor{
					ID: 1,
					Interleave: descpb.InterleaveDescriptor{Ancestors: []descpb.InterleaveDescriptor_Ancestor{
						{TableID: 52, IndexID: 2},
					}},
				},
			},
			otherDescs: nil,
		},
		{ // 7
			err: `invalid interleave: missing table=baz index=2: index-id "2" does not exist`,
			desc: descpb.TableDescriptor{
				Name:                    "foo",
				ID:                      51,
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				FormatVersion:           descpb.InterleavedFormatVersion,
				PrimaryIndex: descpb.IndexDescriptor{
					ID: 1,
					Interleave: descpb.InterleaveDescriptor{Ancestors: []descpb.InterleaveDescriptor_Ancestor{
						{TableID: 52, IndexID: 2},
					}},
				},
			},
			otherDescs: []descpb.TableDescriptor{{
				ID:                      52,
				Name:                    "baz",
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
			}},
		},
		{ // 8
			err: `missing interleave back reference to "foo"@"bar" from "baz"@"qux"`,
			desc: descpb.TableDescriptor{
				Name:                    "foo",
				ID:                      51,
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				PrimaryIndex: descpb.IndexDescriptor{
					ID:   1,
					Name: "bar",
					Interleave: descpb.InterleaveDescriptor{Ancestors: []descpb.InterleaveDescriptor_Ancestor{
						{TableID: 52, IndexID: 2},
					}},
				},
			},
			otherDescs: []descpb.TableDescriptor{{
				ID:                      52,
				Name:                    "baz",
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				PrimaryIndex: descpb.IndexDescriptor{
					ID:   2,
					Name: "qux",
				},
			}},
		},
		{ // 9
			err: `invalid interleave backreference table=52 index=2: referenced table ID 52: descriptor not found`,
			desc: descpb.TableDescriptor{
				Name:                    "foo",
				ID:                      51,
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				PrimaryIndex: descpb.IndexDescriptor{
					ID:            1,
					InterleavedBy: []descpb.ForeignKeyReference{{Table: 52, Index: 2}},
				},
			},
		},
		{ // 10
			err: `invalid interleave backreference table=baz index=2: index-id "2" does not exist`,
			desc: descpb.TableDescriptor{
				Name:                    "foo",
				ID:                      51,
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				PrimaryIndex: descpb.IndexDescriptor{
					ID:            1,
					InterleavedBy: []descpb.ForeignKeyReference{{Table: 52, Index: 2}},
				},
			},
			otherDescs: []descpb.TableDescriptor{{
				ID:                      52,
				Name:                    "baz",
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
			}},
		},
		{ // 11
			err: `broken interleave backward reference from "foo"@"bar" to "baz"@"qux"`,
			desc: descpb.TableDescriptor{
				Name:                    "foo",
				ID:                      51,
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				PrimaryIndex: descpb.IndexDescriptor{
					ID:            1,
					Name:          "bar",
					InterleavedBy: []descpb.ForeignKeyReference{{Table: 52, Index: 2}},
				},
			},
			otherDescs: []descpb.TableDescriptor{{
				Name:                    "baz",
				ID:                      52,
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				PrimaryIndex: descpb.IndexDescriptor{
					ID:   2,
					Name: "qux",
				},
			}},
		},
		{ // 12
			err: `referenced type ID 500: descriptor not found`,
			desc: descpb.TableDescriptor{
				Name:                    "foo",
				ID:                      51,
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				PrimaryIndex: descpb.IndexDescriptor{
					ID:             1,
					Name:           "bar",
					KeyColumnIDs:   []descpb.ColumnID{1},
					KeyColumnNames: []string{"a"},
				},
				Columns: []descpb.ColumnDescriptor{
					{
						Name: "a",
						ID:   1,
						Type: types.MakeEnum(typedesc.TypeIDToOID(500), typedesc.TypeIDToOID(100500)),
					},
				},
			},
		},
		// Add some expressions with invalid type references.
		{ // 13
			err: `referenced type ID 500: descriptor not found`,
			desc: descpb.TableDescriptor{
				Name:                    "foo",
				ID:                      51,
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				PrimaryIndex: descpb.IndexDescriptor{
					ID:             1,
					Name:           "bar",
					KeyColumnIDs:   []descpb.ColumnID{1},
					KeyColumnNames: []string{"a"},
				},
				Columns: []descpb.ColumnDescriptor{
					{
						Name:        "a",
						ID:          1,
						Type:        types.Int,
						DefaultExpr: pointer("a::@100500"),
					},
				},
			},
		},
		{ // 14
			err: `referenced type ID 500: descriptor not found`,
			desc: descpb.TableDescriptor{
				Name:                    "foo",
				ID:                      51,
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				PrimaryIndex: descpb.IndexDescriptor{
					ID:             1,
					Name:           "bar",
					KeyColumnIDs:   []descpb.ColumnID{1},
					KeyColumnNames: []string{"a"},
				},
				Columns: []descpb.ColumnDescriptor{
					{
						Name:        "a",
						ID:          1,
						Type:        types.Int,
						ComputeExpr: pointer("a:::@100500"),
					},
				},
			},
		},
		{ // 15
			err: `referenced type ID 500: descriptor not found`,
			desc: descpb.TableDescriptor{
				Name:                    "foo",
				ID:                      51,
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				Checks: []*descpb.TableDescriptor_CheckConstraint{
					{
						Expr: "a::@100500",
					},
				},
			},
		},
		// Temporary tables.
		{ // 16
			err: "",
			desc: descpb.TableDescriptor{
				Name:                    "foo",
				ID:                      51,
				ParentID:                1,
				UnexposedParentSchemaID: 12345,
				FormatVersion:           descpb.InterleavedFormatVersion,
				Temporary:               true,
			},
		},
	}

	for i, test := range tests {
		descs := catalog.MakeMapDescGetter()
		descs.Descriptors[1] = dbdesc.NewBuilder(&descpb.DatabaseDescriptor{ID: 1}).BuildImmutable()
		for _, otherDesc := range test.otherDescs {
			otherDesc.Privileges = descpb.NewDefaultPrivilegeDescriptor(security.AdminRoleName())
			descs.Descriptors[otherDesc.ID] = NewBuilder(&otherDesc).BuildImmutable()
		}
		desc := NewBuilder(&test.desc).BuildImmutable()
		expectedErr := fmt.Sprintf("%s %q (%d): %s", desc.DescriptorType(), desc.GetName(), desc.GetID(), test.err)
		const validateCrossReferencesOnly = catalog.ValidationLevelCrossReferences &^ (catalog.ValidationLevelCrossReferences >> 1)
		results := catalog.Validate(ctx, descs, catalog.NoValidationTelemetry, validateCrossReferencesOnly, desc)
		if err := results.CombinedError(); err == nil {
			if test.err != "" {
				t.Errorf("%d: expected \"%s\", but found success: %+v", i, expectedErr, test.desc)
			}
		} else if expectedErr != err.Error() {
			t.Errorf("%d: expected \"%s\", but found \"%s\"", i, expectedErr, err.Error())
		}
	}
}

func TestValidatePartitioning(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		err  string
		desc descpb.TableDescriptor
	}{
		{"at least one of LIST or RANGE partitioning must be used",
			descpb.TableDescriptor{
				PrimaryIndex: descpb.IndexDescriptor{
					Partitioning: descpb.PartitioningDescriptor{
						NumColumns: 1,
					},
				},
			},
		},
		{"PARTITION p1: must contain values",
			descpb.TableDescriptor{
				PrimaryIndex: descpb.IndexDescriptor{
					Partitioning: descpb.PartitioningDescriptor{
						NumColumns: 1,
						List:       []descpb.PartitioningDescriptor_List{{Name: "p1"}},
					},
				},
			},
		},
		{"not enough columns in index for this partitioning",
			descpb.TableDescriptor{
				PrimaryIndex: descpb.IndexDescriptor{
					Partitioning: descpb.PartitioningDescriptor{
						NumColumns: 1,
						List:       []descpb.PartitioningDescriptor_List{{Name: "p1", Values: [][]byte{{}}}},
					},
				},
			},
		},
		{"only one LIST or RANGE partitioning may used",
			descpb.TableDescriptor{
				PrimaryIndex: descpb.IndexDescriptor{
					KeyColumnIDs:        []descpb.ColumnID{1},
					KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
					Partitioning: descpb.PartitioningDescriptor{
						NumColumns: 1,
						List:       []descpb.PartitioningDescriptor_List{{}},
						Range:      []descpb.PartitioningDescriptor_Range{{}},
					},
				},
			},
		},
		{"PARTITION name must be non-empty",
			descpb.TableDescriptor{
				Columns: []descpb.ColumnDescriptor{{ID: 1, Type: types.Int}},
				PrimaryIndex: descpb.IndexDescriptor{
					KeyColumnIDs:        []descpb.ColumnID{1},
					KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
					Partitioning: descpb.PartitioningDescriptor{
						NumColumns: 1,
						List:       []descpb.PartitioningDescriptor_List{{}},
					},
				},
			},
		},
		{"PARTITION p1: must contain values",
			descpb.TableDescriptor{
				Columns: []descpb.ColumnDescriptor{{ID: 1, Type: types.Int}},
				PrimaryIndex: descpb.IndexDescriptor{
					KeyColumnIDs:        []descpb.ColumnID{1},
					KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
					Partitioning: descpb.PartitioningDescriptor{
						NumColumns: 1,
						List:       []descpb.PartitioningDescriptor_List{{Name: "p1"}},
					},
				},
			},
		},
		{"PARTITION p1: decoding: empty array",
			descpb.TableDescriptor{
				Columns: []descpb.ColumnDescriptor{{ID: 1, Type: types.Int}},
				PrimaryIndex: descpb.IndexDescriptor{
					KeyColumnIDs:        []descpb.ColumnID{1},
					KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
					Partitioning: descpb.PartitioningDescriptor{
						NumColumns: 1,
						List: []descpb.PartitioningDescriptor_List{{
							Name: "p1", Values: [][]byte{{}},
						}},
					},
				},
			},
		},
		{"PARTITION p1: decoding: int64 varint decoding failed: 0",
			descpb.TableDescriptor{
				Columns: []descpb.ColumnDescriptor{{ID: 1, Type: types.Int}},
				PrimaryIndex: descpb.IndexDescriptor{
					KeyColumnIDs:        []descpb.ColumnID{1},
					KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
					Partitioning: descpb.PartitioningDescriptor{
						NumColumns: 1,
						List: []descpb.PartitioningDescriptor_List{
							{Name: "p1", Values: [][]byte{{0x03}}},
						},
					},
				},
			},
		},
		{"PARTITION p1: superfluous data in encoded value",
			descpb.TableDescriptor{
				Columns: []descpb.ColumnDescriptor{{ID: 1, Type: types.Int}},
				PrimaryIndex: descpb.IndexDescriptor{
					KeyColumnIDs:        []descpb.ColumnID{1},
					KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
					Partitioning: descpb.PartitioningDescriptor{
						NumColumns: 1,
						List: []descpb.PartitioningDescriptor_List{
							{Name: "p1", Values: [][]byte{{0x03, 0x02, 0x00}}},
						},
					},
				},
			},
		},
		{"partitions p1 and p2 overlap",
			descpb.TableDescriptor{
				Columns: []descpb.ColumnDescriptor{{ID: 1, Type: types.Int}},
				PrimaryIndex: descpb.IndexDescriptor{
					KeyColumnIDs:        []descpb.ColumnID{1, 1},
					KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC, descpb.IndexDescriptor_ASC},
					Partitioning: descpb.PartitioningDescriptor{
						NumColumns: 1,
						Range: []descpb.PartitioningDescriptor_Range{
							{Name: "p1", FromInclusive: []byte{0x03, 0x02}, ToExclusive: []byte{0x03, 0x04}},
							{Name: "p2", FromInclusive: []byte{0x03, 0x02}, ToExclusive: []byte{0x03, 0x04}},
						},
					},
				},
			},
		},
		{"PARTITION p1: name must be unique",
			descpb.TableDescriptor{
				Columns: []descpb.ColumnDescriptor{{ID: 1, Type: types.Int}},
				PrimaryIndex: descpb.IndexDescriptor{
					KeyColumnIDs:        []descpb.ColumnID{1},
					KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
					Partitioning: descpb.PartitioningDescriptor{
						NumColumns: 1,
						List: []descpb.PartitioningDescriptor_List{
							{Name: "p1", Values: [][]byte{{0x03, 0x02}}},
							{Name: "p1", Values: [][]byte{{0x03, 0x04}}},
						},
					},
				},
			},
		},
		{"not enough columns in index for this partitioning",
			descpb.TableDescriptor{
				Columns: []descpb.ColumnDescriptor{{ID: 1, Type: types.Int}},
				PrimaryIndex: descpb.IndexDescriptor{
					KeyColumnIDs:        []descpb.ColumnID{1},
					KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
					Partitioning: descpb.PartitioningDescriptor{
						NumColumns: 1,
						List: []descpb.PartitioningDescriptor_List{{
							Name:   "p1",
							Values: [][]byte{{0x03, 0x02}},
							Subpartitioning: descpb.PartitioningDescriptor{
								NumColumns: 1,
								List:       []descpb.PartitioningDescriptor_List{{Name: "p1_1", Values: [][]byte{{}}}},
							},
						}},
					},
				},
			},
		},
		{"PARTITION p1: name must be unique",
			descpb.TableDescriptor{
				Columns: []descpb.ColumnDescriptor{{ID: 1, Type: types.Int}},
				PrimaryIndex: descpb.IndexDescriptor{
					KeyColumnIDs:        []descpb.ColumnID{1, 1},
					KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC, descpb.IndexDescriptor_ASC},
					Partitioning: descpb.PartitioningDescriptor{
						NumColumns: 1,
						List: []descpb.PartitioningDescriptor_List{
							{Name: "p1", Values: [][]byte{{0x03, 0x02}}},
							{
								Name:   "p2",
								Values: [][]byte{{0x03, 0x04}},
								Subpartitioning: descpb.PartitioningDescriptor{
									NumColumns: 1,
									List: []descpb.PartitioningDescriptor_List{
										{Name: "p1", Values: [][]byte{{0x03, 0x02}}},
									},
								},
							},
						},
					},
				},
			},
		},
	}
	for i, test := range tests {
		t.Run(test.err, func(t *testing.T) {
			desc := NewBuilder(&test.desc).BuildImmutableTable()
			err := ValidatePartitioning(desc)
			if !testutils.IsError(err, test.err) {
				t.Errorf(`%d: got "%v" expected "%v"`, i, err, test.err)
			}
		})
	}
}
