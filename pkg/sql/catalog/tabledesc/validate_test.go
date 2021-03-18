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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
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
		},
	},
	{
		obj: descpb.IndexDescriptor{},
		fieldMap: map[string]validationStatusInfo{
			"Name":             {status: thisFieldReferencesNoObjects},
			"ID":               {status: thisFieldReferencesNoObjects},
			"Unique":           {status: thisFieldReferencesNoObjects},
			"Version":          {status: thisFieldReferencesNoObjects},
			"ColumnNames":      {status: iSolemnlySwearThisFieldIsValidated},
			"ColumnDirections": {status: iSolemnlySwearThisFieldIsValidated},
			"StoreColumnNames": {
				status: todoIAmKnowinglyAddingTechDebt,
				reason: "initial import: TODO(features): add validation"},
			"ColumnIDs": {status: iSolemnlySwearThisFieldIsValidated},
			"ExtraColumnIDs": {
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
			"Hidden": {status: thisFieldReferencesNoObjects},
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
		{ // 1
			`empty table name`,
			descpb.TableDescriptor{}},
		{ // 2
			`invalid table ID`,
			descpb.TableDescriptor{ID: 0, Name: "foo"}},
		{ // 3
			`invalid parent database ID`,
			descpb.TableDescriptor{ID: 2, Name: "foo"}},
		{ // 4
			`table must contain at least 1 column`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
			}},
		{ // 5
			`column "" (0): empty column name`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 0},
				},
				NextColumnID: 2,
			}},
		{ // 6
			`table is encoded using version 0 but this client only supports versions 2 and 3`,
			descpb.TableDescriptor{
				ID:       2,
				ParentID: 1,
				Name:     "foo",
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				NextColumnID: 2,
			}},
		{ // 7
			`column "virt" (2): is virtual but is not computed`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
					{ID: 2, Name: "virt", Virtual: true},
				},
				NextColumnID: 3,
			}},
		{ // 8
			`column "bar" (0): invalid column ID`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 0, Name: "bar"},
				},
				NextColumnID: 2,
			}},
		{ // 9
			`table must contain a primary key`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
			}},
		{ // 10
			`column "bar" (1): another column "bar" has the same ID`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
					{ID: 1, Name: "bar"},
				},
				NextColumnID: 2,
			}},
		{ // 11
			`column "bar" (2): another column (1) has the same name`,
			descpb.TableDescriptor{
				ID:            catconstants.CrdbInternalBackwardDependenciesTableID,
				ParentID:      0,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
					{ID: 2, Name: "bar"},
				},
				NextColumnID: 2,
			}},
		{ // 12
			`column "blah" (1): another column "bar" has the same ID`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
					{ID: 1, Name: "blah"},
				},
				NextColumnID: 2,
			}},
		{ // 13
			`at least 1 column family must be specified`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				NextColumnID: 2,
			}},
		{ // 14
			`column family "" (1): the 0th family must have ID 0`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 1},
				},
				NextColumnID: 2,
			}},
		{ // 15
			`column family "baz" (1): another family (0) has the same name`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
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
		{ // 16
			`column family "qux" (0): another family "baz" has the same ID`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
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
		{ // 17
			`column family "baz" (3): another family (0) has the same name`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
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
		{ // 18
			`column family "baz" (0): has 1 column IDs but 0 column names`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "baz", ColumnIDs: []descpb.ColumnID{1}},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
			}},
		{ // 19
			`column family "baz" (0): column "bar" (2): not found`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "baz", ColumnIDs: []descpb.ColumnID{2}, ColumnNames: []string{"bar"}},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
			}},
		{ // 20
			`column family "baz" (0): column "qux" (1): column with same ID in table has different name "bar"`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "baz", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"qux"}},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
			}},
		{ // 21
			`column "bar" (1): non-virtual column not in any column family`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "baz"},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
			}},
		{ // 22
			`column family "qux" (1): column "bar" (1): also in another family "baz" (0)`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
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
		{ // 23
			`column family "fam2" (1): column "virt" (2): a virtual computed column cannot be in a family`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
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
		{ // 24
			`table must contain a primary key`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: descpb.IndexDescriptor{
					ID:               0,
					ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC}},
				NextColumnID: 2,
				NextFamilyID: 1,
			}},
		{ // 25
			`index "bar" (0): invalid index ID`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: descpb.IndexDescriptor{ID: 0, Name: "bar",
					ColumnIDs:        []descpb.ColumnID{0},
					ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC}},
				NextColumnID: 2,
				NextFamilyID: 1,
			}},
		{ // 26
			`index "bar" (2): index must have at least one column`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: descpb.IndexDescriptor{
					ID: 1, Name: "primary", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar"},
					ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
				},
				Indexes: []descpb.IndexDescriptor{
					{ID: 2, Name: "bar"},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
				NextIndexID:  3,
			}},
		{ // 27
			`index "bar" (1): has 1 column IDs but 0 column names`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: descpb.IndexDescriptor{ID: 1, Name: "bar", ColumnIDs: []descpb.ColumnID{1}},
				NextColumnID: 2,
				NextFamilyID: 1,
				NextIndexID:  2,
			}},
		{ // 28
			`index "bar" (1): has 1 column IDs but 2 column names`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
					{ID: 2, Name: "blah"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []descpb.ColumnID{1, 2}, ColumnNames: []string{"bar", "blah"}},
				},
				PrimaryIndex: descpb.IndexDescriptor{ID: 1, Name: "bar",
					ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar", "blah"},
					ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
				},
				NextColumnID: 3,
				NextFamilyID: 1,
				NextIndexID:  2,
			}},
		{ // 29
			`index "bar" (2): another index (1) has the same name`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: descpb.IndexDescriptor{ID: 1, Name: "bar",
					ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar"},
					ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
				},
				Indexes: []descpb.IndexDescriptor{
					{ID: 2, Name: "bar", ColumnIDs: []descpb.ColumnID{1},
						ColumnNames:      []string{"bar"},
						ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
					},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
				NextIndexID:  3,
			}},
		{ // 30
			`index "blah" (1): another index "bar" has the same ID`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: descpb.IndexDescriptor{ID: 1, Name: "bar", ColumnIDs: []descpb.ColumnID{1},
					ColumnNames:      []string{"bar"},
					ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
				},
				Indexes: []descpb.IndexDescriptor{
					{ID: 1, Name: "blah", ColumnIDs: []descpb.ColumnID{1},
						ColumnNames:      []string{"bar"},
						ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
					},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
				NextIndexID:  2,
			}},
		{ // 31
			`index "bar" (1): column "bar" (2): column with same name in table has different ID (1)`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: descpb.IndexDescriptor{ID: 1, Name: "bar", ColumnIDs: []descpb.ColumnID{2},
					ColumnNames:      []string{"bar"},
					ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
				NextIndexID:  2,
			}},
		{ // 32
			`index "bar" (1): column "blah" (1): not found`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: descpb.IndexDescriptor{ID: 1, Name: "bar", ColumnIDs: []descpb.ColumnID{1},
					ColumnNames:      []string{"blah"},
					ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
				NextIndexID:  2,
			}},
		{ // 33
			`index "bar" (1): has 1 column IDs but 0 column directions`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: descpb.IndexDescriptor{ID: 1, Name: "bar", ColumnIDs: []descpb.ColumnID{1},
					ColumnNames: []string{"blah"},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
				NextIndexID:  2,
			}},
		{ // 34
			`index "primary" (1): has 1 STORING column IDs but 0 STORING column names`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
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
					ColumnIDs:        []descpb.ColumnID{1},
					ColumnNames:      []string{"c1"},
					ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
					StoreColumnIDs:   []descpb.ColumnID{2},
				},
				NextColumnID: 3,
				NextFamilyID: 1,
				NextIndexID:  2,
			}},
		{ // 35
			`index "primary" (1): at least one of LIST or RANGE partitioning must be used`,
			// Verify that validatePartitioning is hooked up. The rest of these
			// tests are in TestValidatePartitionion.
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				PrimaryIndex: descpb.IndexDescriptor{
					ID: 1, Name: "primary", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar"},
					ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
					Partitioning: descpb.PartitioningDescriptor{
						NumColumns: 1,
					},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
				NextIndexID:  3,
			}},
		{ // 36
			`index "foo_crdb_internal_bar_shard_5_bar_idx" (2): shard column "does not exist" not found`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
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
					Unique:           true,
					ColumnIDs:        []descpb.ColumnID{1},
					ColumnNames:      []string{"bar"},
					ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
					StoreColumnNames: []string{"crdb_internal_bar_shard_5"},
					StoreColumnIDs:   []descpb.ColumnID{2},
				},
				Indexes: []descpb.IndexDescriptor{
					{ID: 2, Name: "foo_crdb_internal_bar_shard_5_bar_idx",
						ColumnIDs:        []descpb.ColumnID{2, 1},
						ColumnNames:      []string{"crdb_internal_bar_shard_5", "bar"},
						ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC, descpb.IndexDescriptor_ASC},
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
		{ // 37
			`unique without index constraint "bar_unique": referenced table ID (1) does not match this table`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
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
		{ // 38
			`column-id "2" does not exist`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
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
		{ // 39
			`unique without index constraint "bar_unique": column (1): not unique in constraint column ID list`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
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
		{ // 40
			`unique without index constraint "": empty unique without index constraint name`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
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
		{ // 41
			`index "primary" (1): column "v" (2): virtual column cannot be in primary index`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
					{ID: 2, Name: "v", ComputeExpr: &computedExpr, Virtual: true},
				},
				PrimaryIndex: descpb.IndexDescriptor{
					ID:               1,
					Name:             "primary",
					Unique:           true,
					ColumnIDs:        []descpb.ColumnID{1, 2},
					ColumnNames:      []string{"bar", "v"},
					ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC, descpb.IndexDescriptor_ASC},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary",
						ColumnIDs:   []descpb.ColumnID{1},
						ColumnNames: []string{"bar"},
					},
				},
				NextColumnID: 3,
				NextFamilyID: 1,
				NextIndexID:  2,
			}},
		{ // 42
			`index "sec" (2): STORING column "v" (3): virtual column cannot be stored`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "c1"},
					{ID: 2, Name: "c2"},
					{ID: 3, Name: "v", ComputeExpr: &computedExpr, Virtual: true},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []descpb.ColumnID{1, 2}, ColumnNames: []string{"c1", "c2"}},
				},
				PrimaryIndex: descpb.IndexDescriptor{
					ID: 1, Name: "pri", ColumnIDs: []descpb.ColumnID{1},
					ColumnNames:      []string{"c1"},
					ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
				},
				Indexes: []descpb.IndexDescriptor{
					{ID: 2, Name: "sec", ColumnIDs: []descpb.ColumnID{2},
						ColumnNames:      []string{"c2"},
						ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
						StoreColumnNames: []string{"v"},
						StoreColumnIDs:   []descpb.ColumnID{3},
					},
				},
				NextColumnID: 4,
				NextFamilyID: 1,
				NextIndexID:  3,
			}},
	}
	for i, d := range testData {
		t.Run(fmt.Sprintf("#%d %s", i+1, d.err), func(t *testing.T) {
			desc := NewBuilder(&d.desc).BuildImmutableTable()
			expectedErr := fmt.Sprintf("%s %q (%d): %s", desc.DescriptorType(), desc.GetName(), desc.GetID(), d.err)
			if err := catalog.ValidateSelf(desc); err == nil {
				t.Errorf("expected \"%s\", but found success: %+v", expectedErr, d.desc)
			} else if expectedErr != err.Error() {
				t.Errorf("expected \"%s\", but found \"%+v\"", expectedErr, err)
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
		otherDescs []descpb.Descriptor
	}{
		// Foreign keys
		{ // 1
			err: `foreign key "fk": referenced table ID 52: descriptor not found`,
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
		},
		{ // 2
			err: `foreign key "fk": missing back reference from "baz" (52)`,
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
			otherDescs: []descpb.Descriptor{
				{Union: &descpb.Descriptor_Table{Table: &descpb.TableDescriptor{
					ID:                      52,
					Name:                    "baz",
					ParentID:                1,
					UnexposedParentSchemaID: keys.PublicSchemaID,
					FormatVersion:           descpb.InterleavedFormatVersion,
				}}},
			},
		},
		{ // 3
			err: `foreign key back reference "fk": referenced table ID 52: descriptor not found`,
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
		{ // 4
			err: `foreign key back reference "fk": missing foreign key reference in back referenced table "baz" (52)`,
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
			otherDescs: []descpb.Descriptor{
				{Union: &descpb.Descriptor_Table{Table: &descpb.TableDescriptor{
					ID:                      52,
					Name:                    "baz",
					ParentID:                1,
					UnexposedParentSchemaID: keys.PublicSchemaID,
					FormatVersion:           descpb.InterleavedFormatVersion,
				}}},
			},
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
						ID:        2,
						ColumnIDs: []descpb.ColumnID{1, 2},
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
			otherDescs: []descpb.Descriptor{
				{Union: &descpb.Descriptor_Table{Table: &descpb.TableDescriptor{
					ID:                      52,
					Name:                    "baz",
					ParentID:                1,
					UnexposedParentSchemaID: keys.PublicSchemaID,
					FormatVersion:           descpb.InterleavedFormatVersion,
					Indexes: []descpb.IndexDescriptor{
						{
							Unique:       true,
							ColumnIDs:    []descpb.ColumnID{1},
							ReferencedBy: []descpb.ForeignKeyReference{{Table: 51, Index: 2}},
						},
					},
				}}},
			},
		},
		{ // 6
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
						ID:        2,
						ColumnIDs: []descpb.ColumnID{7},
						Unique:    true,
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
			otherDescs: []descpb.Descriptor{
				{Union: &descpb.Descriptor_Table{Table: &descpb.TableDescriptor{
					ID:                      52,
					Name:                    "baz",
					ParentID:                1,
					UnexposedParentSchemaID: keys.PublicSchemaID,
					FormatVersion:           descpb.InterleavedFormatVersion,
					Indexes: []descpb.IndexDescriptor{
						{
							ID:         2,
							Unique:     true,
							ColumnIDs:  []descpb.ColumnID{1},
							ForeignKey: descpb.ForeignKeyReference{Table: 51, Index: 2},
						},
					},
				}}},
			},
		},
		// Interleaves
		{ // 7
			err: `index "" (1): interleave parent (52@2): referenced table ID 52: descriptor not found`,
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
		},
		{ // 8
			err: `index "" (1): interleave parent (52@2): index not found in table "baz"`,
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
			otherDescs: []descpb.Descriptor{
				{Union: &descpb.Descriptor_Table{Table: &descpb.TableDescriptor{
					ID:                      52,
					Name:                    "baz",
					ParentID:                1,
					UnexposedParentSchemaID: keys.PublicSchemaID,
				}}},
			},
		},
		{ // 9
			err: `index "bar" (1): interleave parent (52@2): missing interleave back reference from "baz"@"qux"`,
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
			otherDescs: []descpb.Descriptor{
				{Union: &descpb.Descriptor_Table{Table: &descpb.TableDescriptor{
					ID:                      52,
					Name:                    "baz",
					ParentID:                1,
					UnexposedParentSchemaID: keys.PublicSchemaID,
					PrimaryIndex: descpb.IndexDescriptor{
						ID:   2,
						Name: "qux",
					},
				}}},
			},
		},
		{ // 10
			err: `index "" (1): interleave back reference (52@2): referenced table ID 52: descriptor not found`,
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
		{ // 11
			err: `index "" (1): interleave back reference (52@2): index not found in table "baz"`,
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
			otherDescs: []descpb.Descriptor{
				{Union: &descpb.Descriptor_Table{Table: &descpb.TableDescriptor{
					ID:                      52,
					Name:                    "baz",
					ParentID:                1,
					UnexposedParentSchemaID: keys.PublicSchemaID,
				}}},
			},
		},
		{ // 12
			err: `index "bar" (1): interleave back reference (52@2): missing interleave parent in "baz"@"qux"`,
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
			otherDescs: []descpb.Descriptor{
				{Union: &descpb.Descriptor_Table{Table: &descpb.TableDescriptor{
					Name:                    "baz",
					ID:                      52,
					ParentID:                1,
					UnexposedParentSchemaID: keys.PublicSchemaID,
					PrimaryIndex: descpb.IndexDescriptor{
						ID:   2,
						Name: "qux",
					},
				}}},
			},
		},
		{ // 13
			err: `referenced type ID 500: descriptor not found`,
			desc: descpb.TableDescriptor{
				Name:                    "foo",
				ID:                      51,
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				PrimaryIndex: descpb.IndexDescriptor{
					ID:          1,
					Name:        "bar",
					ColumnIDs:   []descpb.ColumnID{1},
					ColumnNames: []string{"a"},
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
		{ // 14
			err: `referenced type ID 500: descriptor not found`,
			desc: descpb.TableDescriptor{
				Name:                    "foo",
				ID:                      51,
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				PrimaryIndex: descpb.IndexDescriptor{
					ID:          1,
					Name:        "bar",
					ColumnIDs:   []descpb.ColumnID{1},
					ColumnNames: []string{"a"},
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
		{ // 15
			err: `referenced type ID 500: descriptor not found`,
			desc: descpb.TableDescriptor{
				Name:                    "foo",
				ID:                      51,
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				PrimaryIndex: descpb.IndexDescriptor{
					ID:          1,
					Name:        "bar",
					ColumnIDs:   []descpb.ColumnID{1},
					ColumnNames: []string{"a"},
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
		{ // 16
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
		{ // 17
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
		// Multi-region.
		{ // 18
			err: `parent database "parentdb" (52): not multi-region enabled but table has locality GLOBAL`,
			desc: descpb.TableDescriptor{
				Name:                    "foo",
				ID:                      54,
				ParentID:                52,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				FormatVersion:           descpb.InterleavedFormatVersion,
				Temporary:               true,
				LocalityConfig: &descpb.TableDescriptor_LocalityConfig{
					Locality: &descpb.TableDescriptor_LocalityConfig_Global_{
						Global: &descpb.TableDescriptor_LocalityConfig_Global{},
					},
				},
			},
			otherDescs: []descpb.Descriptor{
				{Union: &descpb.Descriptor_Database{Database: &descpb.DatabaseDescriptor{
					Name: "parentdb",
					ID:   52,
				}}},
			},
		},
		{ // 19
			err: `parent database "parentdb" (52): homing region "bad" for REGIONAL BY TABLE table not found in multi-region enum "mrenum" (53)`,
			desc: descpb.TableDescriptor{
				Name:                    "foo",
				ID:                      54,
				ParentID:                52,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				FormatVersion:           descpb.InterleavedFormatVersion,
				Temporary:               true,
				LocalityConfig: &descpb.TableDescriptor_LocalityConfig{
					Locality: &descpb.TableDescriptor_LocalityConfig_RegionalByTable_{
						RegionalByTable: &descpb.TableDescriptor_LocalityConfig_RegionalByTable{
							Region: func() *descpb.RegionName { r := descpb.RegionName("bad"); return &r }(),
						},
					},
				},
			},
			otherDescs: []descpb.Descriptor{
				{Union: &descpb.Descriptor_Database{Database: &descpb.DatabaseDescriptor{
					Name: "parentdb",
					ID:   52,
					RegionConfig: &descpb.DatabaseDescriptor_RegionConfig{
						RegionEnumID: 53,
					},
				}}},
				{Union: &descpb.Descriptor_Type{Type: &descpb.TypeDescriptor{
					Name:           "mrenum",
					ID:             53,
					ParentID:       52,
					ParentSchemaID: keys.PublicSchemaID,
					Kind:           descpb.TypeDescriptor_MULTIREGION_ENUM,
					EnumMembers: []descpb.TypeDescriptor_EnumMember{
						{
							LogicalRepresentation:  "us-east-1",
							PhysicalRepresentation: []byte{2},
						},
						{
							LogicalRepresentation:  "us-east-2",
							PhysicalRepresentation: []byte{2},
						},
						{
							LogicalRepresentation:  "us-east-3",
							PhysicalRepresentation: []byte{2},
						},
					},
					ReferencingDescriptorIDs: []descpb.ID{52, 54},
				}}},
			},
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("#%d %s", i+1, test.err), func(t *testing.T) {
			descs := catalog.MakeMapDescGetter()
			descs.Descriptors[1] = dbdesc.NewBuilder(&descpb.DatabaseDescriptor{ID: 1}).BuildImmutable()
			for _, otherDesc := range test.otherDescs {
				newprivs := descpb.NewDefaultPrivilegeDescriptor(security.AdminRoleName())
				tbl, db, typ, schema := descpb.FromDescriptor(&otherDesc)
				if tbl != nil {
					tbl.Privileges = newprivs
					descs.Descriptors[tbl.ID] = NewBuilder(tbl).BuildImmutable()
				} else if db != nil {
					db.Privileges = newprivs
					descs.Descriptors[db.ID] = dbdesc.NewBuilder(db).BuildImmutable()
				} else if typ != nil {
					typ.Privileges = newprivs
					descs.Descriptors[typ.ID] = typedesc.NewBuilder(typ).BuildImmutable()
				} else if schema != nil {
					schema.Privileges = newprivs
					descs.Descriptors[schema.ID] = schemadesc.NewBuilder(schema).BuildImmutable()
				}
			}
			desc := NewBuilder(&test.desc).BuildImmutable()
			expectedErr := fmt.Sprintf("%s %q (%d): %s", desc.DescriptorType(), desc.GetName(), desc.GetID(), test.err)
			const validateCrossReferencesOnly = catalog.ValidationLevelCrossReferences &^ (catalog.ValidationLevelCrossReferences >> 1)
			if err := catalog.Validate(ctx, descs, validateCrossReferencesOnly, desc).CombinedError(); err == nil {
				if test.err != "" {
					t.Errorf("expected \"%s\", but found success: %+v", expectedErr, test.desc)
				}
			} else if expectedErr != err.Error() {
				t.Errorf("expected \"%s\", but found \"%s\"", expectedErr, err.Error())
			}
		})
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
					ColumnIDs:        []descpb.ColumnID{1},
					ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
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
					ColumnIDs:        []descpb.ColumnID{1},
					ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
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
					ColumnIDs:        []descpb.ColumnID{1},
					ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
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
					ColumnIDs:        []descpb.ColumnID{1},
					ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
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
					ColumnIDs:        []descpb.ColumnID{1},
					ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
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
					ColumnIDs:        []descpb.ColumnID{1},
					ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
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
					ColumnIDs:        []descpb.ColumnID{1, 1},
					ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC, descpb.IndexDescriptor_ASC},
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
					ColumnIDs:        []descpb.ColumnID{1},
					ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
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
					ColumnIDs:        []descpb.ColumnID{1},
					ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
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
					ColumnIDs:        []descpb.ColumnID{1, 1},
					ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC, descpb.IndexDescriptor_ASC},
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
