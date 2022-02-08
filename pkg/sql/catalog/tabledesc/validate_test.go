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

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/internal/validate"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"google.golang.org/protobuf/proto"
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
			"DependsOn":          {status: iSolemnlySwearThisFieldIsValidated},
			"DependsOnTypes":     {status: iSolemnlySwearThisFieldIsValidated},
			"DependedOnBy":       {status: iSolemnlySwearThisFieldIsValidated},
			"MutationJobs":       {status: thisFieldReferencesNoObjects},
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
			"RowLevelTTL":                   {status: iSolemnlySwearThisFieldIsValidated},
			"ExcludeDataFromBackup":         {status: thisFieldReferencesNoObjects},
			"NextConstraintID":              {status: iSolemnlySwearThisFieldIsValidated},
			"DeclarativeSchemaChangerState": {status: iSolemnlySwearThisFieldIsValidated},
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

			"Interleave":                  {status: iSolemnlySwearThisFieldIsValidated},
			"InterleavedBy":               {status: iSolemnlySwearThisFieldIsValidated},
			"Partitioning":                {status: iSolemnlySwearThisFieldIsValidated},
			"Type":                        {status: thisFieldReferencesNoObjects},
			"CreatedExplicitly":           {status: thisFieldReferencesNoObjects},
			"EncodingType":                {status: thisFieldReferencesNoObjects},
			"Sharded":                     {status: iSolemnlySwearThisFieldIsValidated},
			"Disabled":                    {status: thisFieldReferencesNoObjects},
			"GeoConfig":                   {status: thisFieldReferencesNoObjects},
			"Predicate":                   {status: iSolemnlySwearThisFieldIsValidated},
			"UseDeletePreservingEncoding": {status: thisFieldReferencesNoObjects},
			"ConstraintID":                {status: iSolemnlySwearThisFieldIsValidated},
			"CreatedAtNanos":              {status: thisFieldReferencesNoObjects},
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
			"Hidden":                            {status: iSolemnlySwearThisFieldIsValidated},
			"Inaccessible":                      {status: iSolemnlySwearThisFieldIsValidated},
			"GeneratedAsIdentityType":           {status: iSolemnlySwearThisFieldIsValidated},
			"GeneratedAsIdentitySequenceOption": {status: iSolemnlySwearThisFieldIsValidated},
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
			"OnUpdateExpr":              {status: iSolemnlySwearThisFieldIsValidated},
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
			"ConstraintID":      {status: iSolemnlySwearThisFieldIsValidated},
		},
	},
	{
		obj: descpb.UniqueWithoutIndexConstraint{},
		fieldMap: map[string]validationStatusInfo{
			"TableID":      {status: iSolemnlySwearThisFieldIsValidated},
			"ColumnIDs":    {status: iSolemnlySwearThisFieldIsValidated},
			"Name":         {status: thisFieldReferencesNoObjects},
			"Validity":     {status: thisFieldReferencesNoObjects},
			"Predicate":    {status: iSolemnlySwearThisFieldIsValidated},
			"ConstraintID": {status: iSolemnlySwearThisFieldIsValidated},
		},
	},
	{
		obj: descpb.TypeDescriptor{},
		fieldMap: map[string]validationStatusInfo{
			"Name":                          {status: iSolemnlySwearThisFieldIsValidated},
			"ID":                            {status: iSolemnlySwearThisFieldIsValidated},
			"Version":                       {status: thisFieldReferencesNoObjects},
			"ModificationTime":              {status: thisFieldReferencesNoObjects},
			"DrainingNames":                 {status: thisFieldReferencesNoObjects},
			"ParentID":                      {status: iSolemnlySwearThisFieldIsValidated},
			"ParentSchemaID":                {status: iSolemnlySwearThisFieldIsValidated},
			"Kind":                          {status: thisFieldReferencesNoObjects},
			"ArrayTypeID":                   {status: iSolemnlySwearThisFieldIsValidated},
			"EnumMembers":                   {status: iSolemnlySwearThisFieldIsValidated},
			"Alias":                         {status: iSolemnlySwearThisFieldIsValidated},
			"State":                         {status: thisFieldReferencesNoObjects},
			"ReferencingDescriptorIDs":      {status: iSolemnlySwearThisFieldIsValidated},
			"Privileges":                    {status: iSolemnlySwearThisFieldIsValidated},
			"OfflineReason":                 {status: thisFieldReferencesNoObjects},
			"RegionConfig":                  {status: iSolemnlySwearThisFieldIsValidated},
			"DeclarativeSchemaChangerState": {status: thisFieldReferencesNoObjects},
		},
	},
	{
		obj: descpb.DatabaseDescriptor{},
		fieldMap: map[string]validationStatusInfo{
			"Name":                          {status: iSolemnlySwearThisFieldIsValidated},
			"ID":                            {status: iSolemnlySwearThisFieldIsValidated},
			"Version":                       {status: thisFieldReferencesNoObjects},
			"ModificationTime":              {status: thisFieldReferencesNoObjects},
			"DrainingNames":                 {status: thisFieldReferencesNoObjects},
			"Privileges":                    {status: iSolemnlySwearThisFieldIsValidated},
			"Schemas":                       {status: iSolemnlySwearThisFieldIsValidated},
			"State":                         {status: thisFieldReferencesNoObjects},
			"OfflineReason":                 {status: thisFieldReferencesNoObjects},
			"RegionConfig":                  {status: iSolemnlySwearThisFieldIsValidated},
			"DefaultPrivileges":             {status: iSolemnlySwearThisFieldIsValidated},
			"DeclarativeSchemaChangerState": {status: thisFieldReferencesNoObjects},
		},
	},
	{
		obj: descpb.SchemaDescriptor{},
		fieldMap: map[string]validationStatusInfo{
			"Name":                          {status: iSolemnlySwearThisFieldIsValidated},
			"ID":                            {status: iSolemnlySwearThisFieldIsValidated},
			"State":                         {status: thisFieldReferencesNoObjects},
			"OfflineReason":                 {status: thisFieldReferencesNoObjects},
			"ModificationTime":              {status: thisFieldReferencesNoObjects},
			"Version":                       {status: thisFieldReferencesNoObjects},
			"DrainingNames":                 {status: thisFieldReferencesNoObjects},
			"ParentID":                      {status: iSolemnlySwearThisFieldIsValidated},
			"Privileges":                    {status: iSolemnlySwearThisFieldIsValidated},
			"DefaultPrivileges":             {status: iSolemnlySwearThisFieldIsValidated},
			"DeclarativeSchemaChangerState": {status: thisFieldReferencesNoObjects},
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
	generatedAsIdentitySequenceOptionExpr := " START 2 INCREMENT 3 CACHE 10"

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
		{`index "secondary" contains stored column "quux" with unknown ID 123`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
					{ID: 2, Name: "baz"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []descpb.ColumnID{1, 2}, ColumnNames: []string{"bar", "baz"}},
				},
				PrimaryIndex: descpb.IndexDescriptor{
					ID:                  1,
					Name:                "primary",
					KeyColumnIDs:        []descpb.ColumnID{1},
					KeyColumnNames:      []string{"bar"},
					KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
					EncodingType:        descpb.PrimaryIndexEncoding,
					Version:             descpb.PrimaryIndexWithStoredColumnsVersion,
				},
				Indexes: []descpb.IndexDescriptor{{
					ID:                  2,
					Name:                "secondary",
					KeyColumnIDs:        []descpb.ColumnID{1},
					KeyColumnNames:      []string{"bar"},
					KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
					StoreColumnIDs:      []descpb.ColumnID{123},
					StoreColumnNames:    []string{"quux"},
				}},
				NextColumnID: 3,
				NextFamilyID: 1,
				NextIndexID:  3,
			}},
		{`index "secondary" stored column ID 2 should have name "baz", but found name "quux"`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
					{ID: 2, Name: "baz"},
					{ID: 3, Name: "quux"},
				},
				Families: []descpb.ColumnFamilyDescriptor{{
					ID:          0,
					Name:        "primary",
					ColumnIDs:   []descpb.ColumnID{1, 2, 3},
					ColumnNames: []string{"bar", "baz", "quux"},
				}},
				PrimaryIndex: descpb.IndexDescriptor{
					ID:                  1,
					Name:                "primary",
					KeyColumnIDs:        []descpb.ColumnID{1},
					KeyColumnNames:      []string{"bar"},
					KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
					EncodingType:        descpb.PrimaryIndexEncoding,
					Version:             descpb.PrimaryIndexWithStoredColumnsVersion,
				},
				Indexes: []descpb.IndexDescriptor{{
					ID:                  2,
					Name:                "secondary",
					KeyColumnIDs:        []descpb.ColumnID{2},
					KeyColumnNames:      []string{"baz"},
					KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
					KeySuffixColumnIDs:  []descpb.ColumnID{1},
					StoreColumnIDs:      []descpb.ColumnID{2},
					StoreColumnNames:    []string{"quux"},
				}},
				NextColumnID: 4,
				NextFamilyID: 1,
				NextIndexID:  3,
			}},
		{`index "secondary" key suffix column ID 123 is invalid`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
					{ID: 2, Name: "baz"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []descpb.ColumnID{1, 2}, ColumnNames: []string{"bar", "baz"}},
				},
				PrimaryIndex: descpb.IndexDescriptor{
					ID:                  1,
					Name:                "primary",
					KeyColumnIDs:        []descpb.ColumnID{1},
					KeyColumnNames:      []string{"bar"},
					KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
					EncodingType:        descpb.PrimaryIndexEncoding,
					Version:             descpb.PrimaryIndexWithStoredColumnsVersion,
				},
				Indexes: []descpb.IndexDescriptor{{
					ID:                  2,
					Name:                "secondary",
					KeyColumnIDs:        []descpb.ColumnID{2},
					KeyColumnNames:      []string{"baz"},
					KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
					KeySuffixColumnIDs:  []descpb.ColumnID{123},
				}},
				NextColumnID: 3,
				NextFamilyID: 1,
				NextIndexID:  3,
			}},
		{`index "primary" contains deprecated foreign key representation`,
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
				PrimaryIndex: descpb.IndexDescriptor{ID: 1, Name: "primary",
					KeyColumnIDs: []descpb.ColumnID{1}, KeyColumnNames: []string{"bar"},
					KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
					EncodingType:        descpb.PrimaryIndexEncoding,
					Version:             descpb.PrimaryIndexWithStoredColumnsVersion,
					ForeignKey:          descpb.ForeignKeyReference{Table: 123, Index: 456},
					ConstraintID:        1,
				},
				NextColumnID:     2,
				NextFamilyID:     1,
				NextIndexID:      2,
				NextConstraintID: 2,
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
					Partitioning: catpb.PartitioningDescriptor{
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
						Sharded: catpb.ShardedDescriptor{
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
		{`index "sec" cannot store virtual column "c3"`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "c1"},
					{ID: 2, Name: "c2"},
					{ID: 3, Name: "c3", ComputeExpr: &computedExpr, Virtual: true},
				},
				PrimaryIndex: descpb.IndexDescriptor{
					ID:                  1,
					Name:                "primary",
					Unique:              true,
					KeyColumnIDs:        []descpb.ColumnID{1},
					KeyColumnNames:      []string{"c1"},
					KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
					Version:             descpb.PrimaryIndexWithStoredColumnsVersion,
					EncodingType:        descpb.PrimaryIndexEncoding,
					ConstraintID:        1,
				},
				Indexes: []descpb.IndexDescriptor{
					{ID: 2, Name: "sec", KeyColumnIDs: []descpb.ColumnID{2},
						KeyColumnNames:      []string{"c2"},
						KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
						KeySuffixColumnIDs:  []descpb.ColumnID{3},
					},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary",
						ColumnIDs:   []descpb.ColumnID{1, 2},
						ColumnNames: []string{"c1", "c2"},
					},
				},
				NextColumnID:     4,
				NextFamilyID:     1,
				NextIndexID:      3,
				NextConstraintID: 2,
			}},
		{"",
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "c1"},
					{ID: 2, Name: "c2"},
					{ID: 3, Name: "c3", ComputeExpr: &computedExpr, Virtual: true},
				},
				PrimaryIndex: descpb.IndexDescriptor{
					ID:                  1,
					Name:                "primary",
					Unique:              true,
					KeyColumnIDs:        []descpb.ColumnID{1},
					KeyColumnNames:      []string{"c1"},
					KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
					Version:             descpb.PrimaryIndexWithStoredColumnsVersion,
					EncodingType:        descpb.PrimaryIndexEncoding,
					ConstraintID:        1,
				},
				Indexes: []descpb.IndexDescriptor{
					{ID: 2, Name: "sec", KeyColumnIDs: []descpb.ColumnID{2},
						KeyColumnNames:      []string{"c2"},
						KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
						KeySuffixColumnIDs:  []descpb.ColumnID{},
					},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary",
						ColumnIDs:   []descpb.ColumnID{1, 2},
						ColumnNames: []string{"c1", "c2"},
					},
				},
				Mutations: []descpb.DescriptorMutation{
					{
						Descriptor_: &descpb.DescriptorMutation_Index{
							Index: &descpb.IndexDescriptor{
								ID:                  3,
								Name:                "new_primary_key",
								Unique:              true,
								KeyColumnIDs:        []descpb.ColumnID{3},
								KeyColumnNames:      []string{"c3"},
								KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
								Version:             descpb.PrimaryIndexWithStoredColumnsVersion,
								EncodingType:        descpb.PrimaryIndexEncoding,
								ConstraintID:        1,
							},
						},
						Direction: descpb.DescriptorMutation_ADD,
						State:     descpb.DescriptorMutation_DELETE_ONLY,
					},
					{
						Descriptor_: &descpb.DescriptorMutation_Index{
							Index: &descpb.IndexDescriptor{
								ID: 4, Name: "new_sec", KeyColumnIDs: []descpb.ColumnID{2},
								KeyColumnNames:      []string{"c2"},
								KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
								KeySuffixColumnIDs:  []descpb.ColumnID{3},
							},
						},
						Direction: descpb.DescriptorMutation_ADD,
						State:     descpb.DescriptorMutation_DELETE_ONLY,
					},
					{
						Descriptor_: &descpb.DescriptorMutation_PrimaryKeySwap{
							PrimaryKeySwap: &descpb.PrimaryKeySwap{
								OldPrimaryIndexId: 1,
								NewPrimaryIndexId: 3,
								NewIndexes:        []descpb.IndexID{4},
								OldIndexes:        []descpb.IndexID{2},
							},
						},
						Direction: descpb.DescriptorMutation_ADD,
						State:     descpb.DescriptorMutation_DELETE_ONLY,
					},
				},
				NextColumnID:     4,
				NextFamilyID:     1,
				NextIndexID:      5,
				NextConstraintID: 2,
				Privileges:       catpb.NewBasePrivilegeDescriptor(security.AdminRoleName()),
			}},
		{`index "sec" cannot store virtual column "c3"`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "c1"},
					{ID: 2, Name: "c2"},
					{ID: 3, Name: "c3", ComputeExpr: &computedExpr, Virtual: true},
				},
				PrimaryIndex: descpb.IndexDescriptor{
					ID:                  1,
					Name:                "primary",
					Unique:              true,
					KeyColumnIDs:        []descpb.ColumnID{1},
					KeyColumnNames:      []string{"c1"},
					KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
					Version:             descpb.PrimaryIndexWithStoredColumnsVersion,
					EncodingType:        descpb.PrimaryIndexEncoding,
				},
				Indexes: []descpb.IndexDescriptor{
					{ID: 2, Name: "sec", KeyColumnIDs: []descpb.ColumnID{2},
						KeyColumnNames:      []string{"c2"},
						KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
						KeySuffixColumnIDs:  []descpb.ColumnID{3},
					},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary",
						ColumnIDs:   []descpb.ColumnID{1, 2},
						ColumnNames: []string{"c1", "c2"},
					},
				},
				Mutations: []descpb.DescriptorMutation{
					{
						Descriptor_: &descpb.DescriptorMutation_Index{
							Index: &descpb.IndexDescriptor{
								ID:                  3,
								Name:                "new_primary_key",
								Unique:              true,
								KeyColumnIDs:        []descpb.ColumnID{3},
								KeyColumnNames:      []string{"c3"},
								KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
								Version:             descpb.PrimaryIndexWithStoredColumnsVersion,
								EncodingType:        descpb.PrimaryIndexEncoding,
							},
						},
						Direction: descpb.DescriptorMutation_ADD,
						State:     descpb.DescriptorMutation_DELETE_ONLY,
					},
					{
						Descriptor_: &descpb.DescriptorMutation_Index{
							Index: &descpb.IndexDescriptor{
								ID: 4, Name: "new_sec", KeyColumnIDs: []descpb.ColumnID{2},
								KeyColumnNames:      []string{"c2"},
								KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
								KeySuffixColumnIDs:  []descpb.ColumnID{3},
							},
						},
						Direction: descpb.DescriptorMutation_ADD,
						State:     descpb.DescriptorMutation_DELETE_ONLY,
					},
					{
						Descriptor_: &descpb.DescriptorMutation_PrimaryKeySwap{
							PrimaryKeySwap: &descpb.PrimaryKeySwap{
								OldPrimaryIndexId: 1,
								NewPrimaryIndexId: 3,
								NewIndexes:        []descpb.IndexID{4},
								OldIndexes:        []descpb.IndexID{2},
							},
						},
						Direction: descpb.DescriptorMutation_ADD,
						State:     descpb.DescriptorMutation_DELETE_ONLY,
					},
				},
				NextColumnID: 4,
				NextFamilyID: 1,
				NextIndexID:  5,
				Privileges:   catpb.NewBasePrivilegeDescriptor(security.AdminRoleName()),
			}},
		{`index "new_sec" cannot store virtual column "c3"`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "c1"},
					{ID: 2, Name: "c2"},
					{ID: 3, Name: "c3", ComputeExpr: &computedExpr, Virtual: true},
				},
				PrimaryIndex: descpb.IndexDescriptor{
					ID:                  1,
					Name:                "primary",
					Unique:              true,
					KeyColumnIDs:        []descpb.ColumnID{1, 3},
					KeyColumnNames:      []string{"c1", "c3"},
					KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC, descpb.IndexDescriptor_ASC},
					Version:             descpb.PrimaryIndexWithStoredColumnsVersion,
					EncodingType:        descpb.PrimaryIndexEncoding,
				},
				Indexes: []descpb.IndexDescriptor{
					{ID: 2, Name: "sec", KeyColumnIDs: []descpb.ColumnID{2},
						KeyColumnNames:      []string{"c2"},
						KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
						KeySuffixColumnIDs:  []descpb.ColumnID{1, 3},
					},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary",
						ColumnIDs:   []descpb.ColumnID{1, 2},
						ColumnNames: []string{"c1", "c2"},
					},
				},
				Mutations: []descpb.DescriptorMutation{
					{
						Descriptor_: &descpb.DescriptorMutation_Index{
							Index: &descpb.IndexDescriptor{
								ID:                  3,
								Name:                "new_primary_key",
								Unique:              true,
								KeyColumnIDs:        []descpb.ColumnID{1},
								KeyColumnNames:      []string{"c1"},
								KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
								Version:             descpb.PrimaryIndexWithStoredColumnsVersion,
								EncodingType:        descpb.PrimaryIndexEncoding,
							},
						},
						Direction: descpb.DescriptorMutation_ADD,
						State:     descpb.DescriptorMutation_DELETE_ONLY,
					},
					{
						Descriptor_: &descpb.DescriptorMutation_Index{
							Index: &descpb.IndexDescriptor{
								ID: 4, Name: "new_sec", KeyColumnIDs: []descpb.ColumnID{2},
								KeyColumnNames:      []string{"c2"},
								KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
								KeySuffixColumnIDs:  []descpb.ColumnID{1, 3},
							},
						},
						Direction: descpb.DescriptorMutation_ADD,
						State:     descpb.DescriptorMutation_DELETE_ONLY,
					},
					{
						Descriptor_: &descpb.DescriptorMutation_PrimaryKeySwap{
							PrimaryKeySwap: &descpb.PrimaryKeySwap{
								OldPrimaryIndexId: 1,
								NewPrimaryIndexId: 3,
								NewIndexes:        []descpb.IndexID{4},
								OldIndexes:        []descpb.IndexID{2},
							},
						},
						Direction: descpb.DescriptorMutation_ADD,
						State:     descpb.DescriptorMutation_DELETE_ONLY,
					},
				},
				NextColumnID: 4,
				NextFamilyID: 1,
				NextIndexID:  5,
				Privileges:   catpb.NewBasePrivilegeDescriptor(security.AdminRoleName()),
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
						Version:             descpb.PrimaryIndexWithStoredColumnsVersion,
					},
				},
				NextColumnID: 3,
				NextFamilyID: 1,
				NextIndexID:  3,
			}},
		{`computed column "bar" cannot also have an ON UPDATE expression`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{
						ID:           1,
						Name:         "bar",
						ComputeExpr:  proto.String("'blah'"),
						OnUpdateExpr: proto.String("'blah'"),
					},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
			}},
		{`both generated identity and on update expression specified for column "bar"`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{
						ID:                      1,
						Name:                    "bar",
						GeneratedAsIdentityType: catpb.GeneratedAsIdentityType_GENERATED_ALWAYS,
						OnUpdateExpr:            proto.String("'blah'"),
					},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
			}},
		{`both generated identity and on update expression specified for column "bar"`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{
						ID:                      1,
						Name:                    "bar",
						GeneratedAsIdentityType: catpb.GeneratedAsIdentityType_GENERATED_BY_DEFAULT,
						OnUpdateExpr:            proto.String("'blah'"),
					},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
			}},
		{`conflicting NULL/NOT NULL declarations for column "bar"`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar", Nullable: true,
						GeneratedAsIdentityType: catpb.GeneratedAsIdentityType_GENERATED_ALWAYS,
					},
				},
				NextColumnID: 3,
			}},
		{`conflicting NULL/NOT NULL declarations for column "bar"`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar", Nullable: true,
						GeneratedAsIdentityType: catpb.GeneratedAsIdentityType_GENERATED_BY_DEFAULT,
					},
				},
				NextColumnID: 3,
			}},
		{`both generated identity and computed expression specified for column "bar"`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar", GeneratedAsIdentityType: catpb.GeneratedAsIdentityType_GENERATED_ALWAYS,
						ComputeExpr: &computedExpr},
				},
				NextColumnID: 3,
			}},
		{`both generated identity and computed expression specified for column "bar"`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar", GeneratedAsIdentityType: catpb.GeneratedAsIdentityType_GENERATED_BY_DEFAULT,
						ComputeExpr: &computedExpr},
				},
				NextColumnID: 3,
			}},
		{`conflicting NULL/NOT NULL declarations for column "bar"`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar", Nullable: true,
						GeneratedAsIdentityType:           catpb.GeneratedAsIdentityType_GENERATED_ALWAYS,
						GeneratedAsIdentitySequenceOption: &generatedAsIdentitySequenceOptionExpr,
					},
				},
				NextColumnID: 3,
			}},
		{`conflicting NULL/NOT NULL declarations for column "bar"`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar", Nullable: true,
						GeneratedAsIdentityType:           catpb.GeneratedAsIdentityType_GENERATED_BY_DEFAULT,
						GeneratedAsIdentitySequenceOption: &generatedAsIdentitySequenceOptionExpr,
					},
				},
				NextColumnID: 3,
			}},
		{`both generated identity and computed expression specified for column "bar"`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar", GeneratedAsIdentityType: catpb.GeneratedAsIdentityType_GENERATED_ALWAYS,
						GeneratedAsIdentitySequenceOption: &generatedAsIdentitySequenceOptionExpr,
						ComputeExpr:                       &computedExpr},
				},
				NextColumnID: 3,
			}},
		{`both generated identity and computed expression specified for column "bar"`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar", GeneratedAsIdentityType: catpb.GeneratedAsIdentityType_GENERATED_BY_DEFAULT,
						GeneratedAsIdentitySequenceOption: &generatedAsIdentitySequenceOptionExpr,
						ComputeExpr:                       &computedExpr},
				},
				NextColumnID: 3,
			}},
		{`computed column "bar" cannot also have a DEFAULT expression`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{
						ID:          1,
						Name:        "bar",
						ComputeExpr: &computedExpr,
						DefaultExpr: &computedExpr,
					},
				},
				NextColumnID: 2,
			}},
		{`computed column "bar" cannot also have an ON UPDATE expression`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{
						ID:           1,
						Name:         "bar",
						ComputeExpr:  &computedExpr,
						OnUpdateExpr: &computedExpr,
					},
				},
				NextColumnID: 2,
			}},
		{`non-index mutation in state BACKFILLING`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "c1"},
				},
				Mutations: []descpb.DescriptorMutation{
					{
						Descriptor_: &descpb.DescriptorMutation_Column{
							Column: &descpb.ColumnDescriptor{
								ID:   2,
								Name: "c2",
							},
						},
						Direction: descpb.DescriptorMutation_ADD,
						State:     descpb.DescriptorMutation_BACKFILLING,
					},
				},
				NextColumnID: 3,
			}},
		{`non-index mutation in state MERGING`,
			descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "c1"},
				},
				Mutations: []descpb.DescriptorMutation{
					{
						Descriptor_: &descpb.DescriptorMutation_Column{
							Column: &descpb.ColumnDescriptor{
								ID:   2,
								Name: "c2",
							},
						},
						Direction: descpb.DescriptorMutation_ADD,
						State:     descpb.DescriptorMutation_MERGING,
					},
				},
				NextColumnID: 3,
			}},
		{`public index "ruroh" is using the delete preserving encoding`,
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
					ID:                  1,
					Name:                "primary",
					Unique:              true,
					KeyColumnIDs:        []descpb.ColumnID{1},
					KeyColumnNames:      []string{"c1"},
					KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
					Version:             descpb.PrimaryIndexWithStoredColumnsVersion,
					EncodingType:        descpb.PrimaryIndexEncoding,
				},
				Indexes: []descpb.IndexDescriptor{
					{
						ID:                          2,
						Name:                        "ruroh",
						KeyColumnIDs:                []descpb.ColumnID{2},
						KeyColumnNames:              []string{"c2"},
						KeyColumnDirections:         []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
						Version:                     descpb.PrimaryIndexWithStoredColumnsVersion,
						UseDeletePreservingEncoding: true,
					},
				},
				NextColumnID: 3,
				NextIndexID:  3,
				NextFamilyID: 1,
			}},
		{`public index "primary" is using the delete preserving encoding`,
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
					ID:                          1,
					Name:                        "primary",
					Unique:                      true,
					KeyColumnIDs:                []descpb.ColumnID{1},
					KeyColumnNames:              []string{"c1"},
					KeyColumnDirections:         []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
					Version:                     descpb.PrimaryIndexWithStoredColumnsVersion,
					EncodingType:                descpb.PrimaryIndexEncoding,
					UseDeletePreservingEncoding: true,
				},
				NextColumnID: 3,
				NextIndexID:  2,
				NextFamilyID: 1,
			},
		},
		{`column ID 123 found in depended-on-by references, no such column in this relation`,
			descpb.TableDescriptor{
				Name:                    "foo",
				ID:                      51,
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				PrimaryIndex: descpb.IndexDescriptor{
					ID:             1,
					Name:           "primary",
					KeyColumnIDs:   []descpb.ColumnID{1},
					KeyColumnNames: []string{"a"},
				},
				Columns: []descpb.ColumnDescriptor{
					{Name: "a", ID: 1, Type: types.Int},
				},
				DependedOnBy: []descpb.TableDescriptor_Reference{
					{ID: 52, ColumnIDs: []descpb.ColumnID{123}},
				},
			},
		},
		{`index ID 123 found in depended-on-by references, no such index in this relation`,
			descpb.TableDescriptor{
				Name:                    "foo",
				ID:                      51,
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				PrimaryIndex: descpb.IndexDescriptor{
					ID:             1,
					Name:           "primary",
					KeyColumnIDs:   []descpb.ColumnID{1},
					KeyColumnNames: []string{"a"},
				},
				Columns: []descpb.ColumnDescriptor{
					{Name: "a", ID: 1, Type: types.Int},
				},
				DependedOnBy: []descpb.TableDescriptor_Reference{
					{ID: 52, IndexID: 123},
				},
			},
		},
	}
	for i, d := range testData {
		t.Run(d.err, func(t *testing.T) {
			d.desc.Privileges = catpb.NewBasePrivilegeDescriptor(security.RootUserName())
			desc := NewBuilder(&d.desc).BuildImmutableTable()
			expectedErr := fmt.Sprintf("%s %q (%d): %s", desc.DescriptorType(), desc.GetName(), desc.GetID(), d.err)
			err := validate.Self(clusterversion.TestingClusterVersion, desc)
			if d.err == "" && err != nil {
				t.Errorf("%d: expected success, but found error: \"%+v\"", i, err)
			} else if d.err != "" && err == nil {
				t.Errorf("%d: expected \"%s\", but found success: %+v", i, expectedErr, d.desc)
			} else if d.err != "" && expectedErr != err.Error() {
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
			err: `invalid foreign key: missing table=52: referenced table ID 52: referenced descriptor not found`,
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
			err: `invalid foreign key backreference: missing table=52: referenced table ID 52: referenced descriptor not found`,
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
			err: `referenced type ID 500: referenced descriptor not found`,
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
		{ // 5
			err: `referenced type ID 500: referenced descriptor not found`,
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
		{ // 6
			err: `referenced type ID 500: referenced descriptor not found`,
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
		{ // 7
			err: `referenced type ID 500: referenced descriptor not found`,
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
		{ // 8
			err: `referenced type ID 500: referenced descriptor not found`,
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
						Name:         "a",
						ID:           1,
						Type:         types.Int,
						OnUpdateExpr: pointer("a::@100500"),
					},
				},
			},
		},
		// Temporary tables.
		{ // 9
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
		// Views.
		{ // 10
			desc: descpb.TableDescriptor{
				Name:                    "foo",
				ID:                      51,
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				PrimaryIndex: descpb.IndexDescriptor{
					ID:             1,
					Name:           "primary",
					KeyColumnIDs:   []descpb.ColumnID{1},
					KeyColumnNames: []string{"a"},
				},
				Columns: []descpb.ColumnDescriptor{
					{Name: "a", ID: 1, Type: types.Int},
				},
				DependedOnBy: []descpb.TableDescriptor_Reference{
					{ID: 52, ColumnIDs: []descpb.ColumnID{1}},
				},
			},
			otherDescs: []descpb.TableDescriptor{{
				Name:                    "bar",
				ID:                      52,
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				ViewQuery:               "SELECT * FROM foo",
				DependsOn:               []descpb.ID{51},
				Columns: []descpb.ColumnDescriptor{
					{Name: "a", ID: 1, Type: types.Int},
				},
			}},
		},
		{ // 11
			err: `depended-on-by view "bar" (52) has no corresponding depends-on forward reference`,
			desc: descpb.TableDescriptor{
				Name:                    "foo",
				ID:                      51,
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				PrimaryIndex: descpb.IndexDescriptor{
					ID:             1,
					Name:           "primary",
					KeyColumnIDs:   []descpb.ColumnID{1},
					KeyColumnNames: []string{"a"},
				},
				Columns: []descpb.ColumnDescriptor{
					{Name: "a", ID: 1, Type: types.Int},
				},
				DependedOnBy: []descpb.TableDescriptor_Reference{
					{ID: 52, ColumnIDs: []descpb.ColumnID{1}},
				},
			},
			otherDescs: []descpb.TableDescriptor{{
				Name:                    "bar",
				ID:                      52,
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				ViewQuery:               "SELECT a FROM foo",
				Columns: []descpb.ColumnDescriptor{
					{Name: "a", ID: 1, Type: types.Int},
				},
			}},
		},
		{ // 12
			err: `depends-on relation "bar" (52) has no corresponding depended-on-by back reference`,
			desc: descpb.TableDescriptor{
				Name:                    "foo",
				ID:                      51,
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				ViewQuery:               "SELECT a FROM bar",
				DependsOn:               []descpb.ID{52},
				Columns: []descpb.ColumnDescriptor{
					{Name: "a", ID: 1, Type: types.Int},
				},
			},
			otherDescs: []descpb.TableDescriptor{{
				Name:                    "bar",
				ID:                      52,
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				PrimaryIndex: descpb.IndexDescriptor{
					ID:             1,
					Name:           "primary",
					KeyColumnIDs:   []descpb.ColumnID{1},
					KeyColumnNames: []string{"a"},
				},
				Columns: []descpb.ColumnDescriptor{
					{Name: "a", ID: 1, Type: types.Int},
				},
				DependedOnBy: []descpb.TableDescriptor_Reference{{ID: 123}},
			}},
		},
		// Sequences.
		{ // 13
			err: `depended-on-by relation "bar" (52) does not have a column with ID 123`,
			desc: descpb.TableDescriptor{
				Name:                    "foo",
				ID:                      51,
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				SequenceOpts: &descpb.TableDescriptor_SequenceOpts{
					Increment: 1,
				},
				DependedOnBy: []descpb.TableDescriptor_Reference{
					{ID: 52, ColumnIDs: []descpb.ColumnID{123}},
				},
			},
			otherDescs: []descpb.TableDescriptor{{
				Name:                    "bar",
				ID:                      52,
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				PrimaryIndex: descpb.IndexDescriptor{
					ID:             1,
					Name:           "primary",
					KeyColumnIDs:   []descpb.ColumnID{1},
					KeyColumnNames: []string{"a"},
				},
				Columns: []descpb.ColumnDescriptor{
					{Name: "a", ID: 1, Type: types.Int},
				},
			}},
		},
	}

	for i, test := range tests {
		t.Run(test.err, func(t *testing.T) {
			var cb nstree.MutableCatalog
			cb.UpsertDescriptorEntry(dbdesc.NewBuilder(&descpb.DatabaseDescriptor{ID: 1}).BuildImmutable())
			for _, otherDesc := range test.otherDescs {
				otherDesc.Privileges = catpb.NewBasePrivilegeDescriptor(security.AdminRoleName())
				cb.UpsertDescriptorEntry(NewBuilder(&otherDesc).BuildImmutable())
			}
			desc := NewBuilder(&test.desc).BuildImmutable()
			expectedErr := fmt.Sprintf("%s %q (%d): %s", desc.DescriptorType(), desc.GetName(), desc.GetID(), test.err)
			const validateCrossReferencesOnly = catalog.ValidationLevelCrossReferences &^ (catalog.ValidationLevelCrossReferences >> 1)
			results := cb.Validate(ctx, clusterversion.TestingClusterVersion, catalog.NoValidationTelemetry, validateCrossReferencesOnly, desc)
			if err := results.CombinedError(); err == nil {
				if test.err != "" {
					t.Errorf("%d: expected \"%s\", but found success: %+v", i, expectedErr, test.desc)
				}
			} else if expectedErr != err.Error() {
				t.Errorf("%d: expected \"%s\", but found \"%s\"", i, expectedErr, err.Error())
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
					Partitioning: catpb.PartitioningDescriptor{
						NumColumns: 1,
					},
				},
			},
		},
		{"PARTITION p1: must contain values",
			descpb.TableDescriptor{
				PrimaryIndex: descpb.IndexDescriptor{
					Partitioning: catpb.PartitioningDescriptor{
						NumColumns: 1,
						List:       []catpb.PartitioningDescriptor_List{{Name: "p1"}},
					},
				},
			},
		},
		{"not enough columns in index for this partitioning",
			descpb.TableDescriptor{
				PrimaryIndex: descpb.IndexDescriptor{
					Partitioning: catpb.PartitioningDescriptor{
						NumColumns: 1,
						List:       []catpb.PartitioningDescriptor_List{{Name: "p1", Values: [][]byte{{}}}},
					},
				},
			},
		},
		{"only one LIST or RANGE partitioning may used",
			descpb.TableDescriptor{
				PrimaryIndex: descpb.IndexDescriptor{
					KeyColumnIDs:        []descpb.ColumnID{1},
					KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
					Partitioning: catpb.PartitioningDescriptor{
						NumColumns: 1,
						List:       []catpb.PartitioningDescriptor_List{{}},
						Range:      []catpb.PartitioningDescriptor_Range{{}},
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
					Partitioning: catpb.PartitioningDescriptor{
						NumColumns: 1,
						List:       []catpb.PartitioningDescriptor_List{{}},
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
					Partitioning: catpb.PartitioningDescriptor{
						NumColumns: 1,
						List:       []catpb.PartitioningDescriptor_List{{Name: "p1"}},
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
					Partitioning: catpb.PartitioningDescriptor{
						NumColumns: 1,
						List: []catpb.PartitioningDescriptor_List{{
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
					Partitioning: catpb.PartitioningDescriptor{
						NumColumns: 1,
						List: []catpb.PartitioningDescriptor_List{
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
					Partitioning: catpb.PartitioningDescriptor{
						NumColumns: 1,
						List: []catpb.PartitioningDescriptor_List{
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
					Partitioning: catpb.PartitioningDescriptor{
						NumColumns: 1,
						Range: []catpb.PartitioningDescriptor_Range{
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
					Partitioning: catpb.PartitioningDescriptor{
						NumColumns: 1,
						List: []catpb.PartitioningDescriptor_List{
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
					Partitioning: catpb.PartitioningDescriptor{
						NumColumns: 1,
						List: []catpb.PartitioningDescriptor_List{{
							Name:   "p1",
							Values: [][]byte{{0x03, 0x02}},
							Subpartitioning: catpb.PartitioningDescriptor{
								NumColumns: 1,
								List:       []catpb.PartitioningDescriptor_List{{Name: "p1_1", Values: [][]byte{{}}}},
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
					Partitioning: catpb.PartitioningDescriptor{
						NumColumns: 1,
						List: []catpb.PartitioningDescriptor_List{
							{Name: "p1", Values: [][]byte{{0x03, 0x02}}},
							{
								Name:   "p2",
								Values: [][]byte{{0x03, 0x04}},
								Subpartitioning: catpb.PartitioningDescriptor{
									NumColumns: 1,
									List: []catpb.PartitioningDescriptor_List{
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

func TestValidateConstraintID(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		err  string
		desc descpb.TableDescriptor
	}{
		{`constraint id was missing for constraint: PRIMARY KEY with name \"primary\"`,
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
					KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC}},
				NextColumnID: 2,
				NextFamilyID: 1,
				Privileges: catpb.NewPrivilegeDescriptor(
					security.PublicRoleName(),
					privilege.SchemaPrivileges,
					privilege.List{},
					security.RootUserName()),
			}},
		{`constraint id was missing for constraint: UNIQUE with name \"secondary\"`,
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
				Indexes: []descpb.IndexDescriptor{
					{
						ID: 1, Name: "secondary", KeyColumnIDs: []descpb.ColumnID{1}, KeyColumnNames: []string{"bar"},
						KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
						Unique:              true,
					},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
				Privileges: catpb.NewPrivilegeDescriptor(
					security.PublicRoleName(),
					privilege.SchemaPrivileges,
					privilege.List{},
					security.RootUserName()),
			}},
		{`constraint id was missing for constraint: UNIQUE with name \"bad\"`,
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
				UniqueWithoutIndexConstraints: []descpb.UniqueWithoutIndexConstraint{
					{Name: "bad"},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
				Privileges: catpb.NewPrivilegeDescriptor(
					security.PublicRoleName(),
					privilege.SchemaPrivileges,
					privilege.List{},
					security.RootUserName()),
			}},
		{`constraint id was missing for constraint: CHECK with name \"bad\"`,
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
				Checks: []*descpb.TableDescriptor_CheckConstraint{
					{Name: "bad"},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
				Privileges: catpb.NewPrivilegeDescriptor(
					security.PublicRoleName(),
					privilege.SchemaPrivileges,
					privilege.List{},
					security.RootUserName()),
			}},
	}
	for i, test := range tests {
		t.Run(test.err, func(t *testing.T) {
			desc := NewBuilder(&test.desc).BuildImmutableTable()
			err := ValidateConstraints(desc)
			if !testutils.IsError(err, test.err) {
				t.Errorf(`%d: got "%v" expected "%v"`, i, err, test.err)
			}
		})
	}
}
