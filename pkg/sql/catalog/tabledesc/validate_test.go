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
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
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
			"CreateQuery":    {status: thisFieldReferencesNoObjects},
			"CreateAsOfTime": {status: thisFieldReferencesNoObjects},
			"OutboundFKs":    {status: iSolemnlySwearThisFieldIsValidated},
			"InboundFKs":     {status: iSolemnlySwearThisFieldIsValidated},
			"Temporary":      {status: thisFieldReferencesNoObjects},
			"LocalityConfig": {status: iSolemnlySwearThisFieldIsValidated},
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
