// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tabledesc

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/internal/validate"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/semenumpb"
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
	// validate method.
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
			"IsMaterializedView":  {status: thisFieldReferencesNoObjects},
			"RefreshViewRequired": {status: thisFieldReferencesNoObjects},
			"DependsOn":           {status: iSolemnlySwearThisFieldIsValidated},
			"DependsOnTypes":      {status: iSolemnlySwearThisFieldIsValidated},
			"DependsOnFunctions":  {status: iSolemnlySwearThisFieldIsValidated},
			"DependedOnBy":        {status: iSolemnlySwearThisFieldIsValidated},
			"MutationJobs":        {status: thisFieldReferencesNoObjects},
			"SequenceOpts": {status: todoIAmKnowinglyAddingTechDebt,
				reason: "initial import: TODO(features): add validation"},
			"DropTime": {status: thisFieldReferencesNoObjects},
			"ReplacementOf": {status: todoIAmKnowinglyAddingTechDebt,
				reason: "initial import: TODO(bulkio): add validation"},
			"AuditMode":                     {status: thisFieldReferencesNoObjects},
			"DropJobID":                     {status: thisFieldReferencesNoObjects},
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
			"AutoStatsSettings":             {status: iSolemnlySwearThisFieldIsValidated},
			"ForecastStats":                 {status: thisFieldReferencesNoObjects},
			"ImportStartWallTime":           {status: thisFieldReferencesNoObjects},
			"HistogramBuckets":              {status: thisFieldReferencesNoObjects},
			"HistogramSamples":              {status: thisFieldReferencesNoObjects},
			"SchemaLocked":                  {status: thisFieldReferencesNoObjects},
			"ImportEpoch":                   {status: thisFieldReferencesNoObjects},
			"ImportType":                    {status: thisFieldReferencesNoObjects},
			"External": {status: todoIAmKnowinglyAddingTechDebt,
				reason: "TODO(features): add validation that TableID is sane within the same tenant"},
			// LDRJobIDs is checked in StripDanglingBackreferences.
			"LDRJobIDs":            {status: iSolemnlySwearThisFieldIsValidated},
			"ReplicatedPCRVersion": {status: thisFieldReferencesNoObjects},
			"Triggers":             {status: iSolemnlySwearThisFieldIsValidated},
			"NextTriggerID":        {status: thisFieldReferencesNoObjects},
			"Policies":             {status: iSolemnlySwearThisFieldIsValidated},
			"NextPolicyID":         {status: iSolemnlySwearThisFieldIsValidated},
		},
	},
	{
		obj: descpb.IndexDescriptor{},
		fieldMap: map[string]validationStatusInfo{
			"Name":   {status: thisFieldReferencesNoObjects},
			"ID":     {status: thisFieldReferencesNoObjects},
			"Unique": {status: thisFieldReferencesNoObjects},
			// NotVisible is deprecated in favor of Invisibility.
			"NotVisible":          {status: thisFieldReferencesNoObjects},
			"Invisibility":        {status: iSolemnlySwearThisFieldIsValidated},
			"Version":             {status: thisFieldReferencesNoObjects},
			"KeyColumnNames":      {status: iSolemnlySwearThisFieldIsValidated},
			"KeyColumnDirections": {status: iSolemnlySwearThisFieldIsValidated},
			"StoreColumnNames":    {status: iSolemnlySwearThisFieldIsValidated},
			"InvertedColumnKinds": {status: thisFieldReferencesNoObjects},
			"KeyColumnIDs":        {status: iSolemnlySwearThisFieldIsValidated},
			"KeySuffixColumnIDs":  {status: iSolemnlySwearThisFieldIsValidated},
			"StoreColumnIDs":      {status: iSolemnlySwearThisFieldIsValidated},
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
			"UsesFunctionIds":           {status: iSolemnlySwearThisFieldIsValidated},
		},
	},
	{
		obj: descpb.ForeignKeyConstraint{},
		fieldMap: map[string]validationStatusInfo{
			"OriginTableID":       {status: iSolemnlySwearThisFieldIsValidated},
			"OriginColumnIDs":     {status: iSolemnlySwearThisFieldIsValidated},
			"ReferencedColumnIDs": {status: iSolemnlySwearThisFieldIsValidated},
			"ReferencedTableID":   {status: iSolemnlySwearThisFieldIsValidated},
			"Name":                {status: thisFieldReferencesNoObjects},
			"Validity":            {status: thisFieldReferencesNoObjects},
			"OnDelete":            {status: thisFieldReferencesNoObjects},
			"OnUpdate":            {status: thisFieldReferencesNoObjects},
			"Match":               {status: thisFieldReferencesNoObjects},
			"ConstraintID":        {status: iSolemnlySwearThisFieldIsValidated},
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
			"Composite":                     {status: iSolemnlySwearThisFieldIsValidated},
			"ReplicatedPCRVersion":          {status: thisFieldReferencesNoObjects},
		},
	},
	{
		obj: descpb.DatabaseDescriptor{},
		fieldMap: map[string]validationStatusInfo{
			"Name":                          {status: iSolemnlySwearThisFieldIsValidated},
			"ID":                            {status: iSolemnlySwearThisFieldIsValidated},
			"Version":                       {status: thisFieldReferencesNoObjects},
			"ModificationTime":              {status: thisFieldReferencesNoObjects},
			"Privileges":                    {status: iSolemnlySwearThisFieldIsValidated},
			"Schemas":                       {status: iSolemnlySwearThisFieldIsValidated},
			"State":                         {status: thisFieldReferencesNoObjects},
			"OfflineReason":                 {status: thisFieldReferencesNoObjects},
			"RegionConfig":                  {status: iSolemnlySwearThisFieldIsValidated},
			"DefaultPrivileges":             {status: iSolemnlySwearThisFieldIsValidated},
			"DeclarativeSchemaChangerState": {status: thisFieldReferencesNoObjects},
			"SystemDatabaseSchemaVersion":   {status: iSolemnlySwearThisFieldIsValidated},
			"ReplicatedPCRVersion":          {status: thisFieldReferencesNoObjects},
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
			"Functions":                     {status: iSolemnlySwearThisFieldIsValidated},
			"ReplicatedPCRVersion":          {status: thisFieldReferencesNoObjects},
		},
	},
	{
		obj: catpb.AutoStatsSettings{},
		fieldMap: map[string]validationStatusInfo{
			"Enabled":                  {status: iSolemnlySwearThisFieldIsValidated},
			"MinStaleRows":             {status: iSolemnlySwearThisFieldIsValidated},
			"FractionStaleRows":        {status: iSolemnlySwearThisFieldIsValidated},
			"PartialEnabled":           {status: iSolemnlySwearThisFieldIsValidated},
			"PartialMinStaleRows":      {status: iSolemnlySwearThisFieldIsValidated},
			"PartialFractionStaleRows": {status: iSolemnlySwearThisFieldIsValidated},
		},
	},
	{
		obj: descpb.FunctionDescriptor{},
		fieldMap: map[string]validationStatusInfo{
			"Name":                          {status: iSolemnlySwearThisFieldIsValidated},
			"ID":                            {status: iSolemnlySwearThisFieldIsValidated},
			"ParentID":                      {status: iSolemnlySwearThisFieldIsValidated},
			"ParentSchemaID":                {status: iSolemnlySwearThisFieldIsValidated},
			"Params":                        {status: iSolemnlySwearThisFieldIsValidated},
			"ReturnType":                    {status: iSolemnlySwearThisFieldIsValidated},
			"Lang":                          {status: thisFieldReferencesNoObjects},
			"FunctionBody":                  {status: thisFieldReferencesNoObjects},
			"Volatility":                    {status: iSolemnlySwearThisFieldIsValidated},
			"LeakProof":                     {status: iSolemnlySwearThisFieldIsValidated},
			"NullInputBehavior":             {status: thisFieldReferencesNoObjects},
			"Privileges":                    {status: iSolemnlySwearThisFieldIsValidated},
			"DependsOn":                     {status: iSolemnlySwearThisFieldIsValidated},
			"DependsOnTypes":                {status: iSolemnlySwearThisFieldIsValidated},
			"DependedOnBy":                  {status: iSolemnlySwearThisFieldIsValidated},
			"DependsOnFunctions":            {status: iSolemnlySwearThisFieldIsValidated},
			"State":                         {status: thisFieldReferencesNoObjects},
			"OfflineReason":                 {status: thisFieldReferencesNoObjects},
			"ModificationTime":              {status: thisFieldReferencesNoObjects},
			"Version":                       {status: thisFieldReferencesNoObjects},
			"DeclarativeSchemaChangerState": {status: thisFieldReferencesNoObjects},
			"IsProcedure":                   {status: thisFieldReferencesNoObjects},
			"Security":                      {status: thisFieldReferencesNoObjects},
			"ReplicatedPCRVersion":          {status: thisFieldReferencesNoObjects},
		},
	},
	{
		obj: descpb.TriggerDescriptor{},
		fieldMap: map[string]validationStatusInfo{
			"ID":                 {status: iSolemnlySwearThisFieldIsValidated},
			"Name":               {status: iSolemnlySwearThisFieldIsValidated},
			"ActionTime":         {status: thisFieldReferencesNoObjects},
			"Events":             {status: iSolemnlySwearThisFieldIsValidated},
			"NewTransitionAlias": {status: thisFieldReferencesNoObjects},
			"OldTransitionAlias": {status: thisFieldReferencesNoObjects},
			"ForEachRow":         {status: thisFieldReferencesNoObjects},
			"WhenExpr":           {status: iSolemnlySwearThisFieldIsValidated},
			"FuncID":             {status: iSolemnlySwearThisFieldIsValidated},
			"FuncArgs":           {status: thisFieldReferencesNoObjects},
			"FuncBody":           {status: iSolemnlySwearThisFieldIsValidated},
			"Enabled":            {status: thisFieldReferencesNoObjects},
			"DependsOn":          {status: iSolemnlySwearThisFieldIsValidated},
			"DependsOnTypes":     {status: iSolemnlySwearThisFieldIsValidated},
			"DependsOnRoutines":  {status: iSolemnlySwearThisFieldIsValidated},
		},
	},
}

type validationStatusInfo struct {
	status validateStatus
	reason string
}

// ModifyDescriptor is a helper function that invokes the provided closure with
// a predefined, valid, TableDescriptor and then returns the modified result.
// Usage:
//
//	testCaseInvalidDecField: ModifyDescriptor(func(desc *descpb.TableDescriptor) {
//		desc.InboundFKs = nil  // Clear InbounFKs to cause an error.
//	})
func ModifyDescriptor(fn func(*descpb.TableDescriptor)) descpb.TableDescriptor {
	validDesc := descpb.TableDescriptor{
		ID:            4,
		ParentID:      1,
		Name:          "bar",
		FormatVersion: descpb.InterleavedFormatVersion,
		Columns: []descpb.ColumnDescriptor{
			{ID: 1, Name: "bar", Type: types.String},
		},
		Families: []descpb.ColumnFamilyDescriptor{
			{ID: 0, Name: "primary", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar"}},
		},
		PrimaryIndex: descpb.IndexDescriptor{ID: 1, Name: "bar", ConstraintID: 1,
			KeyColumnIDs: []descpb.ColumnID{1}, KeyColumnNames: []string{"bar"},
			KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
			EncodingType:        catenumpb.PrimaryIndexEncoding,
			Version:             descpb.LatestIndexDescriptorVersion,
		},
		OutboundFKs: []descpb.ForeignKeyConstraint{
			{
				ConstraintID:        2,
				Name:                "to_this_table",
				OriginTableID:       4,
				OriginColumnIDs:     []descpb.ColumnID{1},
				ReferencedTableID:   25,
				ReferencedColumnIDs: []descpb.ColumnID{2},
			},
		},
		InboundFKs: []descpb.ForeignKeyConstraint{
			{
				ConstraintID:        2,
				Name:                "from_this_table",
				OriginTableID:       36,
				OriginColumnIDs:     []descpb.ColumnID{3},
				ReferencedTableID:   4,
				ReferencedColumnIDs: []descpb.ColumnID{1},
			},
		},
		NextColumnID:     2,
		NextFamilyID:     1,
		NextIndexID:      3,
		NextConstraintID: 3,
	}
	fn(&validDesc)
	return validDesc
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
	boolTrue := true
	negativeOne := int64(-1)
	negativeOneFloat := float64(-1)
	pointer := func(s string) *string {
		return &s
	}

	testData := []struct {
		err     string
		desc    descpb.TableDescriptor
		version clusterversion.Key
	}{
		{err: `empty relation name`,
			desc: descpb.TableDescriptor{}},
		{err: `invalid table ID 0`,
			desc: descpb.TableDescriptor{ID: 0, Name: "foo"}},
		{err: `invalid parent ID 0`,
			desc: descpb.TableDescriptor{ID: 2, Name: "foo"}},
		{err: `table must contain at least 1 column`,
			desc: descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
			}},
		{err: `empty column name`,
			desc: descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 0},
				},
				NextColumnID: 2,
			}},
		{err: `table is encoded using using version 0, but this client only supports version 3`,
			desc: descpb.TableDescriptor{
				ID:       2,
				ParentID: 1,
				Name:     "foo",
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				NextColumnID: 2,
			}},
		{err: `virtual column "virt" is not computed`,
			desc: descpb.TableDescriptor{
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
		{err: `invalid column ID 0`,
			desc: descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 0, Name: "bar"},
				},
				NextColumnID: 2,
			}},
		{err: `table must contain a primary key`,
			desc: descpb.TableDescriptor{
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
		{err: `duplicate column name: "bar"`,
			desc: descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
					{ID: 2, Name: "bar"},
				},
				NextColumnID: 3,
			}},
		{err: `duplicate column name: "bar"`,
			desc: descpb.TableDescriptor{
				ID:            catconstants.CrdbInternalBackwardDependenciesTableID,
				ParentID:      0,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
					{ID: 2, Name: "bar"},
				},
				NextColumnID: 3,
			}},
		{err: `column "blah" duplicate ID of column "bar": 1`,
			desc: descpb.TableDescriptor{
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
		{err: `at least 1 column family must be specified`,
			desc: descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				NextColumnID: 2,
			}},
		{err: `column "bar" cannot be hidden and inaccessible`,
			desc: descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.FamilyFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar", Hidden: true, Inaccessible: true},
				},
				NextColumnID: 2,
			}},
		{err: `the 0th family must have ID 0`,
			desc: descpb.TableDescriptor{
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
		{err: `duplicate family name: "baz"`,
			desc: descpb.TableDescriptor{
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
		{err: `family "qux" duplicate ID of family "baz": 0`,
			desc: descpb.TableDescriptor{
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
		{err: `duplicate family name: "baz"`,
			desc: descpb.TableDescriptor{
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
		{err: `mismatched column ID size (1) and name size (0)`,
			desc: descpb.TableDescriptor{
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
		{err: `family "baz" contains column reference "bar" with unknown ID 2`,
			desc: descpb.TableDescriptor{
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
		{err: `family "baz" column 1 should have name "bar", but found name "qux"`,
			desc: descpb.TableDescriptor{
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
		{err: `column "bar" is not in any column family`,
			desc: descpb.TableDescriptor{
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
		{err: `column 1 is in both family 0 and 1`,
			desc: descpb.TableDescriptor{
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
		{err: `virtual computed column "virt" cannot be part of a family`,
			desc: descpb.TableDescriptor{
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
		{err: `table must contain a primary key`,
			desc: descpb.TableDescriptor{
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
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC}},
				NextColumnID: 2,
				NextFamilyID: 1,
			}},
		{err: `primary index "p_idx" cannot be not visible`,
			desc: descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar", Type: types.Int},
				},
				PrimaryIndex: descpb.IndexDescriptor{
					ID:                  1,
					Name:                "p_idx",
					KeyColumnIDs:        []descpb.ColumnID{1},
					KeyColumnNames:      []string{"bar"},
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
					Version:             descpb.PrimaryIndexWithStoredColumnsVersion,
					EncodingType:        catenumpb.PrimaryIndexEncoding,
					ConstraintID:        1,
					NotVisible:          true,
					Invisibility:        1.0,
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary",
						ColumnIDs:   []descpb.ColumnID{1},
						ColumnNames: []string{"bar"},
					},
				},
				NextColumnID:     2,
				NextConstraintID: 2,
				NextFamilyID:     1,
				NextIndexID:      2,
			}},
		{err: `invalid index ID 0`,
			desc: descpb.TableDescriptor{
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
					Name:                "bar",
					ConstraintID:        1,
					KeyColumnIDs:        []descpb.ColumnID{0},
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC}},
				NextColumnID:     2,
				NextFamilyID:     1,
				NextConstraintID: 2,
			}},
		{err: `index "bar" must contain at least 1 column`,
			desc: descpb.TableDescriptor{
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
					ID:                  1,
					Name:                "primary",
					ConstraintID:        1,
					KeyColumnIDs:        []descpb.ColumnID{1},
					KeyColumnNames:      []string{"bar"},
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
					EncodingType:        catenumpb.PrimaryIndexEncoding,
					Version:             descpb.LatestIndexDescriptorVersion,
				},
				Indexes: []descpb.IndexDescriptor{
					{ID: 2, Name: "bar"},
				},
				NextColumnID:     2,
				NextFamilyID:     1,
				NextIndexID:      3,
				NextConstraintID: 2,
			}},
		{err: `mismatched column IDs (1) and names (0)`,
			desc: descpb.TableDescriptor{
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
					ID:           1,
					Name:         "bar",
					ConstraintID: 1,
					KeyColumnIDs: []descpb.ColumnID{1},
				},
				NextColumnID:     2,
				NextFamilyID:     1,
				NextIndexID:      2,
				NextConstraintID: 2,
			}},
		{err: `mismatched column IDs (1) and names (2)`,
			desc: descpb.TableDescriptor{
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
				PrimaryIndex: descpb.IndexDescriptor{ID: 1, Name: "bar", ConstraintID: 1,
					KeyColumnIDs: []descpb.ColumnID{1}, KeyColumnNames: []string{"bar", "blah"},
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
				},
				NextColumnID:     3,
				NextFamilyID:     1,
				NextIndexID:      2,
				NextConstraintID: 2,
			}},
		{err: `duplicate index name: "bar"`,
			desc: descpb.TableDescriptor{
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
				PrimaryIndex: descpb.IndexDescriptor{ID: 1, Name: "bar", ConstraintID: 1,
					KeyColumnIDs: []descpb.ColumnID{1}, KeyColumnNames: []string{"bar"},
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
					EncodingType:        catenumpb.PrimaryIndexEncoding,
					Version:             descpb.LatestIndexDescriptorVersion,
				},
				Indexes: []descpb.IndexDescriptor{
					{ID: 2, Name: "bar", KeyColumnIDs: []descpb.ColumnID{1},
						KeyColumnNames:      []string{"bar"},
						KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
					},
				},
				NextColumnID:     2,
				NextFamilyID:     1,
				NextIndexID:      3,
				NextConstraintID: 2,
			}},
		{err: `index "blah" duplicate ID of index "bar": 1`,
			desc: descpb.TableDescriptor{
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
				PrimaryIndex: descpb.IndexDescriptor{ID: 1, Name: "bar", ConstraintID: 1,
					KeyColumnIDs:        []descpb.ColumnID{1},
					KeyColumnNames:      []string{"bar"},
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
					EncodingType:        catenumpb.PrimaryIndexEncoding,
					Version:             descpb.LatestIndexDescriptorVersion,
				},
				Indexes: []descpb.IndexDescriptor{
					{ID: 1, Name: "blah", KeyColumnIDs: []descpb.ColumnID{1},
						KeyColumnNames:      []string{"bar"},
						KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
					},
				},
				NextColumnID:     2,
				NextFamilyID:     1,
				NextIndexID:      2,
				NextConstraintID: 2,
			}},
		{err: `index "bar" contains key column "bar" with unknown ID 2`,
			desc: descpb.TableDescriptor{
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
					ID:                  1,
					Name:                "bar",
					ConstraintID:        1,
					KeyColumnIDs:        []descpb.ColumnID{2},
					KeyColumnNames:      []string{"bar"},
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
				},
				NextColumnID:     2,
				NextFamilyID:     1,
				NextIndexID:      2,
				NextConstraintID: 2,
			}},
		{err: `index "bar" key column ID 1 should have name "bar", but found name "blah"`,
			desc: descpb.TableDescriptor{
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
					ID:                  1,
					Name:                "bar",
					ConstraintID:        1,
					KeyColumnIDs:        []descpb.ColumnID{1},
					KeyColumnNames:      []string{"blah"},
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
				},
				NextColumnID:     2,
				NextFamilyID:     1,
				NextIndexID:      2,
				NextConstraintID: 2,
			}},
		{err: `mismatched column IDs (1) and directions (0)`,
			desc: descpb.TableDescriptor{
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
				PrimaryIndex: descpb.IndexDescriptor{ID: 1, Name: "bar", ConstraintID: 1,
					KeyColumnIDs:   []descpb.ColumnID{1},
					KeyColumnNames: []string{"blah"},
				},
				NextColumnID:     2,
				NextFamilyID:     1,
				NextIndexID:      2,
				NextConstraintID: 2,
			}},
		{err: `mismatched STORING column IDs (1) and names (0)`,
			desc: descpb.TableDescriptor{
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
					ID: 1, Name: "primary", ConstraintID: 1,
					KeyColumnIDs:        []descpb.ColumnID{1},
					KeyColumnNames:      []string{"c1"},
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
					StoreColumnIDs:      []descpb.ColumnID{2},
				},
				NextColumnID:     3,
				NextFamilyID:     1,
				NextIndexID:      2,
				NextConstraintID: 2,
			}},
		{err: `index "secondary" contains stored column "quux" with unknown ID 123`,
			desc: descpb.TableDescriptor{
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
					ConstraintID:        1,
					KeyColumnIDs:        []descpb.ColumnID{1, 2},
					KeyColumnNames:      []string{"bar", "baz"},
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC, catenumpb.IndexColumn_ASC},
					EncodingType:        catenumpb.PrimaryIndexEncoding,
					Version:             descpb.LatestIndexDescriptorVersion,
				},
				Indexes: []descpb.IndexDescriptor{{
					ID:                  2,
					Name:                "secondary",
					KeyColumnIDs:        []descpb.ColumnID{1},
					KeyColumnNames:      []string{"bar"},
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
					StoreColumnIDs:      []descpb.ColumnID{123},
					StoreColumnNames:    []string{"quux"},
				}},
				NextColumnID:     3,
				NextFamilyID:     1,
				NextIndexID:      3,
				NextConstraintID: 2,
			}},
		{err: `index "secondary" stored column ID 2 should have name "baz", but found name "quux"`,
			desc: descpb.TableDescriptor{
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
					ConstraintID:        1,
					KeyColumnIDs:        []descpb.ColumnID{1, 2, 3},
					KeyColumnNames:      []string{"bar", "baz", "quux"},
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC, catenumpb.IndexColumn_ASC, catenumpb.IndexColumn_ASC},
					EncodingType:        catenumpb.PrimaryIndexEncoding,
					Version:             descpb.LatestIndexDescriptorVersion,
				},
				Indexes: []descpb.IndexDescriptor{{
					ID:                  2,
					Name:                "secondary",
					KeyColumnIDs:        []descpb.ColumnID{2},
					KeyColumnNames:      []string{"baz"},
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
					KeySuffixColumnIDs:  []descpb.ColumnID{1},
					StoreColumnIDs:      []descpb.ColumnID{2},
					StoreColumnNames:    []string{"quux"},
				}},
				NextColumnID:     4,
				NextFamilyID:     1,
				NextIndexID:      3,
				NextConstraintID: 2,
			}},
		{err: `index "secondary" key suffix column ID 123 is invalid`,
			desc: descpb.TableDescriptor{
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
					ConstraintID:        1,
					KeyColumnIDs:        []descpb.ColumnID{1, 2},
					KeyColumnNames:      []string{"bar", "baz"},
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC, catenumpb.IndexColumn_ASC},
					EncodingType:        catenumpb.PrimaryIndexEncoding,
					Version:             descpb.LatestIndexDescriptorVersion,
				},
				Indexes: []descpb.IndexDescriptor{{
					ID:                  2,
					Name:                "secondary",
					KeyColumnIDs:        []descpb.ColumnID{2},
					KeyColumnNames:      []string{"baz"},
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
					KeySuffixColumnIDs:  []descpb.ColumnID{123},
				}},
				NextColumnID:     3,
				NextFamilyID:     1,
				NextIndexID:      3,
				NextConstraintID: 2,
			}},
		{err: `index "primary" contains deprecated foreign key representation`,
			desc: descpb.TableDescriptor{
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
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
					EncodingType:        catenumpb.PrimaryIndexEncoding,
					Version:             descpb.LatestIndexDescriptorVersion,
					ForeignKey:          descpb.ForeignKeyReference{Table: 123, Index: 456},
					ConstraintID:        1,
				},
				NextColumnID:     2,
				NextFamilyID:     1,
				NextIndexID:      2,
				NextConstraintID: 2,
			}},
		{err: `primary index "primary" has invalid encoding type 0 in proto, expected 1`,
			desc: descpb.TableDescriptor{
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
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
					EncodingType:        catenumpb.SecondaryIndexEncoding,
					Version:             descpb.LatestIndexDescriptorVersion,
					ConstraintID:        1,
				},
				NextColumnID:     2,
				NextFamilyID:     1,
				NextIndexID:      2,
				NextConstraintID: 2,
			}},
		{err: `secondary index "secondary" has invalid encoding type 1 in proto, expected 0`,
			desc: descpb.TableDescriptor{
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
				PrimaryIndex: descpb.IndexDescriptor{ID: 1, Name: "primary",
					KeyColumnIDs:        []descpb.ColumnID{1, 2},
					KeyColumnNames:      []string{"bar", "baz"},
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC, catenumpb.IndexColumn_ASC},
					EncodingType:        catenumpb.PrimaryIndexEncoding,
					Version:             descpb.LatestIndexDescriptorVersion,
					ConstraintID:        1,
				},
				Indexes: []descpb.IndexDescriptor{{
					ID:                  2,
					Name:                "secondary",
					KeyColumnIDs:        []descpb.ColumnID{2},
					KeyColumnNames:      []string{"baz"},
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
					EncodingType:        catenumpb.PrimaryIndexEncoding,
				}},
				NextColumnID:     3,
				NextFamilyID:     1,
				NextIndexID:      3,
				NextConstraintID: 2,
			}},
		{err: `primary index "primary" must contain column ID 2 in either key or store columns`,
			desc: descpb.TableDescriptor{
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
					ConstraintID:        1,
					KeyColumnIDs:        []descpb.ColumnID{1},
					KeyColumnNames:      []string{"bar"},
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
					EncodingType:        catenumpb.PrimaryIndexEncoding,
					Version:             descpb.LatestIndexDescriptorVersion,
				},
				NextColumnID:     4,
				NextFamilyID:     1,
				NextIndexID:      2,
				NextConstraintID: 2,
			}},
		{err: `index "primary" cannot store virtual column "baz"`,
			desc: descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
					{ID: 2, Name: "baz", Virtual: true, ComputeExpr: &computedExpr},
					{ID: 3, Name: "quux"},
				},
				Families: []descpb.ColumnFamilyDescriptor{{
					ID:          0,
					Name:        "primary",
					ColumnIDs:   []descpb.ColumnID{1, 3},
					ColumnNames: []string{"bar", "quux"},
				}},
				PrimaryIndex: descpb.IndexDescriptor{
					ID:                  1,
					Name:                "primary",
					ConstraintID:        1,
					KeyColumnIDs:        []descpb.ColumnID{1, 3},
					KeyColumnNames:      []string{"bar", "quux"},
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC, catenumpb.IndexColumn_ASC},
					StoreColumnIDs:      []descpb.ColumnID{2},
					StoreColumnNames:    []string{"baz"},
					EncodingType:        catenumpb.PrimaryIndexEncoding,
					Version:             descpb.LatestIndexDescriptorVersion,
				},
				NextColumnID:     4,
				NextFamilyID:     1,
				NextIndexID:      2,
				NextConstraintID: 2,
			}},
		{err: `index "primary" contains duplicate column "bar"`,
			desc: descpb.TableDescriptor{
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
					ConstraintID:        1,
					KeyColumnIDs:        []descpb.ColumnID{1, 1, 3},
					KeyColumnNames:      []string{"bar", "bar", "quux"},
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC, catenumpb.IndexColumn_ASC, catenumpb.IndexColumn_ASC},
					EncodingType:        catenumpb.PrimaryIndexEncoding,
					Version:             descpb.LatestIndexDescriptorVersion,
				},
				NextColumnID:     4,
				NextFamilyID:     1,
				NextIndexID:      2,
				NextConstraintID: 2,
			}},
		{err: `index "primary" key column ID 2 should have name "baz", but found name "quux"`,
			desc: descpb.TableDescriptor{
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
					ConstraintID:        1,
					KeyColumnIDs:        []descpb.ColumnID{1, 2, 3},
					KeyColumnNames:      []string{"bar", "quux", "quux"},
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC, catenumpb.IndexColumn_ASC, catenumpb.IndexColumn_ASC},
					EncodingType:        catenumpb.PrimaryIndexEncoding,
					Version:             descpb.LatestIndexDescriptorVersion,
				},
				NextColumnID:     4,
				NextFamilyID:     1,
				NextIndexID:      2,
				NextConstraintID: 2,
			}},
		{err: `index "primary" stored column ID 2 should have name "baz", but found name "quux"`,
			desc: descpb.TableDescriptor{
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
					ConstraintID:        1,
					KeyColumnIDs:        []descpb.ColumnID{1, 3},
					KeyColumnNames:      []string{"bar", "quux"},
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC, catenumpb.IndexColumn_ASC},
					StoreColumnIDs:      []descpb.ColumnID{2},
					StoreColumnNames:    []string{"quux"},
					EncodingType:        catenumpb.PrimaryIndexEncoding,
					Version:             descpb.LatestIndexDescriptorVersion,
				},
				NextColumnID:     4,
				NextFamilyID:     1,
				NextIndexID:      2,
				NextConstraintID: 2,
			}},
		{err: `index "idx" already contains column "i"`,
			desc: descpb.TableDescriptor{
				Name:          "t",
				ID:            2,
				ParentID:      1,
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "i"},
					{ID: 2, Name: "j"},
				},
				Families: []descpb.ColumnFamilyDescriptor{{
					Name:        "primary",
					ColumnIDs:   []descpb.ColumnID{1, 2},
					ColumnNames: []string{"i", "j"},
				}},
				PrimaryIndex: descpb.IndexDescriptor{
					ID:                  1,
					Name:                "t_pkey",
					ConstraintID:        1,
					KeyColumnIDs:        []descpb.ColumnID{1},
					KeyColumnNames:      []string{"i"},
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
					StoreColumnIDs:      []descpb.ColumnID{2},
					StoreColumnNames:    []string{"j"},
					EncodingType:        catenumpb.PrimaryIndexEncoding,
					Version:             descpb.LatestIndexDescriptorVersion,
				},
				Indexes: []descpb.IndexDescriptor{
					{
						Name:                "idx",
						ID:                  2,
						Version:             descpb.LatestIndexDescriptorVersion,
						EncodingType:        catenumpb.SecondaryIndexEncoding,
						KeyColumnIDs:        []descpb.ColumnID{2},
						KeyColumnNames:      []string{"j"},
						KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
						StoreColumnIDs:      []descpb.ColumnID{1},
						StoreColumnNames:    []string{"i"},
					},
				},
				NextColumnID:     3,
				NextFamilyID:     1,
				NextIndexID:      3,
				NextConstraintID: 2,
				Version:          1,
			},
		},
		{err: `index "primary" contains key column "quux" with unknown ID 3`,
			desc: descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
					{ID: 2, Name: "baz"},
				},
				Families: []descpb.ColumnFamilyDescriptor{{
					ID:          0,
					Name:        "primary",
					ColumnIDs:   []descpb.ColumnID{1, 2},
					ColumnNames: []string{"bar", "baz"},
				}},
				PrimaryIndex: descpb.IndexDescriptor{
					ID:                  1,
					Name:                "primary",
					ConstraintID:        1,
					KeyColumnIDs:        []descpb.ColumnID{1, 2, 3},
					KeyColumnNames:      []string{"bar", "baz", "quux"},
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC, catenumpb.IndexColumn_ASC, catenumpb.IndexColumn_ASC},
					EncodingType:        catenumpb.PrimaryIndexEncoding,
					Version:             descpb.LatestIndexDescriptorVersion,
				},
				NextColumnID:     4,
				NextFamilyID:     1,
				NextIndexID:      2,
				NextConstraintID: 2,
			}},
		{err: `index "primary" contains stored column "quux" with unknown ID 3`,
			desc: descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
					{ID: 2, Name: "baz"},
				},
				Families: []descpb.ColumnFamilyDescriptor{{
					ID:          0,
					Name:        "primary",
					ColumnIDs:   []descpb.ColumnID{1, 2},
					ColumnNames: []string{"bar", "baz"},
				}},
				PrimaryIndex: descpb.IndexDescriptor{
					ID:                  1,
					Name:                "primary",
					ConstraintID:        1,
					KeyColumnIDs:        []descpb.ColumnID{1, 2},
					KeyColumnNames:      []string{"bar", "baz"},
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC, catenumpb.IndexColumn_ASC},
					StoreColumnIDs:      []descpb.ColumnID{3},
					StoreColumnNames:    []string{"quux"},
					EncodingType:        catenumpb.PrimaryIndexEncoding,
					Version:             descpb.LatestIndexDescriptorVersion,
				},
				NextColumnID:     4,
				NextFamilyID:     1,
				NextIndexID:      2,
				NextConstraintID: 2,
			}},
		{err: `index "primary" contains duplicate column "bar"`,
			desc: descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
					{ID: 2, Name: "baz"},
				},
				Families: []descpb.ColumnFamilyDescriptor{{
					ID:          0,
					Name:        "primary",
					ColumnIDs:   []descpb.ColumnID{1, 2},
					ColumnNames: []string{"bar", "baz"},
				}},
				PrimaryIndex: descpb.IndexDescriptor{
					ID:                  1,
					Name:                "primary",
					ConstraintID:        1,
					KeyColumnIDs:        []descpb.ColumnID{1, 2, 1},
					KeyColumnNames:      []string{"bar", "baz", "bar"},
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC, catenumpb.IndexColumn_ASC, catenumpb.IndexColumn_ASC},
					EncodingType:        catenumpb.PrimaryIndexEncoding,
					Version:             descpb.LatestIndexDescriptorVersion,
				},
				NextColumnID:     4,
				NextFamilyID:     1,
				NextIndexID:      2,
				NextConstraintID: 2,
			}},
		{err: `index "primary" already contains column "bar"`,
			desc: descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
					{ID: 2, Name: "baz"},
				},
				Families: []descpb.ColumnFamilyDescriptor{{
					ID:          0,
					Name:        "primary",
					ColumnIDs:   []descpb.ColumnID{1, 2},
					ColumnNames: []string{"bar", "baz"},
				}},
				PrimaryIndex: descpb.IndexDescriptor{
					ID:                  1,
					Name:                "primary",
					ConstraintID:        1,
					KeyColumnIDs:        []descpb.ColumnID{1, 2},
					KeyColumnNames:      []string{"bar", "baz"},
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC, catenumpb.IndexColumn_ASC},
					StoreColumnIDs:      []descpb.ColumnID{1},
					StoreColumnNames:    []string{"bar"},
					EncodingType:        catenumpb.PrimaryIndexEncoding,
					Version:             descpb.LatestIndexDescriptorVersion,
				},
				NextColumnID:     4,
				NextFamilyID:     1,
				NextIndexID:      2,
				NextConstraintID: 2,
			}},
		{err: `index "primary" has duplicates in StoreColumnIDs: [2 2]`,
			desc: descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
					{ID: 2, Name: "baz"},
				},
				Families: []descpb.ColumnFamilyDescriptor{{
					ID:          0,
					Name:        "primary",
					ColumnIDs:   []descpb.ColumnID{1, 2},
					ColumnNames: []string{"bar", "baz"},
				}},
				PrimaryIndex: descpb.IndexDescriptor{
					ID:                  1,
					Name:                "primary",
					ConstraintID:        1,
					KeyColumnIDs:        []descpb.ColumnID{1},
					KeyColumnNames:      []string{"bar"},
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
					StoreColumnIDs:      []descpb.ColumnID{2, 2},
					StoreColumnNames:    []string{"baz", "baz"},
					EncodingType:        catenumpb.PrimaryIndexEncoding,
					Version:             descpb.LatestIndexDescriptorVersion,
				},
				NextColumnID:     4,
				NextFamilyID:     1,
				NextIndexID:      2,
				NextConstraintID: 2,
			}},
		{err: `at least one of LIST or RANGE partitioning must be used`,
			// Verify that validatePartitioning is hooked up. The rest of these
			// tests are in TestValidatePartitionion.
			desc: descpb.TableDescriptor{
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
					ID: 1, Name: "primary", ConstraintID: 1,
					KeyColumnIDs: []descpb.ColumnID{1}, KeyColumnNames: []string{"bar"},
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
					Partitioning: catpb.PartitioningDescriptor{
						NumColumns: 1,
					},
					EncodingType: catenumpb.PrimaryIndexEncoding,
					Version:      descpb.LatestIndexDescriptorVersion,
				},
				NextColumnID:     2,
				NextFamilyID:     1,
				NextIndexID:      3,
				NextConstraintID: 2,
			}},
		{err: `index "foo_crdb_internal_bar_shard_5_bar_idx" refers to non-existent shard column "does not exist"`,
			desc: descpb.TableDescriptor{
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
					ID:                  1,
					ConstraintID:        1,
					Name:                "primary",
					Unique:              true,
					KeyColumnIDs:        []descpb.ColumnID{1},
					KeyColumnNames:      []string{"bar"},
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
					StoreColumnNames:    []string{"crdb_internal_bar_shard_5"},
					StoreColumnIDs:      []descpb.ColumnID{2},
					EncodingType:        catenumpb.PrimaryIndexEncoding,
					Version:             descpb.LatestIndexDescriptorVersion,
				},
				Indexes: []descpb.IndexDescriptor{
					{ID: 2, Name: "foo_crdb_internal_bar_shard_5_bar_idx",
						KeyColumnIDs:        []descpb.ColumnID{2, 1},
						KeyColumnNames:      []string{"crdb_internal_bar_shard_5", "bar"},
						KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC, catenumpb.IndexColumn_ASC},
						Sharded: catpb.ShardedDescriptor{
							IsSharded:    true,
							Name:         "does not exist",
							ShardBuckets: 5,
						},
					},
				},
				NextColumnID:     3,
				NextFamilyID:     1,
				NextIndexID:      3,
				NextConstraintID: 2,
			}},
		{err: `TableID mismatch for unique without index constraint "bar_unique": "1" doesn't match descriptor: "2"`,
			desc: descpb.TableDescriptor{
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
				NextColumnID:     2,
				NextFamilyID:     1,
				NextConstraintID: 2,
				UniqueWithoutIndexConstraints: []descpb.UniqueWithoutIndexConstraint{
					{
						TableID:      1,
						ColumnIDs:    []descpb.ColumnID{1},
						Name:         "bar_unique",
						ConstraintID: 1,
					},
				},
			}},
		{err: `unique without index constraint "bar_unique" contains unknown column "2"`,
			desc: descpb.TableDescriptor{
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
				NextColumnID:     2,
				NextFamilyID:     1,
				NextConstraintID: 2,
				UniqueWithoutIndexConstraints: []descpb.UniqueWithoutIndexConstraint{
					{
						TableID:      2,
						ConstraintID: 1,
						ColumnIDs:    []descpb.ColumnID{1, 2},
						Name:         "bar_unique",
					},
				},
			}},
		{err: `unique without index constraint "bar_unique" contains duplicate column "1"`,
			desc: descpb.TableDescriptor{
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
				NextColumnID:     2,
				NextFamilyID:     1,
				NextConstraintID: 2,
				UniqueWithoutIndexConstraints: []descpb.UniqueWithoutIndexConstraint{
					{
						TableID:      2,
						ConstraintID: 1,
						ColumnIDs:    []descpb.ColumnID{1, 1},
						Name:         "bar_unique",
					},
				},
			}},
		{err: `empty constraint name`,
			desc: descpb.TableDescriptor{
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
				NextColumnID:     2,
				NextFamilyID:     1,
				NextConstraintID: 3,
				PrimaryIndex: descpb.IndexDescriptor{
					ID: 1, ConstraintID: 1, Name: "primary",
					KeyColumnIDs: []descpb.ColumnID{1}, KeyColumnNames: []string{"bar"},
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
				},
				UniqueWithoutIndexConstraints: []descpb.UniqueWithoutIndexConstraint{
					{
						TableID:      2,
						ConstraintID: 2,
						ColumnIDs:    []descpb.ColumnID{1},
					},
				},
			}},
		{err: `index "sec" cannot store virtual column "c3"`,
			desc: descpb.TableDescriptor{
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
					KeyColumnIDs:        []descpb.ColumnID{1, 2},
					KeyColumnNames:      []string{"c1", "c2"},
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC, catenumpb.IndexColumn_ASC},
					Version:             descpb.LatestIndexDescriptorVersion,
					EncodingType:        catenumpb.PrimaryIndexEncoding,
					ConstraintID:        1,
				},
				Indexes: []descpb.IndexDescriptor{
					{ID: 2, Name: "sec", KeyColumnIDs: []descpb.ColumnID{2},
						KeyColumnNames:      []string{"c2"},
						KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
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
		{err: ``,
			desc: descpb.TableDescriptor{
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
					KeyColumnIDs:        []descpb.ColumnID{1, 2},
					KeyColumnNames:      []string{"c1", "c2"},
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC, catenumpb.IndexColumn_ASC},
					Version:             descpb.LatestIndexDescriptorVersion,
					EncodingType:        catenumpb.PrimaryIndexEncoding,
					ConstraintID:        1,
				},
				Indexes: []descpb.IndexDescriptor{
					{ID: 2, Name: "sec", KeyColumnIDs: []descpb.ColumnID{2},
						KeyColumnNames:      []string{"c2"},
						KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
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
								KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
								Version:             descpb.LatestIndexDescriptorVersion,
								EncodingType:        catenumpb.PrimaryIndexEncoding,
								ConstraintID:        2,
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
								KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
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
				NextConstraintID: 3,
				Privileges:       catpb.NewBasePrivilegeDescriptor(username.AdminRoleName()),
			}},
		{err: `index "sec" cannot store virtual column "c3"`,
			desc: descpb.TableDescriptor{
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
					ConstraintID:        1,
					Unique:              true,
					KeyColumnIDs:        []descpb.ColumnID{1, 2},
					KeyColumnNames:      []string{"c1", "c2"},
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC, catenumpb.IndexColumn_ASC},
					Version:             descpb.LatestIndexDescriptorVersion,
					EncodingType:        catenumpb.PrimaryIndexEncoding,
				},
				Indexes: []descpb.IndexDescriptor{
					{ID: 2, Name: "sec", KeyColumnIDs: []descpb.ColumnID{2},
						KeyColumnNames:      []string{"c2"},
						KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
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
								ConstraintID:        2,
								Unique:              true,
								KeyColumnIDs:        []descpb.ColumnID{3},
								KeyColumnNames:      []string{"c3"},
								KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
								Version:             descpb.LatestIndexDescriptorVersion,
								EncodingType:        catenumpb.PrimaryIndexEncoding,
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
								KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
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
				NextConstraintID: 3,
				Privileges:       catpb.NewBasePrivilegeDescriptor(username.AdminRoleName()),
			}},
		{err: `index "new_sec" cannot store virtual column "c3"`,
			desc: descpb.TableDescriptor{
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
					ConstraintID:        1,
					Unique:              true,
					KeyColumnIDs:        []descpb.ColumnID{1, 2},
					KeyColumnNames:      []string{"c1", "c2"},
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC, catenumpb.IndexColumn_ASC},
					Version:             descpb.LatestIndexDescriptorVersion,
					EncodingType:        catenumpb.PrimaryIndexEncoding,
				},
				Indexes: []descpb.IndexDescriptor{
					{ID: 2, Name: "sec", KeyColumnIDs: []descpb.ColumnID{2},
						KeyColumnNames:      []string{"c2"},
						KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
						KeySuffixColumnIDs:  []descpb.ColumnID{1},
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
								ConstraintID:        2,
								KeyColumnIDs:        []descpb.ColumnID{1},
								KeyColumnNames:      []string{"c1"},
								KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
								Version:             descpb.LatestIndexDescriptorVersion,
								EncodingType:        catenumpb.PrimaryIndexEncoding,
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
								KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
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
				NextColumnID:     4,
				NextFamilyID:     1,
				NextIndexID:      5,
				NextConstraintID: 3,
				Privileges:       catpb.NewBasePrivilegeDescriptor(username.AdminRoleName()),
			}},
		{err: `index "sec" cannot store virtual column "v"`,
			desc: descpb.TableDescriptor{
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
					ID: 1, Name: "pri", ConstraintID: 1,
					KeyColumnIDs:        []descpb.ColumnID{1, 2},
					KeyColumnNames:      []string{"c1", "c2"},
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC, catenumpb.IndexColumn_ASC},
					EncodingType:        catenumpb.PrimaryIndexEncoding,
					Version:             descpb.LatestIndexDescriptorVersion,
				},
				Indexes: []descpb.IndexDescriptor{
					{ID: 2, Name: "sec", KeyColumnIDs: []descpb.ColumnID{2},
						KeyColumnNames:      []string{"c2"},
						KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
						StoreColumnNames:    []string{"v"},
						StoreColumnIDs:      []descpb.ColumnID{3},
					},
				},
				NextColumnID:     4,
				NextFamilyID:     1,
				NextIndexID:      3,
				NextConstraintID: 2,
			}},
		{err: `index "sec" already contains column "c2"`,
			desc: descpb.TableDescriptor{
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
					ID: 1, Name: "pri", ConstraintID: 1,
					KeyColumnIDs:        []descpb.ColumnID{1, 2},
					KeyColumnNames:      []string{"c1", "c2"},
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC, catenumpb.IndexColumn_ASC},
					EncodingType:        catenumpb.PrimaryIndexEncoding,
					Version:             descpb.LatestIndexDescriptorVersion,
				},
				Indexes: []descpb.IndexDescriptor{
					{ID: 2, Name: "sec", KeyColumnIDs: []descpb.ColumnID{2},
						KeyColumnNames:      []string{"c2"},
						KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
						StoreColumnNames:    []string{"c2"},
						StoreColumnIDs:      []descpb.ColumnID{2},
						Version:             descpb.LatestIndexDescriptorVersion,
					},
				},
				NextColumnID:     3,
				NextFamilyID:     1,
				NextIndexID:      3,
				NextConstraintID: 2,
			}},
		{err: `computed column "bar" cannot also have an ON UPDATE expression`,
			desc: descpb.TableDescriptor{
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
		{err: `both generated identity and on update expression specified for column "bar"`,
			desc: descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{
						ID:                      1,
						Name:                    "bar",
						GeneratedAsIdentityType: catpb.GeneratedAsIdentityType_GENERATED_ALWAYS,
						UsesSequenceIds:         []descpb.ID{32},
						OnUpdateExpr:            proto.String("'blah'"),
					},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
			}},
		{err: `both generated identity and on update expression specified for column "bar"`,
			desc: descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{
						ID:                      1,
						Name:                    "bar",
						GeneratedAsIdentityType: catpb.GeneratedAsIdentityType_GENERATED_BY_DEFAULT,
						UsesSequenceIds:         []descpb.ID{32},

						OnUpdateExpr: proto.String("'blah'"),
					},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
			}},
		{err: `conflicting NULL/NOT NULL declarations for column "bar"`,
			desc: descpb.TableDescriptor{
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
		{err: `conflicting NULL/NOT NULL declarations for column "bar"`,
			desc: descpb.TableDescriptor{
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
		{err: `both generated identity and computed expression specified for column "bar"`,
			desc: descpb.TableDescriptor{
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
		{err: `both generated identity and computed expression specified for column "bar"`,
			desc: descpb.TableDescriptor{
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
		{err: `conflicting NULL/NOT NULL declarations for column "bar"`,
			desc: descpb.TableDescriptor{
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
		{err: `conflicting NULL/NOT NULL declarations for column "bar"`,
			desc: descpb.TableDescriptor{
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
		{err: `both generated identity and computed expression specified for column "bar"`,
			desc: descpb.TableDescriptor{
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
		{err: `both generated identity and computed expression specified for column "bar"`,
			desc: descpb.TableDescriptor{
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
		{err: `computed column "bar" cannot also have a DEFAULT expression`,
			desc: descpb.TableDescriptor{
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
		{err: `computed column "bar" cannot also have an ON UPDATE expression`,
			desc: descpb.TableDescriptor{
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
		{err: `non-index mutation in state BACKFILLING`,
			desc: descpb.TableDescriptor{
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
		{err: `non-index mutation in state MERGING`,
			desc: descpb.TableDescriptor{
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
		{err: `public index "ruroh" is using the delete preserving encoding`,
			desc: descpb.TableDescriptor{
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
					ConstraintID:        1,
					Unique:              true,
					KeyColumnIDs:        []descpb.ColumnID{1, 2},
					KeyColumnNames:      []string{"c1", "c2"},
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC, catenumpb.IndexColumn_ASC},
					Version:             descpb.LatestIndexDescriptorVersion,
					EncodingType:        catenumpb.PrimaryIndexEncoding,
				},
				Indexes: []descpb.IndexDescriptor{
					{
						ID:                          2,
						Name:                        "ruroh",
						KeyColumnIDs:                []descpb.ColumnID{2},
						KeyColumnNames:              []string{"c2"},
						KeyColumnDirections:         []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
						Version:                     descpb.LatestIndexDescriptorVersion,
						UseDeletePreservingEncoding: true,
					},
				},
				NextColumnID:     3,
				NextIndexID:      3,
				NextFamilyID:     1,
				NextConstraintID: 2,
			}},
		{err: `public index "primary" is using the delete preserving encoding`,
			desc: descpb.TableDescriptor{
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
					ConstraintID:                1,
					KeyColumnIDs:                []descpb.ColumnID{1},
					KeyColumnNames:              []string{"c1"},
					KeyColumnDirections:         []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
					Version:                     descpb.LatestIndexDescriptorVersion,
					EncodingType:                catenumpb.PrimaryIndexEncoding,
					UseDeletePreservingEncoding: true,
				},
				NextColumnID:     3,
				NextIndexID:      2,
				NextFamilyID:     1,
				NextConstraintID: 2,
			},
		},
		{err: `column ID 123 found in depended-on-by references, no such column in this relation`,
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
					{ID: 52, ColumnIDs: []descpb.ColumnID{123}},
				},
			},
		},
		{err: `index ID 123 found in depended-on-by references, no such index in this relation`,
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
					{ID: 52, IndexID: 123},
				},
			},
		},
		{err: `Setting sql_stats_automatic_collection_enabled may not be set on virtual table`,
			desc: descpb.TableDescriptor{
				ID:            catconstants.MinVirtualID,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				NextColumnID:      2,
				AutoStatsSettings: &catpb.AutoStatsSettings{Enabled: &boolTrue},
			}},
		{err: `Setting sql_stats_automatic_partial_collection_enabled may not be set on virtual table`,
			desc: descpb.TableDescriptor{
				ID:            catconstants.MinVirtualID,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				NextColumnID:      2,
				AutoStatsSettings: &catpb.AutoStatsSettings{PartialEnabled: &boolTrue},
			}},
		{err: `Setting sql_stats_automatic_collection_enabled may not be set on a view or sequence`,
			desc: descpb.TableDescriptor{
				Name:                    "bar",
				ID:                      52,
				ParentID:                1,
				FormatVersion:           descpb.InterleavedFormatVersion,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				ViewQuery:               "SELECT * FROM foo",
				DependsOn:               []descpb.ID{51},
				NextColumnID:            2,
				Columns: []descpb.ColumnDescriptor{
					{Name: "a", ID: 1, Type: types.Int},
				},
				Privileges:        catpb.NewBasePrivilegeDescriptor(username.AdminRoleName()),
				AutoStatsSettings: &catpb.AutoStatsSettings{Enabled: &boolTrue},
			}},
		{err: `Setting sql_stats_automatic_collection_enabled may not be set on a view or sequence`,
			desc: descpb.TableDescriptor{
				ID:            51,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "a", Type: types.Int},
				},
				SequenceOpts: &descpb.TableDescriptor_SequenceOpts{
					Increment: 1,
				},
				PrimaryIndex: descpb.IndexDescriptor{
					ID:                  1,
					Name:                "primary",
					Unique:              true,
					KeyColumnIDs:        []descpb.ColumnID{1},
					KeyColumnNames:      []string{"a"},
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
					Version:             descpb.PrimaryIndexWithStoredColumnsVersion,
					EncodingType:        catenumpb.PrimaryIndexEncoding,
					ConstraintID:        1,
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary",
						ColumnIDs:   []descpb.ColumnID{1},
						ColumnNames: []string{"a"},
					},
				},
				NextColumnID:      2,
				NextFamilyID:      1,
				NextIndexID:       5,
				NextConstraintID:  2,
				Privileges:        catpb.NewBasePrivilegeDescriptor(username.AdminRoleName()),
				AutoStatsSettings: &catpb.AutoStatsSettings{Enabled: &boolTrue},
			},
		},
		{err: `invalid integer value for sql_stats_automatic_collection_min_stale_rows: cannot be set to a negative value: -1`,
			desc: descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				NextColumnID:      2,
				AutoStatsSettings: &catpb.AutoStatsSettings{MinStaleRows: &negativeOne},
			}},
		{err: `invalid integer value for sql_stats_automatic_partial_collection_min_stale_rows: cannot be set to a negative value: -1`,
			desc: descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				NextColumnID:      2,
				AutoStatsSettings: &catpb.AutoStatsSettings{PartialMinStaleRows: &negativeOne},
			}},
		{err: `invalid float value for sql_stats_automatic_collection_fraction_stale_rows: cannot set to a negative value: -1.000000`,
			desc: descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				NextColumnID:      2,
				AutoStatsSettings: &catpb.AutoStatsSettings{FractionStaleRows: &negativeOneFloat},
			}},
		{err: `invalid float value for sql_stats_automatic_partial_collection_fraction_stale_rows: cannot set to a negative value: -1.000000`,
			desc: descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar"},
				},
				NextColumnID:      2,
				AutoStatsSettings: &catpb.AutoStatsSettings{PartialFractionStaleRows: &negativeOneFloat},
			}},
		{err: `row-level TTL expiration expression "missing_col" refers to unknown columns`,
			desc: descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "a"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "fam", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"a"}},
				},
				PrimaryIndex: descpb.IndexDescriptor{
					ID:                  1,
					Name:                "primary",
					Unique:              true,
					KeyColumnIDs:        []descpb.ColumnID{1},
					KeyColumnNames:      []string{"a"},
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
					Version:             descpb.PrimaryIndexWithStoredColumnsVersion,
					EncodingType:        catenumpb.PrimaryIndexEncoding,
					ConstraintID:        1,
				},
				NextColumnID:     2,
				NextFamilyID:     1,
				NextIndexID:      2,
				NextConstraintID: 2,
				RowLevelTTL: &catpb.RowLevelTTL{
					ExpirationExpr: catpb.Expression("missing_col"),
				},
			}},
		{err: `"ttl_expire_after" and/or "ttl_expiration_expression" must be set`,
			desc: descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "a"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "fam", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"a"}},
				},
				PrimaryIndex: descpb.IndexDescriptor{
					ID:                  1,
					Name:                "primary",
					Unique:              true,
					KeyColumnIDs:        []descpb.ColumnID{1},
					KeyColumnNames:      []string{"a"},
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
					Version:             descpb.PrimaryIndexWithStoredColumnsVersion,
					EncodingType:        catenumpb.PrimaryIndexEncoding,
					ConstraintID:        1,
				},
				NextColumnID:     2,
				NextFamilyID:     1,
				NextIndexID:      2,
				NextConstraintID: 2,
				RowLevelTTL: &catpb.RowLevelTTL{
					SelectBatchSize: 5,
				},
			}},
		{err: `expected column crdb_internal_expiration: column "crdb_internal_expiration" does not exist`,
			desc: descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "a"},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "fam", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"a"}},
				},
				PrimaryIndex: descpb.IndexDescriptor{
					ID:                  1,
					Name:                "primary",
					Unique:              true,
					KeyColumnIDs:        []descpb.ColumnID{1},
					KeyColumnNames:      []string{"a"},
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
					Version:             descpb.PrimaryIndexWithStoredColumnsVersion,
					EncodingType:        catenumpb.PrimaryIndexEncoding,
					ConstraintID:        1,
				},
				NextColumnID:     2,
				NextFamilyID:     1,
				NextIndexID:      2,
				NextConstraintID: 2,
				RowLevelTTL: &catpb.RowLevelTTL{
					DurationExpr: catpb.Expression("INTERVAL '2 minutes'"),
				},
			}},
		{err: `expected DEFAULT expression of crdb_internal_expiration to be current_timestamp():::TIMESTAMPTZ + INTERVAL '2 minutes'`,
			desc: descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "a"},
					{
						ID:           2,
						Name:         "crdb_internal_expiration",
						Hidden:       true,
						OnUpdateExpr: pointer("current_timestamp():::TIMESTAMPTZ + INTERVAL '2 minutes'"),
					},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "fam", ColumnIDs: []descpb.ColumnID{1, 2}, ColumnNames: []string{"a", "crdb_internal_expiration"}},
				},
				PrimaryIndex: descpb.IndexDescriptor{
					ID:                  1,
					Name:                "primary",
					Unique:              true,
					KeyColumnIDs:        []descpb.ColumnID{1},
					KeyColumnNames:      []string{"a"},
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
					StoreColumnIDs:      []descpb.ColumnID{2},
					StoreColumnNames:    []string{"crdb_internal_expiration"},
					Version:             descpb.PrimaryIndexWithStoredColumnsVersion,
					EncodingType:        catenumpb.PrimaryIndexEncoding,
					ConstraintID:        1,
				},
				NextColumnID:     3,
				NextFamilyID:     1,
				NextIndexID:      2,
				NextConstraintID: 2,
				RowLevelTTL: &catpb.RowLevelTTL{
					DurationExpr: catpb.Expression("INTERVAL '2 minutes'"),
				},
			}},
		{err: `expected ON UPDATE expression of crdb_internal_expiration to be current_timestamp():::TIMESTAMPTZ + INTERVAL '2 minutes'`,
			desc: descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "a"},
					{
						ID:          2,
						Name:        "crdb_internal_expiration",
						Hidden:      true,
						DefaultExpr: pointer("current_timestamp():::TIMESTAMPTZ + INTERVAL '2 minutes'"),
					},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "fam", ColumnIDs: []descpb.ColumnID{1, 2}, ColumnNames: []string{"a", "crdb_internal_expiration"}},
				},
				PrimaryIndex: descpb.IndexDescriptor{
					ID:                  1,
					Name:                "primary",
					Unique:              true,
					KeyColumnIDs:        []descpb.ColumnID{1},
					KeyColumnNames:      []string{"a"},
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
					StoreColumnIDs:      []descpb.ColumnID{2},
					StoreColumnNames:    []string{"crdb_internal_expiration"},
					Version:             descpb.PrimaryIndexWithStoredColumnsVersion,
					EncodingType:        catenumpb.PrimaryIndexEncoding,
					ConstraintID:        1,
				},
				NextColumnID:     3,
				NextFamilyID:     1,
				NextIndexID:      2,
				NextConstraintID: 2,
				RowLevelTTL: &catpb.RowLevelTTL{
					DurationExpr: catpb.Expression("INTERVAL '2 minutes'"),
				},
			}},
		{err: `"ttl_select_batch_size" must be at least 1`,
			desc: descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "a"},
					{
						ID:           2,
						Name:         "crdb_internal_expiration",
						Hidden:       true,
						OnUpdateExpr: pointer("current_timestamp():::TIMESTAMPTZ + INTERVAL '2 minutes'"),
						DefaultExpr:  pointer("current_timestamp():::TIMESTAMPTZ + INTERVAL '2 minutes'"),
					},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "fam", ColumnIDs: []descpb.ColumnID{1, 2}, ColumnNames: []string{"a", "crdb_internal_expiration"}},
				},
				PrimaryIndex: descpb.IndexDescriptor{
					ID:                  1,
					Name:                "primary",
					Unique:              true,
					KeyColumnIDs:        []descpb.ColumnID{1},
					KeyColumnNames:      []string{"a"},
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
					StoreColumnIDs:      []descpb.ColumnID{2},
					StoreColumnNames:    []string{"crdb_internal_expiration"},
					Version:             descpb.PrimaryIndexWithStoredColumnsVersion,
					EncodingType:        catenumpb.PrimaryIndexEncoding,
					ConstraintID:        1,
				},
				NextColumnID:     3,
				NextFamilyID:     1,
				NextIndexID:      2,
				NextConstraintID: 2,
				RowLevelTTL: &catpb.RowLevelTTL{
					DurationExpr:    catpb.Expression("INTERVAL '2 minutes'"),
					SelectBatchSize: -2,
				},
			}},
		{err: `unknown mutation ID 123 associated with job ID 456`,
			desc: descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				MutationJobs: []descpb.TableDescriptor_MutationJob{
					{MutationID: 123, JobID: 456},
				},
				Columns: []descpb.ColumnDescriptor{
					{
						ID:   1,
						Name: "bar",
					},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
			}},
		{err: `two job IDs 12345 and 45678 mapped to the same mutation ID 1`,
			desc: descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				MutationJobs: []descpb.TableDescriptor_MutationJob{
					{MutationID: 1, JobID: 12345},
					{MutationID: 1, JobID: 45678},
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
								KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
								Version:             descpb.LatestIndexDescriptorVersion,
								EncodingType:        catenumpb.PrimaryIndexEncoding,
								ConstraintID:        1,
							},
						},
						Direction:  descpb.DescriptorMutation_ADD,
						State:      descpb.DescriptorMutation_DELETE_ONLY,
						MutationID: 1,
					},
				},
				Columns: []descpb.ColumnDescriptor{
					{
						ID:   1,
						Name: "bar",
					},
				},
				Families: []descpb.ColumnFamilyDescriptor{
					{ID: 0, Name: "primary", ColumnIDs: []descpb.ColumnID{1}, ColumnNames: []string{"bar"}},
				},
				NextColumnID: 2,
				NextFamilyID: 1,
			}},
		{err: `invisibility is incompatible with value for not_visible`,
			desc: descpb.TableDescriptor{
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
				PrimaryIndex: descpb.IndexDescriptor{ID: 1, Name: "bar", ConstraintID: 1,
					KeyColumnIDs: []descpb.ColumnID{1}, KeyColumnNames: []string{"bar"},
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
					EncodingType:        catenumpb.PrimaryIndexEncoding,
					Version:             descpb.LatestIndexDescriptorVersion,
				},
				Indexes: []descpb.IndexDescriptor{
					{ID: 2, Name: "invisible", KeyColumnIDs: []descpb.ColumnID{1},
						KeyColumnNames:      []string{"bar"},
						KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
						NotVisible:          false,
						Invisibility:        1,
					},
				},
				NextColumnID:     2,
				NextFamilyID:     1,
				NextIndexID:      3,
				NextConstraintID: 2,
			}},
		{err: `invalid outbound foreign key "to_this_table": origin table ID should be 4. got 99`,
			desc: ModifyDescriptor(func(desc *descpb.TableDescriptor) {
				desc.OutboundFKs[0].OriginTableID = 99
			})},
		{err: `invalid outbound foreign key "to_this_table": no origin columns`,
			desc: ModifyDescriptor(func(desc *descpb.TableDescriptor) {
				desc.OutboundFKs[0].OriginColumnIDs = nil
			})},
		{err: `invalid outbound foreign key "to_this_table": mismatched number of referenced and origin columns`,
			desc: ModifyDescriptor(func(desc *descpb.TableDescriptor) {
				desc.OutboundFKs[0].ReferencedColumnIDs = []descpb.ColumnID{1, 2, 3}
			})},
		{err: `invalid outbound foreign key "to_this_table" from table "bar" (4): missing origin column=13`,
			desc: ModifyDescriptor(func(desc *descpb.TableDescriptor) {
				desc.OutboundFKs[0].OriginColumnIDs = []descpb.ColumnID{13}
			})},
		{err: `invalid inbound foreign key "from_this_table": referenced table ID should be 4. got 99`,
			desc: ModifyDescriptor(func(desc *descpb.TableDescriptor) {
				desc.InboundFKs[0].ReferencedTableID = 99
			})},
		{err: `invalid inbound foreign key "from_this_table": no referenced columns`,
			desc: ModifyDescriptor(func(desc *descpb.TableDescriptor) {
				desc.InboundFKs[0].ReferencedColumnIDs = nil
			})},
		{err: `invalid inbound foreign key "from_this_table": mismatched number of referenced and origin columns`,
			desc: ModifyDescriptor(func(desc *descpb.TableDescriptor) {
				desc.InboundFKs[0].OriginColumnIDs = []descpb.ColumnID{1, 2, 3}
			})},
		{err: `invalid inbound foreign key "from_this_table" to table "bar" (4): missing referenced column=13`,
			desc: ModifyDescriptor(func(desc *descpb.TableDescriptor) {
				desc.InboundFKs[0].ReferencedColumnIDs = []descpb.ColumnID{13}
			})},
		{err: `trigger "blah" has ID 0 not less than NextTrigger value 0 for table`,
			desc: ModifyDescriptor(func(desc *descpb.TableDescriptor) {
				desc.Triggers = []descpb.TriggerDescriptor{
					{
						ID:         0,
						Name:       "blah",
						ActionTime: semenumpb.TriggerActionTime_BEFORE,
						Events: []*descpb.TriggerDescriptor_Event{
							{Type: semenumpb.TriggerEventType_INSERT},
						},
						FuncID:   5,
						FuncBody: "BEGIN RETURN NULL; END",
					},
				}
			})},
		{err: `duplicate trigger name: "blah"`,
			desc: ModifyDescriptor(func(desc *descpb.TableDescriptor) {
				desc.NextTriggerID = 2
				desc.Triggers = []descpb.TriggerDescriptor{
					{
						ID:         0,
						Name:       "blah",
						ActionTime: semenumpb.TriggerActionTime_BEFORE,
						Events: []*descpb.TriggerDescriptor_Event{
							{Type: semenumpb.TriggerEventType_INSERT},
						},
						FuncID:            5,
						FuncBody:          "BEGIN RETURN NULL; END",
						DependsOnRoutines: []descpb.ID{5},
					},
					{
						ID:   1,
						Name: "blah",
					},
				}
			})},
		{err: `trigger "blah" contains unknown column "baz"`,
			desc: ModifyDescriptor(func(desc *descpb.TableDescriptor) {
				desc.NextTriggerID = 1
				desc.Triggers = []descpb.TriggerDescriptor{
					{
						ID:         0,
						Name:       "blah",
						ActionTime: semenumpb.TriggerActionTime_BEFORE,
						Events: []*descpb.TriggerDescriptor_Event{
							{Type: semenumpb.TriggerEventType_INSERT},
							{Type: semenumpb.TriggerEventType_UPDATE, ColumnNames: []string{"bar", "baz"}},
						},
					},
				}
			})},
		{err: `at or near "abc": syntax error`,
			desc: ModifyDescriptor(func(desc *descpb.TableDescriptor) {
				desc.NextTriggerID = 1
				desc.Triggers = []descpb.TriggerDescriptor{
					{
						ID:         0,
						Name:       "blah",
						ActionTime: semenumpb.TriggerActionTime_BEFORE,
						Events: []*descpb.TriggerDescriptor_Event{
							{Type: semenumpb.TriggerEventType_INSERT},
						},
						FuncID:   5,
						FuncBody: "abc",
					},
				}
			})},
		{err: `column is identity without sequence references "bar"`,
			desc: descpb.TableDescriptor{
				ID:            2,
				ParentID:      1,
				Name:          "foo",
				FormatVersion: descpb.InterleavedFormatVersion,
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "bar", GeneratedAsIdentityType: catpb.GeneratedAsIdentityType_GENERATED_ALWAYS},
				},
				NextColumnID: 2,
			}},
		{err: `policy ID was missing for policy "pol"`,
			desc: ModifyDescriptor(func(desc *descpb.TableDescriptor) {
				desc.NextPolicyID = 1
				desc.Policies = []descpb.PolicyDescriptor{
					{
						ID:   0,
						Name: "pol",
					},
				}
			}),
		},
		{err: `empty policy name`,
			desc: ModifyDescriptor(func(desc *descpb.TableDescriptor) {
				desc.NextPolicyID = 2
				desc.Policies = []descpb.PolicyDescriptor{
					{
						ID:   1,
						Name: "",
					},
				}
			}),
		},
		{err: `duplicate policy name: "pol"`,
			desc: ModifyDescriptor(func(desc *descpb.TableDescriptor) {
				desc.NextPolicyID = 3
				desc.Policies = []descpb.PolicyDescriptor{
					{
						ID:      1,
						Name:    "pol",
						Type:    catpb.PolicyType_PERMISSIVE,
						Command: catpb.PolicyCommand_ALL,
					},
					{
						ID:      2,
						Name:    "pol",
						Type:    catpb.PolicyType_RESTRICTIVE,
						Command: catpb.PolicyCommand_INSERT,
					},
				}
			}),
		},
		{err: `policy ID 10 in policy "pol_new" already in use by "pol_old"`,
			desc: ModifyDescriptor(func(desc *descpb.TableDescriptor) {
				desc.NextPolicyID = 11
				desc.Policies = []descpb.PolicyDescriptor{
					{
						ID:      10,
						Name:    "pol_old",
						Type:    catpb.PolicyType_RESTRICTIVE,
						Command: catpb.PolicyCommand_UPDATE,
					},
					{
						ID:      10,
						Name:    "pol_new",
						Type:    catpb.PolicyType_PERMISSIVE,
						Command: catpb.PolicyCommand_DELETE,
					},
				}
			}),
		},
		{err: `policy "pol" has ID 20, which is not less than the NextPolicyID value 5 for the table`,
			desc: ModifyDescriptor(func(desc *descpb.TableDescriptor) {
				desc.NextPolicyID = 5
				desc.Policies = []descpb.PolicyDescriptor{
					{
						ID:      20,
						Name:    "pol",
						Type:    catpb.PolicyType_PERMISSIVE,
						Command: catpb.PolicyCommand_SELECT,
					},
				}
			}),
		},
		{err: `policy "pol" has an unknown policy type POLICYTYPE_UNUSED`,
			desc: ModifyDescriptor(func(desc *descpb.TableDescriptor) {
				desc.NextPolicyID = 2
				desc.Policies = []descpb.PolicyDescriptor{
					{
						ID:      1,
						Name:    "pol",
						Type:    0,
						Command: catpb.PolicyCommand_ALL,
					},
				}
			}),
		},
		{err: `policy "pol" has an unknown policy command POLICYCOMMAND_UNUSED`,
			desc: ModifyDescriptor(func(desc *descpb.TableDescriptor) {
				desc.NextPolicyID = 2
				desc.Policies = []descpb.PolicyDescriptor{
					{
						ID:      1,
						Name:    "pol",
						Type:    catpb.PolicyType_PERMISSIVE,
						Command: 0,
					},
				}
			}),
		},
	}

	for i, d := range testData {
		t.Run(d.err, func(t *testing.T) {
			d.desc.Privileges = catpb.NewBasePrivilegeDescriptor(username.RootUserName())
			desc := NewBuilder(&d.desc).BuildImmutableTable()
			expectedErr := fmt.Sprintf("%s %q (%d): %s", desc.DescriptorType(), desc.GetName(), desc.GetID(), d.err)
			clusterVersion := clusterversion.TestingClusterVersion
			version := d.version
			if version != 0 {
				clusterVersion = clusterversion.ClusterVersion{
					Version: version.Version(),
				}
			}
			err := validate.Self(clusterVersion, desc)
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
		fnDescs    []descpb.FunctionDescriptor
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
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "foo_1", Type: types.String},
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
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "baz_1", Type: types.String},
				},
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
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Type: types.String, Name: "foo_1"},
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
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Type: types.String, Name: "baz_1"},
				},
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
						Type: types.MakeEnum(catid.TypeIDToOID(500), catid.TypeIDToOID(100500)),
					},
				},
			},
		},
		{ // 5
			err: `invalid foreign key backreference from table "baz" (52): missing origin column=2`,
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
						OriginColumnIDs:     []descpb.ColumnID{2},
					},
				},
			},
			otherDescs: []descpb.TableDescriptor{{
				ID:                      52,
				Name:                    "baz",
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				FormatVersion:           descpb.InterleavedFormatVersion,
				OutboundFKs: []descpb.ForeignKeyConstraint{
					{
						Name:                "fk",
						ReferencedTableID:   51,
						ReferencedColumnIDs: []descpb.ColumnID{1},
						OriginTableID:       52,
						OriginColumnIDs:     []descpb.ColumnID{2},
					},
				},
			}},
		},
		{ // 6
			err: `invalid outbound foreign key backreference from table "baz" (52): missing referenced column=2`,
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
				OutboundFKs: []descpb.ForeignKeyConstraint{
					{
						Name:                "fk",
						ReferencedTableID:   52,
						ReferencedColumnIDs: []descpb.ColumnID{2},
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
				InboundFKs: []descpb.ForeignKeyConstraint{
					{
						Name:                "fk",
						ReferencedTableID:   52,
						ReferencedColumnIDs: []descpb.ColumnID{2},
						OriginTableID:       51,
						OriginColumnIDs:     []descpb.ColumnID{1},
					},
				},
			}},
		},
		// Add some expressions with invalid type references.
		{ // 7
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
						Name:        "a",
						ID:          1,
						Type:        types.Int,
						ComputeExpr: pointer("a:::@100500"),
					},
				},
			},
		},
		{ // 9
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
		{ // 10
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
		{ // 11
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
		{ // 12
			err: ``,
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
		{ // 13
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
		{ // 14
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
		{ // 15
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
		{ // 16
			// This case deals with a bug in version 21.1 and prior when
			// ALTER TABLE ... ADD COLUMN ... DEFAULT nextval(...) would set the
			// backreference ID to be 0 because it set up the backreference before
			// calling AllocateIDs.
			desc: descpb.TableDescriptor{
				Name:                    "foo",
				ID:                      51,
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				SequenceOpts: &descpb.TableDescriptor_SequenceOpts{
					Increment: 1,
				},
				DependedOnBy: []descpb.TableDescriptor_Reference{
					{ID: 52, ColumnIDs: []descpb.ColumnID{0}},
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
				DependsOn: []descpb.ID{51},
			}},
		},
		{ // 17
			err: `invalid depended-on-by relation back reference: referenced descriptor ID 100: referenced descriptor not found`,
			desc: descpb.TableDescriptor{
				Name:                    "foo",
				ID:                      51,
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				DependedOnBy: []descpb.TableDescriptor_Reference{
					{ID: 100},
				},
			},
		},
		{ // 18
			err: `depended-on-by function "f" (100) has no corresponding depends-on forward reference`,
			desc: descpb.TableDescriptor{
				Name:                    "foo",
				ID:                      51,
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				DependedOnBy: []descpb.TableDescriptor_Reference{
					{ID: 100},
				},
			},
			fnDescs: []descpb.FunctionDescriptor{
				{ID: 100, Name: "f"},
			},
		},
		{ // 19
			err: `depends-on function "f" (100) has no corresponding depended-on-by back reference`,
			desc: descpb.TableDescriptor{
				Name:                    "foo",
				ID:                      51,
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				ViewQuery:               "some query",
				DependsOnFunctions:      []descpb.ID{100},
			},
			fnDescs: []descpb.FunctionDescriptor{
				{ID: 100, Name: "f"},
			},
		},
		{ // 20
			err: `invalid depends-on function back reference: referenced function ID 100: referenced descriptor not found`,
			desc: descpb.TableDescriptor{
				Name:                    "foo",
				ID:                      51,
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				DependsOnFunctions:      []descpb.ID{100},
				Checks: []*descpb.TableDescriptor_CheckConstraint{
					{
						Expr:         "[Function 100100]()",
						ConstraintID: 1,
					},
				},
			},
		},
		{ // 21
			err: `depends-on function "f" (100) has no corresponding depended-on-by back reference`,
			desc: descpb.TableDescriptor{
				Name:                    "foo",
				ID:                      51,
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				DependsOnFunctions:      []descpb.ID{100},
				Checks: []*descpb.TableDescriptor_CheckConstraint{
					{
						Expr:         "[Function 100100]()",
						ConstraintID: 1,
					},
				},
			},
			fnDescs: []descpb.FunctionDescriptor{
				{ID: 100, Name: "f"},
			},
		},
		{ // 22
			err: `depends-on function "f" (100) has no corresponding depended-on-by back reference`,
			desc: descpb.TableDescriptor{
				Name:                    "foo",
				ID:                      51,
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				DependsOnFunctions:      []descpb.ID{100},
				Checks: []*descpb.TableDescriptor_CheckConstraint{
					{
						Expr:         "[Function 100100]()",
						ConstraintID: 1,
					},
				},
			},
			fnDescs: []descpb.FunctionDescriptor{
				{
					ID:   100,
					Name: "f",
					DependedOnBy: []descpb.FunctionDescriptor_Reference{
						{ID: 51},
					},
				},
			},
		},
		{ // 23
			err: ``,
			desc: descpb.TableDescriptor{
				Name:                    "foo",
				ID:                      51,
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				DependsOnFunctions:      []descpb.ID{100},
				Checks: []*descpb.TableDescriptor_CheckConstraint{
					{
						Expr:         "[Function 100100]()",
						ConstraintID: 1,
					},
				},
			},
			fnDescs: []descpb.FunctionDescriptor{
				{
					ID:   100,
					Name: "f",
					DependedOnBy: []descpb.FunctionDescriptor_Reference{
						{ID: 51, ConstraintIDs: []descpb.ConstraintID{1}},
					},
				},
			},
		},
		// Composite types.
		{ // 24
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
						Type: types.NewCompositeType(catid.TypeIDToOID(500), catid.TypeIDToOID(100500), nil, nil),
					},
				},
			},
		},
		{ // 25
			err: `invalid depends-on function back reference: referenced function ID 100: referenced descriptor not found`,
			desc: descpb.TableDescriptor{
				Name:                    "foo",
				ID:                      51,
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				Columns: []descpb.ColumnDescriptor{
					{
						ID:              1,
						Type:            types.Int,
						UsesFunctionIds: []descpb.ID{100},
					},
				},
			},
		},
		{ // 26
			err: `depends-on function "f" (100) has no corresponding depended-on-by back reference`,
			desc: descpb.TableDescriptor{
				Name:                    "foo",
				ID:                      51,
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				Columns: []descpb.ColumnDescriptor{
					{
						ID:              1,
						Type:            types.Int,
						UsesFunctionIds: []descpb.ID{100},
					},
				},
			},
			fnDescs: []descpb.FunctionDescriptor{
				{
					ID:   100,
					Name: "f",
				},
			},
		},
		{ // 27
			err: `depends-on function "f" (100) has no corresponding depended-on-by back reference`,
			desc: descpb.TableDescriptor{
				Name:                    "foo",
				ID:                      51,
				ParentID:                1,
				UnexposedParentSchemaID: keys.PublicSchemaID,
				Columns: []descpb.ColumnDescriptor{
					{
						ID:              1,
						Type:            types.Int,
						UsesFunctionIds: []descpb.ID{100},
					},
				},
			},
			fnDescs: []descpb.FunctionDescriptor{
				{
					ID:   100,
					Name: "f",
					DependedOnBy: []descpb.FunctionDescriptor_Reference{
						{ID: 51},
					},
				},
			},
		},
	}

	for i, test := range tests {
		t.Run(test.err, func(t *testing.T) {
			var cb nstree.MutableCatalog
			cb.UpsertDescriptor(dbdesc.NewBuilder(&descpb.DatabaseDescriptor{ID: 1}).BuildImmutable())
			for _, otherDesc := range test.otherDescs {
				otherDesc.Privileges = catpb.NewBasePrivilegeDescriptor(username.AdminRoleName())
				cb.UpsertDescriptor(NewBuilder(&otherDesc).BuildImmutable())
			}
			for _, fnDesc := range test.fnDescs {
				cb.UpsertDescriptor(funcdesc.NewBuilder(&fnDesc).BuildImmutable())
			}
			desc := NewBuilder(&test.desc).BuildImmutable()
			expectedErr := fmt.Sprintf("%s %q (%d): %s", desc.DescriptorType(), desc.GetName(), desc.GetID(), test.err)
			const validateCrossReferencesOnly = catalog.ValidationLevelBackReferences &^ catalog.ValidationLevelSelfOnly
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
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
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
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
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
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
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
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
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
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
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
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
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
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC, catenumpb.IndexColumn_ASC},
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
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
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
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
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
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC, catenumpb.IndexColumn_ASC},
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
		{`constraint ID was missing for constraint \"primary\"`,
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
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC}},
				NextColumnID: 2,
				NextFamilyID: 1,
				Privileges: catpb.NewPrivilegeDescriptor(
					username.PublicRoleName(),
					privilege.SchemaPrivileges,
					privilege.List{},
					username.RootUserName()),
			}},
		{`constraint ID was missing for constraint \"secondary\"`,
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
					ID: 1, ConstraintID: 1, Name: "primary", KeyColumnIDs: []descpb.ColumnID{1}, KeyColumnNames: []string{"bar"},
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC}},
				Indexes: []descpb.IndexDescriptor{
					{
						ID: 2, Name: "secondary", KeyColumnIDs: []descpb.ColumnID{1}, KeyColumnNames: []string{"bar"},
						KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
						Unique:              true,
					},
				},
				NextColumnID:     2,
				NextFamilyID:     1,
				NextConstraintID: 2,
				Privileges: catpb.NewPrivilegeDescriptor(
					username.PublicRoleName(),
					privilege.SchemaPrivileges,
					privilege.List{},
					username.RootUserName()),
			}},
		{`constraint ID was missing for constraint \"bad\"`,
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
					ID: 1, ConstraintID: 1, Name: "primary", KeyColumnIDs: []descpb.ColumnID{1}, KeyColumnNames: []string{"bar"},
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC}},
				UniqueWithoutIndexConstraints: []descpb.UniqueWithoutIndexConstraint{
					{Name: "bad"},
				},
				NextColumnID:     2,
				NextConstraintID: 2,
				NextFamilyID:     1,
				Privileges: catpb.NewPrivilegeDescriptor(
					username.PublicRoleName(),
					privilege.SchemaPrivileges,
					privilege.List{},
					username.RootUserName()),
			}},
		{`constraint ID was missing for constraint \"bad\"`,
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
					ID: 1, ConstraintID: 1, Name: "primary", KeyColumnIDs: []descpb.ColumnID{1}, KeyColumnNames: []string{"bar"},
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC}},
				Checks: []*descpb.TableDescriptor_CheckConstraint{
					{Name: "bad"},
				},
				NextColumnID:     2,
				NextConstraintID: 2,
				NextFamilyID:     1,
				Privileges: catpb.NewPrivilegeDescriptor(
					username.PublicRoleName(),
					privilege.SchemaPrivileges,
					privilege.List{},
					username.RootUserName()),
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
