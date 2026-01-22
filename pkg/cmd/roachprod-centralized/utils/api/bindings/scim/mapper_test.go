// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scim

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/auth"
	filtertypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFieldMapper_BuildMapping(t *testing.T) {
	mapper := NewFieldMapper()
	err := mapper.BuildMapping(reflect.TypeOf(auth.User{}))
	require.NoError(t, err)

	// Test SCIM -> Internal mapping (now uses struct field names, not db tags)
	tests := []struct {
		scimField        string
		expectedInternal string
	}{
		{"userName", "Email"},
		{"emails", "Email"},
		{"active", "Active"},
		{"name.formatted", "FullName"},
		{"displayName", "FullName"},
		{"externalId", "OktaUserID"},
		{"id", "ID"},
		{"meta.created", "CreatedAt"},
		{"meta.lastModified", "UpdatedAt"},
	}

	for _, tt := range tests {
		t.Run(tt.scimField, func(t *testing.T) {
			internal, ok := mapper.GetInternalField(tt.scimField)
			assert.True(t, ok, "SCIM field %q should be mapped", tt.scimField)
			assert.Equal(t, tt.expectedInternal, internal)
		})
	}
}

func TestFieldMapper_BuildMapping_MultipleSCIMFieldsPerInternal(t *testing.T) {
	mapper := NewFieldMapper()
	err := mapper.BuildMapping(reflect.TypeOf(auth.User{}))
	require.NoError(t, err)

	// Both "userName" and "emails" should map to "Email" (struct field name)
	internal1, ok1 := mapper.GetInternalField("userName")
	assert.True(t, ok1)
	assert.Equal(t, "Email", internal1)

	internal2, ok2 := mapper.GetInternalField("emails")
	assert.True(t, ok2)
	assert.Equal(t, "Email", internal2)

	// Both "name.formatted" and "displayName" should map to "FullName" (struct field name)
	internal3, ok3 := mapper.GetInternalField("name.formatted")
	assert.True(t, ok3)
	assert.Equal(t, "FullName", internal3)

	internal4, ok4 := mapper.GetInternalField("displayName")
	assert.True(t, ok4)
	assert.Equal(t, "FullName", internal4)
}

func TestFieldMapper_BidirectionalMapping(t *testing.T) {
	mapper := NewFieldMapper()
	err := mapper.BuildMapping(reflect.TypeOf(auth.User{}))
	require.NoError(t, err)

	// Test Internal -> SCIM mapping (reverse, uses struct field names)
	tests := []struct {
		internalField      string
		expectedSCIMFields []string
	}{
		{"Email", []string{"userName", "emails"}},
		{"FullName", []string{"name.formatted", "displayName"}},
		{"Active", []string{"active"}},
		{"OktaUserID", []string{"externalId"}},
		{"ID", []string{"id"}},
		{"CreatedAt", []string{"meta.created"}},
		{"UpdatedAt", []string{"meta.lastModified"}},
	}

	for _, tt := range tests {
		t.Run(tt.internalField, func(t *testing.T) {
			scimFields, ok := mapper.GetSCIMFields(tt.internalField)
			assert.True(t, ok, "Internal field %q should have SCIM mappings", tt.internalField)
			assert.ElementsMatch(t, tt.expectedSCIMFields, scimFields)
		})
	}
}

func TestFieldMapper_InvalidStructType(t *testing.T) {
	mapper := NewFieldMapper()

	// Test with non-struct type
	err := mapper.BuildMapping(reflect.TypeOf("not a struct"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "expected struct type")
}

func TestFieldMapper_FieldsWithoutSCIMTags(t *testing.T) {
	mapper := NewFieldMapper()
	err := mapper.BuildMapping(reflect.TypeOf(auth.User{}))
	require.NoError(t, err)

	// SlackHandle doesn't have a SCIM tag, so it should not be in the mapping
	_, ok := mapper.GetInternalField("slack_handle")
	assert.False(t, ok)

	// LastLoginAt doesn't have a SCIM tag
	_, ok = mapper.GetInternalField("last_login_at")
	assert.False(t, ok)
}

func TestFieldMapper_ToMap(t *testing.T) {
	mapper := NewFieldMapper()
	err := mapper.BuildMapping(reflect.TypeOf(auth.User{}))
	require.NoError(t, err)

	mapping := mapper.ToMap()

	// Verify essential mappings exist (now uses struct field names)
	assert.NotEmpty(t, mapping)
	assert.Equal(t, "Email", mapping["userName"])
	assert.Equal(t, "Email", mapping["emails"])
	assert.Equal(t, "Active", mapping["active"])
	assert.Equal(t, "FullName", mapping["name.formatted"])
	assert.Equal(t, "FullName", mapping["displayName"])
	assert.Equal(t, "OktaUserID", mapping["externalId"])

	// Verify that modifying the returned map doesn't affect internal state
	mapping["test"] = "should_not_persist"
	newMapping := mapper.ToMap()
	_, ok := newMapping["test"]
	assert.False(t, ok, "ToMap should return a copy, not internal state")
}

func TestGetUserFieldMapping(t *testing.T) {
	mapping := GetUserFieldMapping()

	// Verify essential mappings exist (tests package-level init, uses struct field names)
	assert.NotEmpty(t, mapping)
	assert.Equal(t, "Email", mapping["userName"])
	assert.Equal(t, "Active", mapping["active"])
	assert.Equal(t, "FullName", mapping["name.formatted"])
	assert.Equal(t, "OktaUserID", mapping["externalId"])
}

func TestTranslateUserFilterFields(t *testing.T) {
	// Test that filter translation works with reflection-based mapping
	filterSet := filtertypes.FilterSet{
		Filters: []filtertypes.FieldFilter{
			{Field: "userName", Operator: filtertypes.OpEqual, Value: "test@example.com"},
			{Field: "active", Operator: filtertypes.OpEqual, Value: true},
			{Field: "name.formatted", Operator: filtertypes.OpContains, Value: "John"},
		},
	}

	translated := TranslateUserFilterFields(filterSet)

	// Verify all fields were translated to struct field names
	assert.Equal(t, "Email", translated.Filters[0].Field)
	assert.Equal(t, "Active", translated.Filters[1].Field)
	assert.Equal(t, "FullName", translated.Filters[2].Field)

	// Verify other fields remain unchanged
	assert.Equal(t, filtertypes.OpEqual, translated.Filters[0].Operator)
	assert.Equal(t, "test@example.com", translated.Filters[0].Value)
}

func TestTranslateUserFilterFields_WithSorting(t *testing.T) {
	filterSet := filtertypes.FilterSet{
		Filters: []filtertypes.FieldFilter{
			{Field: "emails", Operator: filtertypes.OpEqual, Value: "user@example.com"},
		},
		Sort: &filtertypes.SortParams{
			SortBy:    "displayName",
			SortOrder: filtertypes.SortAscending,
		},
	}

	translated := TranslateUserFilterFields(filterSet)

	// Verify filter field was translated to struct field name
	assert.Equal(t, "Email", translated.Filters[0].Field)

	// Verify sort field was translated to struct field name
	require.NotNil(t, translated.Sort)
	assert.Equal(t, "FullName", translated.Sort.SortBy)
	assert.Equal(t, filtertypes.SortAscending, translated.Sort.SortOrder)
}

func TestTranslateUserFilterFields_UnmappedFields(t *testing.T) {
	// Fields that don't have SCIM mappings should remain unchanged
	filterSet := filtertypes.FilterSet{
		Filters: []filtertypes.FieldFilter{
			{Field: "unknownField", Operator: filtertypes.OpEqual, Value: "value"},
		},
	}

	translated := TranslateUserFilterFields(filterSet)

	// Unmapped field should remain unchanged
	assert.Equal(t, "unknownField", translated.Filters[0].Field)
}

func TestFieldMapper_ThreadSafety(t *testing.T) {
	mapper := NewFieldMapper()
	err := mapper.BuildMapping(reflect.TypeOf(auth.User{}))
	require.NoError(t, err)

	// Run concurrent reads to verify thread safety
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			_, _ = mapper.GetInternalField("userName")
			_ = mapper.ToMap()
			done <- true
		}()
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify mapper still works correctly after concurrent access
	internal, ok := mapper.GetInternalField("userName")
	assert.True(t, ok)
	assert.Equal(t, "Email", internal)
}

func TestFieldMapper_BuildMapping_Group(t *testing.T) {
	mapper := NewFieldMapper()
	err := mapper.BuildMapping(reflect.TypeOf(auth.Group{}))
	require.NoError(t, err)

	// Test SCIM -> Internal mapping for Group (now uses struct field names)
	tests := []struct {
		scimField        string
		expectedInternal string
	}{
		{"id", "ID"},
		{"externalId", "ExternalID"},
		{"displayName", "DisplayName"},
		{"meta.created", "CreatedAt"},
		{"meta.lastModified", "UpdatedAt"},
	}

	for _, tt := range tests {
		t.Run(tt.scimField, func(t *testing.T) {
			internal, ok := mapper.GetInternalField(tt.scimField)
			assert.True(t, ok, "SCIM field %q should be mapped", tt.scimField)
			assert.Equal(t, tt.expectedInternal, internal)
		})
	}
}

func TestGetGroupFieldMapping(t *testing.T) {
	mapping := GetGroupFieldMapping()

	// Verify essential mappings exist (tests package-level init, uses struct field names)
	assert.NotEmpty(t, mapping)
	assert.Equal(t, "ID", mapping["id"])
	assert.Equal(t, "ExternalID", mapping["externalId"])
	assert.Equal(t, "DisplayName", mapping["displayName"])
	assert.Equal(t, "CreatedAt", mapping["meta.created"])
	assert.Equal(t, "UpdatedAt", mapping["meta.lastModified"])
}

func TestTranslateGroupFilterFields(t *testing.T) {
	// Test that filter translation works with Group reflection-based mapping
	filterSet := filtertypes.FilterSet{
		Filters: []filtertypes.FieldFilter{
			{Field: "displayName", Operator: filtertypes.OpEqual, Value: "Engineering"},
			{Field: "externalId", Operator: filtertypes.OpEqual, Value: "okta-group-123"},
		},
	}

	translated := TranslateGroupFilterFields(filterSet)

	// Verify all fields were translated to Group struct field names
	assert.Equal(t, "DisplayName", translated.Filters[0].Field)
	assert.Equal(t, "ExternalID", translated.Filters[1].Field)

	// Verify other fields remain unchanged
	assert.Equal(t, filtertypes.OpEqual, translated.Filters[0].Operator)
	assert.Equal(t, "Engineering", translated.Filters[0].Value)
}

func TestTranslateGroupFilterFields_WithSorting(t *testing.T) {
	filterSet := filtertypes.FilterSet{
		Filters: []filtertypes.FieldFilter{
			{Field: "displayName", Operator: filtertypes.OpContains, Value: "Eng"},
		},
		Sort: &filtertypes.SortParams{
			SortBy:    "displayName",
			SortOrder: filtertypes.SortAscending,
		},
	}

	translated := TranslateGroupFilterFields(filterSet)

	// Verify filter field was translated to struct field name
	assert.Equal(t, "DisplayName", translated.Filters[0].Field)

	// Verify sort field was translated to struct field name
	require.NotNil(t, translated.Sort)
	assert.Equal(t, "DisplayName", translated.Sort.SortBy)
	assert.Equal(t, filtertypes.SortAscending, translated.Sort.SortOrder)
}

func TestFieldMapper_BuildMapping_GroupMember(t *testing.T) {
	mapper := NewFieldMapper()
	err := mapper.BuildMapping(reflect.TypeOf(auth.GroupMember{}))
	require.NoError(t, err)

	// Test SCIM -> Internal mapping for GroupMember
	// SCIM uses "value" for member references, which maps to struct field "UserID"
	internal, ok := mapper.GetInternalField("value")
	assert.True(t, ok, "SCIM field 'value' should be mapped")
	assert.Equal(t, "UserID", internal)
}

func TestGetGroupMemberFieldMapping(t *testing.T) {
	mapping := GetGroupMemberFieldMapping()

	// Verify essential mappings exist (tests package-level init, uses struct field names)
	assert.NotEmpty(t, mapping)
	assert.Equal(t, "UserID", mapping["value"])
}

func TestTranslateGroupMemberFilterFields(t *testing.T) {
	// Test that filter translation works for SCIM path filters like members[value eq "uuid"]
	filterSet := filtertypes.FilterSet{
		Filters: []filtertypes.FieldFilter{
			{Field: "value", Operator: filtertypes.OpEqual, Value: "550e8400-e29b-41d4-a716-446655440000"},
		},
	}

	translated := TranslateGroupMemberFilterFields(filterSet)

	// Verify "value" was translated to struct field "UserID"
	assert.Equal(t, "UserID", translated.Filters[0].Field)

	// Verify other fields remain unchanged
	assert.Equal(t, filtertypes.OpEqual, translated.Filters[0].Operator)
	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", translated.Filters[0].Value)
}

func TestTranslateGroupMemberFilterFields_StartsWith(t *testing.T) {
	// Test that filter translation works for "sw" (starts with) operator
	// This is used for SCIM path filters like members[value sw "550e"]
	filterSet := filtertypes.FilterSet{
		Filters: []filtertypes.FieldFilter{
			{Field: "value", Operator: filtertypes.OpStartsWith, Value: "550e"},
		},
	}

	translated := TranslateGroupMemberFilterFields(filterSet)

	// Verify "value" was translated to struct field "UserID"
	assert.Equal(t, "UserID", translated.Filters[0].Field)

	// Verify operator and value remain unchanged
	assert.Equal(t, filtertypes.OpStartsWith, translated.Filters[0].Operator)
	assert.Equal(t, "550e", translated.Filters[0].Value)
}
