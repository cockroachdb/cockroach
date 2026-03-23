// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scim

import (
	"fmt"
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/auth"
	filtertypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/types"
)

var (
	// userMapper is the reflection-based field mapper for User model.
	// It is initialized at package init time by extracting SCIM tags from the User struct.
	userMapper *FieldMapper

	// groupMapper is the reflection-based field mapper for Group model.
	// It is initialized at package init time by extracting SCIM tags from the Group struct.
	groupMapper *FieldMapper

	// groupMemberMapper is the reflection-based field mapper for GroupMember model.
	// It maps SCIM member reference field names (e.g., "value") to database columns (e.g., "user_id").
	groupMemberMapper *FieldMapper
)

func init() {
	// Build reflection-based mapping from User struct tags
	userMapper = NewFieldMapper()
	if err := userMapper.BuildMapping(reflect.TypeOf(auth.User{})); err != nil {
		panic(fmt.Sprintf("failed to build SCIM User field mapping: %v", err))
	}

	// Build reflection-based mapping from Group struct tags
	groupMapper = NewFieldMapper()
	if err := groupMapper.BuildMapping(reflect.TypeOf(auth.Group{})); err != nil {
		panic(fmt.Sprintf("failed to build SCIM Group field mapping: %v", err))
	}

	// Build reflection-based mapping from GroupMember struct tags
	groupMemberMapper = NewFieldMapper()
	if err := groupMemberMapper.BuildMapping(reflect.TypeOf(auth.GroupMember{})); err != nil {
		panic(fmt.Sprintf("failed to build SCIM GroupMember field mapping: %v", err))
	}
}

// GetUserFieldMapping returns the SCIM→Internal field mapping for User.
// This mapping is generated from struct tags and cached at init time.
func GetUserFieldMapping() map[string]string {
	return userMapper.ToMap()
}

// TranslateUserFilterFields translates SCIM field names in a FilterSet to internal database field names.
// It uses the reflection-based mapping generated from User struct tags.
func TranslateUserFilterFields(filterSet filtertypes.FilterSet) filtertypes.FilterSet {
	return translateFilterFieldsWithMapping(filterSet, GetUserFieldMapping())
}

// GetGroupFieldMapping returns the SCIM→Internal field mapping for Group.
// This mapping is generated from struct tags and cached at init time.
func GetGroupFieldMapping() map[string]string {
	return groupMapper.ToMap()
}

// TranslateGroupFilterFields translates SCIM field names in a FilterSet to internal database field names.
// It uses the reflection-based mapping generated from Group struct tags.
func TranslateGroupFilterFields(filterSet filtertypes.FilterSet) filtertypes.FilterSet {
	return translateFilterFieldsWithMapping(filterSet, GetGroupFieldMapping())
}

// GetGroupMemberFieldMapping returns the SCIM→Internal field mapping for GroupMember.
// This maps SCIM member reference field names (e.g., "value") to database columns (e.g., "user_id").
func GetGroupMemberFieldMapping() map[string]string {
	return groupMemberMapper.ToMap()
}

// TranslateGroupMemberFilterFields translates SCIM field names in a FilterSet to internal database field names.
// It uses the reflection-based mapping generated from GroupMember struct tags.
// This is used for SCIM path filters like members[value eq "uuid"] where "value" maps to "user_id".
func TranslateGroupMemberFilterFields(filterSet filtertypes.FilterSet) filtertypes.FilterSet {
	return translateFilterFieldsWithMapping(filterSet, GetGroupMemberFieldMapping())
}

// translateFilterFieldsWithMapping translates SCIM field names using the provided mapping.
func translateFilterFieldsWithMapping(
	filterSet filtertypes.FilterSet, mapping map[string]string,
) filtertypes.FilterSet {
	// Translate filter fields
	for i := range filterSet.Filters {
		if domainField, ok := mapping[filterSet.Filters[i].Field]; ok {
			filterSet.Filters[i].Field = domainField
		}
	}

	// Translate sort field
	if filterSet.Sort != nil {
		if domainField, ok := mapping[filterSet.Sort.SortBy]; ok {
			filterSet.Sort.SortBy = domainField
		}
	}

	return filterSet
}
