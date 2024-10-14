// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cloud

import (
	"testing"

	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
)

func TestGetTagsValues(t *testing.T) {
	IAMUserNameKey := "IAMUserName"
	createdAtKey := "CreatedAt"

	tcIAMUserNameValue := "user.name"
	tcCreatedAtValue := "2021-09-20T17:05:53Z"

	testCases := []struct {
		description         string
		inputTags           []ec2types.Tag
		expectedIAMUserName string
		expectedCreatedAt   string
	}{
		{
			description:         "untagged",
			inputTags:           []ec2types.Tag{},
			expectedIAMUserName: "",
			expectedCreatedAt:   "",
		},
		{
			description:         "only IAMUserName tag present",
			inputTags:           []ec2types.Tag{{Key: &IAMUserNameKey, Value: &tcIAMUserNameValue}},
			expectedIAMUserName: tcIAMUserNameValue,
			expectedCreatedAt:   "",
		},
		{
			description:         "only CreatedAt tag present",
			inputTags:           []ec2types.Tag{{Key: &createdAtKey, Value: &tcCreatedAtValue}},
			expectedIAMUserName: "",
			expectedCreatedAt:   tcCreatedAtValue,
		},
		{
			description: "IAMUserName and CreatedAt tags present",
			inputTags: []ec2types.Tag{
				{Key: &IAMUserNameKey, Value: &tcIAMUserNameValue},
				{Key: &createdAtKey, Value: &tcCreatedAtValue}},
			expectedIAMUserName: tcIAMUserNameValue,
			expectedCreatedAt:   tcCreatedAtValue,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			returnedIAMUserName, returnedCreatedAt := getTagsValues(tc.inputTags)
			assert.EqualValues(t, tc.expectedIAMUserName, returnedIAMUserName)
			assert.EqualValues(t, tc.expectedCreatedAt, returnedCreatedAt)
		})
	}
}

func TestGetIAMUserNameFromKeyname(t *testing.T) {
	// expected format: ${username}-${28 characters hash}
	testCases := []struct {
		description    string
		inputKeyname   string
		expectedOutput string
	}{
		{"less than 29 characters", "username-SHA1HashBase64Encod", ""},
		{"exactly 29 characters", "username-SHA1HashBase64Encode", ""},
		{"more than 29 characters but unexpected format", "username-SHA1HashBase64Encoded", ""},
		{"normal", "username-SHA1HashBase64Encoded1234567", "username"},
		{"username contains dot", "user.name-SHA1HashBase64Encoded1234567", "user.name"},
		{"username contains dashes", "a-b-c---d-e-f--g---SHA1HashBase64Encoded1234567", "a-b-c---d-e-f--g--"},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			returnedIAMUserName := getIAMUserNameFromKeyname(tc.inputKeyname)
			assert.EqualValues(t, tc.expectedOutput, returnedIAMUserName)
		})
	}
}
