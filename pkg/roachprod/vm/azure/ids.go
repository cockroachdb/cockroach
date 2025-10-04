// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package azure

import (
	"fmt"
	"regexp"

	"github.com/cockroachdb/errors"
)

// The Azure API embeds quite a bit of useful data in a resource ID,
// however the golang SDK doesn't provide a built-in means of parsing
// this back out.
//
// This file can go away if
// https://github.com/Azure/azure-sdk-for-go/issues/3080
// is solved.

// azureIDPattern matches
// /subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/{namespace}/{type}/{name}[/childType/childName ...]
// If child resource type name pair(s) are not present, the last matching
// group will be empty. Otherwise, the last matching group will contain all
// additional pairs.
var azureIDPattern = regexp.MustCompile(
	`/subscriptions/([^/]+)/resourceGroups/([^/]+)/providers/([^/]+)/([^/]+)/([^/]+)(?:/(.*))?`)

// azureID defines a fully qualified Azure Resource ID which must contain a
// subscription ID, a resourceGroup name, a provider namespace, and at least
// one type and name pair. The first type name pair describes the resourceType
// and resourceName. For examples, reference unit tests.
//
// Additional type name pairs are optional, and if they exist, this Resource ID
// is describing a child resource or sometimes referred to as a subresource.
//
// Template:
//
//	/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/{namespace}/{type}/{name}[/childType/childName ...]
type azureID struct {
	// childResourceTypeNamePairs does not have its name pairs parsed into
	// struct fields because if present, the number of pairs is ambiguous
	childResourceTypeNamePairs string
	provider                   string
	resourceGroup              string
	resourceName               string
	resourceType               string
	subscription               string
}

func (id azureID) String() string {
	s := fmt.Sprintf("/subscriptions/%s/resourceGroups/%s/providers/%s/%s/%s",
		id.subscription, id.resourceGroup, id.provider, id.resourceType, id.resourceName)
	if id.childResourceTypeNamePairs != "" {
		s += "/" + id.childResourceTypeNamePairs
	}
	return s
}

func parseAzureID(id string) (azureID, error) {
	parts := azureIDPattern.FindStringSubmatch(id)
	if len(parts) != 7 {
		return azureID{}, errors.Errorf("could not parse Azure ID %q", id)
	}
	if parts[1] == "" || parts[2] == "" || parts[3] == "" || parts[4] == "" || parts[5] == "" {
		return azureID{}, errors.Errorf("Azure ID %q is missing a required field(s)", id)
	}
	ret := azureID{
		subscription:               parts[1],
		resourceGroup:              parts[2],
		provider:                   parts[3],
		resourceType:               parts[4],
		resourceName:               parts[5],
		childResourceTypeNamePairs: parts[6],
	}
	return ret, nil
}
