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
//
// Currently, we are not using any Azure Resource IDs that contain child
// resource type name pairs, so we do not save them as a field in azureID. If
// we need to use them in the future we can add a field to azureID and set
// that field in parseAzureID.
// Note: the number of child resource pairs is ambiguous and currently the
// entire remainder of the string gets matched to the last group if a trailing
// '/' is present after resourceName.
var azureIDPattern = regexp.MustCompile(
	`/subscriptions/([^/]+)/resourceGroups/([^/]+)/providers/([^/]+)/([^/]+)/([^/]+)(?:/(.*))?`)

// azureID defines a fully qualified Azure Resource ID which must contain a
// subscription ID, a resourceGroup name, a provider namespace, and at least
// one type and name pair. The first type name pair describes the resourceType
// and resourceName. For examples, reference unit tests.
//
// Additional type name pairs are optional, and if they exist, this Resource ID
// is describing a child resource or sometimes referred to as a subresource.
// This struct does not contain a field for child resources. If a
// child resource ID needs to be parsed, this struct must be extended.
//
// Template:
//
//	/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/{namespace}/{type}/{name}[/childType/childName ...]
type azureID struct {
	provider      string
	resourceGroup string
	resourceName  string
	resourceType  string
	subscription  string
}

// String does not account for child resources since they are not saved after
// being parsed
func (id azureID) String() string {
	s := fmt.Sprintf("/subscriptions/%s/resourceGroups/%s/providers/%s/%s/%s",
		id.subscription, id.resourceGroup, id.provider, id.resourceType, id.resourceName)
	return s
}

func parseAzureID(id string) (azureID, error) {
	parts := azureIDPattern.FindStringSubmatch(id)
	if len(parts) != 7 {
		return azureID{}, errors.Errorf("could not parse Azure ID %q", id)
	}
	ret := azureID{
		subscription:  parts[1],
		resourceGroup: parts[2],
		provider:      parts[3],
		resourceType:  parts[4],
		resourceName:  parts[5],
	}
	return ret, nil
}
