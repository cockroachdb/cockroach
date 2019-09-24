// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package azure

import (
	"regexp"

	"fmt"

	"github.com/cockroachdb/errors"
)

// The Azure API embeds quite a bit of useful data in a resource ID,
// however the golang SDK doesn't provide a built-in means of parsing
// this back out.
//
// This file can go away if
// https://github.com/Azure/azure-sdk-for-go/issues/3080
// is solved.

var azureIdPattern = regexp.MustCompile(
	"/subscriptions/(.+)/resourceGroups/(.+)/providers/(.+?)/(.+?)/(.+)")

type azureId struct {
	provider      string
	resourceGroup string
	resourceName  string
	resourceType  string
	subscription  string
}

func (id azureId) String() string {
	return fmt.Sprintf("/subscriptions/%s/resourceGroups/%s/providers/%s/%s/%s",
		id.subscription, id.resourceGroup, id.provider, id.resourceType, id.resourceName)
}

func parseAzureId(id string) (azureId, error) {
	parts := azureIdPattern.FindStringSubmatch(id)
	if len(parts) == 0 {
		return azureId{}, errors.Errorf("could not parse Azure ID %q", id)
	}
	ret := azureId{
		subscription:  parts[1],
		resourceGroup: parts[2],
		provider:      parts[3],
		resourceType:  parts[4],
		resourceName:  parts[5],
	}
	return ret, nil
}
