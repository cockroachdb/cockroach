// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cloudinfo

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"regexp"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/httputil"
)

const (
	aws                   = "aws"
	awsMetadataEndpoint   = "http://instance-data.ec2.internal/latest/dynamic/instance-identity/document"
	gcp                   = "gcp"
	gcpMetadataEndpoint   = "http://metadata.google.internal/computeMetadata/v1/instance/"
	azure                 = "azure"
	azureMetadataEndpoint = "http://169.254.169.254/metadata/instance?api-version=2018-10-01"
	instanceClass         = "instanceClass"
	region                = "region"
)

var enabled = true

// Disable disables cloud detection until the returned function is called.
// Used for tests that trigger diagnostics updates.
func Disable() (restore func()) {
	enabled = false
	return func() { enabled = true }
}

// client is necessary to provide a struct for mocking http requests
// in testing.
type client struct {
	httpClient *httputil.Client
}

type metadataReqHeader struct {
	key   string
	value string
}

// getAWSInstanceMetadata tries to access the AWS instance metadata
// endpoint to provide metadata about the node. The metadata structure
// is described at:
// https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instance-identity-documents.html
func (cli *client) getAWSInstanceMetadata(
	ctx context.Context, metadataElement string,
) (bool, string, string) {
	body, err := cli.getInstanceMetadata(ctx, awsMetadataEndpoint, []metadataReqHeader{})

	if err != nil {
		return false, "", ""
	}

	instanceMetadata := struct {
		InstanceClass string `json:"instanceType"`
		Region        string `json:"Region"`
	}{}

	if err := json.Unmarshal(body, &instanceMetadata); err != nil {
		return false, "", ""
	}

	switch metadataElement {
	case instanceClass:
		return true, aws, instanceMetadata.InstanceClass
	case region:
		return true, aws, instanceMetadata.Region
	default:
		return false, "", ""
	}
}

// getGCPInstanceMetadata tries to access the AWS instance metadata
// endpoint to provide metadata about the node. The metadata structure
// is described at:
// https://cloud.google.com/compute/docs/storing-retrieving-metadata
func (cli *client) getGCPInstanceMetadata(
	ctx context.Context, metadataElement string,
) (bool, string, string) {
	var endpointPattern string
	var requestEndpoint = gcpMetadataEndpoint

	switch metadataElement {
	case instanceClass:
		requestEndpoint += "machine-type"
		endpointPattern = `machineTypes\/(.+)$`
	case region:
		requestEndpoint += "zone"
		endpointPattern = `zones\/(.+)$`
	default:
		return false, "", ""
	}

	body, err := cli.getInstanceMetadata(ctx, requestEndpoint, []metadataReqHeader{{
		"Metadata-Flavor", "Google",
	}})

	if err != nil {
		return false, "", ""
	}

	resultRE := regexp.MustCompile(endpointPattern)

	result := resultRE.FindStringSubmatch(string(body))

	// Regex should only have 2 values: matched string and
	// capture group containing the machineTypes value.
	if len(result) != 2 {
		return false, "", ""
	}

	return true, gcp, result[1]

}

// getAzureInstanceMetadata tries to access the AWS instance metadata
// endpoint to provide metadata about the node. The metadata structure
// is described at:
// https://docs.microsoft.com/en-us/azure/virtual-machines/windows/instance-metadata-service
func (cli *client) getAzureInstanceMetadata(
	ctx context.Context, metadataElement string,
) (bool, string, string) {
	body, err := cli.getInstanceMetadata(ctx, azureMetadataEndpoint, []metadataReqHeader{{
		"Metadata", "true",
	}})

	if err != nil {
		return false, "", ""
	}

	instanceMetadata := struct {
		ComputeEnv struct {
			InstanceClass string `json:"vmSize"`
			Region        string `json:"Location"`
		} `json:"compute"`
	}{}

	if err := json.Unmarshal(body, &instanceMetadata); err != nil {
		return false, "", ""
	}

	switch metadataElement {
	case instanceClass:
		return true, azure, instanceMetadata.ComputeEnv.InstanceClass
	case region:
		return true, azure, instanceMetadata.ComputeEnv.Region
	default:
		return false, "", ""
	}
}

func (cli *client) getInstanceMetadata(
	ctx context.Context, url string, headers []metadataReqHeader,
) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	for _, header := range headers {
		req.Header.Set(header.key, header.value)
	}

	resp, err := cli.httpClient.Do(req)

	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	return ioutil.ReadAll(resp.Body)
}

// getCloudInfo provides a generic interface to iterate over the
// defined cloud functions, attempting to determine which platform
// the node is running on, as well as the value of the requested metadata
// element.
func getCloudInfo(ctx context.Context, metadataElement string) (provider string, element string) {
	if !enabled {
		return "", ""
	}

	const timeout = 500 * time.Millisecond
	cli := client{httputil.NewClientWithTimeout(timeout)}

	// getCloudMetadata lets us iterate over all of the functions to check
	// the defined clouds for the metadata element we're looking for.
	getCloudMetadata := []struct {
		get func(context.Context, string) (bool, string, string)
	}{
		{cli.getAWSInstanceMetadata},
		{cli.getGCPInstanceMetadata},
		{cli.getAzureInstanceMetadata},
	}

	var success bool

	for _, c := range getCloudMetadata {
		success, provider, element = c.get(ctx, metadataElement)
		if success {
			return provider, element
		}
	}
	return "", ""
}

// GetInstanceClass returns the node's instance provider (e.g. AWS) and
// the name given to its instance class (e.g. m5a.large).
func GetInstanceClass(ctx context.Context) (providerName string, instanceClassName string) {
	return getCloudInfo(ctx, instanceClass)
}

// GetInstanceRegion returns the node's instance provider (e.g. AWS) and
// the name given to its region (e.g. us-east-1d).
func GetInstanceRegion(ctx context.Context) (providerName string, regionName string) {
	return getCloudInfo(ctx, region)
}
