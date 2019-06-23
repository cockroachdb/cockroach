// Copyright 2016 The Cockroach Authors.
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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestAWSInstanceMetadataParsing(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// b is a response taken from the AWS instance identity
	// document internal endpoint on May 2 2019
	b := []byte(`{
		"devpayProductCodes" : null,
		"marketplaceProductCodes" : null,
		"version" : "2017-09-30",
		"pendingTime" : "2019-04-03T13:48:24Z",
		"imageId" : "ami-0a313d6098716f372",
		"instanceType" : "m5a.large",
		"billingProducts" : null,
		"instanceId" : "i-095b80809c3607e88",
		"availabilityZone" : "us-east-1d",
		"kernelId" : null,
		"ramdiskId" : null,
		"accountId" : "55153",
		"architecture" : "x86_64",
		"privateIp" : "172.31.29.00",
		"region" : "us-east-1"
		}`)

	s, p, i := parseAWSInstanceMetadata(b)

	if !s {
		t.Fatalf("expected parsing to succeed")
	}

	if p != aws {
		t.Fatalf("expected parsing to deduce AWS")
	}

	if i != "m5a.large" {
		t.Fatalf("expected parsing to get instanceType m5a.large")
	}
}

func TestGCPInstanceMetadataParsing(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// b is a response taken from the GCP internal metadata
	// endpoint on May 2 2019
	b := []byte(`projects/93358566124/machineTypes/g1-small`)

	s, p, i := parseGCPInstanceMetadata(b)

	if !s {
		t.Fatalf("expected parsing to succeed")
	}

	if p != gcp {
		t.Fatalf("expected parsing to deduce GCP")
	}

	if i != "g1-small" {
		t.Fatalf("expected parsing to get machineTypes g1-small")
	}
}

func TestAzureInstanceMetadataParsing(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// b is a response taken from the Azure internal metadata
	// endpoint on May 2 2019
	b := []byte(`{  
		"compute":{  
		   "azEnvironment":"AzurePublicCloud",
		   "location":"eastus",
		   "name":"mock-instance-class",
		   "offer":"Debian",
		   "osType":"Linux",
		   "placementGroupId":"",
		   "plan":{  
			  "name":"",
			  "product":"",
			  "publisher":""
		   },
		   "platformFaultDomain":"0",
		   "platformUpdateDomain":"0",
		   "provider":"Microsoft.Compute",
		   "publicKeys":[  
			  {  
				 "keyData":"ssh-rsa AAAA...",
				 "path":"/home/..."
			  }
		   ],
		   "publisher":"credativ",
		   "resourceGroupName":"Default-Storage-EastUS2",
		   "sku":"9-backports",
		   "subscriptionId":"eebc0b2a-9ff2-499c-9e75",
		   "tags":"",
		   "version":"9.20190313.0",
		   "vmId":"fd978cc8-ed9a-439e-b3e5",
		   "vmScaleSetName":"",
		   "vmSize":"Standard_D2s_v3",
		   "zone":""
		},
		"network":{  
		   "interface":[  
			  {  
				 "ipv4":{  
					"ipAddress":[  
					   {  
						  "privateIpAddress":"10.0.0.5",
						  "publicIpAddress":"13.82.189.00"
					   }
					],
					"subnet":[  
					   {  
						  "address":"10.0.0.0",
						  "prefix":"24"
					   }
					]
				 },
				 "ipv6":{  
					"ipAddress":[  
	 
					]
				 },
				 "macAddress":"000D3A5414F6"
			  }
		   ]
		}
	 }`)

	s, p, i := parseAzureInstanceMetadata(b)

	if !s {
		t.Fatalf("expected parsing to succeed")
	}

	if p != azure {
		t.Fatalf("expected parsing to deduce Azure")
	}

	if i != "Standard_D2s_v3" {
		t.Fatalf("expected parsing to get machineTypes Standard_D2s_v3")
	}
}
