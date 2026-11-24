// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package aws

import (
	"encoding/json"
	"os"
	"sort"
)

// The below directives create two files, the first is a terraform main file
// that lives in the terraform subdirectory. The second file it produces is the
// bindata embedded.go file with the asset produced by running
// `terraform output`.

//go:generate terraformgen -o terraform/main.tf

// awsConfig holds all of the required information to create and manage aws
// cloud resources.
//
// The struct is constructed by deserializing json that follows the form of
// the below example.
//
//	{
//	  "regions": {
//	    "sensitive": false,
//	    "type": "list",
//	    "value": [
//	        {
//	            "ami_id": "ami-48630c2e",
//	            "region": "ap-northeast-1",
//	            "security_group": "sg-0006e480d77a10104",
//	            "subnets": {
//	                "ap-northeast-1a": "subnet-0d144db3c9e47edf5",
//	                "ap-northeast-1c": "subnet-02fcaaa6212fc3c1a",
//	                "ap-northeast-1d": "subnet-0e9006ef8b3bef61f"
//	            }
//	        }
//	    ]
//	}
//
// It has this awkward structure to deal with the terraform serialization
// of lists. Ideally terraform would output an artifact whose structure mirrors
// the serialization of the desired data structure elegantly but instead we use
// but it was deemed more straightforward to utilize the json.Unmarshaler
// interface to construct a well formed value from the terraform output.
type awsConfig struct {
	// Regions is a slice of region structs sorted by name.
	Regions []AWSRegion
	// AZByName maps from availability zone name to a struct which itself refers
	// back to a region.
	AZByName map[string]*availabilityZone
}

type AWSRegion struct {
	Name              string            `json:"region"`
	SecurityGroup     string            `json:"security_group"`
	AMI_X86_64        string            `json:"ami_id"`
	AMI_ARM64         string            `json:"ami_id_arm64"`
	AMI_FIPS          string            `json:"ami_id_fips"`
	AvailabilityZones availabilityZones `json:"subnets"`
}

type availabilityZone struct {
	Name     string
	SubnetID string
	Region   *AWSRegion // set up in awsConfig.UnmarshalJSON
}

// UnmarshalJSON implement json.Unmarshaler.
//
// The extra logic is used to sort the data by region and to hook up the
// azByName map to point from AZ name to availabilityZone and to set up the
// backpointers from availabilityZone to region.
func (c *awsConfig) UnmarshalJSON(data []byte) error {
	type raw struct {
		Regions struct {
			Value []AWSRegion `json:"value"`
		} `json:"regions"`
	}
	var v raw
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	*c = awsConfig{
		Regions:  v.Regions.Value,
		AZByName: make(map[string]*availabilityZone),
	}
	sort.Slice(c.Regions, func(i, j int) bool {
		return c.Regions[i].Name < c.Regions[j].Name
	})
	for i := range c.Regions {
		r := &c.Regions[i]
		for i := range r.AvailabilityZones {
			az := &r.AvailabilityZones[i]
			az.Region = r
			c.AZByName[az.Name] = az
		}
	}
	return nil
}

func (c *awsConfig) regionNames() (names []string) {
	for _, r := range c.Regions {
		names = append(names, r.Name)
	}
	return names
}

func (c *awsConfig) getAvailabilityZone(azName string) *availabilityZone {
	return c.AZByName[azName]
}

func (c *awsConfig) availabilityZoneNames() (zoneNames []string) {
	for _, r := range c.Regions {
		for _, az := range r.AvailabilityZones {
			zoneNames = append(zoneNames, az.Name)
		}
	}
	sort.Strings(zoneNames)
	return zoneNames
}

// availabilityZones is a slice of availabilityZone which implements
// json.Marshaler and json.Unmarshaler.
type availabilityZones []availabilityZone

func (s *availabilityZones) UnmarshalJSON(data []byte) error {
	var m map[string]string
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}
	*s = make(availabilityZones, 0, len(m))
	for az, sn := range m {
		*s = append(*s, availabilityZone{
			Name:     az,
			SubnetID: sn,
		})
	}
	sort.Slice(*s, func(i, j int) bool {
		return (*s)[i].Name < (*s)[j].Name
	})
	return nil
}

// awsConfigValue implements pflag.Value and is used to accept a path flag and
// use it to open a file and parse an awsConfig.
type awsConfigValue struct {
	path string
	awsConfig
}

// Set is part of the pflag.Value interface.
func (c *awsConfigValue) Set(path string) (err error) {
	if path == "" {
		return nil
	}
	c.path = path
	var data []byte
	if path == "embedded:config.json" {
		data = configJson
	} else if path == "embedded:old.json" {
		data = oldJson
	} else {
		data, err = os.ReadFile(path)
	}
	if err != nil {
		return err
	}
	err = json.Unmarshal(data, &c.awsConfig)
	if err != nil {
		return err
	}
	return nil
}

// Type is part of the pflag.Value interface.
func (c *awsConfigValue) Type() string {
	return "aws config path"
}

// String is part of the pflag.Value interface.
func (c *awsConfigValue) String() string {
	if c.path == "" {
		return "see config.json"
	}
	return c.path
}
