// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package aws

import (
	"encoding/json"
	"io/ioutil"
	"sort"
	"strings"
)

// The below directives create two files, the first is a terraform main file
// that lives in the terraform subdirectory. The second file it produces is the
// bindata embedded.go file with the asset produced by running
// `terraform output`.

//go:generate terraformgen -o terraform/main.tf
//go:generate go-bindata -mode 0600 -modtime 1400000000 -pkg aws -o embedded.go config.json old.json
//go:generate gofmt -s -w embedded.go
//go:generate goimports -w embedded.go

// awsConfig holds all of the required information to create and manage aws
// cloud resources.
//
// The struct is constructed by deserializing json that follows the form of
// the below example.
//
//  {
//    "regions": {
//      "sensitive": false,
//      "type": "list",
//      "value": [
//          {
//              "ami_id": "ami-48630c2e",
//              "region": "ap-northeast-1",
//              "security_group": "sg-0006e480d77a10104",
//              "subnets": {
//                  "ap-northeast-1a": "subnet-0d144db3c9e47edf5",
//                  "ap-northeast-1c": "subnet-02fcaaa6212fc3c1a",
//                  "ap-northeast-1d": "subnet-0e9006ef8b3bef61f"
//              }
//          }
//      ]
//  }
//
// It has this awkward structure to deal with the terraform serialization
// of lists. Ideally terraform would output an artifact whose structure mirrors
// the serialization of the desired data structure elegantly but instead we use
// but it was deemed more straightforward to utilize the json.Unmarshaler
// interface to construct a well formed value from the terraform output.
type awsConfig struct {
	// regions is a slice of region structs sorted by name.
	regions []awsRegion
	// azByName maps from availability zone name to a struct which itself refers
	// back to a region.
	azByName map[string]*availabilityZone
}

type awsRegion struct {
	Name              string            `json:"region"`
	SecurityGroup     string            `json:"security_group"`
	AMI               string            `json:"ami_id"`
	AvailabilityZones availabilityZones `json:"subnets"`
}

type availabilityZone struct {
	name     string
	subnetID string
	region   *awsRegion // set up in awsConfig.UnmarshalJSON
}

// UnmarshalJSON implement json.Unmarshaler.
//
// The extra logic is used to sort the data by region and to hook up the
// azByName map to point from AZ name to availabilityZone and to set up the
// backpointers from availabilityZone to region.
func (c *awsConfig) UnmarshalJSON(data []byte) error {
	type raw struct {
		Regions struct {
			Value []awsRegion `json:"value"`
		} `json:"regions"`
	}
	var v raw
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	*c = awsConfig{
		regions:  v.Regions.Value,
		azByName: make(map[string]*availabilityZone),
	}
	sort.Slice(c.regions, func(i, j int) bool {
		return c.regions[i].Name < c.regions[j].Name
	})
	for i := range c.regions {
		r := &c.regions[i]
		for i := range r.AvailabilityZones {
			az := &r.AvailabilityZones[i]
			az.region = r
			c.azByName[az.name] = az
		}
	}
	return nil
}

func (c *awsConfig) getRegion(name string) *awsRegion {
	i := sort.Search(len(c.regions), func(i int) bool {
		return c.regions[i].Name >= name
	})
	if i < len(c.regions) && c.regions[i].Name == name {
		return &c.regions[i]
	}
	return nil
}

func (c *awsConfig) regionNames() (names []string) {
	for _, r := range c.regions {
		names = append(names, r.Name)
	}
	return names
}

func (c *awsConfig) getAvailabilityZone(azName string) *availabilityZone {
	return c.azByName[azName]
}

func (c *awsConfig) availabilityZoneNames() (zoneNames []string) {
	for _, r := range c.regions {
		for _, az := range r.AvailabilityZones {
			zoneNames = append(zoneNames, az.name)
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
			name:     az,
			subnetID: sn,
		})
	}
	sort.Slice(*s, func(i, j int) bool {
		return (*s)[i].name < (*s)[j].name
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
	if strings.HasPrefix(path, "embedded:") {
		data, err = Asset(path[strings.Index(path, ":")+1:])
	} else {
		data, err = ioutil.ReadFile(path)
	}
	if err != nil {
		return err
	}
	return json.Unmarshal(data, &c.awsConfig)
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
