// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Levon Lloyd (levon.lloyd@gmail.com)

package storage

import (
	"reflect"
	"testing"

	"github.com/golang/glog"
)

var testConfig = ZoneConfig{
	Replicas: map[string][]DiskType{
		"a": []DiskType{SSD, HDD},
		"b": []DiskType{SSD, HDD},
	},
}

func TestZoneConfigRoundTrip(t *testing.T) {
	yaml, err := testConfig.ToYAML()
	if err != nil {
		glog.Fatalf("failed converting to yaml: %v", err)
	}
	parsedZoneConfig, err := ParseZoneConfig(yaml)
	if err != nil {
		glog.Fatalf("failed parsing config: %v", err)
	}
	if !reflect.DeepEqual(testConfig, *parsedZoneConfig) {
		glog.Fatalf("yaml round trip configs differ.\nOriginal: %+v\nParse: %+v\n", testConfig, parsedZoneConfig)
	}
}
