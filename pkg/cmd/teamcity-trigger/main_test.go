// Copyright 2016 The Cockroach Authors.
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
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Tamir Duberstein (tamird@gmail.com)

package main

import (
	"strings"
	"testing"
)

func TestRunTC(t *testing.T) {
	count := 0
	runTC(func(parameters map[string]string) {
		count++
		if pkg, ok := parameters["env.PKG"]; ok {
			if strings.Contains(pkg, "/vendor/") {
				t.Errorf("unexpected package %s", pkg)
			}
		} else {
			t.Errorf("parameters did not include package: %+v", parameters)
		}
	})
	if count == 0 {
		t.Fatal("no builds were created")
	}
}
