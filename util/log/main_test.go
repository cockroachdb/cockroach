// Copyright 2015 The Cockroach Authors.
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
// Author: Ben Darnell

package log

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	tmpDir, err := ioutil.TempDir("", "logtest")
	if err != nil {
		Fatalf("could not create temporary directory: %s", err)
	}
	defer func() {
		if err := os.RemoveAll(tmpDir); err != nil {
			Errorf("failed to clean up temp directory: %s", err)
		}
	}()
	logDir = tmpDir
	os.Exit(m.Run())
}
