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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter.mattis@gmail.com)

// +build acceptance

package acceptance

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/acceptance/localcluster"
	"github.com/cockroachdb/cockroach/util"
)

func TestBuildInfo(t *testing.T) {
	l := localcluster.Create(1, stopper)
	l.Start()
	defer l.Stop()

	util.SucceedsWithin(t, 10*time.Second, func() error {
		select {
		case <-stopper:
			t.Fatalf("interrupted")
			return nil
		case <-time.After(200 * time.Millisecond):
			break
		}
		var r struct {
			BuildInfo map[string]string
		}
		if err := l.Nodes[0].GetJSON("", "/_status/local/", &r); err != nil {
			return err
		}
		for _, key := range []string{"goVersion", "tag", "time", "dependencies"} {
			if val, ok := r.BuildInfo[key]; !ok {
				t.Errorf("build info missing for \"%s\"", key)
			} else if val == "" {
				t.Errorf("build info not set for \"%s\"", key)
			}
		}
		return nil
	})
}
