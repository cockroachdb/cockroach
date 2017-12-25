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

package acceptance

import (
	"context"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestDockerNodeJS(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	ctx := context.Background()
	testDockerSuccess(ctx, t, "node.js", []string{"/bin/sh", "-c", strings.Replace(nodeJS, "%v", "", 1)})
	testDockerFail(ctx, t, "node.js", []string{"/bin/sh", "-c", strings.Replace(nodeJS, "%v", "fail", 1)})
}

const nodeJS = `
set -e
cd /mnt/data/node

export SHOULD_FAIL=%v
# Get access to globally installed node modules.
export NODE_PATH=$NODE_PATH:/usr/lib/node
/usr/lib/node/.bin/mocha .
`
