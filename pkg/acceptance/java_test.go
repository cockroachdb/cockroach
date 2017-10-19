// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package acceptance

import (
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/log"

	"golang.org/x/net/context"
)

func TestDockerJava(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	ctx := context.Background()
	testDockerSuccess(ctx, t, "java", []string{"/bin/sh", "-c", strings.Replace(java, "%v", "test", 2)})
	testDockerFail(ctx, t, "java", []string{"/bin/sh", "-c", strings.Replace(java, "%v", "foobar", 2)})
}

const java = `
set -e
cd /testdata/java
# See: https://basildoncoder.com/blog/postgresql-jdbc-client-certificates.html
openssl pkcs8 -topk8 -inform PEM -outform DER -in /certs/node.key -out key.pk8 -nocrypt

mvn %v -o
rm -rf target
`
