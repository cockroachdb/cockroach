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
	classPath := ".:lib/junit.jar:lib/hamcrest.jar:lib/postgres.jar:src/main/java"
	testDockerSuccess(ctx, t, dockerTestConfig{
		name:  "java",
		cmd:   []string{"/bin/sh", "-c", strings.Replace(java, "%v", classPath, 2)},
		binds: []string{"java_test"},
	})
	// Ensure that having a bad classpath results in the test failing.
	testDockerFail(ctx, t, dockerTestConfig{
		name:  "java",
		cmd:   []string{"/bin/sh", "-c", strings.Replace(java, "%v", ``, 2)},
		binds: []string{"java_test"},
	})
}

const java = `
set -e
cd /java_test
# See: https://basildoncoder.com/blog/postgresql-jdbc-client-certificates.html
openssl pkcs8 -topk8 -inform PEM -outform DER -in /certs/node.key -out key.pk8 -nocrypt

export PATH=$PATH:/usr/lib/jvm/java-1.8-openjdk/bin
javac -cp %v src/main/java/main.java
java -cp %v org.junit.runner.JUnitCore main
`
