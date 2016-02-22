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
//
// Author: Matt Jibson (mjibson@cockroachlabs.com)

// +build acceptance

package acceptance

import "testing"

func substStatementTest(special string) []string {
	return []string{"/bin/sh", "-c", "acceptance/runjava.sh",
		"TestStatements.tmpl.java", "s/%v/" + special + "/g", "TestStatements.java"}
}

func TestDockerJava(t *testing.T) {
	testDockerSuccess(t, "java", substStatementTest("Int(2, 3)"))
	testDockerFail(t, "java", substStatementTest(`String(2, "a")`))
}
