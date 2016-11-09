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
	"flag"
	"log"
	"os"
	"strings"

	"github.com/abourget/teamcity"
	"github.com/kisielk/gotool"
)

var buildTypeID = flag.String("build", "Cockroach_Nightlies_Stress", "the TeamCity build ID to start")
var branchName = flag.String("branch", "", "the VCS branch to build")
var addParams = flag.String("add-params", "", "comma-separated list of key=value build parameters to add")
var pkgs = flag.String("pkgs", "github.com/cockroachdb/cockroach/...", "packages to invoke the build for (using env.PKG); empty for invoking a single build without env.PKG")

const teamcityAPIUserEnv = "TC_API_USER"
const teamcityAPIPasswordEnv = "TC_API_PASSWORD"

func makeAddParams() map[string]string {
	sl := strings.Split(*addParams, ",")
	for i := range sl {
		sl[i] = strings.TrimSpace(sl[i])
	}
	m := map[string]string{}
	for _, kv := range sl {
		if kv == "" {
			continue
		}
		matches := strings.SplitN(kv, "=", 2)
		if len(matches) != 2 {
			log.Fatalf("unable to parse '%s'", kv)
		}
		m[matches[0]] = matches[1]
	}
	return m
}

func main() {
	flag.Parse()

	username, ok := os.LookupEnv(teamcityAPIUserEnv)
	if !ok {
		log.Fatalf("teamcity API username environment variable %s is not set", teamcityAPIUserEnv)
	}
	password, ok := os.LookupEnv(teamcityAPIPasswordEnv)
	if !ok {
		log.Fatalf("teamcity API password environment variable %s is not set", teamcityAPIPasswordEnv)
	}
	importPaths := []string{""}
	if len(*pkgs) > 0 {
		importPaths = gotool.ImportPaths([]string{*pkgs})
	}

	client := teamcity.New("teamcity.cockroachdb.com", username, password)
	for _, params := range []map[string]string{
		// TODO(tamird): also run a regular build?
		{"env.GOFLAGS": "-race"},
		{"env.TAGS": "deadlock"},
	} {
		for key, val := range makeAddParams() {
			_, ok := params[key]
			if ok {
				log.Fatalf("cannot overwrite existing param %s", key)
			}
			params[key] = val
		}
		for _, importPath := range importPaths {
			if importPath == "" {
				delete(params, "env.PKG")
			} else {
				params["env.PKG"] = importPath
			}
			build, err := client.QueueBuild(*buildTypeID, *branchName, params)
			if err != nil {
				log.Fatalf("failed to create teamcity build (*buildTypeID=%s *branchName=%s, params=%+v): %s", *buildTypeID, *branchName, params, err)
			}
			log.Printf("created teamcity build (*buildTypeID=%s *branchName=%s, params=%+v): %s", *buildTypeID, *branchName, params, build)
		}
	}
}
