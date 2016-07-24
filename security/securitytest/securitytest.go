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
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

// Package securitytest embeds the TLS test certificates.
package securitytest

import (
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"golang.org/x/net/context"
)

//go:generate go-bindata -pkg securitytest -mode 0644 -modtime 1400000000 -o ./embedded.go -ignore README.md -prefix ../../resource ../../resource/test_certs/...
//go:generate gofmt -s -w embedded.go
//go:generate goimports -w embedded.go

// RestrictedCopy creates an on-disk copy of the embedded security asset
// with the provided path. The copy will be created in the provided directory.
// Returns the path of the file and a cleanup function that will delete the file.
//
// The file will have restrictive file permissions (0600), making it
// appropriate for usage by libraries that require security assets to have such
// restrictive permissions.
func RestrictedCopy(t util.Tester, path, tempdir, name string) string {
	contents, err := Asset(path)
	if err != nil {
		if t == nil {
			log.Fatal(context.TODO(), err)
		} else {
			t.Fatal(err)
		}
	}
	return util.CreateRestrictedFile(t, contents, tempdir, name)
}
