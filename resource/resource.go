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
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

// Package resource embeds into the Cockroach certain data such as web html
// and stylesheets.
package resource

//go:generate tsc -p ui/ts/

// If you add or change any files to the ui folder, you'll need to generate and
// make build for the new and/or updated files to appear.
//
// If you're planning on doing any development of the ui, add a -debug flag
// before the -pkg flag in the go:generate command below. This flag will
// instruct go-bindata to just proxy your local files so you'll be able to edit
// them live.  However, it will only proxy those files that were present the
// last time you ran go generate.
// Be sure to remove this flag and go generate before creating a PR. Also, make
// sure you clear the page cache when debugging or you might not see the
// changes.
//go:generate go-bindata -pkg resource -mode 0644 -modtime 1400000000 -o ./embedded.go ./ui/...

//go:generate gofmt -s -w embedded.go
//go:generate goimports -w embedded.go
