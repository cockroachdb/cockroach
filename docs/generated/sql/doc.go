// Copyright 2017 The Cockroach Authors.
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

/*
Package sql is a dummy package that contains documentation generated from
sources in `pkg/sql/parser` using `docgen`.

These files are copied to the `docs` repository where they are included in pages
that document SQL. This happens periodically to ensure they stay up-to-date, for
instance during the the release process, but it is also strongly encouraged to
open a docs PR with the updated generated files following any PR that changes
them, so that the relevant context is still fresh in everyone's mind in case any
questions come up.
*/
package sql

//go:generate go run ../../../pkg/cmd/docgen/main.go ../../../pkg/cmd/docgen/funcs.go functions .
//go:generate go run ../../../pkg/cmd/docgen/main.go ../../../pkg/cmd/docgen/diagrams.go grammar bnf --addr ../../../pkg/sql/parser/sql.y bnf
