// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ui

import (
	"embed"
	"io/fs"
)

// This file deals with embedding assets used by /debug/tracez.

//go:embed node_modules/list.js/dist/list.min.js
var listJS embed.FS

//go:embed templates/tracing/html_template.html
// SpansTableTemplateSrc contains a template used by /debug/tracez
var SpansTableTemplateSrc string

// ListJS exposes list.js package
var ListJS fs.FS

func init() {
	f, err := fs.Sub(listJS, "node_modules/list.js/dist")
	if err != nil {
		panic(err)
	}
	ListJS = f
}
