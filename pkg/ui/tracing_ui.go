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

//go:embed dist_vendor/*
var vendorFiles embed.FS

// VendorFS exposes the list.js package.
var VendorFS fs.FS

//go:embed templates/tracing/html_template.html
// SpansTableTemplateSrc contains a template used by /debug/tracez
var SpansTableTemplateSrc string

func init() {
	var err error
	VendorFS, err = fs.Sub(vendorFiles, "dist_vendor")
	if err != nil {
		panic(err)
	}
}
