// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package distoss embeds the assets for the OSS version of the web UI into the
// Cockroach binary.

//go:build !bazel
// +build !bazel

package distoss

import (
	"embed"
	"io/fs"

	"github.com/cockroachdb/cockroach/pkg/ui"
)

//go:embed assets/*
var assets embed.FS

func init() {
	var err error
	ui.Assets, err = fs.Sub(assets, "assets")
	if err != nil {
		panic(err)
	}
	ui.HaveUI = true
}
