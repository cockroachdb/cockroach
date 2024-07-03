// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package distoss embeds the assets for the OSS version of the web UI into the
// Cockroach binary.

//go:build !bazel
// +build !bazel

package distoss

import (
	"embed"
	"io/fs"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/ui"
)

//go:embed assets/*
var assets embed.FS
var loadOnce sync.Once
var lazyLoadedAssets fs.FS

func init() {
	ui.Assets = func() fs.FS {
		loadOnce.Do(func() {
			var err error
			lazyLoadedAssets, err = fs.Sub(assets, "assets")
			if err != nil {
				panic(err)
			}
		})
		return lazyLoadedAssets
	}
	ui.HaveUI = true
}
