// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package distccl embeds the assets for the CCL version of the web UI into the
// Cockroach binary.

//go:build !bazel
// +build !bazel

package distccl

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
