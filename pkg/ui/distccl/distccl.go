// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

// Package distccl embeds the assets for the CCL version of the web UI into the
// Cockroach binary.

//go:build bazel
// +build bazel

package distccl

import (
	"bytes"
	_ "embed"
	"io/fs"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/ui"
	"github.com/cockroachdb/cockroach/pkg/util/assetbundle"
)

//go:embed assets.tar.zst
var assets []byte
var loadOnce sync.Once
var lazyLoadedAssets fs.FS

func init() {
	ui.Assets = func() fs.FS {
		loadOnce.Do(func() {
			var err error
			lazyLoadedAssets, err = assetbundle.AsFS(bytes.NewBuffer(assets))
			if err != nil {
				panic(err)
			}
		})
		return lazyLoadedAssets
	}
	ui.HaveUI = true
}
