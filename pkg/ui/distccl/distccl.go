// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package distccl embeds the assets for the CCL version of the web UI into the
// Cockroach binary.

//go:build bazel

package distccl

import (
	"bytes"
	_ "embed"

	"github.com/cockroachdb/cockroach/pkg/ui"
	"github.com/cockroachdb/cockroach/pkg/util/assetbundle"
)

//go:embed assets.tar.zst
var assets []byte

func init() {
	fs, err := assetbundle.AsFS(bytes.NewBuffer(assets))
	if err != nil {
		panic(err)
	}
	ui.Assets = fs
	ui.HaveUI = true
}
