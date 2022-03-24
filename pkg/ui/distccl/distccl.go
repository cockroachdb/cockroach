// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

// Package distccl embeds the assets for the CCL version of the web UI into the
// Cockroach binary.

package distccl

import (
	"embed"

	"github.com/cockroachdb/cockroach/pkg/ui"
	"github.com/cockroachdb/cockroach/pkg/ui/buildutil"
)

//go:embed assets
var assets embed.FS

func init() {
	ui.Assets = assets
	ui.HaveUI = true

	assetHashes := make(map[string]string)
	err := buildutil.HashFilesInDir(&assetHashes, ui.Assets)
	if err != nil {
		panic(err)
	}
	ui.AssetHashes = assetHashes
}
