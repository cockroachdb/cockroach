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

package distoss

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
