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
)

//go:embed assets
var assets embed.FS

func init() {
	ui.Assets = assets
	ui.HaveUI = true
}
