// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package embedding

import "github.com/cockroachdb/cockroach/pkg/settings"

// VectorizationEnabled controls whether vectorization features (embed(),
// embed_chunks(), embed_image(), CREATE/DROP VECTORIZER, and the
// vectorizer background job) are available. When disabled, these
// operations return a user-facing error.
var VectorizationEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.vectorize.enabled",
	"enables vectorization features including embed() builtins and CREATE VECTORIZER",
	false,
	settings.WithPublic,
)
