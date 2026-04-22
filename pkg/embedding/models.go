// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package embedding

import (
	"strings"

	"github.com/cockroachdb/errors"
)

// ModelInfo describes an embedding model's static metadata.
type ModelInfo struct {
	// Dims is the output embedding dimension.
	Dims int
	// Provider is the provider prefix (empty for local models,
	// "openai" for OpenAI, etc.).
	Provider string
	// MaxTokens is the model's maximum input token limit.
	MaxTokens int
}

// IsLocal returns true if the model runs locally (no provider prefix).
func (m ModelInfo) IsLocal() bool {
	return m.Provider == ""
}

// modelRegistry maps model specification strings to their metadata.
var modelRegistry = map[string]ModelInfo{
	// Local ONNX models.
	"all-MiniLM-L6-v2": {
		Dims:      384,
		Provider:  "",
		MaxTokens: 256,
	},

	// OpenAI models.
	"openai/text-embedding-3-small": {
		Dims:      1536,
		Provider:  "openai",
		MaxTokens: 8191,
	},
	"openai/text-embedding-3-large": {
		Dims:      3072,
		Provider:  "openai",
		MaxTokens: 8191,
	},
	"openai/text-embedding-ada-002": {
		Dims:      1536,
		Provider:  "openai",
		MaxTokens: 8191,
	},

	// Google Vertex AI models.
	"google/text-embedding-004": {
		Dims:      768,
		Provider:  "google",
		MaxTokens: 2048,
	},
	"google/text-multilingual-embedding-002": {
		Dims:      768,
		Provider:  "google",
		MaxTokens: 2048,
	},
}

// ParseModelSpec splits a model specification string into its provider
// and model name components. For example, "openai/text-embedding-3-small"
// returns ("openai", "text-embedding-3-small"). For local models like
// "all-MiniLM-L6-v2", provider is empty and model is the full string.
func ParseModelSpec(spec string) (provider, model string) {
	if i := strings.IndexByte(spec, '/'); i >= 0 {
		return spec[:i], spec[i+1:]
	}
	return "", spec
}

// LookupModel returns the ModelInfo for the given model specification.
// Returns an error if the model is not in the registry.
func LookupModel(spec string) (ModelInfo, error) {
	info, ok := modelRegistry[spec]
	if !ok {
		return ModelInfo{}, errors.Newf("unknown embedding model %q", spec)
	}
	return info, nil
}
