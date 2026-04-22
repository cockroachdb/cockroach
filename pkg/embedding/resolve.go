// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package embedding

import (
	"net/url"

	"github.com/cockroachdb/cockroach/pkg/embedding/google"
	"github.com/cockroachdb/cockroach/pkg/embedding/openai"
	"github.com/cockroachdb/errors"
)

// ResolveRemoteEmbedder constructs a remote Embedder for the given
// model specification and external connection URI. The provider is
// determined by parsing the model spec (e.g.,
// "openai/text-embedding-3-small" routes to the OpenAI client). Each
// provider extracts the credentials it needs from the URI.
func ResolveRemoteEmbedder(modelSpec, connURI string) (Embedder, error) {
	provider, modelName := ParseModelSpec(modelSpec)
	if provider == "" {
		return nil, errors.Newf(
			"model %q is a local model; use GetEngine() instead", modelSpec,
		)
	}

	info, err := LookupModel(modelSpec)
	if err != nil {
		return nil, err
	}

	u, err := url.Parse(connURI)
	if err != nil {
		return nil, errors.Wrap(err, "parsing connection URI")
	}

	switch provider {
	case "openai":
		apiKey := u.Query().Get("api_key")
		if apiKey == "" {
			return nil, errors.Newf(
				"external connection missing api_key parameter",
			)
		}
		return openai.NewClient(apiKey, modelName, info.Dims), nil

	case "google":
		// Extract the region from the hostname (e.g.,
		// "us-east1-aiplatform.googleapis.com" → "us-east1").
		region := google.RegionFromHost(u.Host)
		if region == "" {
			return nil, errors.Newf(
				"cannot determine region from host %q; expected "+
					"REGION-aiplatform.googleapis.com", u.Host,
			)
		}

		multimodal := info.SupportsImage()

		// Service account credentials (auto-refreshing tokens).
		if creds := u.Query().Get("credentials"); creds != "" {
			saKey, err := google.ParseServiceAccountKey(creds)
			if err != nil {
				return nil, err
			}
			project := u.Query().Get("project")
			if project == "" {
				project = saKey.ProjectID
			}
			if project == "" {
				return nil, errors.Newf(
					"external connection missing project parameter " +
						"and service account JSON has no project_id",
				)
			}
			return google.NewClientWithServiceAccount(
				saKey, project, region, modelName, info.Dims,
				multimodal,
			), nil
		}

		// Static access token (short-lived, for testing).
		accessToken := u.Query().Get("access_token")
		if accessToken == "" {
			return nil, errors.Newf(
				"external connection missing credentials or " +
					"access_token parameter",
			)
		}
		project := u.Query().Get("project")
		if project == "" {
			return nil, errors.Newf(
				"external connection missing project parameter",
			)
		}
		return google.NewClient(
			accessToken, project, region, modelName, info.Dims,
			multimodal,
		), nil

	default:
		return nil, errors.Newf(
			"unsupported embedding provider: %q", provider,
		)
	}
}
