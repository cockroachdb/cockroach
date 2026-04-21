// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package dbconsole

import (
	"context"
	"net/http"

	"github.com/cockroachdb/cockroach/pkg/server/apiutil"
	"github.com/cockroachdb/cockroach/pkg/settings"
)

// Feature represents a feature-flagged DB Console page. Each feature gets a
// dedicated cluster setting (dbconsole.feature_flag.<name>) that acts as a
// toggle. When enabled, the feature appears in the sidebar and its BFF
// endpoints accept requests. When disabled, the sidebar entry is hidden and
// endpoints return 403.
type Feature struct {
	Name        string
	Title       string
	Description string
	RoutePath   string
	Setting     *settings.BoolSetting
}

var features []*Feature

// RegisterFeature adds a feature to the registry and creates its backing
// cluster setting. Call this from init() in each feature's Go file.
func RegisterFeature(f *Feature) {
	f.Setting = settings.RegisterBoolSetting(
		settings.ApplicationLevel,
		settings.InternalKey("dbconsole.feature_flag."+f.Name),
		f.Description,
		false,
		settings.WithPublic,
	)
	features = append(features, f)
}

// GetFeatures returns all registered features.
func GetFeatures() []*Feature {
	return features
}

// LookupFeature finds a feature by name, or returns nil.
func LookupFeature(name string) *Feature {
	for _, f := range features {
		if f.Name == name {
			return f
		}
	}
	return nil
}

// requireFeatureEnabled checks whether the named feature is enabled. If not,
// it writes a 403 JSON response and returns false. Handlers should return
// immediately when this returns false.
func requireFeatureEnabled(
	ctx context.Context, name string, sv *settings.Values, w http.ResponseWriter,
) bool {
	f := LookupFeature(name)
	if f == nil || !f.Setting.Get(sv) {
		apiutil.WriteJSONResponse(
			ctx, w, http.StatusForbidden,
			ErrorResponse{Error: "feature " + name + " is not enabled"},
		)
		return false
	}
	return true
}
