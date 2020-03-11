// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geoindex

import (
	"context"
	"encoding/json"
	"math/rand"
	"net/url"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/golang/geo/s2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestS2Index(t *testing.T) {
	rng, _ := randutil.NewPseudoRand()
	// rng.Seed(0)

	ctx := context.Background()
	i, err := New(Config{S2: &S2Config{}})
	require.NoError(t, err)

	const indexSize = 10
	bounds := usaBounds()
	indexed := make([]*geo.Geography, indexSize)
	for idx := range indexed {
		indexed[idx] = randRegion(rng, bounds)
	}

	store := newIndexStore()
	for idx := range indexed {
		keys, err := i.InvertedIndexKeys(ctx, indexed[idx])
		require.NoError(t, err)
		store.Write(keys, idx)
	}

	queryRegion := geo.NewGeography(geopb.UnknownSRID, randPoint(rng, bounds))
	query, err := i.Contains(ctx, queryRegion)
	require.NoError(t, err)

	matched := make([]bool, len(indexed))
	matchedIdxs := store.Read(query.Union, nil)
	for _, matchedIdx := range matchedIdxs {
		matched[matchedIdx] = true
		if query.Filter != nil {
			keep, err := query.Filter(queryRegion, indexed[matchedIdx])
			require.NoError(t, err)
			if !keep {
				matched[matchedIdx] = false
			}
		}
	}

	for idx := range indexed {
		expected, err := geo.STContainsGeography(queryRegion, indexed[idx])
		assert.NoError(t, err)
		assert.Equal(t, expected, matched[idx], "%s contains %s", queryRegion, indexed[idx])
	}

	if t.Failed() {
		t.Log(toDebugVizURI(indexed...))
	}
}

func usaBounds() s2.Rect {
	b := s2.NewRectBounder()
	b.AddPoint(s2.PointFromLatLng(s2.LatLngFromDegrees(18.91619, -171.791110603)))
	b.AddPoint(s2.PointFromLatLng(s2.LatLngFromDegrees(71.3577635769, -66.96466)))
	return b.RectBound()
}

func randRegion(rng *rand.Rand, bounds s2.Rect) *geo.Geography {
	switch rng.Intn(1) {
	case 0:
		return geo.NewGeography(geopb.UnknownSRID, randPoint(rng, bounds))
	case 1:
		return geo.NewGeography(geopb.UnknownSRID, randRect(rng, bounds))
	}
	panic(`unreachable`)
}

func randPoint(rng *rand.Rand, bounds s2.Rect) s2.Point {
	lo, hi := bounds.Lo(), bounds.Hi()
	lat := lo.Lat.Degrees() + rng.Float64()*(hi.Lat.Degrees()-lo.Lat.Degrees())
	lng := lo.Lng.Degrees() + rng.Float64()*(hi.Lng.Degrees()-lo.Lng.Degrees())
	return s2.PointFromLatLng(s2.LatLngFromDegrees(lat, lng))
}

func randRect(rng *rand.Rand, bounds s2.Rect) *s2.Polygon {
	b := s2.NewRectBounder()
	b.AddPoint(randPoint(rng, bounds))
	b.AddPoint(randPoint(rng, bounds))
	rect := b.RectBound()
	return s2.PolygonFromLoops([]*s2.Loop{s2.LoopFromPoints([]s2.Point{
		s2.PointFromLatLng(rect.Vertex(0)),
		s2.PointFromLatLng(rect.Vertex(1)),
		s2.PointFromLatLng(rect.Vertex(2)),
		s2.PointFromLatLng(rect.Vertex(3)),
		s2.PointFromLatLng(rect.Vertex(0)),
	})})
}

func toDebugVizURI(features ...*geo.Geography) string {
	return `http://geojson.io/#data=data:application/json,` +
		url.PathEscape(toGeoJSONFeatureCollection(features...))
}

func toGeoJSONFeatureCollection(features ...*geo.Geography) string {
	var featureCollection struct {
		Type     string            `json:"type"`
		Features []json.RawMessage `json:"features"`
	}
	featureCollection.Type = `FeatureCollection`
	for idx, feature := range features {
		properties := map[string]interface{}{
			`name`: strconv.Itoa(idx),
		}
		buf, err := geo.STAsGeoJSONGeography(feature, properties)
		if err != nil {
			panic(err)
		}
		featureCollection.Features = append(featureCollection.Features, json.RawMessage(buf))
	}
	buf, err := json.Marshal(featureCollection)
	if err != nil {
		panic(err)
	}
	return string(buf)
}
