// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cdctest

import (
	gojson "encoding/json"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/workload/rand"
)

func extractFieldFromJSON(d json.JSON) (interface{}, error) {
	switch d.Type() {
	case json.ArrayJSONType:
		iter, _ := d.AsArray()
		ret := make([]interface{}, 0)
		for i := 0; i < len(iter); i++ {
			if i, err := extractFieldFromJSON(iter[i]); err == nil {
				ret = append(ret, i)
			}
		}
		return ret, nil
	case json.ObjectJSONType:
		if g, err := geo.ParseGeographyFromGeoJSON([]byte(d.String())); err == nil {
			return rand.DatumToGoSQL(tree.NewDGeography(g))
		}
		if g, err := geo.ParseGeometryFromGeoJSON([]byte(d.String())); err == nil {
			return rand.DatumToGoSQL(tree.NewDGeometry(g))
		}
	default:
		break
	}
	var ret interface{}
	err := gojson.Unmarshal([]byte(d.String()), &ret)
	return ret, err
}

func parseJSON(d json.JSON) (map[string]interface{}, error) {
	if d == nil {
		return nil, nil
	}
	ret := make(map[string]interface{})
	switch d.Type() {
	case json.ObjectJSONType:
		iter, _ := d.ObjectIter()
		for iter.Next() {
			if i, err := extractFieldFromJSON(iter.Value()); err == nil {
				ret[iter.Key()] = i
			} else {
				return nil, err
			}
		}
		return ret, nil
	default:
		var ret map[string]interface{}
		err := gojson.Unmarshal([]byte(d.String()), &ret)
		return ret, err
	}
}
