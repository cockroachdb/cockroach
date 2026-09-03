// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package avro

import (
	"fmt"
	"reflect"

	"github.com/cockroachdb/errors"
)

// logicalAttrs are the schema attributes that the Avro parsing canonical form
// strips but that change how consumers interpret decoded values. Together
// they identify a logical type: logicalType names it (date, time-micros,
// timestamp-micros, decimal, uuid, ...) and precision/scale parameterize
// decimal.
var logicalAttrs = []string{`logicalType`, `precision`, `scale`}

// compareLogicalAttrs compares the logical type annotations of the local and
// registered schemas at a single schema position. String-form schemas cannot
// carry annotations, so only the object form contributes values.
func compareLogicalAttrs(local, registered interface{}, path string) error {
	localObj, _ := local.(map[string]interface{})
	registeredObj, _ := registered.(map[string]interface{})
	for _, attr := range logicalAttrs {
		localVal, localOK := logicalAttr(localObj, attr)
		registeredVal, registeredOK := logicalAttr(registeredObj, attr)
		if localOK == registeredOK && (!localOK || reflect.DeepEqual(localVal, registeredVal)) {
			continue
		}
		return errors.Newf(
			`avro schema attribute %q at %s is %s in this changefeed's schema but %s in the `+
				`registered schema; the attribute does not change the binary encoding, so it is `+
				`invisible to the schema fingerprint, but consumers use it to interpret decoded `+
				`values, so it must match exactly; register a schema version whose logical type `+
				`annotations match`,
			attr, describePath(path),
			describeAttr(localVal, localOK), describeAttr(registeredVal, registeredOK))
	}
	return nil
}

// logicalAttr reads attr from the object form of a schema. It applies the one
// defaulting rule the Avro spec gives these attributes: a decimal's scale
// defaults to 0 when absent, so omitting "scale" and writing "scale":0 are
// the same logical type.
func logicalAttr(obj map[string]interface{}, attr string) (interface{}, bool) {
	if obj == nil {
		return nil, false
	}
	if v, ok := obj[attr]; ok {
		return v, true
	}
	if attr == `scale` && obj[`logicalType`] == `decimal` {
		return float64(0), true
	}
	return nil, false
}

func describePath(path string) string {
	if path == `` {
		return `the schema root`
	}
	return fmt.Sprintf(`%q`, path)
}

func describeAttr(v interface{}, ok bool) string {
	if !ok {
		return `absent`
	}
	if s, ok := v.(string); ok {
		return fmt.Sprintf(`%q`, s)
	}
	return fmt.Sprintf(`%v`, v)
}
