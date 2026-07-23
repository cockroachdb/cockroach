// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package avro

import (
	"encoding/json"
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/linkedin/goavro/v2"
)

// This file holds the exact-match helpers used by the key path in read-only
// registry mode (schema_registry.go): key bytes are the Kafka partitioning
// and compaction identity, so a registered key schema must match the local
// one exactly — by parsing-canonical fingerprint plus a positional walk over
// the logical type annotations the canonical form strips (see
// logical_types.go). Values instead go through the structural compatibility
// check in compat.go.

// SchemaFingerprint returns the CRC-64-AVRO (Rabin) fingerprint of the Avro
// parsing-canonical form of schema. Schemas with the same fingerprint produce
// identical binary encodings, but may still differ in attributes the canonical
// form strips, such as logical type annotations; see CheckLogicalTypesMatch.
// An error is returned if schema is not valid Avro.
func SchemaFingerprint(schema string) (uint64, error) {
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return 0, err
	}
	return codec.Rabin, nil
}

// CheckLogicalTypesMatch returns an error if two Avro schemas with equal
// parsing-canonical fingerprints disagree on their logical type annotations
// (logicalType, precision, scale).
//
// The Rabin fingerprint hashes the Avro parsing canonical form, which keeps
// only the attributes that determine the binary byte layout (name, type, fields,
// symbols, items, values, size) and strips everything else. That is the right
// equivalence for decodability: a fingerprint match guarantees that a consumer
// decoding with the registered schema recovers exactly the raw values we encoded.
// But logical type annotations are among the stripped attributes, and they tell
// consumers how to interpret those raw values: a long written as timestamp-micros
// but read as timestamp-millis is off by 1000x, and decimal bytes written with scale 4
// but read with scale 2 are off by 100x.
func CheckLogicalTypesMatch(localSchema, registeredSchema string) error {
	var local, registered interface{}
	if err := json.Unmarshal([]byte(localSchema), &local); err != nil {
		return errors.NewAssertionErrorWithWrappedErrf(err, "parsing local avro schema")
	}
	if err := json.Unmarshal([]byte(registeredSchema), &registered); err != nil {
		return errors.Wrap(err, "parsing registered avro schema")
	}
	return checkLogicalTypesMatch(local, registered, ``)
}

// checkLogicalTypesMatch recursively compares the logical type annotations of
// two schema JSON trees known to be canonically identical. path accumulates
// record field names and union branch indexes (e.g. "after[1].t[1]") for
// error messages.
func checkLogicalTypesMatch(local, registered interface{}, path string) error {
	if err := compareLogicalAttrs(local, registered, path); err != nil {
		return err
	}
	switch l := local.(type) {
	case []interface{}:
		// A union. The canonical form preserves branch count and order, so a
		// shape mismatch here means the fingerprint-equality precondition was
		// violated.
		r, ok := registered.([]interface{})
		if !ok || len(r) != len(l) {
			return errors.AssertionFailedf(
				`fingerprint-equal schemas disagree on union shape at %s`, describePath(path))
		}
		for i := range l {
			if err := checkLogicalTypesMatch(l[i], r[i], fmt.Sprintf(`%s[%d]`, path, i)); err != nil {
				return err
			}
		}
	case map[string]interface{}:
		r, ok := registered.(map[string]interface{})
		if !ok {
			// Object form opposite string form (e.g. {"type":"long"} vs
			// "long") can only pass the fingerprint comparison for an
			// annotation-free primitive, which compareLogicalAttrs above has
			// already accepted; there is nothing nested to descend into.
			return nil
		}
		if lFields, ok := l[`fields`].([]interface{}); ok {
			rFields, rok := r[`fields`].([]interface{})
			if !rok || len(rFields) != len(lFields) {
				return errors.AssertionFailedf(
					`fingerprint-equal schemas disagree on record fields at %s`, describePath(path))
			}
			for i := range lFields {
				lField, lok := lFields[i].(map[string]interface{})
				rField, rok := rFields[i].(map[string]interface{})
				if !lok || !rok {
					return errors.AssertionFailedf(
						`fingerprint-equal schemas have a malformed record field at %s`, describePath(path))
				}
				name, _ := lField[`name`].(string)
				childPath := name
				if path != `` {
					childPath = path + `.` + name
				}
				if err := checkLogicalTypesMatch(lField[`type`], rField[`type`], childPath); err != nil {
					return err
				}
			}
		}
		for _, attr := range []string{`items`, `values`} {
			// Array element and map value schemas.
			lNested, ok := l[attr]
			if !ok {
				continue
			}
			rNested, rok := r[attr]
			if !rok {
				return errors.AssertionFailedf(
					`fingerprint-equal schemas disagree on %q at %s`, attr, describePath(path))
			}
			if err := checkLogicalTypesMatch(lNested, rNested, path+`.`+attr); err != nil {
				return err
			}
		}
	}
	// Strings (primitive types and named-type references) carry no
	// annotations and nothing nested; the definition a reference points at is
	// compared where it appears.
	return nil
}
