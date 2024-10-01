// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rel

import (
	"fmt"
	"unsafe"
)

// GetAttribute returns the requested attribute from the passed entity. If
// the entity is of a type not defined for this schema, an error will be
// returned. If the entity is nil, an error will be returned. If the entity
// does not have this attribute defined, an error will be returned. If the
// entity does not have a populated value for this attribute, a nil value
// will be returned without an error.
func (sc *Schema) GetAttribute(attribute Attr, v interface{}) (interface{}, error) {
	ord, err := sc.getOrdinal(attribute)
	if err != nil {
		return nil, err
	}
	ti, value, err := getEntityValueInfo(sc, v)
	if err != nil {
		return nil, err
	}

	fi, ok := ti.attrFields[ord]
	if !ok {
		// We avoid using errors.Errorf here because this is a hot path that
		// showed up when profiling TestRandomSyntaxSchemaChangeColumn. Errorf
		// calls runtime.Callers to construct the call stack, which is expensive.
		// No production code actually uses this error message, so using a
		// simpler error doesn't have much downside.
		return nil, fmt.Errorf("no field defined on type")
	}

	// There may be more than one field which stores this attribute. At most one
	// such field should be populated.
	for i := range fi {
		got := fi[i].value(unsafe.Pointer(value.Pointer()))
		if got != nil {
			return got, nil
		}
	}
	// No field was populated.
	return nil, nil
}
