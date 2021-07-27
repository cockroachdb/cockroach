// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package eavtest

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/eav"
	"github.com/cockroachdb/errors"
)

// AttributeMetadata defines a name and type.
type AttributeMetadata struct {
	Name string
	Type eav.Type
}

type attribute struct {
	AttributeMetadata
	ord eav.Ordinal
}

func (a attribute) String() string       { return a.Name }
func (a attribute) Ordinal() eav.Ordinal { return a.ord }
func (a attribute) Type() eav.Type       { return a.AttributeMetadata.Type }

func (a attribute) parseValue(value interface{}) (eav.Value, error) {
	// Allow for a special-case nil value.
	if str, isStr := value.(string); isStr && str == "nil" {
		return nil, nil
	}
	switch typ := a.Type(); typ {
	case eav.TypeInt32, eav.TypeInt64, eav.TypeUint32:
		var iv int64
		switch value := value.(type) {
		case float64:
			iv = int64(value)
		case int:
			iv = int64(value)
		default:
			return nil, errors.Errorf("illegal number type %T", value)
		}
		switch typ {
		case eav.TypeInt32:
			v := eav.Int32(iv)
			return &v, nil
		case eav.TypeInt64:
			v := eav.Int64(iv)
			return &v, nil
		case eav.TypeUint32:
			v := eav.Uint32(iv)
			return &v, nil
		default:
			panic("unreachable")
		}
	case eav.TypeString:
		sv, ok := value.(string)
		if !ok {
			return nil, errors.Errorf("numbers must be string, got %T", value)
		}
		return (*eav.String)(&sv), nil
	default:
		return nil, errors.Errorf("unknown type %s for attribute %s", typ, a.Name)
	}
}
