// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package exprutil

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// KVOptionValidationMap holds the valid entries and their policies for
// KVOptions.
type KVOptionValidationMap map[string]KVStringOptValidate

// KVStringOptValidate indicates the requested validation of a TypeAsStringOpts
// option.
type KVStringOptValidate string

// KVStringOptValidate values.
const (
	KVStringOptAny            KVStringOptValidate = `any`
	KVStringOptRequireNoValue KVStringOptValidate = `no-value`
	KVStringOptRequireValue   KVStringOptValidate = `value`
)

func (m KVOptionValidationMap) validate(opt tree.KVOption) error {
	k := string(opt.Key)
	validate, ok := m[k]
	if !ok {
		return errors.Errorf("invalid option %q", k)
	}

	if opt.Value == nil {
		if validate == KVStringOptRequireValue {
			return errors.Errorf("option %q requires a value", k)
		}
	} else if validate == KVStringOptRequireNoValue {
		return errors.Errorf("option %q does not take a value", k)
	}
	return nil
}
