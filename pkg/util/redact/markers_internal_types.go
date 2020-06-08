// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package redact

import "reflect"

// RegisterSafeType registers a data type to always be considered safe
// during the production of redactable strings.
func RegisterSafeType(t reflect.Type) {
	safeTypeRegistry[t] = true
}

// safeTypeRegistry registers Go data types that are to be always
// considered safe, even when they don't implement SafeValue.
var safeTypeRegistry = map[reflect.Type]bool{}

func isSafeValue(a interface{}) bool {
	return safeTypeRegistry[reflect.TypeOf(a)]
}
