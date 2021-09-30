// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package a

import (
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/cockroachdb/redact/interfaces"
)

type someOtherType string

func init() {
	var ss1 redact.SafeString = "foo %d"
	var ss2 interfaces.SafeString = "foo %d"
	var os someOtherType = "foo %d"

	errors.New(string(ss1))
	errors.New(string(ss2))
	errors.New(string(os)) // want `message argument is not a constant expression`
}
