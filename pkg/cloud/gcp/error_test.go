// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gcp

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/googleapis/gax-go/v2/apierror"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/googleapi"
)

func TestErrorBehaviour(t *testing.T) {
	orig := &googleapi.Error{
		Code:    403,
		Message: "ACCESS DENIED. ALL YOUR BASE ARE BELONG TO US",
	}
	apiError, ok := apierror.ParseError(orig, false)
	if ok {
		orig.Wrap(apiError)
	}
	wrap1 := errors.Wrap(orig, "wrap1")
	wrap2 := errors.Wrap(wrap1, "wrap2")
	assert.Equal(t, "wrap1: googleapi: Error 403: ACCESS DENIED. ALL YOUR BASE ARE BELONG TO US", wrap1.Error())
	assert.Equal(t, "wrap2: wrap1: grpc: ACCESS DENIED. ALL YOUR BASE ARE BELONG TO US [code 2/Unknown]", wrap2.Error())
}
