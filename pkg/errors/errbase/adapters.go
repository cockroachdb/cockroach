// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package errbase

import (
	goErr "errors"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	pkgErr "github.com/pkg/errors"
)

// This file provides the library the ability to encode/decode
// standard error types.

// errors.errorString from base Go does not need an encoding
// function, because the base encoding logic in EncodeLeaf() is
// able to extract everything about it.

// we can then decode it exactly.
func decodeErrorString(msg string, _ []string, _ protoutil.SimpleMessage) error {
	return goErr.New(msg)
}

// errors.fundamental from github.com/pkg/errors cannot be encoded
// exactly because it includes a non-serializable stack trace
// object. In order to work with it, we encode it by dumping
// the stack trace in a safe reporting detail field, and decode
// it as an opaqueLeaf instance in this package.

func encodePkgFundamental(err error) (msg string, safe []string, _ protoutil.SimpleMessage) {
	msg = err.Error()
	iErr := err.(interface{ StackTrace() pkgErr.StackTrace })
	safeDetails := []string{fmt.Sprintf("%+v", iErr.StackTrace())}
	return msg, safeDetails, nil
}

// errors.withMessage from github.com/pkg/errors can be encoded
// exactly because it just has a message prefix. The base encoding
// logic in EncodeWrapper() is able to extract everything from it.

// we can then decode it exactly.
func decodeWithMessage(cause error, msgPrefix string, _ []string, _ protoutil.SimpleMessage) error {
	return pkgErr.WithMessage(cause, msgPrefix)
}

// errors.withStack from github.com/pkg/errors cannot be encoded
// exactly because it includes a non-serializable stack trace
// object. In order to work with it, we encode it by dumping
// the stack trace in a safe reporting detail field, and decode
// it as an opaqueWrapper instance in this package.

func encodePkgWithStack(err error) (msgPrefix string, safe []string, _ protoutil.SimpleMessage) {
	iErr := err.(interface{ StackTrace() pkgErr.StackTrace })
	safeDetails := []string{fmt.Sprintf("%+v", iErr.StackTrace())}
	return "" /* withStack does not have a message prefix */, safeDetails, nil
}

func init() {
	baseErr := goErr.New("")
	RegisterLeafDecoder(FullTypeName(baseErr), decodeErrorString)

	pkgE := pkgErr.New("")
	RegisterLeafEncoder(FullTypeName(pkgE), encodePkgFundamental)

	RegisterWrapperDecoder(FullTypeName(pkgErr.WithMessage(baseErr, "")), decodeWithMessage)

	ws := pkgErr.WithStack(baseErr)
	RegisterWrapperEncoder(FullTypeName(ws), encodePkgWithStack)
}
