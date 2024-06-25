// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"github.com/cockroachdb/cockroach/pkg/cloud/uris"
	"github.com/cockroachdb/errors"
)

// URI represents a string expression containing a URL. During formatting, the
// URL is sanitized to remove sensitive information such as passwords and keys.
type URI struct {
	Expr      Expr
	kms       bool
	sanitized bool
}

type URIs []URI

var _ NodeFormatter = URI{}
var _ NodeFormatter = URIs{}

// NewURI constructs a URI wrapper around the expression, which is a string
// literal containing an unsantized URL.
func NewURI(e Expr) URI {
	return URI{Expr: e}
}

// NewKMSURI constructs a URI wrapper around the expression, which is a string
// literal containing an unsantized KMS URL.
func NewKMSURI(e Expr) URI {
	return URI{Expr: e, kms: true}
}

// NewSanitizedURI constructs a URI wrapper around the string, which is a
// sanitized URL that should not be re-sanitized.
func NewSanitizedURI(sanitized string) URI {
	return URI{Expr: NewStrVal(sanitized), sanitized: true}
}

// NewSanitizedKMSURI constructs a URI wrapper around the string, which is a
// sanitized KMS URL that should not be re-sanitized.
func NewSanitizedKMSURI(sanitized string) URI {
	return URI{Expr: NewStrVal(sanitized), kms: true, sanitized: true}
}

// Format implements the NodeFormatter interface. If the URI wraps a string
// literal, we sanitize the URL contained in the string literal.
func (uri URI) Format(ctx *FmtCtx) {
	switch n := uri.Expr.(type) {
	case *StrVal, *DString:
		if uri.sanitized ||
			ctx.HasAnyFlags(FmtShowPasswords|FmtHideConstants|FmtMarkRedactionNode|FmtConstantsAsUnderscores) {
			ctx.FormatNode(n)
			return
		}
		var raw string
		var sanitized string
		var err error
		if str, ok := uri.Expr.(*StrVal); ok {
			raw = str.RawString()
		} else {
			raw = string(MustBeDString(uri.Expr))
		}
		if raw == "_" {
			// If we've re-parsed a URI formatted with FmtHideConstants, we should not
			// try to interpret it as a URL but should leave it as-is.
			ctx.FormatNode(n)
			return
		}
		if uri.kms {
			sanitized, err = uris.RedactKMSURI(raw)
		} else {
			sanitized, err = uris.SanitizeExternalStorageURI(raw, nil)
		}
		if err != nil {
			// We couldn't interpret the string as a URL. Redact the entire thing in
			// case it is a malformed URL containing secrets.
			ctx.WriteString(PasswordSubstitution)
			return
		}
		ctx.FormatNode(NewStrVal(sanitized))
	case *Placeholder:
		ctx.FormatNode(n)
	default:
		// We don't want to fail to sanitize other literals, so disallow other types
		// of expressions (which should already be disallowed by the parser anyway).
		panic(errors.AssertionFailedf("expected *StrVal, *DString, or *Placeholder, found %T", n))
	}
}

// Format implements the NodeFormatter interface.
func (uris URIs) Format(ctx *FmtCtx) {
	if len(uris) > 1 {
		ctx.WriteString("(")
	}
	for i, uri := range uris {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(uri)
	}
	if len(uris) > 1 {
		ctx.WriteString(")")
	}
}

// Exprs returns a slice of unwrapped Exprs from the URIs. These Exprs are not
// sanitized.
func (uris URIs) Exprs() Exprs {
	exprs := make(Exprs, len(uris))
	for i := range uris {
		exprs[i] = uris[i].Expr
	}
	return exprs
}
