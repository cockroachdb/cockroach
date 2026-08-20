// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlstats

import "github.com/cockroachdb/errors"

// secretError is the sentinel for MarkSecretError; it never surfaces as an
// error itself.
var secretError = errors.New("error may quote a secret")

// MarkSecretError marks an error attributed to a statement that may carry a
// secret: a sensitive cluster setting value, a role password, or the argument
// list of an EXECUTE. Any such error may quote the secret (e.g. an HBA config
// parse error echoes the offending tokens), so the mark is applied based on
// statement identity, without assuming which error paths can embed the value.
// The mark survives wrapping; recording surfaces detect it with IsSecretError
// and reduce the recorded text to its redaction-safe parts. The
// client-visible error is unaffected.
func MarkSecretError(err error) error {
	return errors.Mark(err, secretError)
}

// IsSecretError reports whether err carries the MarkSecretError mark. It is
// nil-safe.
func IsSecretError(err error) bool {
	return errors.Is(err, secretError)
}
