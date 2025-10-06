// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backuputils

import (
	"net/url"
	"path"

	"github.com/cockroachdb/cockroach/pkg/cloud"
)

// URLSeparator represents the standard separator used in backup URLs.
const URLSeparator = '/'

// RedactURIForErrorMessage redacts any storage secrets before returning a URI which is safe to
// return to the client in an error message.
func RedactURIForErrorMessage(uri string) string {
	redactedURI, err := cloud.SanitizeExternalStorageURI(uri, []string{})
	if err != nil {
		return "<uri_failed_to_redact>"
	}
	return redactedURI
}

// JoinURLPath forces a relative path join by removing any leading slash, then
// re-prepending it later.
//
// Stores are an odd combination of absolute and relative path.
// They present as absolute paths, since they contain a hostname. URL.Parse
// thus prepends each URL.Path with a leading slash.
// But some schemes, e.g. nodelocal, can legally travel _above_ the ostensible
// root (e.g. nodelocal://0/.../). This is not typically possible in file
// paths, and the standard path package doesn't like it. Specifically, it will
// clean up something like nodelocal://0/../ to nodelocal://0. This is normally
// correct behavior, but is wrong here.
//
// In point of fact we block this URLs resolved this way elsewhere. But we
// still want to make sure to resolve the paths correctly here. We don't want
// to accidentally correct an unauthorized file path to an authorized one, then
// write a backup to an unexpected place or print the wrong error message on
// a restore.
func JoinURLPath(args ...string) string {
	argsCopy := make([]string, 0)
	for _, arg := range args {
		if len(arg) == 0 {
			continue
		}
		// We only want non-empty tokens.
		argsCopy = append(argsCopy, arg)
	}
	if len(argsCopy) == 0 {
		return path.Join(argsCopy...)
	}

	// We have at least 1 arg, and each has at least length 1.
	isAbs := false
	if argsCopy[0][0] == URLSeparator {
		isAbs = true
		argsCopy[0] = argsCopy[0][1:]
	}
	joined := path.Join(argsCopy...)
	if isAbs {
		joined = string(URLSeparator) + joined
	}
	return joined
}

// AppendPaths appends the tailDir to the `path` of the passed in uris.
func AppendPaths(uris []string, tailDir ...string) ([]string, error) {
	retval := make([]string, len(uris))
	for i, uri := range uris {
		parsed, err := url.Parse(uri)
		if err != nil {
			return nil, err
		}
		joinArgs := append([]string{parsed.Path}, tailDir...)
		parsed.Path = JoinURLPath(joinArgs...)
		retval[i] = parsed.String()
	}
	return retval, nil
}
