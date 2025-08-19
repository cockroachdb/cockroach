// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backuputils

import (
	"encoding/hex"
	"net/url"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/errors"
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
		appended, err := AppendPath(uri, tailDir...)
		if err != nil {
			return nil, err
		}
		retval[i] = appended
	}
	return retval, nil
}

// AppendPath appends the tailDir to the `path` of the passed in uri.
func AppendPath(uri string, tailDir ...string) (string, error) {
	parsed, err := url.Parse(uri)
	if err != nil {
		return "", err
	}
	joinArgs := append([]string{parsed.Path}, tailDir...)
	parsed.Path = JoinURLPath(joinArgs...)
	return parsed.String(), nil
}

// EncodeDescendingTS encodes a time.Time in a way such that later timestamps
// sort lexicographically before earlier timestamps. It is encoded as a hex
// string with millisecond precision.
//
// Note: This encoding only supports times within 292 million years of the Unix
// epoch. If you have a time after that, welcome to the 21st century, I hope you
// enjoy your stay.
func EncodeDescendingTS(ts time.Time) string {
	var buffer []byte
	buffer = encoding.EncodeUvarintDescending(buffer, uint64(ts.UnixMilli()))
	return hex.EncodeToString(buffer)
}

// AbsoluteBackupPathInCollectionURI returns the absolute path of a backup
// assuming the root is the collection URI. Backup URI represents the URI that
// points to the directory containing the backup manifest of the backup. Since
// this is an absolute path, it always starts with `/`. Any trailing slash is
// also removed.
//
// Example:
//
//	collectionURI: "nodelocal://1/collection"
//	backupURI: "nodelocal://1/collection/path/to/backup"
//	returns: "/path/to/backup"
func AbsoluteBackupPathInCollectionURI(collectionURI string, backupURI string) (string, error) {
	backupURL, err := url.Parse(backupURI)
	if err != nil {
		return "", err
	}
	collectionURL, err := url.Parse(collectionURI)
	if err != nil {
		return "", err
	}

	if backupURL.Scheme != collectionURL.Scheme || backupURL.Host != collectionURL.Host {
		return "", errors.New("backup URI does not share the same scheme and host as collection URI")
	}

	collectionPath := path.Clean(collectionURL.Path)
	if collectionPath == "." {
		collectionPath = ""
	}
	backupPath := path.Clean(backupURL.Path)
	if backupPath == "." {
		backupPath = ""
	}

	relPath, found := strings.CutPrefix(backupPath, collectionPath)
	if !found {
		return "", errors.New("backup URI not contained within collection URI")
	}

	relPath = strings.TrimSuffix(relPath, string(URLSeparator))
	if len(relPath) == 0 || relPath[0] != URLSeparator {
		relPath = string(URLSeparator) + relPath
	}
	return relPath, nil
}

// NormalizeSubdir takes a provided full backup subdirectory and normalizes it
// to the form /YYYY/MM/DD-HHMMSS.SS with a leading slash and no trailing slash.
func NormalizeSubdir(subdir string) (string, error) {
	subdirPattern := regexp.MustCompile(`\/?\d{4}\/\d{2}\/\d{2}-\d{6}\.\d{2}\/?`)
	if !subdirPattern.Match([]byte(subdir)) {
		return "", errors.Newf(
			`provided subdir "%s" does not match expected format YYYY/MM/DD-HHMMSS.SS`, subdir,
		)
	}
	normalized := strings.TrimSuffix(subdir, string(URLSeparator))
	if normalized[0] != URLSeparator {
		normalized = string(URLSeparator) + normalized
	}
	return normalized, nil
}
