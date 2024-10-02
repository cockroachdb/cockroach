// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package httputil

import "net/http"

// EtagHandler creates an http.Handler middleware that wraps another HTTP
// handler, adding support for the If-None-Match request header and ETag
// response header based on pre-computed file hashes. All responses include an
// ETag header with the hash provided in contentHashes. When a client provides
// an If-None-Match header with the hash found in contentHashes, no file is
// served and an HTTP 304 with no body is sent to clients instead, to indicate
// that the client's stale cache entry is still valid.
//
//   - contentHashes is a map of URL path (including a leading "/") to the ETag
//     value to use for that file
//   - next is the next handler in the http.Handler chain, used
func EtagHandler(contentHashes map[string]string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if contentHashes == nil {
			// If the hashed content map is erased, turn this into a no-op handler.
			next.ServeHTTP(w, r)
			return
		}

		ifNoneMatch := r.Header.Get("If-None-Match")
		checksum, checksumFound := contentHashes[r.URL.Path]
		// ETag header values are always wrapped in double-quotes
		wrappedChecksum := `"` + checksum + `"`

		if checksumFound {
			// Always add the ETag header for assets that support hash-based caching.
			//
			// * If the client requested the asset with the correct has in the
			//   If-None-Match header, its cache is stale! Returning the ETag again is
			//   required to indicate which hash should be used for the next request.
			// * If the client requested the asset with no If-None-Match header or an
			//   incorrect If-None-Match header, the content has changed since the
			//   last value and must be served with its identifying hash.
			w.Header().Add("ETag", wrappedChecksum)
		}

		if ifNoneMatch != "" && wrappedChecksum == ifNoneMatch {
			// The client still has this asset cached, but its cache is stale.
			// Return 304 with no body to tell the client that its cached version is
			// still fresh, and that it can use the provided ETag for its next
			// request.
			w.WriteHeader(304)
			return
		}

		// Either the client didn't send the correct hash, sent no hash, or the
		// requested asset isn't eligible for hash-based caching. Pass this
		// request to the next handler in the chain.
		next.ServeHTTP(w, r)
	})
}
