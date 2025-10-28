// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package future

import (
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
)

func HandleFuture(w http.ResponseWriter, r *http.Request) {
	// Get the requested path and strip the /future prefix
	requestPath := r.URL.Path
	requestPath = strings.TrimPrefix(requestPath, "/future")
	requestPath = strings.TrimPrefix(requestPath, "/")

	// Default to index.html for root
	if requestPath == "" {
		requestPath = "index.html"
	}

	// Clean the path to prevent directory traversal
	requestPath = path.Clean(requestPath)

	// Construct the full file path relative to the assets directory
	// The assets directory is assumed to be relative to the current working directory
	assetsDir := "pkg/ui/future/assets"
	fullPath := filepath.Join(assetsDir, requestPath)

	// Security check: ensure the resolved path is still within the assets directory
	absAssetsDir, err := filepath.Abs(assetsDir)
	if err != nil {
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	absFullPath, err := filepath.Abs(fullPath)
	if err != nil {
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Ensure the file path is within the assets directory
	if !strings.HasPrefix(absFullPath, absAssetsDir) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	// Check if the file exists and is not a directory
	fileInfo, err := os.Stat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			http.Error(w, "File not found", http.StatusNotFound)
			return
		}
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Don't serve directories
	if fileInfo.IsDir() {
		// Try to serve index.html from the directory
		indexPath := filepath.Join(fullPath, "index.html")
		if _, err := os.Stat(indexPath); err == nil {
			fullPath = indexPath
		} else {
			http.Error(w, "Not found", http.StatusNotFound)
			return
		}
	}

	// Set no-cache headers to ensure files are always read from disk
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Expires", "0")

	// Serve the file
	http.ServeFile(w, r, fullPath)
}
