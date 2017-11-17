// Copyright 2015 The Cockroach Authors.
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

// Package ui embeds the assets for the web UI into the Cockroach binary.
//
// By default, it serves a stub web UI. Linking with distoss or distccl will
// replace the stubs with the OSS UI or the CCL UI, respectively. The exported
// symbols in this package are thus function pointers instead of functions so
// that they can be mutated by init hooks.
package ui

import (
	"fmt"
	"os"

	"github.com/cockroachdb/cockroach/pkg/build"
)

var indexHTML = []byte(fmt.Sprintf(`<!DOCTYPE html>
<title>CockroachDB</title>
Binary built without web UI.
<hr>
<em>%s</em>`, build.GetInfo().Short()))

// Asset loads and returns the asset for the given name. It returns an error if
// the asset could not be found or could not be loaded.
var Asset = func(name string) ([]byte, error) {
	if name == "index.html" {
		return indexHTML, nil
	}
	return nil, os.ErrNotExist
}

// AssetDir returns the file names below a certain directory in the embedded
// filesystem.
//
// For example, if the embedded filesystem contains the following hierarchy:
//
//     data/
//       foo.txt
//       img/
//         a.png
//         b.png
//
// AssetDir("") returns []string{"data"}
// AssetDir("data") returns []string{"foo.txt", "img"}
// AssetDir("data/img") returns []string{"a.png", "b.png"}
// AssetDir("foo.txt") and AssetDir("notexist") return errors
var AssetDir = func(name string) ([]string, error) {
	if name == "" {
		return []string{"index.html"}, nil
	}
	return nil, os.ErrNotExist
}

// AssetInfo loads and returns metadata for the asset with the given name. It
// returns an error if the asset could not be found or could not be loaded.
var AssetInfo func(name string) (os.FileInfo, error)
