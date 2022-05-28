// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package securityassets

import (
	"io/ioutil"
	"os"
)

// AssetLoader describes the functions necessary to read certificate and key files.
type AssetLoader struct {
	ReadDir  func(dirname string) ([]os.FileInfo, error)
	ReadFile func(filename string) ([]byte, error)
	Stat     func(name string) (os.FileInfo, error)
}

// defaultAssetLoader uses real filesystem calls.
var defaultAssetLoader = AssetLoader{
	ReadDir:  ioutil.ReadDir,
	ReadFile: ioutil.ReadFile,
	Stat:     os.Stat,
}

// assetLoaderImpl is used to list/read/stat security assets.
var assetLoaderImpl = defaultAssetLoader

// GetAssetLoader returns the active asset loader.
func GetAssetLoader() AssetLoader {
	return assetLoaderImpl
}

// SetAssetLoader overrides the asset loader with the passed-in one.
func SetAssetLoader(al AssetLoader) {
	assetLoaderImpl = al
}

// ResetAssetLoader restores the asset loader to the default value.
func ResetAssetLoader() {
	assetLoaderImpl = defaultAssetLoader
}
