// Copyright 2020-2021 Buf Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bufconfig

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/bufbuild/buf/private/pkg/storage"
)

func readConfig(
	ctx context.Context,
	provider Provider,
	readBucket storage.ReadBucket,
	options ...ReadConfigOption,
) (*Config, error) {
	readConfigOptions := newReadConfigOptions()
	for _, option := range options {
		option(readConfigOptions)
	}
	if readConfigOptions.override != "" {
		var data []byte
		var err error
		switch filepath.Ext(readConfigOptions.override) {
		case ".json", ".yaml", ".yml":
			data, err = os.ReadFile(readConfigOptions.override)
			if err != nil {
				return nil, fmt.Errorf("could not read file: %v", err)
			}
		default:
			data = []byte(readConfigOptions.override)
		}
		return provider.GetConfigForData(ctx, data)
	}
	return provider.GetConfig(ctx, readBucket)
}

type readConfigOptions struct {
	override string
}

func newReadConfigOptions() *readConfigOptions {
	return &readConfigOptions{}
}
