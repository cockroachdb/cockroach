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

package bufwire

import (
	"github.com/bufbuild/buf/private/buf/bufconfig"
	"github.com/bufbuild/buf/private/bufpkg/bufimage"
)

type imageConfig struct {
	image  bufimage.Image
	config *bufconfig.Config
}

func newImageConfig(image bufimage.Image, config *bufconfig.Config) *imageConfig {
	return &imageConfig{
		image:  image,
		config: config,
	}
}

func (i *imageConfig) Image() bufimage.Image {
	return i.image
}

func (i *imageConfig) Config() *bufconfig.Config {
	return i.config
}
