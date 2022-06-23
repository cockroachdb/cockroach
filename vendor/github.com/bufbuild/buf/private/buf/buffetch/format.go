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

package buffetch

const (
	// formatBin is the binary format.
	formatBin = "bin"
	// formatBingz is the binary gzipped format.
	formatBingz = "bingz"
	// formatDir is the directory format.
	formatDir = "dir"
	// formatGit is the git format.
	formatGit = "git"
	// formatJSON is the JSON format.
	formatJSON = "json"
	// formatJSONGZ is the JSON gzipped format.
	formatJSONGZ = "jsongz"
	// formatMod is the module format.
	formatMod = "mod"
	// formatTar is the tar format.
	formatTar = "tar"
	// formatTargz is the tar gzipped format.
	formatTargz = "targz"
	// formatZip is the zip format.
	formatZip = "zip"
)

var (
	// sorted
	imageFormats = []string{
		formatBin,
		formatBingz,
		formatJSON,
		formatJSONGZ,
	}
	// sorted
	imageFormatsNotDeprecated = []string{
		formatBin,
		formatJSON,
	}
	// sorted
	sourceFormats = []string{
		formatDir,
		formatGit,
		formatTar,
		formatTargz,
		formatZip,
	}
	// sorted
	sourceFormatsNotDeprecated = []string{
		formatDir,
		formatGit,
		formatTar,
		formatZip,
	}
	// sorted
	moduleFormats = []string{
		formatMod,
	}
	// sorted
	moduleFormatsNotDeprecated = []string{
		formatMod,
	}
	// sorted
	sourceOrModuleFormats = []string{
		formatDir,
		formatGit,
		formatMod,
		formatTar,
		formatTargz,
		formatZip,
	}
	// sorted
	sourceOrModuleFormatsNotDeprecated = []string{
		formatDir,
		formatGit,
		formatMod,
		formatTar,
		formatZip,
	}
	// sorted
	allFormats = []string{
		formatBin,
		formatBingz,
		formatDir,
		formatGit,
		formatJSON,
		formatJSONGZ,
		formatMod,
		formatTar,
		formatTargz,
		formatZip,
	}
	// sorted
	allFormatsNotDeprecated = []string{
		formatBin,
		formatDir,
		formatGit,
		formatJSON,
		formatMod,
		formatTar,
		formatZip,
	}

	deprecatedCompressionFormatToReplacementFormat = map[string]string{
		formatBingz:  formatBin,
		formatJSONGZ: formatJSON,
		formatTargz:  formatTar,
	}
)
