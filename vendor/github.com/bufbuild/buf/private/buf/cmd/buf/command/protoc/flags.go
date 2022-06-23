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

package protoc

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/bufbuild/buf/private/buf/buffetch"
	"github.com/bufbuild/buf/private/bufpkg/bufanalysis"
	"github.com/bufbuild/buf/private/pkg/normalpath"
	"github.com/bufbuild/buf/private/pkg/stringutil"
	"github.com/spf13/pflag"
)

const (
	includeDirPathsFlagName       = "proto_path"
	includeImportsFlagName        = "include_imports"
	includeSourceInfoFlagName     = "include_source_info"
	printFreeFieldNumbersFlagName = "print_free_field_numbers"
	outputFlagName                = "descriptor_set_out"
	pluginPathValuesFlagName      = "plugin"
	errorFormatFlagName           = "error_format"
	byDirFlagName                 = "by-dir"

	pluginFakeFlagName = "protoc_plugin_fake"

	encodeFlagName          = "encode"
	decodeFlagName          = "decode"
	decodeRawFlagName       = "decode_raw"
	descriptorSetInFlagName = "descriptor_set_in"
)

var (
	defaultIncludeDirPaths = []string{"."}
	defaultErrorFormat     = "gcc"
)

type flags struct {
	IncludeDirPaths       []string
	IncludeImports        bool
	IncludeSourceInfo     bool
	PrintFreeFieldNumbers bool
	Output                string
	ErrorFormat           string
	ByDir                 bool
}

type env struct {
	flags

	PluginNamesSortedByOutIndex []string
	PluginNameToPluginInfo      map[string]*pluginInfo
	FilePaths                   []string
}

type flagsBuilder struct {
	flags

	PluginPathValues []string

	Encode          string
	Decode          string
	DecodeRaw       bool
	DescriptorSetIn []string

	pluginFake        []string
	pluginNameToValue map[string]*pluginValue
}

func newFlagsBuilder() *flagsBuilder {
	return &flagsBuilder{
		pluginNameToValue: make(map[string]*pluginValue),
	}
}

func (f *flagsBuilder) Bind(flagSet *pflag.FlagSet) {
	flagSet.StringSliceVarP(
		&f.IncludeDirPaths,
		includeDirPathsFlagName,
		"I",
		// cannot set default due to recursive flag parsing
		// no way to differentiate between default and set for now
		// perhaps we could rework pflag usage somehow
		nil,
		`The include directory paths. This is equivalent to roots in Buf.`,
	)
	flagSet.BoolVar(
		&f.IncludeImports,
		includeImportsFlagName,
		false,
		`Include imports in the resulting FileDescriptorSet.`,
	)
	flagSet.BoolVar(
		&f.IncludeSourceInfo,
		includeSourceInfoFlagName,
		false,
		`Include source info in the resulting FileDescriptorSet.`,
	)
	flagSet.BoolVar(
		&f.PrintFreeFieldNumbers,
		printFreeFieldNumbersFlagName,
		false,
		`Print the free field numbers of all messages.`,
	)
	flagSet.StringVarP(
		&f.Output,
		outputFlagName,
		"o",
		"",
		fmt.Sprintf(
			`The location to write the FileDescriptorSet. Must be one of format %s.`,
			buffetch.ImageFormatsString,
		),
	)
	flagSet.StringVar(
		&f.ErrorFormat,
		errorFormatFlagName,
		// cannot set default due to recursive flag parsing
		// no way to differentiate between default and set for now
		// perhaps we could rework pflag usage somehow
		"",
		fmt.Sprintf(
			`The error format to use. Must be one of format %s.`,
			stringutil.SliceToString(bufanalysis.AllFormatStringsWithAliases),
		),
	)
	flagSet.StringSliceVar(
		&f.PluginPathValues,
		pluginPathValuesFlagName,
		nil,
		`The paths to the plugin executables to use, either in the form "path/to/protoc-gen-foo" or "protoc-gen-foo=path/to/binary".`,
	)
	flagSet.BoolVar(
		&f.ByDir,
		byDirFlagName,
		false,
		`Execute parallel plugin calls for every directory containing .proto files.`,
	)

	// MUST be a StringArray instead of StringSlice so we do not split on commas
	// Otherwise --go_out=foo=bar,baz=bat:out would be treated as --go_out=foo=bar --go_out=baz=bat:out
	flagSet.StringArrayVar(
		&f.pluginFake,
		pluginFakeFlagName,
		nil,
		`If you are calling this, you should not be.`,
	)
	_ = flagSet.MarkHidden(pluginFakeFlagName)

	flagSet.StringVar(
		&f.Encode,
		encodeFlagName,
		"",
		`Not supported by buf.`,
	)
	_ = flagSet.MarkHidden(encodeFlagName)
	flagSet.StringVar(
		&f.Decode,
		decodeFlagName,
		"",
		`Not supported by buf.`,
	)
	_ = flagSet.MarkHidden(decodeFlagName)
	flagSet.BoolVar(
		&f.DecodeRaw,
		decodeRawFlagName,
		false,
		`Not supported by buf.`,
	)
	_ = flagSet.MarkHidden(decodeRawFlagName)
	flagSet.StringSliceVar(
		&f.DescriptorSetIn,
		descriptorSetInFlagName,
		nil,
		`Not supported by buf.`,
	)
	_ = flagSet.MarkHidden(descriptorSetInFlagName)
}

func (f *flagsBuilder) Normalize(flagSet *pflag.FlagSet, name string) string {
	if name != "descriptor_set_out" && strings.HasSuffix(name, "_out") {
		f.pluginFakeParse(name, "_out", true)
		return pluginFakeFlagName
	}
	if strings.HasSuffix(name, "_opt") {
		f.pluginFakeParse(name, "_opt", false)
		return pluginFakeFlagName
	}
	return strings.Replace(name, "-", "_", -1)
}

func (f *flagsBuilder) Build(args []string) (*env, error) {
	pluginNameToPluginInfo := make(map[string]*pluginInfo)
	seenFlagFilePaths := make(map[string]struct{})
	filePaths, err := f.buildRec(args, pluginNameToPluginInfo, seenFlagFilePaths)
	if err != nil {
		return nil, err
	}
	if err := f.checkUnsupported(); err != nil {
		return nil, err
	}
	for pluginName, pluginInfo := range pluginNameToPluginInfo {
		if pluginInfo.Out == "" && len(pluginInfo.Opt) > 0 {
			return nil, newCannotSpecifyOptWithoutOutError(pluginName)
		}
		if pluginInfo.Out == "" && pluginInfo.Path != "" {
			return nil, newCannotSpecifyPathWithoutOutError(pluginName)
		}
	}
	pluginNamesSortedByOutIndex, err := f.getPluginNamesSortedByOutIndex(pluginNameToPluginInfo)
	if err != nil {
		return nil, err
	}
	if len(f.IncludeDirPaths) == 0 {
		f.IncludeDirPaths = defaultIncludeDirPaths
	} else {
		f.IncludeDirPaths = splitIncludeDirPaths(f.IncludeDirPaths)
	}
	if f.ErrorFormat == "" {
		f.ErrorFormat = defaultErrorFormat
	}
	if len(filePaths) == 0 {
		return nil, errNoInputFiles
	}
	return &env{
		flags:                       f.flags,
		PluginNamesSortedByOutIndex: pluginNamesSortedByOutIndex,
		PluginNameToPluginInfo:      pluginNameToPluginInfo,
		FilePaths:                   filePaths,
	}, nil
}

func (f *flagsBuilder) pluginFakeParse(name string, suffix string, isOut bool) {
	pluginName := strings.TrimSuffix(name, suffix)
	pluginValue, ok := f.pluginNameToValue[pluginName]
	if !ok {
		pluginValue = newPluginValue()
		f.pluginNameToValue[pluginName] = pluginValue
	}
	index := len(f.pluginFake)
	if isOut {
		pluginValue.OutIndexes = append(pluginValue.OutIndexes, index)
	} else {
		pluginValue.OptIndexes = append(pluginValue.OptIndexes, index)
	}
}

func (f *flagsBuilder) buildRec(
	args []string,
	pluginNameToPluginInfo map[string]*pluginInfo,
	seenFlagFilePaths map[string]struct{},
) ([]string, error) {
	if err := f.parsePluginNameToPluginInfo(pluginNameToPluginInfo); err != nil {
		return nil, err
	}
	filePaths := make([]string, 0, len(args))
	for _, arg := range args {
		if len(arg) == 0 {
			return nil, errArgEmpty
		}
		if arg[0] != '@' {
			filePaths = append(filePaths, arg)
		} else {
			flagFilePath := normalpath.Unnormalize(arg[1:])
			if _, ok := seenFlagFilePaths[flagFilePath]; ok {
				return nil, newRecursiveReferenceError(flagFilePath)
			}
			seenFlagFilePaths[flagFilePath] = struct{}{}
			data, err := os.ReadFile(flagFilePath)
			if err != nil {
				return nil, err
			}
			var flagFilePathArgs []string
			for _, flagFilePathArg := range strings.Split(string(data), "\n") {
				flagFilePathArg = strings.TrimSpace(flagFilePathArg)
				if flagFilePathArg != "" {
					flagFilePathArgs = append(flagFilePathArgs, flagFilePathArg)
				}
			}
			subFlagsBuilder := newFlagsBuilder()
			flagSet := pflag.NewFlagSet(flagFilePath, pflag.ContinueOnError)
			subFlagsBuilder.Bind(flagSet)
			flagSet.SetNormalizeFunc(normalizeFunc(subFlagsBuilder.Normalize))
			if err := flagSet.Parse(flagFilePathArgs); err != nil {
				return nil, err
			}
			subFilePaths, err := subFlagsBuilder.buildRec(
				flagSet.Args(),
				pluginNameToPluginInfo,
				seenFlagFilePaths,
			)
			if err != nil {
				return nil, err
			}
			if err := f.merge(subFlagsBuilder); err != nil {
				return nil, err
			}
			filePaths = append(filePaths, subFilePaths...)
		}
	}
	return filePaths, nil
}

// we need to bind a separate flags as pflags overrides the values with defaults if you bind again
// note that pflags does not error on duplicates so we do not either
func (f *flagsBuilder) merge(subFlagsBuilder *flagsBuilder) error {
	f.IncludeDirPaths = append(f.IncludeDirPaths, subFlagsBuilder.IncludeDirPaths...)
	if subFlagsBuilder.IncludeImports {
		f.IncludeImports = true
	}
	if subFlagsBuilder.IncludeSourceInfo {
		f.IncludeSourceInfo = true
	}
	if subFlagsBuilder.PrintFreeFieldNumbers {
		f.PrintFreeFieldNumbers = true
	}
	if subFlagsBuilder.Output != "" {
		f.Output = subFlagsBuilder.Output
	}
	if subFlagsBuilder.ErrorFormat != "" {
		f.ErrorFormat = subFlagsBuilder.ErrorFormat
	}
	if subFlagsBuilder.ByDir {
		f.ByDir = true
	}
	f.PluginPathValues = append(f.PluginPathValues, subFlagsBuilder.PluginPathValues...)
	if subFlagsBuilder.Encode != "" {
		f.Encode = subFlagsBuilder.Encode
	}
	if subFlagsBuilder.Decode != "" {
		f.Decode = subFlagsBuilder.Decode
	}
	if subFlagsBuilder.DecodeRaw {
		f.DecodeRaw = true
	}
	f.DescriptorSetIn = append(f.DescriptorSetIn, subFlagsBuilder.DescriptorSetIn...)
	return nil
}

func (f *flagsBuilder) parsePluginNameToPluginInfo(pluginNameToPluginInfo map[string]*pluginInfo) error {
	for pluginName, pluginValue := range f.pluginNameToValue {
		switch len(pluginValue.OutIndexes) {
		case 0:
		case 1:
			out := f.pluginFake[pluginValue.OutIndexes[0]]
			var opt string
			if isOutNotAFullPath(out) {
				split := strings.SplitN(out, ":", 2)
				switch len(split) {
				case 1:
				case 2:
					out = split[1]
					opt = split[0]
				}
			}
			pluginInfo, ok := pluginNameToPluginInfo[pluginName]
			if !ok {
				pluginInfo = newPluginInfo()
				pluginNameToPluginInfo[pluginName] = pluginInfo
			}
			pluginInfo.Out = out
			if opt != "" {
				for _, value := range strings.Split(opt, ",") {
					if value := strings.TrimSpace(value); value != "" {
						pluginInfo.Opt = append(pluginInfo.Opt, value)
					} else {
						return newEmptyOptError(pluginName)
					}
				}
			}
		default:
			return newDuplicateOutError(pluginName)
		}
		if len(pluginValue.OptIndexes) > 0 {
			pluginInfo, ok := pluginNameToPluginInfo[pluginName]
			if !ok {
				pluginInfo = newPluginInfo()
				pluginNameToPluginInfo[pluginName] = pluginInfo
			}
			for _, optIndex := range pluginValue.OptIndexes {
				for _, value := range strings.Split(f.pluginFake[optIndex], ",") {
					if value := strings.TrimSpace(value); value != "" {
						pluginInfo.Opt = append(pluginInfo.Opt, value)
					} else {
						return newEmptyOptError(pluginName)
					}
				}
			}
		}
	}
	for _, pluginPathValue := range f.PluginPathValues {
		var pluginName string
		var pluginPath string
		switch split := strings.SplitN(pluginPathValue, "=", 2); len(split) {
		case 0:
			return newPluginPathValueEmptyError()
		case 1:
			pluginName = filepath.Base(split[0])
			pluginPath = split[0]
		case 2:
			pluginName = split[0]
			pluginPath = split[1]
		default:
			return newPluginPathValueInvalidError(pluginPathValue)
		}
		if !strings.HasPrefix(pluginName, "protoc-gen-") {
			return newPluginPathNameInvalidPrefixError(pluginName)
		}
		pluginName = strings.TrimPrefix(pluginName, "protoc-gen-")
		pluginInfo, ok := pluginNameToPluginInfo[pluginName]
		if !ok {
			pluginInfo = newPluginInfo()
			pluginNameToPluginInfo[pluginName] = pluginInfo
		}
		if pluginInfo.Path != "" {
			return newDuplicatePluginPathError(pluginName)
		}
		pluginInfo.Path = pluginPath
	}
	return nil
}

func (f *flagsBuilder) getPluginNamesSortedByOutIndex(
	pluginNameToPluginInfo map[string]*pluginInfo,
) ([]string, error) {
	pluginNames := make([]string, 0, len(pluginNameToPluginInfo))
	for pluginName := range pluginNameToPluginInfo {
		pluginNames = append(pluginNames, pluginName)
	}
	var err error
	sort.Slice(
		pluginNames,
		func(i int, j int) bool {
			pluginName1 := pluginNames[i]
			pluginName2 := pluginNames[j]
			pluginValue1, ok := f.pluginNameToValue[pluginName1]
			if !ok {
				err = fmt.Errorf("no value for plugin name %q inside pluginNameToValue", pluginName1)
				return false
			}
			pluginValue2, ok := f.pluginNameToValue[pluginName2]
			if !ok {
				err = fmt.Errorf("no value for plugin name %q inside pluginNameToValue", pluginName2)
				return false
			}
			if len(pluginValue1.OutIndexes) != 1 {
				err = fmt.Errorf("%d out indexes for plugin name %q", len(pluginValue1.OutIndexes), pluginName1)
				return false
			}
			if len(pluginValue2.OutIndexes) != 1 {
				err = fmt.Errorf("%d out indexes for plugin name %q", len(pluginValue2.OutIndexes), pluginName2)
				return false
			}
			return pluginValue1.OutIndexes[0] < pluginValue2.OutIndexes[0]
		},
	)
	if err != nil {
		return nil, err
	}
	return pluginNames, nil
}

func (f *flagsBuilder) checkUnsupported() error {
	if f.Encode != "" {
		return newEncodeNotSupportedError()
	}
	if f.Decode != "" {
		return newDecodeNotSupportedError()
	}
	if f.DecodeRaw {
		return newDecodeRawNotSupportedError()
	}
	if len(f.DescriptorSetIn) > 0 {
		return newDescriptorSetInNotSupportedError()
	}
	return nil
}

type pluginValue struct {
	OutIndexes []int
	OptIndexes []int
}

func newPluginValue() *pluginValue {
	return &pluginValue{}
}

func normalizeFunc(f func(*pflag.FlagSet, string) string) func(*pflag.FlagSet, string) pflag.NormalizedName {
	return func(flagSet *pflag.FlagSet, name string) pflag.NormalizedName {
		return pflag.NormalizedName(f(flagSet, name))
	}
}

// https://github.com/protocolbuffers/protobuf/blob/336ed1820a4f2649c9aa3953d5059b03b7a77100/src/google/protobuf/compiler/command_line_interface.cc#L1699-L1705
//
// This roughly supports the equivalent of Java's -classpath flag.
// Note that for filenames such as "foo:bar" on unix, this breaks, but our goal is to match
// this flag from protoc.
func splitIncludeDirPaths(includeDirPaths []string) []string {
	copyIncludeDirPaths := make([]string, 0, len(includeDirPaths))
	for _, includeDirPath := range includeDirPaths {
		// protocolbuffers/protobuf has true for omit_empty
		for _, splitIncludeDirPath := range strings.Split(includeDirPath, includeDirPathSeparator) {
			if len(splitIncludeDirPath) > 0 {
				copyIncludeDirPaths = append(copyIncludeDirPaths, splitIncludeDirPath)
			}
		}
	}
	return copyIncludeDirPaths
}
