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

package internal

import (
	"context"
	"strings"

	"github.com/bufbuild/buf/private/bufpkg/bufanalysis"
	"github.com/bufbuild/buf/private/pkg/normalpath"
	"github.com/bufbuild/buf/private/pkg/protosource"
	"github.com/bufbuild/buf/private/pkg/protoversion"
	"github.com/bufbuild/buf/private/pkg/stringutil"
	"go.opencensus.io/trace"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

// Runner is a runner.
type Runner struct {
	logger       *zap.Logger
	ignorePrefix string
}

// NewRunner returns a new Runner.
func NewRunner(logger *zap.Logger, options ...RunnerOption) *Runner {
	runner := &Runner{
		logger: logger,
	}
	for _, option := range options {
		option(runner)
	}
	return runner
}

// RunnerOption is an option for a new Runner.
type RunnerOption func(*Runner)

// RunnerWithIgnorePrefix returns a new RunnerOption that sets the comment ignore prefix.
//
// This will result in failures where the location has "ignore_prefix id" in the leading
// comment being ignored.
//
// The default is to not enable comment ignores.
func RunnerWithIgnorePrefix(ignorePrefix string) RunnerOption {
	return func(runner *Runner) {
		runner.ignorePrefix = ignorePrefix
	}
}

// Check runs the Rules.
func (r *Runner) Check(ctx context.Context, config *Config, previousFiles []protosource.File, files []protosource.File) ([]bufanalysis.FileAnnotation, error) {
	rules := config.Rules
	if len(rules) == 0 {
		return nil, nil
	}
	ctx, span := trace.StartSpan(ctx, "check")
	span.AddAttributes(
		trace.Int64Attribute("num_files", int64(len(files))),
		trace.Int64Attribute("num_rules", int64(len(rules))),
	)
	defer span.End()

	ignoreFunc := r.newIgnoreFunc(config)
	var fileAnnotations []bufanalysis.FileAnnotation
	resultC := make(chan *result, len(rules))
	for _, rule := range rules {
		rule := rule
		go func() {
			iFileAnnotations, iErr := rule.check(ignoreFunc, previousFiles, files)
			resultC <- newResult(iFileAnnotations, iErr)
		}()
	}
	var err error
	for i := 0; i < len(rules); i++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case result := <-resultC:
			fileAnnotations = append(fileAnnotations, result.FileAnnotations...)
			err = multierr.Append(err, result.Err)
		}
	}
	if err != nil {
		return nil, err
	}
	bufanalysis.SortFileAnnotations(fileAnnotations)
	return fileAnnotations, nil
}

func (r *Runner) newIgnoreFunc(config *Config) IgnoreFunc {
	return func(id string, descriptors []protosource.Descriptor, locations []protosource.Location) bool {
		if idIsIgnored(id, descriptors, config) {
			return true
		}
		// if ignorePrefix is empty, comment ignores are not enabled for the runner
		// this is the case with breaking changes
		if r.ignorePrefix != "" && config.AllowCommentIgnores &&
			locationsAreIgnored(id, r.ignorePrefix, locations, config) {
			return true
		}
		if config.IgnoreUnstablePackages {
			for _, descriptor := range descriptors {
				if descriptorPackageIsUnstable(descriptor) {
					return true
				}
			}
		}
		return false
	}
}

func idIsIgnored(id string, descriptors []protosource.Descriptor, config *Config) bool {
	for _, descriptor := range descriptors {
		// OR of descriptors
		if idIsIgnoredForDescriptor(id, descriptor, config) {
			return true
		}
	}
	return false
}

func idIsIgnoredForDescriptor(id string, descriptor protosource.Descriptor, config *Config) bool {
	if descriptor == nil {
		return false
	}
	path := descriptor.File().Path()
	if normalpath.MapHasEqualOrContainingPath(config.IgnoreRootPaths, path, normalpath.Relative) {
		return true
	}
	if id == "" {
		return false
	}
	ignoreRootPaths, ok := config.IgnoreIDToRootPaths[id]
	if !ok {
		return false
	}
	return normalpath.MapHasEqualOrContainingPath(ignoreRootPaths, path, normalpath.Relative)
}

func locationsAreIgnored(id string, ignorePrefix string, locations []protosource.Location, config *Config) bool {
	// we already check that ignorePrefix is non-empty, but just doing here for safety
	if id == "" || ignorePrefix == "" {
		return false
	}
	fullIgnorePrefix := ignorePrefix + " " + id
	for _, location := range locations {
		if location != nil {
			if leadingComments := location.LeadingComments(); leadingComments != "" {
				for _, line := range stringutil.SplitTrimLinesNoEmpty(leadingComments) {
					if strings.HasPrefix(line, fullIgnorePrefix) {
						return true
					}
				}
			}
		}
	}
	return false
}

func descriptorPackageIsUnstable(descriptor protosource.Descriptor) bool {
	if descriptor == nil {
		return false
	}
	packageVersion, ok := protoversion.NewPackageVersionForPackage(descriptor.File().Package())
	if !ok {
		return false
	}
	return packageVersion.StabilityLevel() != protoversion.StabilityLevelStable
}

type result struct {
	FileAnnotations []bufanalysis.FileAnnotation
	Err             error
}

func newResult(fileAnnotations []bufanalysis.FileAnnotation, err error) *result {
	return &result{
		FileAnnotations: fileAnnotations,
		Err:             err,
	}
}
