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

package buflint

import (
	"context"

	"github.com/bufbuild/buf/private/buf/bufcheck/buflint/buflintconfig"
	"github.com/bufbuild/buf/private/buf/bufcheck/buflint/internal/buflintcheck"
	"github.com/bufbuild/buf/private/buf/bufcheck/internal"
	"github.com/bufbuild/buf/private/bufpkg/bufanalysis"
	"github.com/bufbuild/buf/private/bufpkg/bufimage"
	"github.com/bufbuild/buf/private/bufpkg/bufimage/bufimageutil"
	"github.com/bufbuild/buf/private/pkg/protosource"
	"go.uber.org/zap"
)

type handler struct {
	logger *zap.Logger
	runner *internal.Runner
}

func newHandler(logger *zap.Logger) *handler {
	return &handler{
		logger: logger,
		// linting allows for comment ignores
		// note that comment ignores still need to be enabled within the config
		// for a given check, this just says that comment ignores are allowed
		// in the first place
		runner: internal.NewRunner(
			logger,
			internal.RunnerWithIgnorePrefix(buflintcheck.CommentIgnorePrefix),
		),
	}
}

func (h *handler) Check(
	ctx context.Context,
	config *buflintconfig.Config,
	image bufimage.Image,
) ([]bufanalysis.FileAnnotation, error) {
	files, err := protosource.NewFilesUnstable(ctx, bufimageutil.NewInputFiles(image.Files())...)
	if err != nil {
		return nil, err
	}
	return h.runner.Check(ctx, configToInternalConfig(config), nil, files)
}

func configToInternalConfig(config *buflintconfig.Config) *internal.Config {
	return &internal.Config{
		Rules:               rulesToInternalRules(config.Rules),
		IgnoreIDToRootPaths: config.IgnoreIDToRootPaths,
		IgnoreRootPaths:     config.IgnoreRootPaths,
		AllowCommentIgnores: config.AllowCommentIgnores,
	}
}

func rulesToInternalRules(rules []buflintconfig.Rule) []*internal.Rule {
	if rules == nil {
		return nil
	}
	internalRules := make([]*internal.Rule, len(rules))
	for i, rule := range rules {
		internalRules[i] = rule.InternalRule()
	}
	return internalRules
}
