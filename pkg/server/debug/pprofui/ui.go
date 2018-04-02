// Copyright 2018 The Cockroach Authors.
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

package pprofui

import (
	"context"
	"fmt"
	"io"

	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func pprofCtx(ctx context.Context) context.Context {
	return log.WithLogTag(ctx, "pprof", nil)
}

// fakeUI implements pprof's driver.UI.
type fakeUI struct{}

func (*fakeUI) ReadLine(prompt string) (string, error) { return "", io.EOF }

func (*fakeUI) Print(args ...interface{}) {
	msg := fmt.Sprint(args...)
	log.InfofDepth(pprofCtx(context.Background()), 1, "%s", msg)
}

func (*fakeUI) PrintErr(args ...interface{}) {
	msg := fmt.Sprint(args...)
	log.WarningfDepth(pprofCtx(context.Background()), 1, "%s", msg)
}

func (*fakeUI) IsTerminal() bool {
	return false
}

func (*fakeUI) WantBrowser() bool {
	return false
}

func (*fakeUI) SetAutoComplete(complete func(string) string) {}
