// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pprofui

import (
	"context"
	"fmt"
	"io"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/logtags"
)

func pprofCtx(ctx context.Context) context.Context {
	return logtags.AddTag(ctx, "pprof", nil)
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
