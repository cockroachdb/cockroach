// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package pprofui

import (
	"context"
	"fmt"
	"io"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logtags"
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
