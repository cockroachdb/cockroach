// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"
)

var debugCheckLogConfigCmd = &cobra.Command{
	Use:   "check-log-config",
	Short: "test the log config passed via --log",
	RunE:  runDebugCheckLogConfig,
}

var debugLogChanSel logconfig.ChannelList

func runDebugCheckLogConfig(cmd *cobra.Command, args []string) error {
	if err := setupLogging(context.Background(), cmd,
		true /* isServerCmd */, false /* applyconfig */); err != nil {
		return err
	}
	if cliCtx.ambiguousLogDir {
		fmt.Fprintf(stderr, "warning: ambiguous configuration, consider overriding the logging directory\n")
	}

	c := cliCtx.logConfig
	r, err := yaml.Marshal(&c)
	if err != nil {
		return errors.Wrap(err, "printing configuration")
	}

	fmt.Println("# configuration after validation:")
	fmt.Println(string(r))

	_, key := c.Export(debugLogChanSel)
	fmt.Println("# graphical diagram URL:")
	fmt.Printf("http://www.plantuml.com/plantuml/uml/%s\n", key)

	return nil
}
