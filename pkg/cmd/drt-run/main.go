// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"
	"os"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"
)

var rootCmd = &cobra.Command{
	Use:   "drt-run [config-file]",
	Short: "tool for managing long-running workloads and roachtest operations",
	Long: `drt-run is a tool for managing long-running workloads and a regular
cadence of workload operations

Examples:

  drt-run config.yaml

The above command will run workloads specified in config.yaml as well
as any operations specified in that config file. See testdata/config.yaml
for an example of a config file for use with drt-run.
`,
	Args: cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		configFile := args[0]
		return runDRT(configFile)
	},
}

func runDRT(configFile string) (retErr error) {
	errChan := make(chan error, 1)
	defer func() {
		if retErr == nil {
			select {
			case err := <-errChan:
				retErr = err
				return
			default:
				return
			}
		}
	}()
	c := config{}
	var wg sync.WaitGroup
	defer wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f, err := os.Open(configFile)
	if err != nil {
		return err
	}
	decoder := yaml.NewDecoder(f)
	if err := decoder.Decode(&c); err != nil {
		return err
	}

	eventL := makeEventLogger(os.Stdout, c)
	wr := makeWorkloadRunner(c, eventL)
	wg.Add(1)

	go func() {
		defer close(errChan)
		defer wg.Done()
		if err := wr.Run(ctx); err != nil {
			errChan <- err
		}
	}()

	or, err := makeOpsRunner(c.Operations.Parallelism /* parallelism */, c, eventL)
	if err != nil {
		return err
	}

	hh := httpHandler{
		ctx:    ctx,
		w:      wr,
		o:      or,
		eventL: eventL,
	}
	_ = hh.startHTTPServer(8080, "localhost")
	or.Run(ctx)

	return nil
}

func main() {
	_ = roachprod.InitProviders()
	if err := rootCmd.Execute(); err != nil {
		// Cobra has already printed the error message.
		os.Exit(1)
	}
}
