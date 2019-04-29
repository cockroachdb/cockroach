// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package cli

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"

	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logflags"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

var kvRunFlags = pflag.NewFlagSet(`kv-run`, pflag.ContinueOnError)
var usePebble = kvRunFlags.Bool("use-pebble", false, "Use Pebble storage engine.")
var storeDir = kvRunFlags.String(
	"store-dir", "",
	"KV store directory. If not provided, a temporary directory will be used.")
var dropStore = kvRunFlags.Bool("drop-store", false, "Drop the existing KV database, if it exists")

func init() {
	AddSubCmd(func(userFacing bool) *cobra.Command {
		var runCmd = SetCmdDefaults(&cobra.Command{
			Use:   `kv-run`,
			Short: `run a workload's operations against a kv storage engine`,
		})

		// cmdHelper handles common workload command logic, such as error handling and
		// options validation.
		cmdHelper := func(
			gen workload.Generator, fn func(gen workload.Generator) error,
		) func(*cobra.Command, []string) {
			return HandleErrs(func(cmd *cobra.Command, args []string) error {
				if ls := cmd.Flags().Lookup(logflags.LogToStderrName); ls != nil {
					if !ls.Changed {
						// Unless the settings were overridden by the user, default to logging
						// to stderr.
						_ = ls.Value.Set(log.Severity_INFO.String())
					}
				}

				if h, ok := gen.(workload.Hookser); ok {
					if h.Hooks().Validate != nil {
						if err := h.Hooks().Validate(); err != nil {
							return errors.Wrapf(err, "could not validate")
						}
					}
				}
				return fn(gen)
			})
		}

		// runRun makes a storage engine and invokes the main runner
		runRun := func(gen workload.Generator) error {
			ctx := context.Background()

			startPProfEndPoint(ctx)

			var dir string
			var tempdir string
			if *storeDir == "" {
				var err error
				tempdir, err = ioutil.TempDir("", "testing")
				if err != nil {
					return err
				}
				dir = tempdir
			} else {
				dir = *storeDir
			}

			if *dropStore {
				for {
					err := os.RemoveAll(dir)
					if err == nil {
						break
					}
					if !*tolerateErrors {
						return err
					}
					log.Infof(ctx, "retrying after error during init: %v", err)
				}
			}

			r, err := engine.NewRocksDB(
				engine.RocksDBConfig{
					Dir: dir,
				},
				engine.RocksDBCache{},
			)
			if err != nil {
				return err
			}
			defer func() {
				if tempdir != "" {
					err := os.RemoveAll(tempdir)
					if err != nil {
						fmt.Printf("Failed to remove temporary directory: %s: %s\n", tempdir, err.Error())
					}
				}
			}()

			// TODO: this channel is instantiated here because it depends on `exitSignals`
			// which is in this same package. I think ideally this logic should be in
			// `KvOpsRun`.
			done := make(chan os.Signal, 3)
			signal.Notify(done, exitSignals...)

			opts := workload.KvOpsRunOptions{
				Duration:       *duration,
				Histograms:     *histograms,
				MaxOps:         *maxOps,
				MaxRate:        *maxRate,
				Ramp:           *ramp,
				TolerateErrors: *tolerateErrors,
			}

			return workload.KvOpsRun(ctx, done, r, gen.(workload.KvOpser), gen.(workload.Hookser), opts)
		}

		for _, meta := range workload.Registered() {
			gen := meta.New()
			if _, ok := gen.(workload.KvOpser); !ok {
				// If KvOpser is not implemented, this would just fail at runtime,
				// so omit it.
				continue
			}

			var genFlags *pflag.FlagSet
			if f, ok := gen.(workload.Flagser); ok {
				genFlags = f.Flags().FlagSet
			}

			genRunCmd := SetCmdDefaults(&cobra.Command{
				Use:   meta.Name,
				Short: meta.Description,
				Long:  meta.Description + meta.Details,
				Args:  cobra.ArbitraryArgs,
			})
			genRunCmd.Flags().AddFlagSet(kvRunFlags)
			genRunCmd.Flags().AddFlagSet(sharedFlags)
			genRunCmd.Flags().AddFlagSet(genFlags)
			genRunCmd.Run = cmdHelper(gen, runRun)
			if userFacing && !meta.PublicFacing {
				genRunCmd.Hidden = true
			}
			runCmd.AddCommand(genRunCmd)
		}
		return runCmd
	})
}
