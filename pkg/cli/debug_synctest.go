// Copyright 2018 The Cockroach Authors.
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
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

var debugSyncTestCmd = &cobra.Command{
	Use:   "synctest <empty-dir> <nemesis-script>",
	Short: "Run a log-like workload that can help expose filesystem anomalies",
	Long: `
synctest is a tool to verify filesystem consistency in the presence of I/O errors.
It takes a directory (required to be initially empty and created on demand) into
which data will be written and a nemesis script which receives a single argument
that is either "on" or "off".

The nemesis script will be called with a parameter of "on" when the filesystem
underlying the given directory should be "disturbed". It is called with "off"
to restore the undisturbed state (note that "off" must be idempotent).

synctest will run run across multiple "epochs", each terminated by an I/O error
injected by the nemesis. After each epoch, the nemesis is turned off and the
written data is reopened, checked for data loss, new data is written, and
the nemesis turned back on. In the absence of unexpected error or user interrupt,
this process continues indefinitely.
`,
	Args: cobra.ExactArgs(2),
	RunE: runDebugSyncTest,
}

type scriptNemesis string

func (sn scriptNemesis) exec(arg string) error {
	b, err := exec.Command(string(sn), arg).CombinedOutput()
	if err != nil {
		return errors.WithDetailf(err, "command output:\n%s", string(b))
	}
	fmt.Fprintf(stderr, "%s %s: %s", sn, arg, b)
	return nil
}

func (sn scriptNemesis) On() error {
	return sn.exec("on")
}

func (sn scriptNemesis) Off() error {
	return sn.exec("off")
}

func runDebugSyncTest(cmd *cobra.Command, args []string) error {
	// TODO(tschottdorf): make this a flag.
	duration := 10 * time.Minute

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	nem := scriptNemesis(args[1])
	if err := nem.Off(); err != nil {
		return errors.Wrap(err, "unable to disable nemesis at beginning of run")
	}

	var generation int
	var lastSeq int64
	for {
		dir := filepath.Join(args[0], strconv.Itoa(generation))
		curLastSeq, err := runSyncer(ctx, dir, lastSeq, nem)
		if err != nil {
			return err
		}
		lastSeq = curLastSeq
		if curLastSeq == 0 {
			if ctx.Err() != nil {
				// Clean shutdown.
				return nil
			}
			// RocksDB dir got corrupted.
			generation++
			continue
		}
	}
}

type nemesisI interface {
	On() error
	Off() error
}

func runSyncer(
	ctx context.Context, dir string, expSeq int64, nemesis nemesisI,
) (lastSeq int64, _ error) {
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	db, err := OpenEngine(dir, stopper, OpenEngineOptions{})
	if err != nil {
		if expSeq == 0 {
			// Failed on first open, before we tried to corrupt anything. Hard stop.
			return 0, err
		}
		fmt.Fprintln(stderr, "RocksDB directory", dir, "corrupted:", err)
		return 0, nil // trigger reset
	}

	buf := make([]byte, 128)
	var seq int64
	key := func() roachpb.Key {
		seq++
		return encoding.EncodeUvarintAscending(buf[:0:0], uint64(seq))
	}

	check := func(kv storage.MVCCKeyValue) error {
		expKey := key()
		if !bytes.Equal(kv.Key.Key, expKey) {
			return errors.Errorf(
				"found unexpected key %q (expected %q)", kv.Key.Key, expKey,
			)
		}
		return nil // want more
	}

	fmt.Fprintf(stderr, "verifying existing sequence numbers...")
	if err := db.MVCCIterate(roachpb.KeyMin, roachpb.KeyMax, storage.MVCCKeyAndIntentsIterKind, check); err != nil {
		return 0, err
	}
	// We must not lose writes, but sometimes we get extra ones (i.e. we caught an
	// error but the write actually went through).
	if expSeq != 0 && seq < expSeq {
		return 0, errors.Errorf("highest persisted sequence number is %d, but expected at least %d", seq, expSeq)
	}
	fmt.Fprintf(stderr, "done (seq=%d).\nWriting new entries:\n", seq)

	waitFailure := time.After(time.Duration(rand.Int63n(5 * time.Second.Nanoseconds())))

	if err := stopper.RunAsyncTask(ctx, "syncer", func(ctx context.Context) {
		<-waitFailure
		if err := nemesis.On(); err != nil {
			panic(err)
		}
		defer func() {
			if err := nemesis.Off(); err != nil {
				panic(err)
			}
		}()
		<-stopper.ShouldQuiesce()
	}); err != nil {
		return 0, err
	}

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, drainSignals...)

	write := func() (_ int64, err error) {
		defer func() {
			// Catch any RocksDB NPEs. They do occur when enough
			// faults are being injected.
			if r := recover(); r != nil {
				if err == nil {
					err = errors.New("recovered panic on write")
				}
				err = errors.Wrapf(err, "%v", r)
			}
		}()

		k, v := key(), []byte("payload")
		switch seq % 2 {
		case 0:
			if err := db.PutUnversioned(k, v); err != nil {
				seq--
				return seq, err
			}
			if err := db.Flush(); err != nil {
				seq--
				return seq, err
			}
		default:
			b := db.NewBatch()
			if err := b.PutUnversioned(k, v); err != nil {
				seq--
				return seq, err
			}
			if err := b.Commit(true /* sync */); err != nil {
				seq--
				return seq, err
			}
		}
		return seq, nil
	}

	for {
		if lastSeq, err := write(); err != nil {
			// Exercise three cases:
			// 1. no more writes after first failure
			// 2. one more attempt, failure persists
			// 3. two more attempts, file system healed for last attempt
			for n := rand.Intn(3); n >= 0; n-- {
				if n == 1 {
					if err := nemesis.Off(); err != nil {
						return 0, err
					}
				}
				fmt.Fprintf(stderr, "error after seq %d (trying %d additional writes): %v\n", lastSeq, n, err)
				lastSeq, err = write()
			}
			fmt.Fprintf(stderr, "error after seq %d: %v\n", lastSeq, err)
			// Intentionally swallow the error to get into the next epoch.
			return lastSeq, nil
		}
		select {
		case sig := <-ch:
			return seq, errors.Errorf("interrupted (%v)", sig)
		case <-ctx.Done():
			return 0, nil
		default:
		}
	}
}
