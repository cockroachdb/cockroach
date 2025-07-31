// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// drainAndShutdown attempts to drain the server and then shut it
// down.
func drainAndShutdown(ctx context.Context, c serverpb.AdminClient, targetNode string) (err error) {
	hardError, remainingWork, err := doDrain(ctx, c, targetNode)
	if hardError {
		return err
	}

	if remainingWork {
		log.Warningf(ctx, "graceful shutdown may not have completed successfully; check the node's logs for details.")
	}

	if err != nil {
		log.Warningf(ctx, "drain did not complete successfully; hard shutdown may cause disruption")
	}
	// We have already performed the drain above, so now go straight to
	// shutdown. We try twice just in case there is a transient error.
	hardErr, err := doShutdown(ctx, c, targetNode)
	if err != nil && !hardErr {
		log.Warningf(ctx, "hard shutdown attempt failed, retrying: %v", err)
		_, err = doShutdown(ctx, c, targetNode)
	}
	return errors.Wrap(err, "hard shutdown failed")
}

// doDrain calls a graceful drain.
//
// If the function returns hardError true, then the caller should not
// proceed with an alternate strategy (it's likely the server has gone
// away).
func doDrain(
	ctx context.Context, c serverpb.AdminClient, targetNode string,
) (hardError, remainingWork bool, err error) {
	// The next step is to drain. The timeout is configurable
	// via --drain-wait.
	if drainCtx.drainWait == 0 {
		return doDrainNoTimeout(ctx, c, targetNode)
	}

	if err := timeutil.RunWithTimeout(ctx, "get-drain-settings", 5*time.Second, func(ctx context.Context) error {
		shutdownSettings, err := c.Settings(ctx, &serverpb.SettingsRequest{
			Keys: []string{
				string(server.DrainWait.InternalKey()),
				string(server.ConnectionShutdownTimeout.InternalKey()),
				string(server.JobShutdownTimeout.InternalKey()),
				string(server.QueryShutdownTimeout.InternalKey()),
				string(kvserver.LeaseTransferPerIterationTimeout.InternalKey()),
			},
		})
		if err != nil {
			return err
		}
		minWait := 0 * time.Second
		for k, v := range shutdownSettings.KeyValues {
			wait, err := time.ParseDuration(v.Value)
			if err != nil {
				return err
			}
			minWait += wait
			// query_wait is used twice during draining, so count it twice here.
			if k == string(server.QueryShutdownTimeout.InternalKey()) {
				minWait += wait
			}
		}
		if minWait > drainCtx.drainWait {
			fmt.Fprintf(stderr, "warning: --drain-wait is %s, but the server.shutdown.{drain,query,jobs,connection,lease_transfer}_wait "+
				"cluster settings require a value of at least %s; using the larger value\n",
				drainCtx.drainWait, minWait)
			drainCtx.drainWait = minWait
		}
		return nil
	}); err != nil {
		fmt.Fprintf(stderr, "warning: could not check drain related cluster settings: %v\n", err)
	}

	err = timeutil.RunWithTimeout(ctx, "drain", drainCtx.drainWait, func(ctx context.Context) (err error) {
		hardError, remainingWork, err = doDrainNoTimeout(ctx, c, targetNode)
		return err
	})
	if errors.HasType(err, (*timeutil.TimeoutError)(nil)) || grpcutil.IsTimeout(err) {
		log.Infof(ctx, "drain timed out: %v", err)
		err = errors.New("drain timeout, consider adjusting --drain-wait, especially under " +
			"custom server.shutdown cluster settings")
	}
	return
}

func doDrainNoTimeout(
	ctx context.Context, c serverpb.AdminClient, targetNode string,
) (hardError, remainingWork bool, err error) {
	defer func() {
		if grpcutil.IsWaitingForInit(err) {
			log.Infof(ctx, "%v", err)
			err = errors.New("node cannot be drained before it has been initialized")
		}
	}()

	var (
		remaining     = uint64(math.MaxUint64)
		prevRemaining = uint64(math.MaxUint64)
		verbose       = false
	)
	for ; ; prevRemaining = remaining {
		// Tell the user we're starting to drain. This enables the user to
		// mentally prepare for something to take some time, as opposed to
		// wondering why nothing is happening.
		fmt.Fprintf(stderr, "node is draining... ") // notice no final newline.

		// Send a drain request with the drain bit set and the shutdown bit
		// unset.
		stream, err := c.Drain(ctx, &serverpb.DrainRequest{
			DoDrain:  true,
			Shutdown: false,
			NodeId:   targetNode,
			Verbose:  verbose,
		})
		if err != nil {
			fmt.Fprintf(stderr, "\n") // finish the line started above.
			return !grpcutil.IsTimeout(err), remaining > 0, errors.Wrap(err, "error sending drain request")
		}
		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				// Done.
				break
			}
			if err != nil {
				// Unexpected error.
				fmt.Fprintf(stderr, "\n") // finish the line started above.
				log.Infof(ctx, "graceful drain failed: %v", err)
				return false, remaining > 0, err
			}

			if resp.IsDraining {
				// We want to assert that the node is quitting, and tell the
				// story about how much work was performed in logs for
				// debugging.
				remaining = resp.DrainRemainingIndicator
				finalString := ""
				if remaining == 0 {
					finalString = " (complete)"
				}

				// We use stderr so that the stdout output remains a
				// simple 'ok' in case of success (for compatibility with
				// scripts).
				fmt.Fprintf(stderr, "remaining: %d%s\n", remaining, finalString)
			} else {
				// Either the server has decided it wanted to stop quitting; or
				// we're running a pre-20.1 node which doesn't populate IsDraining.
				// In either case, we need to stop sending drain requests.
				remaining = 0
				fmt.Fprintf(stderr, "done\n")
			}

			if resp.DrainRemainingDescription != "" {
				// Only show this information in the log; we'd use this for debugging.
				// (This can be revealed e.g. via --logtostderr.)
				log.Infof(ctx, "drain details: %s\n", resp.DrainRemainingDescription)
			}

			// Iterate until end of stream, which indicates the drain is
			// complete.
		}
		if remaining == 0 {
			// No more work to do.
			break
		}

		// If range lease transfer stalls or the number of remaining leases
		// somehow increases, verbosity is set to help with troubleshooting.
		if remaining >= prevRemaining {
			verbose = true
		}

		// Avoid a busy wait with high CPU/network usage if the server
		// replies with an incomplete drain too quickly.
		time.Sleep(200 * time.Millisecond)
	}
	return false, remaining > 0, nil
}

// doShutdown attempts to trigger a server shutdown *without*
// draining. Use doDrain() prior to perform a drain, or
// drainAndShutdown() to combine both.
func doShutdown(
	ctx context.Context, c serverpb.AdminClient, targetNode string,
) (hardError bool, err error) {
	defer func() {
		if err != nil {
			if grpcutil.IsWaitingForInit(err) {
				log.Infof(ctx, "encountered error: %v", err)
				err = errors.New("node cannot be shut down before it has been initialized")
				err = errors.WithHint(err, "You can still stop the process using a service manager or a signal.")
				hardError = true
			}
			if grpcutil.IsClosedConnection(err) {
				// This most likely means that we shut down successfully. Note
				// that sometimes the connection can be shut down even before a
				// DrainResponse gets sent back to us, so we don't require a
				// response on the stream (see #14184).
				err = nil
			}
		}
	}()

	// We use a shorter timeout because a shutdown request has nothing
	// else to do than shut down the node immediately.
	err = timeutil.RunWithTimeout(ctx, "hard shutdown", 10*time.Second, func(ctx context.Context) error {
		// Send a drain request with the drain bit unset (no drain).
		// and the shutdown bit set.
		stream, err := c.Drain(ctx, &serverpb.DrainRequest{NodeId: targetNode, Shutdown: true})
		if err != nil {
			return errors.Wrap(err, "error sending shutdown request")
		}
		for {
			_, err := stream.Recv()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
		}
	})
	if !errors.HasType(err, (*timeutil.TimeoutError)(nil)) {
		hardError = true
	}
	return hardError, err
}
