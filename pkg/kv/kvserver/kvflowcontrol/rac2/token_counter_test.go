// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rac2

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/dustin/go-humanize"
	"github.com/stretchr/testify/require"
)

func TestTokenAdjustment(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var (
		ctx         = context.Background()
		counter     *tokenCounter
		adjustments []adjustment
	)

	datadriven.RunTest(t, "testdata/token_adjustment",
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				counter = newTokenCounter(cluster.MakeTestingClusterSettings())
				adjustments = nil
				return ""

			case "adjust":
				require.NotNilf(t, counter, "uninitialized flow controller (did you use 'init'?)")

				for _, line := range strings.Split(d.Input, "\n") {
					parts := strings.Fields(line)
					require.Len(t, parts, 2, "expected form 'class={regular,elastic} delta={+,-}<size>")

					var delta kvflowcontrol.Tokens
					var wc admissionpb.WorkClass

					// Parse class={regular,elastic}.
					parts[0] = strings.TrimSpace(parts[0])
					require.True(t, strings.HasPrefix(parts[0], "class="))
					parts[0] = strings.TrimPrefix(strings.TrimSpace(parts[0]), "class=")
					switch parts[0] {
					case "regular":
						wc = admissionpb.RegularWorkClass
					case "elastic":
						wc = admissionpb.ElasticWorkClass
					}

					// Parse delta={+,-}<size>
					parts[1] = strings.TrimSpace(parts[1])
					require.True(t, strings.HasPrefix(parts[1], "delta="))
					parts[1] = strings.TrimPrefix(strings.TrimSpace(parts[1]), "delta=")
					require.True(t, strings.HasPrefix(parts[1], "+") || strings.HasPrefix(parts[1], "-"))
					isPositive := strings.Contains(parts[1], "+")
					parts[1] = strings.TrimPrefix(parts[1], "+")
					parts[1] = strings.TrimPrefix(parts[1], "-")
					bytes, err := humanize.ParseBytes(parts[1])
					require.NoError(t, err)
					delta = kvflowcontrol.Tokens(int64(bytes))
					if !isPositive {
						delta = -delta
					}
					counter.adjust(ctx, wc, delta, false /* admin */)
					adjustments = append(adjustments, adjustment{
						wc:    wc,
						delta: delta,
						post: tokensPerWorkClass{
							regular: counter.tokens(admissionpb.RegularWorkClass),
							elastic: counter.tokens(admissionpb.ElasticWorkClass),
						},
					})
				}
				return ""

			case "history":
				limit := counter.testingGetLimit()

				var buf strings.Builder
				buf.WriteString("                   regular |  elastic\n")
				buf.WriteString(fmt.Sprintf("                  %8s | %8s\n",
					printTrimmedTokens(limit.regular),
					printTrimmedTokens(limit.elastic),
				))
				buf.WriteString("======================================\n")
				for _, h := range adjustments {
					buf.WriteString(fmt.Sprintf("%s\n", h))
				}
				return buf.String()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

type adjustment struct {
	wc    admissionpb.WorkClass
	delta kvflowcontrol.Tokens
	post  tokensPerWorkClass
}

func printTrimmedTokens(t kvflowcontrol.Tokens) string {
	return strings.ReplaceAll(t.String(), " ", "")
}

func (h adjustment) String() string {
	comment := ""
	if h.post.regular <= 0 {
		comment = "regular"
	}
	if h.post.elastic <= 0 {
		if len(comment) == 0 {
			comment = "elastic"
		} else {
			comment = "regular and elastic"
		}
	}
	if len(comment) != 0 {
		comment = fmt.Sprintf(" (%s blocked)", comment)
	}
	return fmt.Sprintf("%8s %7s  %8s | %8s%s",
		printTrimmedTokens(h.delta),
		h.wc,
		printTrimmedTokens(h.post.regular),
		printTrimmedTokens(h.post.elastic),
		comment,
	)
}

func TestTokenCounter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	limits := tokensPerWorkClass{
		regular: 50,
		elastic: 50,
	}
	settings := cluster.MakeTestingClusterSettings()
	kvflowcontrol.ElasticTokensPerStream.Override(ctx, &settings.SV, int64(limits.elastic))
	kvflowcontrol.RegularTokensPerStream.Override(ctx, &settings.SV, int64(limits.regular))
	counter := newTokenCounter(settings)

	assertStateReset := func(t *testing.T) {
		available, handle := counter.TokensAvailable(admissionpb.ElasticWorkClass)
		require.True(t, available)
		require.Nil(t, handle)
		require.Equal(t, limits.regular, counter.tokens(admissionpb.RegularWorkClass))
		require.Equal(t, limits.elastic, counter.tokens(admissionpb.ElasticWorkClass))
	}

	t.Run("tokens_available", func(t *testing.T) {
		// Initially, tokens should be available for both regular and elastic work
		// classes.
		available, handle := counter.TokensAvailable(admissionpb.RegularWorkClass)
		require.True(t, available)
		require.Nil(t, handle)

		available, handle = counter.TokensAvailable(admissionpb.ElasticWorkClass)
		require.True(t, available)
		require.Nil(t, handle)
		assertStateReset(t)
	})

	t.Run("deduct_partial", func(t *testing.T) {
		// Try deducting more tokens than available. Only the available tokens
		// should be deducted.
		granted := counter.TryDeduct(ctx, admissionpb.RegularWorkClass, limits.regular+50)
		require.Equal(t, limits.regular, granted)

		// Now there should be no tokens available for regular work class.
		available, handle := counter.TokensAvailable(admissionpb.RegularWorkClass)
		require.False(t, available)
		require.NotNil(t, handle)
		counter.Return(ctx, admissionpb.RegularWorkClass, limits.regular)
		assertStateReset(t)
	})

	t.Run("tokens_unavailable", func(t *testing.T) {
		// Deduct tokens without checking availability, going into debt.
		counter.Deduct(ctx, admissionpb.ElasticWorkClass, limits.elastic+50)
		// Tokens should now be in debt, meaning future deductions will also go
		// into further debt, or on TryDeduct, deduct no tokens at all.
		granted := counter.TryDeduct(ctx, admissionpb.ElasticWorkClass, 50)
		require.Equal(t, kvflowcontrol.Tokens(0), granted)
		// Return tokens to bring the counter out of debt.
		counter.Return(ctx, admissionpb.ElasticWorkClass, limits.elastic+50)
		assertStateReset(t)
	})

	t.Run("wait_no_tokens", func(t *testing.T) {
		// Use up all the tokens trying to deduct the maximum+1
		// (tokensPerWorkClass) tokens. There should be exactly tokensPerWorkClass
		// tokens granted.
		granted := counter.TryDeduct(ctx, admissionpb.RegularWorkClass, limits.regular+1)
		require.Equal(t, limits.regular, granted)
		// There should be no tokens available for regular work and a handle
		// returned.
		available, handle := counter.TokensAvailable(admissionpb.RegularWorkClass)
		require.False(t, available)
		require.NotNil(t, handle)
		counter.Return(ctx, admissionpb.RegularWorkClass, limits.regular)
		// Wait on the handle to be unblocked and expect that there are tokens
		// available when the wait channel is signaled.
		<-handle.WaitChannel()
		haveTokens := handle.ConfirmHaveTokensAndUnblockNextWaiter()
		require.True(t, haveTokens)
		// Wait on the handle to be unblocked again, this time try deducting such
		// that there are no tokens available after.
		counter.Deduct(ctx, admissionpb.RegularWorkClass, limits.regular)
		<-handle.WaitChannel()
		haveTokens = handle.ConfirmHaveTokensAndUnblockNextWaiter()
		require.False(t, haveTokens)
		// Return the tokens deducted from the first wait above.
		counter.Return(ctx, admissionpb.RegularWorkClass, limits.regular)
		assertStateReset(t)
	})

	t.Run("wait_multi_goroutine", func(t *testing.T) {
		// Create a group of goroutines which will race on deducting tokens, each
		// requires exactly the limit, so only one will succeed at a time.
		numGoroutines := 5
		tokensRequested := limits.regular
		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func() {
				defer wg.Done()
				remaining := limits.regular

				for {
					granted := counter.TryDeduct(ctx, admissionpb.RegularWorkClass, remaining)
					remaining = remaining - granted
					if remaining == 0 {
						break
					}
					// If not enough tokens are granted, wait for tokens to become
					// available.
					available, handle := counter.TokensAvailable(admissionpb.RegularWorkClass)
					if !available {
						<-handle.WaitChannel()
						// This may or may not have raced with another goroutine, there's
						// no guarantee we have tokens here.
						handle.ConfirmHaveTokensAndUnblockNextWaiter()
					}
				}

				// Ensure all requested tokens are granted eventually and return the
				// tokens back to the counter.
				require.Equal(t, kvflowcontrol.Tokens(0), remaining)
				counter.Return(ctx, admissionpb.RegularWorkClass, tokensRequested)
			}()
		}
		wg.Wait()
		assertStateReset(t)
	})
}
