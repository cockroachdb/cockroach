// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package debug

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/spf13/cobra"
)

var webhookServerSlowCmd = &cobra.Command{
	Use:   "webhook-server-slow [transient error interval ms] ...[range delays ms]",
	Short: "webhook-server-slow opens an http server on 9707 to which cdc's webhook can emit a table with a numeric unique 'id' column",
	Long: `
webhook-server-slow opens an http server on 9707 to which cdc's webhook can emit a table with a numeric unique 'id' column.
You can specify a transient error interval in milliseconds, which will cause the server to return a 500 error at that interval.
You can also specify a list of range delays in milliseconds, which will cause the server to sleep for that duration before
acknowledging requests in the first 3 ranges (assumed to be 0-9, 10-19, and 20-29).

For example: 
	webhook-server-slow 1000 10 20 30

Will cause the server to return a 500 error every second and sleep for 10ms, 20ms, and 30ms for the first three ranges respectively.
	`,
	RunE: webhookServerSlow,
	Args: cobra.MinimumNArgs(0),
}

const (
	WebhookServerSlowPort = 9707
)

func webhookServerSlow(cmd *cobra.Command, args []string) error {
	type state struct {
		seen  map[string]struct{}
		size  int64
		dupes int
	}
	var mu syncutil.Mutex

	serverState := state{
		seen:  make(map[string]struct{}),
		size:  0,
		dupes: 0,
	}

	var lastTransientErrorTime atomic.Value
	lastTransientErrorTime.Store(timeutil.Now())

	const (
		defaultTransientErrorInterval = 1000 // Default transient error frequency in ms
	)
	defaultRangeDelays := []time.Duration{} // Default range delays in ms (no delay)

	// Parse the arguments
	transientErrorInterval := defaultTransientErrorInterval
	rangeDelays := defaultRangeDelays

	if len(args) > 0 {
		if interval, err := strconv.Atoi(args[0]); err == nil {
			transientErrorInterval = interval
		} else {
			return fmt.Errorf("invalid transient error interval")
		}
	}

	if len(args) > 1 {
		rangeDelays = make([]time.Duration, len(args[1:]))
		for i, delay := range args[1:] {
			if d, err := strconv.Atoi(delay); err == nil {
				rangeDelays[i] = time.Duration(d) * time.Millisecond
			} else {
				return fmt.Errorf("invalid range delay at index %d", i)
			}
		}
	}

	// Log the parsed or default values
	log.Printf("Transient Error Interval: %d ms", transientErrorInterval)
	log.Printf("Range Delays: %v ms", rangeDelays)

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Length  int `json:"length"`
			Payload []struct {
				After struct {
					ID  int `json:"id"`
					VAL int `json:"val"`
				} `json:"after"`
				Before struct {
					ID  int `json:"id"`
					VAL int `json:"val"`
				} `json:"before"`
				Updated string `json:"updated"`
			} `json:"payload"`
		}

		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		now := timeutil.Now()
		lastTime := lastTransientErrorTime.Load().(time.Time)
		if now.Sub(lastTime) >= time.Duration(transientErrorInterval)*time.Millisecond {
			lastTransientErrorTime.Store(now)
			http.Error(w, "transient sink error", http.StatusInternalServerError)
			return
		}

		var before, after, d int
		// To avoid sleeping while holding the lock, we'll collect the sleep durations
		// in a slice and then sleep after releasing the lock.
		var sleepDurations []time.Duration

		func() {
			mu.Lock()
			defer mu.Unlock()
			before = len(serverState.seen)
			after = before
			sleepDurations = make([]time.Duration, 0, len(req.Payload))

			// TODO(#146097): add check for ordering guarantees using resolved timestamps and event timestamps
			for _, i := range req.Payload {
				id := i.After.ID
				seenKey := fmt.Sprintf("%d-%s", id, i.Updated)
				if _, ok := serverState.seen[seenKey]; !ok {
					serverState.seen[seenKey] = struct{}{}
					after++

					sleepDurationIndex := id / 10
					if sleepDurationIndex < len(rangeDelays) {
						sleepDurations = append(sleepDurations, rangeDelays[sleepDurationIndex])
					}
				} else {
					serverState.dupes++
				}
			}
			if r.ContentLength > 0 {
				serverState.size += r.ContentLength
			}
			d = serverState.dupes
		}()

		for _, sleepDuration := range sleepDurations {
			if sleepDuration > 0 {
				time.Sleep(sleepDuration)
			}
		}

		const printEvery = 100
		if before/printEvery != after/printEvery {
			log.Printf("keys seen: %d (%d dupes); %.1f MB", after, d, float64(serverState.size)/float64(1<<20))
		}
	})
	mux.HandleFunc("/reset", func(w http.ResponseWriter, r *http.Request) {
		func() {
			mu.Lock()
			defer mu.Unlock()
			clear(serverState.seen)
			serverState.dupes = 0
			serverState.size = 0
		}()
		log.Printf("reset")
	})
	mux.HandleFunc("/unique", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		l := len(serverState.seen)
		fmt.Fprintf(w, "%d", l)
	})
	mux.HandleFunc("/dupes", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		fmt.Fprintf(w, "%d", serverState.dupes)
	})

	mux.HandleFunc("/exit", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		go func() {
			time.Sleep(time.Millisecond * 5)
			exit.WithCode(exit.Success())
		}()
	})

	cert, err := genKeyPair()
	if err != nil {
		return err
	}
	log.Printf("starting server on port %d", WebhookServerSlowPort)
	return (&http.Server{
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}},
		Handler:   mux,
		Addr:      fmt.Sprintf(":%d", WebhookServerSlowPort),
	}).ListenAndServeTLS("", "")
}
