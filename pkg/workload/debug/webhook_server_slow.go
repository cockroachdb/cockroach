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
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/spf13/cobra"
)

var webhookServerSlowCmd = &cobra.Command{
	Use:   "webhook-server-slow [transient error freq ms] ...[range delays ms]",
	Short: "webhook-server-slow opens an http server on 3000 to which cdc's webhook can emit a table with a numeric unique 'id' column",
	RunE:  webhookServerSlow,
	Args:  cobra.MinimumNArgs(0),
}

const (
	WebhookServerSlowPort = 9707
)

func webhookServerSlow(cmd *cobra.Command, args []string) error {
	var (
		mu    syncutil.Mutex
		seen  = map[string]struct{}{}
		size  int64
		dupes int
	)
	// Add a variable to track the last time a transient error occurred.
	lastTransientErrorTime := timeutil.Now()

	const (
		defaultTransientErrorFrequency = 1000 // Default transient error frequency in ms
	)
	defaultRangeDelays := []time.Duration{} // Default range delays in ms (no delay)

	// Parse the arguments
	transientErrorFrequency := defaultTransientErrorFrequency
	rangeDelays := defaultRangeDelays

	if len(args) > 0 {
		if freq, err := strconv.Atoi(args[0]); err == nil {
			transientErrorFrequency = freq
		} else {
			log.Printf("Invalid transient error frequency, using default: %d ms", defaultTransientErrorFrequency)
		}
	}

	if len(args) > 1 {
		rangeDelays = make([]time.Duration, len(args[1:]))
		for i, delay := range args[1:] {
			if d, err := strconv.Atoi(delay); err == nil {
				rangeDelays[i] = time.Duration(d) * time.Millisecond
			} else {
				log.Printf("Invalid range delay at index %d, using default: 0 ms", i)
				rangeDelays[i] = 0 * time.Millisecond
			}
		}
	}

	// Log the parsed or default values
	log.Printf("Transient Error Frequency: %d ms", transientErrorFrequency)
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
		if now.Sub(lastTransientErrorTime) >= time.Duration(transientErrorFrequency)*time.Millisecond {
			lastTransientErrorTime = now
			http.Error(w, "transient sink error", http.StatusInternalServerError)
			return
		}

		var before, after, d int
		func() {
			mu.Lock()
			defer mu.Unlock()
			before = len(seen)
			after = before
			// TODO(cdc): add check for ordering guarantees using resolved timestamps and event timestamps
			for _, i := range req.Payload {
				id := i.After.ID
				seenKey := fmt.Sprintf("%d-%s", id, i.Updated)
				if _, ok := seen[seenKey]; !ok {
					seen[seenKey] = struct{}{}
					after++

					sleepDurationIndex := id / 10
					if sleepDurationIndex < len(rangeDelays) {
						timeToSleep := rangeDelays[sleepDurationIndex]
						time.Sleep(timeToSleep)
					}
				} else {
					dupes++
				}
			}
			if r.ContentLength > 0 {
				size += r.ContentLength
			}
			d = dupes
		}()
		const printEvery = 100
		if before/printEvery != after/printEvery {
			log.Printf("keys seen: %d (%d dupes); %.1f MB", after, d, float64(size)/float64(1<<20))
		}
	})
	mux.HandleFunc("/reset", func(w http.ResponseWriter, r *http.Request) {
		func() {
			mu.Lock()
			defer mu.Unlock()
			seen = make(map[string]struct{}, len(seen))
			dupes = 0
			size = 0
		}()
		log.Printf("reset")
	})
	mux.HandleFunc("/unique", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		l := len(seen)
		fmt.Fprintf(w, "%d", l)
	})
	mux.HandleFunc("/dupes", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		fmt.Fprintf(w, "%d", dupes)
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
