// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package debug

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"net/http"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/spf13/cobra"
)

var webhookServerCmd = &cobra.Command{
	Use:   "webhook-server",
	Short: "webhook-server opens an http server on 3000 to which cdc's webhook can emit a table with a numeric unique 'id' column",
	RunE:  webhookServer,
	Args:  cobra.NoArgs,
}

const (
	port = 3000
)

func webhookServer(cmd *cobra.Command, args []string) error {
	var (
		mu   syncutil.Mutex
		seen = map[int]struct{}{}
	)
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Length  int `json:"length"`
			Payload []struct {
				After struct {
					ID int `json:"id"`
				} `json:"after"`
			} `json:"payload"`
		}

		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			log.Printf("decoding body: %v", err)
			return
		}
		var before, after int
		func() {
			mu.Lock()
			defer mu.Unlock()
			before = len(seen)
			after = before
			for _, i := range req.Payload {
				if _, ok := seen[i.After.ID]; !ok {
					after++
				}
				seen[i.After.ID] = struct{}{}
			}
		}()
		const printEvery = 10000
		if before/printEvery != after/printEvery {
			log.Printf("keys seen: %d", after)
		}
	})
	mux.HandleFunc("/reset", func(w http.ResponseWriter, r *http.Request) {
		func() {
			mu.Lock()
			defer mu.Unlock()
			seen = make(map[int]struct{}, len(seen))
		}()
		log.Printf("reset")
	})
	mux.HandleFunc("/count", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		l := len(seen)
		log.Printf("keys seen: %d", l)
		fmt.Fprintf(w, "%d", l)
	})

	cert, err := genKeyPair()
	if err != nil {
		return err
	}
	log.Printf("starting server on port %d", port)
	return (&http.Server{
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}},
		Handler:   mux,
		Addr:      fmt.Sprintf(":%d", port),
	}).ListenAndServeTLS("", "")
}

func genKeyPair() (tls.Certificate, error) {
	now := timeutil.Now()
	tpl := &x509.Certificate{
		Subject:               pkix.Name{CommonName: "localhost"},
		SerialNumber:          big.NewInt(now.Unix()),
		NotBefore:             now,
		NotAfter:              now.AddDate(0, 0, 30), // Valid for one day
		BasicConstraintsValid: true,
		IsCA:                  true,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
	}

	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return tls.Certificate{}, err
	}

	cert, err := x509.CreateCertificate(rand.Reader, tpl, tpl, k.Public(), k)
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.Certificate{PrivateKey: k, Certificate: [][]byte{cert}}, nil
}
