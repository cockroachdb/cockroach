// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl"
)

var options struct {
	listenAddress string
	targetAddress string
	cert          string
	key           string
	verify        bool
}

func main() {
	if err := run(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func run() error {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage:  %s [options]\n", os.Args[0])
		flag.PrintDefaults()
	}

	flag.StringVar(&options.listenAddress, "listen", "127.0.0.1:5432",
		"Listen address for incoming connections")
	flag.StringVar(&options.cert, "cert-file", "server.crt",
		"file containing PEM-encoded x509 certificate for listen adress")
	flag.StringVar(&options.key, "key-file", "server.key",
		"file containing PEM-encoded x509 key for listen address")
	flag.StringVar(&options.targetAddress, "target", "127.0.0.1:26257",
		"Address to proxy to (a Postgres-compatible server)")
	flag.BoolVar(&options.verify, "verify", true,
		"If false, use InsecureSkipVerify=true. For testing only.")
	flag.Parse()

	ln, err := net.Listen("tcp", options.listenAddress)
	if err != nil {
		return err
	}
	defer ln.Close()

	log.Println("Listening on", ln.Addr())

	cer, err := tls.LoadX509KeyPair(options.cert, options.key)
	if err != nil {
		return err
	}
	opts := sqlproxyccl.Options{
		IncomingTLSConfig: &tls.Config{Certificates: []tls.Certificate{cer}},
		OutgoingTLSConfig: &tls.Config{
			InsecureSkipVerify: !options.verify,
		},
		OutgoingAddrFromParams: func(map[string]string) (addr string, clientErr error) {
			// TODO(asubiotto): implement the actual translation here once it is clear
			// how this will work.
			return options.targetAddress, nil
		},
	}

	return sqlproxyccl.Serve(ln, opts)
}
