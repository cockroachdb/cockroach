package main

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"io/ioutil"
	"net"
	"path/filepath"
)

func main() {
	var certsDir = flag.String("certs-dir", "./certs-dir", "certificates directory")

	flag.Parse()

	caCertPath := filepath.Join(*certsDir, "ca.crt")

	caCert, err := ioutil.ReadFile(caCertPath)
	if err != nil {
		panic(err)
	}

	certPool := x509.NewCertPool()
	certPool.AppendCertsFromPEM(caCert)

	clientConfig := &tls.Config{
		CipherSuites: []uint16{
			tls.TLS_RSA_WITH_AES_128_GCM_SHA256,
		},
		MinVersion:         tls.VersionTLS12,
		MaxVersion:         tls.VersionTLS12,
		RootCAs:            certPool,
		InsecureSkipVerify: false,
		ServerName:         "localhost",
	}

	netClient, err := net.Dial("tcp4", "127.0.0.1:8080")
	if nil != err {
		panic(err)
	}
	defer netClient.Close()

	tlsClient := tls.Client(netClient, clientConfig)
	defer tlsClient.Close()

	err = tlsClient.Handshake()
	if nil != err {
		panic(err)
	}
}
