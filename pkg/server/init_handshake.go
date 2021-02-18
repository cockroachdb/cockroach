// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/errors"
)

const (
	initServiceName = "temp-init-service"
)

// Blob used for symetric exchange of node public CA keys
type nodeHostnameAndCA struct {
	HostAddress   string `json:"host_address"`
	CACertificate []byte `json:"ca_certificate"`
	HMAC          []byte `json:"hmac,omitempty"`
}

// internode trust delivery struct
type nodeTrustBundle struct {
	// Bundle of all certificates needed to initialize nodes.
	Bundle CertificateBundle `json:"certificate_bundle"`
	// HMAC of the CA key/cert for the internode service inside the Bundle above.
	HMAC   []byte            `json:"hmac,omitempty"`
}

// Symetric server/client CA blob signature
func createSignedNodeHostnameAndCa(hostAddress string, caCert []byte, secretToken []byte) (nodeHostnameAndCA, error) {
	signedMessage := nodeHostnameAndCA{
		HostAddress:   hostAddress,
		CACertificate: caCert,
	}
	h := hmac.New(sha256.New, secretToken)
	h.Write([]byte(hostAddress))
	h.Write(caCert)
	signedMessage.HMAC = h.Sum(nil)

	return signedMessage, nil
}

func (n *nodeHostnameAndCA) validHMAC(secretToken []byte) bool {
	h := hmac.New(sha256.New, secretToken)
	h.Write([]byte(n.HostAddress))
	h.Write(n.CACertificate)
	expectedMac := h.Sum(nil)
	return hmac.Equal(expectedMac, n.HMAC)
}

func validTrustBundle(bundle nodeTrustBundle, secretToken []byte) bool {
	h := hmac.New(sha256.New, secretToken)
	h.Write(bundle.Bundle.InterNode.CACertificate)
	h.Write(bundle.Bundle.InterNode.CAKey)
	expectedMac := h.Sum(nil)
	return hmac.Equal(expectedMac, bundle.HMAC)
}

// helper function to extract signatures
func pemToSignature(caCertPEM []byte) (signature string, err error) {
	caCert, _ := pem.Decode(caCertPEM)
	if nil == caCert {
		return "", errors.New("failed to parse valid PEM from CACertificate blob")
	}

	cert, err := x509.ParseCertificate(caCert.Bytes)
	if nil != err {
		return "", errors.New("failed to parse valid certificate from CACertificate blob")
	}

	signature = hex.EncodeToString(cert.Signature)
	return signature, nil
}

func createNodeInitTempCertificates(hostname string, lifespan time.Duration) (certs ServiceCertificateBundle, err error) {
	caCert, caKey, err := security.CreateCACertAndKey(lifespan, initServiceName)
	if err != nil {
		return certs, err
	}
	serviceCert, serviceKey, err := security.CreateServiceCertAndKey(lifespan, initServiceName, hostname, caCert, caKey)
	if err != nil {
		return certs, err
	}

	certs = ServiceCertificateBundle{
		CACertificate:   caCert,
		CAKey:           caKey,
		HostCertificate: serviceCert,
		HostKey:         serviceKey,
	}
	return certs, nil
}

// tlsInitHandshaker takes in a list of peers
type tlsInitHandshaker struct {
	ctx     context.Context
	peers   []string
	scratch []byte
	server  *http.Server

	token                  []byte
	certsDir               string
	listenHost, listenPort string

	tempCerts    ServiceCertificateBundle
	trustedPeers chan nodeHostnameAndCA
	finishedInit chan bool
}

func (t *tlsInitHandshaker) init() error {
	serverCert, err := tls.X509KeyPair(t.tempCerts.HostCertificate, t.tempCerts.HostKey)
	if err != nil {
		return err
	}

	certpool := x509.NewCertPool()
	certpool.AppendCertsFromPEM(t.tempCerts.CACertificate)
	serviceTLSConf := &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		RootCAs:      certpool,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/trustInit", t.onTrustInit)
	mux.HandleFunc("/deliverBundle", t.onDeliverBundle)

	t.server = &http.Server{
		Addr:      fmt.Sprintf("%s:%s", t.listenHost, t.listenPort),
		Handler:   mux,
		TLSConfig: serviceTLSConf,
	}
}

// Handler for initial challenge and ack containing the ephemeral node CAs
func (t *tlsInitHandshaker) onTrustInit(res http.ResponseWriter, req *http.Request) {
	var challenge nodeHostnameAndCA

	// TODO(aaron-crl): [Security] make this more error resilient to size and shape attacks
	err := json.NewDecoder(req.Body).Decode(&challenge)
	if err != nil {
		apiV2InternalError(req.Context(), err, res)
		return
	}
	defer req.Body.Close()

	if !challenge.validHMAC(t.token) {
		http.Error(res, "invalid message", http.StatusBadRequest)
		return
	}

	// send the valid node infomation to the trustedPeers channel
	t.trustedPeers <- challenge

	// acknowledge validation to client
	hostAddress := fmt.Sprintf("%s:%s", t.listenHost, t.listenPort)
	ack, err := createSignedNodeHostnameAndCa(hostAddress, t.tempCerts.CACertificate, t.token)
	if err != nil {
		apiV2InternalError(req.Context(), err, res)
		return
	}
	if err := json.NewEncoder(res).Encode(ack); err != nil {
		apiV2InternalError(req.Context(), err, res)
		return
	}
}

// Handler to allow peer to deliver internode CA trust bundle.
func (t *tlsInitHandshaker) onDeliverBundle(res http.ResponseWriter, req *http.Request) {
	bundle := nodeTrustBundle{}
	err := json.NewDecoder(req.Body).Decode(&bundle)
	defer req.Body.Close()
	if err != nil {
		http.Error(res, "invalid message", http.StatusBadRequest)
		return
	}
	if validTrustBundle(bundle, t.token) {
		// successfully provisioned
		t.finishedInit <- true
	}
	// TODO(bilal): Save bundle to certs directory.
}

func (t *tlsInitHandshaker) startServer() {
	// Start the server.
	_ = t.server.ListenAndServeTLS("", "")
}

func (t *tlsInitHandshaker) stopServer() {
	if t.server == nil {
		return
	}

	_ = t.server.Shutdown(t.ctx)
	t.server = nil
}

func (t *tlsInitHandshaker) startClient(peerHostname string, selfAddress string) {
	// Sleep for 500ms between attempts.
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-t.ctx.Done():
			return
		case <-ticker.C:
		}

		peerHostnameAndCa, err := t.getPeerCACert(peerHostname, selfAddress)
		if err == nil {
			t.trustedPeers <- peerHostnameAndCa
			return
		}
	}
}

func (t *tlsInitHandshaker) getPeerCACert(peerAddress string, selfAddress string) (nodeHostnameAndCA, error) {
	// TODO(aaron-crl): [Enhancement] add TLS protocol level checks to make sure remote certificate matches profered one
	// This is non critical due to HMAC but would be good hygiene

	// Connect to HTTPS endpoint unverified (effectively HTTP) with POST of challenge
	// HMAC(hostname + node CA public certificate, secretToken).
	clientTransport := http.DefaultTransport.(*http.Transport).Clone()
	clientTransport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	client := &http.Client{Transport: clientTransport}

	challenge, err := createSignedNodeHostnameAndCa(selfAddress, t.tempCerts.CACertificate, t.token)
	if err != nil {
		return nodeHostnameAndCA{}, err
	}

	// Poll until valid or timeout
	var body bytes.Buffer
	json.NewEncoder(&body).Encode(challenge)
	res, err := client.Post("https://"+peerAddress+"/trustInit", "application/json; charset=utf-8", &body)
	if err != nil {
		return nodeHostnameAndCA{}, err
	}

	// read and validate server provided ack
	// HMAC(hostname + server CA public certificate, secretToken)
	var msg nodeHostnameAndCA
	if err := json.NewDecoder(res.Body).Decode(&msg); err != nil {
		return nodeHostnameAndCA{}, err
	}
	defer res.Body.Close()

	// confirm response HMAC, if valid return peer bundle
	if !msg.validHMAC(t.token) {
		return nodeHostnameAndCA{}, errors.New("invalid HMAC signature on message from server")
	}
	return msg, nil
}

func sendSecret(address string, peerCACert []byte, caBundle nodeTrustBundle) (err error) {
	rootCAs, _ := x509.SystemCertPool()
	rootCAs.AppendCertsFromPEM(peerCACert)

	clientTransport := http.DefaultTransport.(*http.Transport).Clone()
	clientTransport.TLSClientConfig = &tls.Config{RootCAs: rootCAs}
	client := &http.Client{Transport: clientTransport}

	body := new(bytes.Buffer)
	json.NewEncoder(body).Encode(caBundle)

	_, err = client.Post("https://"+address+"/deliverBundle", "application/json; charset=utf-8", body)
	return err
}

// InitHandshake starts off an inter-node TLS handshake protocol, as described
// here: https://github.com/cockroachdb/cockroach/pull/51991 . This method
// negotiates an inter-node CA and puts it in certsDir.
func InitHandshake(ctx context.Context, cfg *base.Config, token string, peers []string, certsDir string, listenAddr, listenPort string) error {
	lifespan := 60 * time.Second
	ctx, cancel := context.WithTimeout(ctx, lifespan)
	defer cancel()
	tempCerts, err := createNodeInitTempCertificates(listenAddr, lifespan)
	if err != nil {
		return errors.Wrap(err, "Failed to create certificates")
	}

	handshaker := &tlsInitHandshaker{
		ctx:          ctx,
		peers:        peers,
		token:        []byte(token),
		certsDir:     certsDir,
		listenHost:   listenAddr,
		listenPort:   listenPort,
		tempCerts:    tempCerts,
		trustedPeers: make(chan nodeHostnameAndCA, len(peers)),
		finishedInit: make(chan bool, 1),
	}
	if err := handshaker.init(); err != nil {
		return err
	}

	go handshaker.startServer()
	defer handshaker.stopServer()

	// start peer bind processes
	for _, peerAddress := range peers {
		go handshaker.startClient(peerAddress, fmt.Sprintf("%s:%s", listenAddr, listenPort))
	}

	// wait until we have numNodes - 1 valid peers
	peerCACerts := make(map[string]([]byte))
	certsReceived := 0
	for len(peers) > certsReceived {
		select {
		case p := <- handshaker.trustedPeers:
			peerCACerts[p.HostAddress] = p.CACertificate
			certsReceived++
		case <-ctx.Done():
			return errors.New("timed out before peers connected")
		}
	}

	// Order nodes by certificates.
	trustLeader := true
	selfSignature, _ := pemToSignature(tempCerts.CACertificate)

	for _, peerCertPEM := range peerCACerts {
		peerSignature, _ := pemToSignature(peerCertPEM)
		if selfSignature > peerSignature {
			trustLeader = false
		}
	}

	// Initialize if this node is the trust leader. If not, wait for trust bundle
	// to come from another node.
	if trustLeader {
		var b CertificateBundle
		// TODO(bilal): See if we can get rid of the need to store a base.Config
		// pointer. This is the only place in this method where it is necessary.
		if err := b.InitializeFromConfig(*cfg); err != nil {
			return err
		}
		// The internode CA cert/key is signed for the HMAC.
		trustBundle := nodeTrustBundle{Bundle: b}
		h := hmac.New(sha256.New, []byte(token))
		h.Write(b.InterNode.CACertificate)
		h.Write(b.InterNode.CAKey)
		trustBundle.HMAC = h.Sum(nil)
		// TODO(bilal): Only copy CA{Certificate,Key}s when sending over to other nodes.

		// For each peer, use its CA to establish a secure connection and deliver the trust bundle.
		for p := range peerCACerts {
			if err := sendSecret(p, peerCACerts[p], trustBundle); err != nil {
				return err
			}
		}
	}
	select {
	case <-handshaker.finishedInit:
	case <-ctx.Done():
		return errors.New("timed out before trust bundle received from leader")
	}

	return nil
}
