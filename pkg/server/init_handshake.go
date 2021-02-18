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
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

const (
	initServiceName     = "temp-init-service"
	defaultInitLifespan = 120 * time.Second

	trustInitURL     = "/trustInit/"
	deliverBundleURL = "/deliverBundle/"
)

var errInvalidHMAC = errors.New("invalid HMAC signature")

// nodeHostnameAndCA is a message struct used for symmetric exchange of node
// public CA keys.
type nodeHostnameAndCA struct {
	HostAddress   string `json:"host_address"`
	CACertificate []byte `json:"ca_certificate"`
	HMAC          []byte `json:"hmac,omitempty"`
}

// createNodeHostnameAndCA creates a nodeHostnameAndCA message for sending to a peer node.
func createNodeHostnameAndCA(hostAddress string, caCert []byte, secretToken []byte) (nodeHostnameAndCA, error) {
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

// nodeTrustBundle is a message struct to deliver the initialization bundle.
type nodeTrustBundle struct {
	// Bundle of all certificates needed to initialize nodes.
	Bundle CertificateBundle `json:"certificate_bundle"`
	// HMAC of the CA key/cert for the internode service inside the Bundle above.
	HMAC   []byte            `json:"hmac,omitempty"`
}

// Sets the HMAC signature field in n based on a signature of all CA
// certificate/keys in the Bundle. Converse of validHMAC.
func (n *nodeTrustBundle) signHMAC(secretToken []byte) {
	h := hmac.New(sha256.New, secretToken)
	h.Write(n.Bundle.InterNode.CACertificate)
	h.Write(n.Bundle.InterNode.CAKey)
	h.Write(n.Bundle.UserAuth.CACertificate)
	h.Write(n.Bundle.UserAuth.CAKey)
	h.Write(n.Bundle.SQLService.CACertificate)
	h.Write(n.Bundle.SQLService.CAKey)
	h.Write(n.Bundle.RPCService.CACertificate)
	h.Write(n.Bundle.RPCService.CAKey)
	h.Write(n.Bundle.AdminUIService.CACertificate)
	h.Write(n.Bundle.AdminUIService.CAKey)
	n.HMAC = h.Sum(nil)
}

// Verifies the HMAC signature field set in n to match that of the bundled
// CA certificates and fields. Converse of signHMAC.
func (n *nodeTrustBundle) validHMAC(secretToken []byte) bool {
	h := hmac.New(sha256.New, secretToken)
	h.Write(n.Bundle.InterNode.CACertificate)
	h.Write(n.Bundle.InterNode.CAKey)
	h.Write(n.Bundle.UserAuth.CACertificate)
	h.Write(n.Bundle.UserAuth.CAKey)
	h.Write(n.Bundle.SQLService.CACertificate)
	h.Write(n.Bundle.SQLService.CAKey)
	h.Write(n.Bundle.RPCService.CACertificate)
	h.Write(n.Bundle.RPCService.CAKey)
	h.Write(n.Bundle.AdminUIService.CACertificate)
	h.Write(n.Bundle.AdminUIService.CAKey)
	expectedMac := h.Sum(nil)
	return hmac.Equal(expectedMac, n.HMAC)
}

// Helper function to extract signatures.
func pemToSignature(caCertPEM []byte) (signature []byte, err error) {
	caCert, _ := pem.Decode(caCertPEM)
	if nil == caCert {
		return nil, errors.New("failed to parse valid PEM from CACertificate blob")
	}

	cert, err := x509.ParseCertificate(caCert.Bytes)
	if nil != err {
		return nil, errors.New("failed to parse valid certificate from CACertificate blob")
	}

	return cert.Signature, nil
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

func sendBadRequestError(ctx context.Context, err error, w http.ResponseWriter) {
	http.Error(w, "invalid request message", http.StatusBadRequest)
	log.Error(ctx, err.Error())
}

func generateURLForClient(peer string, endpoint string) string {
	return fmt.Sprintf("https://%s%s", peer, endpoint)
}

// tlsInitHandshaker takes in a list of peers
type tlsInitHandshaker struct {
	ctx     context.Context
	scratch []byte
	server  *http.Server

	token      []byte
	certsDir   string
	listenHost string
	listenPort int

	tempCerts    ServiceCertificateBundle
	trustedPeers chan nodeHostnameAndCA
	finishedInit chan bool
	errors       chan error
}

func (t *tlsInitHandshaker) init() error {
	serverCert, err := tls.X509KeyPair(t.tempCerts.HostCertificate, t.tempCerts.HostKey)
	if err != nil {
		return err
	}

	certpool := x509.NewCertPool()
	if ok := certpool.AppendCertsFromPEM(t.tempCerts.CACertificate); !ok {
		return errors.New("could not add temp CA certificate to cert pool")
	}
	serviceTLSConf := &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		RootCAs:      certpool,
	}

	mux := http.NewServeMux()
	mux.HandleFunc(trustInitURL, t.onTrustInit)
	mux.HandleFunc(deliverBundleURL, t.onDeliverBundle)

	t.server = &http.Server{
		Addr:      fmt.Sprintf("%s:%d", t.listenHost, t.listenPort),
		Handler:   mux,
		TLSConfig: serviceTLSConf,
	}
	return nil
}

// Handler for initial challenge and ack containing the ephemeral node CAs
func (t *tlsInitHandshaker) onTrustInit(res http.ResponseWriter, req *http.Request) {
	var challenge nodeHostnameAndCA

	// TODO(aaron-crl): [Security] Make this more error resilient to size and shape attacks.
	err := json.NewDecoder(req.Body).Decode(&challenge)
	if err != nil {
		sendBadRequestError(t.ctx, errors.Wrap(err, "error when unmarshalling challenge"), res)
		return
	}
	defer req.Body.Close()

	if !challenge.validHMAC(t.token) {
		sendBadRequestError(t.ctx, errInvalidHMAC, res)
		// Non-blocking channel send.
		select {
		case t.errors <- errInvalidHMAC:
		default:
		}

		return
	}

	t.trustedPeers <- challenge

	// Acknowledge validation to the client.
	hostAddress := fmt.Sprintf("%s:%d", t.listenHost, t.listenPort)
	ack, err := createNodeHostnameAndCA(hostAddress, t.tempCerts.CACertificate, t.token)
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
		sendBadRequestError(t.ctx, errors.Wrap(err, "error when unmarshalling bundle"), res)
		return
	}
	if bundle.validHMAC(t.token) {
		// Successfully provisioned.
		close(t.finishedInit)
	}
	// TODO(bilal): Save bundle to certs directory.
}

func (t *tlsInitHandshaker) startServer(listener net.Listener) {
	go func() {
		// Start the server.
		_ = t.server.ServeTLS(listener, "", "")
	}()
}

func (t *tlsInitHandshaker) stopServer() {
	if t.server == nil {
		return
	}

	_ = t.server.Shutdown(t.ctx)
	t.server = nil
}

func (t *tlsInitHandshaker) getPeerCACert(peerAddress string, selfAddress string) (nodeHostnameAndCA, error) {
	// TODO(aaron-crl): [Enhancement] Add TLS protocol level checks to make sure
	// remote certificate matches preferred one. This is non critical due to HMAC
	// but would be good hygiene.

	// Connect to HTTPS endpoint unverified (effectively HTTP) with POST of challenge
	// HMAC(hostname + node CA public certificate, secretToken).
	clientTransport := http.DefaultTransport.(*http.Transport).Clone()
	// TODO(bilal): Export some of the helpers in pkg/security/tls.go to
	// instantiate TLS config.
	clientTransport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	client := &http.Client{Transport: clientTransport}

	challenge, err := createNodeHostnameAndCA(selfAddress, t.tempCerts.CACertificate, t.token)
	if err != nil {
		return nodeHostnameAndCA{}, err
	}

	var body bytes.Buffer
	_ = json.NewEncoder(&body).Encode(challenge)
	res, err := client.Post(generateURLForClient(peerAddress, trustInitURL), "application/json; charset=utf-8", &body)
	if err != nil {
		return nodeHostnameAndCA{}, err
	}

	// Read and validate server provided ack.
	// HMAC(hostname + server CA public certificate, secretToken)
	var msg nodeHostnameAndCA
	if err != nil {
		return nodeHostnameAndCA{}, err
	}
	if res.StatusCode != 200 {
		return nodeHostnameAndCA{}, errors.Errorf("unexpected error returned from peer: HTTP %d", res.StatusCode)
	}
	if err := json.NewDecoder(res.Body).Decode(&msg); err != nil {
		return nodeHostnameAndCA{}, err
	}
	defer res.Body.Close()

	if !msg.validHMAC(t.token) {
		return nodeHostnameAndCA{}, errInvalidHMAC
	}
	return msg, nil
}

func (t *tlsInitHandshaker) runClient(peerHostname string, selfAddress string) {
	// Sleep for 500ms between attempts.
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-t.finishedInit:
			return
		case <-t.ctx.Done():
			return
		case <-ticker.C:
		}

		peerHostnameAndCa, err := t.getPeerCACert(peerHostname, selfAddress)
		if err != nil {
			log.Errorf(t.ctx, "error when attempting to get peer CA certificate: %s", err)
			// Non-blocking channel send.
			select {
			case t.errors <- err:
			default:
			}
			if err == errInvalidHMAC {
				return
			}
			continue
		}
		t.trustedPeers <- peerHostnameAndCa
		return
	}
}

func sendSecret(address string, peerCACert []byte, caBundle nodeTrustBundle) (err error) {
	rootCAs, _ := x509.SystemCertPool()
	rootCAs.AppendCertsFromPEM(peerCACert)

	clientTransport := http.DefaultTransport.(*http.Transport).Clone()
	// TODO(bilal): Export some of the helpers in pkg/security/tls.go to
	// instantiate TLS config.
	clientTransport.TLSClientConfig = &tls.Config{RootCAs: rootCAs}
	client := &http.Client{Transport: clientTransport}

	var body bytes.Buffer
	_ = json.NewEncoder(&body).Encode(caBundle)

	_, err = client.Post(generateURLForClient(address, deliverBundleURL), "application/json; charset=utf-8", &body)
	return err
}

// InitHandshake starts off an inter-node TLS handshake protocol, as described
// here: https://github.com/cockroachdb/cockroach/pull/51991 . This method
// negotiates an inter-node CA and puts it in certsDir.
func InitHandshake(
	ctx context.Context,
	cfg *base.Config,
	token string,
	numExpectedPeers int,
	peers []string,
	certsDir string,
	listener net.Listener,
) error {
	ctx, cancel := context.WithTimeout(ctx, defaultInitLifespan)
	defer cancel()

	addr := listener.Addr()
	var listenAddr string
	var listenPort int
	switch netAddr := addr.(type) {
	case *net.TCPAddr:
		listenAddr = netAddr.IP.String()
		listenPort = netAddr.Port
	default:
		return errors.New("unsupported listener protocol: only TCP listeners supported")
	}
	tempCerts, err := createNodeInitTempCertificates(listenAddr, defaultInitLifespan)
	if err != nil {
		return errors.Wrap(err, "failed to create certificates")
	}

	handshaker := &tlsInitHandshaker{
		ctx:          ctx,
		token:        []byte(token),
		certsDir:     certsDir,
		listenHost:   listenAddr,
		listenPort:   listenPort,
		tempCerts:    tempCerts,
		trustedPeers: make(chan nodeHostnameAndCA, numExpectedPeers),
		errors:       make(chan error, numExpectedPeers*2),
		finishedInit: make(chan bool, 1),
	}
	if err := handshaker.init(); err != nil {
		return errors.Wrap(err, "error when initializing tls handshaker")
	}

	handshaker.startServer(listener)
	defer handshaker.stopServer()

	// start peer bind processes
	for _, peerAddress := range peers {
		go handshaker.runClient(peerAddress, fmt.Sprintf("%s:%d", listenAddr, listenPort))
	}

	// wait until we have numNodes - 1 valid peers
	peerCACerts := make(map[string]([]byte))
	for len(peerCACerts) < numExpectedPeers {
		select {
		case p := <-handshaker.trustedPeers:
			peerCACerts[p.HostAddress] = p.CACertificate
		case err := <-handshaker.errors:
			if err == errInvalidHMAC {
				// Either this peer, or another peer, has the wrong token. Fail
				// fast.
				return errors.New("invalid signature in messages from peers; likely due to token mismatch")
			}
		case <-ctx.Done():
			return errors.New("context cancelled before peers connected")
		}
	}

	// Order nodes by certificates.
	trustLeader := true
	selfSignature, err := pemToSignature(tempCerts.CACertificate)
	if err != nil {
		return err
	}

	for _, peerCertPEM := range peerCACerts {
		peerSignature, err := pemToSignature(peerCertPEM)
		if err != nil {
			return err
		}
		if bytes.Compare(selfSignature, peerSignature) > 0 {
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
			return errors.Wrap(err, "error when creating initialization bundle")
		}
		peerInit := b.ToPeerInitBundle()
		trustBundle := nodeTrustBundle{Bundle: peerInit}
		trustBundle.signHMAC(handshaker.token)
		// For each peer, use its CA to establish a secure connection and deliver the trust bundle.
		for p := range peerCACerts {
			if err := sendSecret(p, peerCACerts[p], trustBundle); err != nil {
				return errors.Wrap(err, "error when sending bundle to peers as leader")
			}
		}
		return nil
	}

	select {
	case <-handshaker.finishedInit:
	case <-ctx.Done():
		return errors.New("context cancelled before init bundle received from leader")
	}

	return nil
}
