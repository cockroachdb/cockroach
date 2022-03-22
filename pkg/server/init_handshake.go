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
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

const (
	initServiceName     = "temp-init-service"
	caCommonName        = "Cockroach CA"
	defaultInitLifespan = 10 * time.Minute

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
func createNodeHostnameAndCA(
	hostAddress string, caCert []byte, secretToken []byte,
) (nodeHostnameAndCA, error) {
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
	HMAC []byte `json:"hmac,omitempty"`
}

// Calculate and return the HMAC of CA certs and keys in the bundle. Does not
// compare against the HMAC field.
func (n *nodeTrustBundle) computeHMAC(secretToken []byte) []byte {
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
	return h.Sum(nil)
}

// Sets the HMAC signature field in n based on a signature of all CA
// certificate/keys in the Bundle. Converse of validHMAC.
func (n *nodeTrustBundle) signHMAC(secretToken []byte) {
	n.HMAC = n.computeHMAC(secretToken)
}

// Verifies the HMAC signature field set in n to match that of the bundled
// CA certificates and fields. Converse of signHMAC.
func (n *nodeTrustBundle) validHMAC(secretToken []byte) bool {
	expectedMac := n.computeHMAC(secretToken)
	return hmac.Equal(expectedMac, n.HMAC)
}

// Helper function to extract signatures.
func pemToSignature(caCertPEM []byte) ([]byte, error) {
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

func createNodeInitTempCertificates(
	ctx context.Context, hostnames []string, lifespan time.Duration,
) (certs ServiceCertificateBundle, err error) {
	log.Ops.Infof(ctx, "creating temporary initial certificates for hosts %+v, duration %s", hostnames, lifespan)

	caCtx := logtags.AddTag(ctx, "create-temp-ca", nil)
	caCertPEM, caKeyPEM, err := security.CreateCACertAndKey(caCtx, log.Ops.Infof, lifespan, initServiceName)
	if err != nil {
		return certs, err
	}
	serviceCtx := logtags.AddTag(ctx, "create-temp-service", nil)
	serviceCertPEM, serviceKeyPEM, err := security.CreateServiceCertAndKey(
		serviceCtx,
		log.Ops.Infof,
		lifespan,
		security.NodeUser,
		hostnames,
		caCertPEM,
		caKeyPEM,
		false, /* serviceCertIsAlsoValidAsClient */
	)
	if err != nil {
		return certs, err
	}
	caCert := pem.EncodeToMemory(caCertPEM)
	caKey := pem.EncodeToMemory(caKeyPEM)
	serviceCert := pem.EncodeToMemory(serviceCertPEM)
	serviceKey := pem.EncodeToMemory(serviceKeyPEM)

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
	log.Ops.Warningf(ctx, "bad request: %s", err)
}

func generateURLForClient(peer string, endpoint string) string {
	return fmt.Sprintf("https://%s%s", peer, endpoint)
}

// tlsInitHandshaker takes in a list of peers
type tlsInitHandshaker struct {
	server *http.Server

	token      []byte
	certsDir   string
	listenAddr string

	tempCerts    ServiceCertificateBundle
	trustedPeers chan nodeHostnameAndCA
	finishedInit chan *CertificateBundle
	errors       chan error
	wg           sync.WaitGroup
}

func (t *tlsInitHandshaker) init(ctx context.Context) error {
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
	mux.HandleFunc(trustInitURL, enhanceHandlerContextWithHTTPClient(ctx, t.onTrustInit))
	mux.HandleFunc(deliverBundleURL, enhanceHandlerContextWithHTTPClient(ctx, t.onDeliverBundle))

	t.server = &http.Server{
		Addr:      t.listenAddr,
		Handler:   mux,
		TLSConfig: serviceTLSConf,
	}
	return nil
}

func enhanceHandlerContextWithHTTPClient(
	baseCtx context.Context, fn func(ctx context.Context, w http.ResponseWriter, req *http.Request),
) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		ctx := logtags.AddTag(baseCtx, "peer", req.RemoteAddr)
		fn(ctx, w, req)
	}
}

// Handler for initial challenge and ack containing the ephemeral node CAs.
func (t *tlsInitHandshaker) onTrustInit(
	ctx context.Context, res http.ResponseWriter, req *http.Request,
) {
	var challenge nodeHostnameAndCA

	// TODO(aaron-crl): [Security] Make this more error resilient to size and shape attacks.
	err := json.NewDecoder(req.Body).Decode(&challenge)
	if err != nil {
		sendBadRequestError(ctx, errors.Wrap(err, "error when unmarshalling challenge"), res)
		return
	}
	defer req.Body.Close()

	if !challenge.validHMAC(t.token) {
		sendBadRequestError(ctx, errInvalidHMAC, res)
		// Non-blocking channel send.
		select {
		case t.errors <- errInvalidHMAC:
		default:
		}

		return
	}

	log.Ops.Infof(ctx, "received valid challenge and CA from: %s", challenge.HostAddress)

	t.trustedPeers <- challenge

	// Acknowledge validation to the client.
	ack, err := createNodeHostnameAndCA(t.listenAddr, t.tempCerts.CACertificate, t.token)
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
func (t *tlsInitHandshaker) onDeliverBundle(
	ctx context.Context, res http.ResponseWriter, req *http.Request,
) {
	bundle := nodeTrustBundle{}
	err := json.NewDecoder(req.Body).Decode(&bundle)
	defer req.Body.Close()
	if err != nil {
		sendBadRequestError(ctx, errors.Wrap(err, "error when unmarshalling bundle"), res)
		return
	}
	if !bundle.validHMAC(t.token) {
		sendBadRequestError(ctx, errors.New("invalid bundle HMAC"), res)
		return
	}

	log.Ops.Infof(ctx, "received valid cert bundle from trust leader")
	// Successfully provisioned.
	select {
	case t.finishedInit <- &bundle.Bundle:
		// Done.
	case <-ctx.Done():
		log.Ops.Warningf(ctx, "context canceled while receiving bundle")
	}
}

func (t *tlsInitHandshaker) startServer(listener net.Listener) error {
	return t.server.ServeTLS(listener, "", "")
}

func (t *tlsInitHandshaker) stopServer() {
	// We send an unlinked context to the shutdown command. This is to ensure
	// that the server cleans up all goroutines before exiting. If t.ctx is
	// already canceled, we could leak goroutines as Shutdown won't wait for
	// all goroutines to close.
	_ = t.server.Shutdown(context.Background())
}

func (t *tlsInitHandshaker) getClientForTransport(transport *http.Transport) *http.Client {
	return &http.Client{
		Timeout:   1 * time.Second,
		Transport: transport,
	}
}

func (t *tlsInitHandshaker) getInsecureClient() *http.Client {
	// Connect to HTTPS endpoint unverified (effectively HTTP) with POST of challenge
	// HMAC(hostname + node CA public certificate, secretToken).
	clientTransport := http.DefaultTransport.(*http.Transport).Clone()
	clientTransport.TLSClientConfig = &tls.Config{
		InsecureSkipVerify: true,
	}
	return t.getClientForTransport(clientTransport)
}

func (t *tlsInitHandshaker) getClient(rootCAs *x509.CertPool) *http.Client {
	// TODO(aaron-crl): [Enhancement] Add TLS protocol level checks to make sure
	// remote certificate matches preferred one. This is non critical due to HMAC
	// but would be good hygiene.
	//
	// TODO(bilal): Export some of the helpers in pkg/security/tls.go to
	// instantiate TLS config.
	clientTransport := http.DefaultTransport.(*http.Transport).Clone()
	clientTransport.TLSClientConfig = &tls.Config{
		RootCAs: rootCAs,
	}
	return t.getClientForTransport(clientTransport)
}

func (t *tlsInitHandshaker) getPeerCACert(
	client *http.Client, peerAddress string, selfAddress string,
) (nodeHostnameAndCA, error) {
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
	defer res.Body.Close()

	if res.StatusCode != 200 {
		return nodeHostnameAndCA{}, errors.Errorf("unexpected error returned from peer: HTTP %d", res.StatusCode)
	}

	// Read and validate server provided ack.
	// HMAC(hostname + server CA public certificate, secretToken)
	var msg nodeHostnameAndCA
	if err := json.NewDecoder(res.Body).Decode(&msg); err != nil {
		return nodeHostnameAndCA{}, err
	}
	defer res.Body.Close()

	if !msg.validHMAC(t.token) {
		return nodeHostnameAndCA{}, errInvalidHMAC
	}
	return msg, nil
}

func (t *tlsInitHandshaker) runClient(
	ctx context.Context, peerHostname string, selfAddress string,
) {
	// Sleep for 500ms between attempts.
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	client := t.getInsecureClient()
	defer client.CloseIdleConnections()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		peerHostnameAndCa, err := t.getPeerCACert(client, peerHostname, selfAddress)
		if err != nil {
			log.Ops.Warningf(ctx, "peer CA retrieval error: %v", err)
			// Non-blocking channel send.
			select {
			case t.errors <- err:
			default:
			}
			if errors.Is(err, errInvalidHMAC) {
				return
			}
			continue
		}
		select {
		case t.trustedPeers <- peerHostnameAndCa:
		case <-ctx.Done():
		}
		return
	}
}

func (t *tlsInitHandshaker) sendBundle(
	ctx context.Context, address string, peerCACert []byte, caBundle nodeTrustBundle,
) (err error) {
	rootCAs, _ := x509.SystemCertPool()
	rootCAs.AppendCertsFromPEM(peerCACert)

	client := t.getClient(rootCAs)
	defer client.CloseIdleConnections()

	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(caBundle); err != nil {
		return err
	}

	every := log.Every(time.Second)

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	var lastError error
	for {
		select {
		case <-ctx.Done():
			if lastError != nil {
				return lastError
			}
			return errors.Errorf("context canceled before init bundle sent to %s", address)
		case <-ticker.C:
		}

		res, err := client.Post(generateURLForClient(address, deliverBundleURL), "application/json; charset=utf-8", bytes.NewReader(body.Bytes()))
		if err == nil {
			res.Body.Close()
			break
		}
		lastError = err
		if every.ShouldLog() {
			log.Ops.Warningf(ctx, "cannot send bundle: %v", err)
		}
	}

	return nil
}

func initHandshakeHelper(
	ctx context.Context,
	reporter func(string, ...interface{}),
	cfg *base.Config,
	token string,
	numExpectedNodes int,
	peers []string,
	certsDir string,
	listener net.Listener,
) error {
	if len(token) == 0 {
		return errors.AssertionFailedf("programming error: token cannot be empty")
	}
	if numExpectedNodes <= 0 {
		return errors.AssertionFailedf("programming error: must expect more than 1 node")
	}
	numExpectedPeers := numExpectedNodes - 1

	addr := listener.Addr()
	var listenHost string
	switch netAddr := addr.(type) {
	case *net.TCPAddr:
		listenHost = netAddr.IP.String()
	default:
		return errors.New("unsupported listener protocol: only TCP listeners supported")
	}
	tempCerts, err := createNodeInitTempCertificates(ctx, []string{listenHost}, defaultInitLifespan)
	if err != nil {
		return errors.Wrap(err, "failed to create certificates")
	}

	log.Infof(ctx, "initializing temporary TLS handshake server, listen addr: %s", addr)
	handshaker := &tlsInitHandshaker{
		token:        []byte(token),
		certsDir:     certsDir,
		listenAddr:   addr.String(),
		tempCerts:    tempCerts,
		trustedPeers: make(chan nodeHostnameAndCA, numExpectedPeers),
		errors:       make(chan error, numExpectedPeers*2),
		finishedInit: make(chan *CertificateBundle, 1),
	}
	if err := handshaker.init(ctx); err != nil {
		return errors.Wrap(err, "error when initializing tls handshaker")
	}

	// Wait for the server and all the clients to terminate before returning.
	defer handshaker.wg.Wait()

	peerCACerts := make(map[string]([]byte))

	if numExpectedPeers > 0 {
		handshaker.wg.Add(1)
		go func() {
			defer handshaker.wg.Done()

			log.Ops.Infof(ctx, "starting handshake server")
			defer log.Ops.Infof(ctx, "handshake server stopped")
			if err := handshaker.startServer(listener); !errors.Is(err, http.ErrServerClosed) {
				log.Ops.Errorf(ctx, "handshake server failed: %v", err)
			}
		}()
		// Terminate the server before exiting.
		defer handshaker.stopServer()

		for _, peerAddress := range peers {
			handshaker.wg.Add(1)
			go func(peerAddress string) {
				defer handshaker.wg.Done()

				peerCtx := logtags.AddTag(ctx, "peer", log.SafeOperational(peerAddress))
				log.Ops.Infof(peerCtx, "starting handshake client for peer")
				handshaker.runClient(peerCtx, peerAddress, addr.String())
			}(peerAddress)
		}

		if reporter != nil {
			reporter("waiting for handshake for %d peers", numExpectedPeers)
		}

		// Wait until we have numExpectedPeers peer certificates.
		for len(peerCACerts) < numExpectedPeers {
			select {
			case p := <-handshaker.trustedPeers:
				log.Ops.Infof(ctx, "received CA certificate for peer: %s", p.HostAddress)
				if reporter != nil {
					reporter("trusted peer: %s", p.HostAddress)
				}
				peerCACerts[p.HostAddress] = p.CACertificate

			case err := <-handshaker.errors:
				if errors.Is(err, errInvalidHMAC) {
					// Either this peer, or another peer, has the wrong token. Fail
					// fast.
					log.Ops.Errorf(ctx, "HMAC error from client when connecting to peer: %v", err)
					return errors.New("invalid signature in messages from peers; likely due to token mismatch")
				}
				log.Ops.Warningf(ctx, "error from client when connecting to peers (retrying): %s", err)

			case <-ctx.Done():
				return errors.New("context canceled before all peers connected")
			}
		}
		log.Ops.Infof(ctx, "received response from all peers; choosing trust leader")
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
		if reporter != nil {
			reporter("generating cert bundle for cluster")
		}
		log.Ops.Infof(ctx, "we are trust leader; initializing certificate bundle")
		leaderCtx := logtags.AddTag(ctx, "trust-leader", nil)

		var b CertificateBundle
		// TODO(bilal): See if we can get rid of the need to store a base.Config
		// pointer. This is the only place in this method where it is necessary.
		if err := b.InitializeFromConfig(leaderCtx, *cfg); err != nil {
			return errors.Wrap(err, "error when creating initialization bundle")
		}

		if numExpectedPeers > 0 {
			peerInit, err := collectLocalCABundle(cfg.SSLCertsDir)
			if err != nil {
				return errors.Wrap(err, "error when loading initialization bundle")
			}

			trustBundle := nodeTrustBundle{Bundle: peerInit}
			trustBundle.signHMAC(handshaker.token)

			if reporter != nil {
				reporter("sending cert bundle to peers")
			}

			// For each peer, use its CA to establish a secure connection and deliver the trust bundle.
			for p := range peerCACerts {
				peerCtx := logtags.AddTag(leaderCtx, "peer", p)
				log.Ops.Infof(peerCtx, "delivering bundle to peer")
				if err := handshaker.sendBundle(peerCtx, p, peerCACerts[p], trustBundle); err != nil {
					// TODO(bilal): sendBundle should fail fast instead of retrying (or
					// waiting for ctx cancellation) if the error returned is due to a
					// mismatching CA cert than peerCACerts[p]. This would likely mean
					// a man-in-the-middle attack, or a node restart / replacement since
					// the start of this handshake.
					return errors.Wrap(err, "error when sending bundle to peers as leader")
				}
			}
		}
		return nil
	}

	if reporter != nil {
		reporter("waiting for cert bundle")
	}
	log.Ops.Infof(ctx, "we are not trust leader; now waiting for bundle from trust leader")

	select {
	case b := <-handshaker.finishedInit:
		if reporter != nil {
			reporter("received cert bundle")
		}

		if b == nil {
			return errors.New("expected non-nil init bundle to be received from trust leader")
		}
		log.Ops.Infof(ctx, "received bundle, now initializing node certificate files")
		return b.InitializeNodeFromBundle(ctx, *cfg)

	case <-ctx.Done():
		return errors.New("context canceled before init bundle received from leader")
	}
}

// InitHandshake starts off an inter-node TLS handshake protocol, as described
// here: https://github.com/cockroachdb/cockroach/pull/51991 . This method
// negotiates an inter-node CA and puts it in certsDir.
func InitHandshake(
	ctx context.Context,
	reporter func(string, ...interface{}),
	cfg *base.Config,
	token string,
	numExpectedNodes int,
	peers []string,
	certsDir string,
	listener net.Listener,
) error {
	// TODO(bilal): Allow defaultInitLifespan to be configurable, possibly through
	// base.Config.
	return contextutil.RunWithTimeout(ctx, "init handshake", defaultInitLifespan, func(ctx context.Context) error {
		ctx = logtags.AddTag(ctx, "init-tls-handshake", nil)
		return initHandshakeHelper(ctx, reporter, cfg, token, numExpectedNodes, peers, certsDir, listener)
	})
}
