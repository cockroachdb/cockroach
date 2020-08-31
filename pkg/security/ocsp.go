// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package security

import (
	"bytes"
	"context"
	"crypto"
	"crypto/x509"
	"io/ioutil"
	"net/http"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"golang.org/x/crypto/ocsp"
	"golang.org/x/sync/errgroup"
)

// makeOCSPVerifier returns a function intended for use with
// tls.Config.VerifyPeerCertificate. If enabled, any certificate
// containing an OCSP url will be verified.
//
// TODO(bdarnell): Use VerifyConnection instead of VerifyPeerCertificate (in Go 1.15)
// This is necessary to support OCSP stapling.
func makeOCSPVerifier(settings TLSSettings) func([][]byte, [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
		if !settings.ocspEnabled() {
			return nil
		}

		return contextutil.RunWithTimeout(context.Background(), "OCSP verification", settings.ocspTimeout(),
			func(ctx context.Context) error {
				// Per-conn telemetry counter.
				telemetry.Inc(ocspChecksCounter)

				errG, gCtx := errgroup.WithContext(ctx)
				for _, chain := range verifiedChains {
					// Ignore the last cert in the chain; it's the root and if it
					// has an issuer we don't have it so we can't do an OCSP check
					// on it.
					for i := 0; i < len(chain)-1; i++ {
						cert := chain[i]
						if len(cert.OCSPServer) > 0 {
							issuer := chain[i+1]
							errG.Go(func() error {
								return verifyOCSP(gCtx, settings, cert, issuer)
							})
						}
					}
				}

				return errG.Wait()
			})
	}
}

// ocspChecksCounter counts the number of connections that are
// undergoing OCSP validations. This counter exists so that the value
// of ocspCheckWithOCSPServerInCertCounter can be interpreted as a
// percentage.
var ocspChecksCounter = telemetry.GetCounterOnce("server.ocsp.conn-verifications")

// ocspCheckWithOCSPServerInCert counts the number of certificate
// verifications performed with a populated OCSPServer field in one of
// the certs in the validation chain.
var ocspCheckWithOCSPServerInCertCounter = telemetry.GetCounterOnce("server.ocsp.cert-verifications")

func verifyOCSP(ctx context.Context, settings TLSSettings, cert, issuer *x509.Certificate) error {
	if len(cert.OCSPServer) == 0 {
		return nil
	}

	// Per-cert telemetry counter. We only count requests when there is
	// an OCSP server to check in the first place.
	telemetry.Inc(ocspCheckWithOCSPServerInCertCounter)

	var errs []error
	for _, url := range cert.OCSPServer {
		ok, err := queryOCSP(ctx, url, cert, issuer)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if !ok {
			return errors.Newf("OCSP server says cert is revoked: %v", cert)
		}
		return nil
	}

	if settings.ocspStrict() {
		switch len(errs) {
		case 0:
			panic("can't happen: OCSP failed but errs is empty")
		case 1:
			return errors.Wrap(errs[0], "OCSP check failed in strict mode")
		default:
			// TODO(bdarnell): If there were more than two servers, this
			// drops subsequent messages on the floor.
			return errors.Wrap(errors.CombineErrors(errs[0], errs[1]), "OCSP check failed in strict mode")
		}
	}
	// Non-strict mode: log errors and continue.
	log.Warningf(ctx, "OCSP check failed in non-strict mode: %v", errs)
	return nil
}

// queryOCSP sends an OCSP request for the given cert to a server. If
// the server returns a valid OCSP response with status "good",
// returns (true, nil). If it returns a valid response with status
// "revoked", returns (false, nil). All other outcomes return a
// non-nil error.
func queryOCSP(ctx context.Context, url string, cert, issuer *x509.Certificate) (bool, error) {
	ocspReq, err := ocsp.CreateRequest(cert, issuer, &ocsp.RequestOptions{
		// OCSP defaults to SHA1, so this option might be incompatible
		// with older OCSP servers. But it seems unlikely that anyone who
		// cares enough about security to opt into OCSP would still be
		// using one.
		Hash: crypto.SHA256,
	})
	if err != nil {
		return false, err
	}
	// TODO(bdarnell): If len(ocspReq) < 255, the RFC says it MAY be
	// sent as a GET instead of a POST, which permits caching in the
	// HTTP layer.
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(ocspReq))
	if err != nil {
		return false, err
	}
	httpReq.Header.Add("Content-Type", "application/ocsp-request")
	httpResp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return false, err
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusOK {
		return false, errors.Newf("OCSP server returned status code %v", errors.Safe(httpResp.StatusCode))
	}
	if ct := httpResp.Header.Get("Content-Type"); ct != "application/ocsp-response" {
		return false, errors.Newf("OCSP server returned unexpected content-type %q", errors.Safe(ct))
	}

	httpBody, err := ioutil.ReadAll(httpResp.Body)
	if err != nil {
		return false, err
	}

	ocspResp, err := ocsp.ParseResponseForCert(httpBody, cert, issuer)
	if err != nil {
		return false, err
	}
	if ocspResp == nil {
		return false, errors.Newf("OCSP response for cert %v not found", cert)
	}
	switch ocspResp.Status {
	case ocsp.Good:
		return true, nil
	case ocsp.Revoked:
		return false, nil
	default:
		return false, errors.Newf("OCSP returned status %v", errors.Safe(ocspResp.Status))
	}
}
