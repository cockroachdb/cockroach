// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// TODO(aaron-crl): This uses the CertsLocator from the security package
// Getting about half way to integration with the certificate manager
// While I'd originally hoped to decouple it completely, I realized
// it would create an even larger headache if we maintained default
// certificate locations in multiple places.

package server

import (
	"context"
	"io/ioutil"
	"os"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/logtags"
)

// Define default certificate lifespan of 366 days
// TODO(aaron-crl): Put this in the config map.
const initLifespan = time.Minute * 60 * 24 * 366

// CertificateBundle manages the collection of certificates used by a
// CockroachDB node.
type CertificateBundle struct {
	InterNode      ServiceCertificateBundle
	UserAuth       ServiceCertificateBundle
	SQLService     ServiceCertificateBundle
	RPCService     ServiceCertificateBundle
	AdminUIService ServiceCertificateBundle
}

// ServiceCertificateBundle is a container for the CA and host node certs.
type ServiceCertificateBundle struct {
	CACertificate   []byte
	CAKey           []byte
	HostCertificate []byte // This will be blank if unused (in the user case).
	HostKey         []byte // This will be blank if unused (in the user case).
}

// Helper function to load cert and key for a service.
func (sb *ServiceCertificateBundle) loadServiceCertAndKey(
	certPath string, keyPath string,
) (err error) {
	sb.HostCertificate, err = loadCertificateFile(certPath)
	if err != nil {
		return
	}
	sb.HostKey, err = loadKeyFile(keyPath)
	if err != nil {
		return
	}
	return
}

// Helper function to load cert and key for a service CA.
func (sb *ServiceCertificateBundle) loadCACertAndKey(certPath string, keyPath string) (err error) {
	sb.CACertificate, err = loadCertificateFile(certPath)
	if err != nil {
		return
	}
	sb.CAKey, err = loadKeyFile(keyPath)
	if err != nil {
		return
	}
	return
}

// LoadUserAuthCACertAndKey loads host certificate and key from disk or fails with error.
func (sb *ServiceCertificateBundle) loadOrCreateUserAuthCACertAndKey(
	ctx context.Context,
	caCertPath string,
	caKeyPath string,
	initLifespan time.Duration,
	serviceName string,
) (err error) {
	log.Ops.Infof(ctx, "attempting to load CA cert: %s", caCertPath)
	// Attempt to load cert into ServiceCertificateBundle.
	sb.CACertificate, err = loadCertificateFile(caCertPath)
	if err != nil {
		if oserror.IsNotExist(err) {
			log.Ops.Infof(ctx, "not found; auto-generating")
			// Certificate not found, attempt to create both cert and key now.
			err = sb.createServiceCA(ctx, caCertPath, caKeyPath, initLifespan, serviceName)
			if err != nil {
				return err
			}

			// Both key and cert should now be populated.
			return nil
		}

		// Some error unrelated to file existence occurred.
		return err
	}

	log.Ops.Infof(ctx, "found; loading CA key: %s", caKeyPath)
	// Load the key only if it exists.
	sb.CAKey, err = loadKeyFile(caKeyPath)
	if !oserror.IsNotExist(err) {
		// An error returned but it was not that the file didn't exist;
		// this is an error.
		return err
	} else {
		log.Ops.Infof(ctx, "CA key not found")
	}

	return nil
}

// loadOrCreateServiceCertificates will attempt to load the service cert/key
// into the service bundle.
// * If they do not exist:
//   It will attempt to load the service CA cert/key pair.
//   * If they do not exist:
//     It will generate the service CA cert/key pair.
//     It will persist these to disk and store them
//       in the ServiceCertificateBundle.
//   It will generate the service cert/key pair.
//   It will persist these to disk and store them
//     in the ServiceCertificateBundle.
func (sb *ServiceCertificateBundle) loadOrCreateServiceCertificates(
	ctx context.Context,
	serviceCertPath string,
	serviceKeyPath string,
	caCertPath string,
	caKeyPath string,
	initLifespan time.Duration,
	commonName string,
	serviceName string,
	hostname string,
) error {
	ctx = logtags.AddTag(ctx, "service", serviceName)

	var err error
	log.Ops.Infof(ctx, "attempting to load service cert: %s", serviceCertPath)
	// Check if the service cert and key already exist, if it does return early.
	sb.HostCertificate, err = loadCertificateFile(serviceCertPath)
	if err == nil {
		log.Ops.Infof(ctx, "found; loading service key: %s", serviceKeyPath)
		// Cert file exists, now load key.
		sb.HostKey, err = loadKeyFile(serviceKeyPath)
		if err != nil {
			// Check if we failed to load the key?
			if oserror.IsNotExist(err) {
				// Cert exists but key doesn't, this is an error.
				return errors.Wrapf(err,
					"failed to load service certificate key for %q expected key at %q",
					serviceCertPath, serviceKeyPath)
			}
			return errors.Wrap(err, "something went wrong loading service key")
		}
		// Both certificate and key should be successfully loaded.
		log.Ops.Infof(ctx, "service cert is ready")
		return nil
	}
	// TODO(aaron-crl, knz): err != nil is not handled here.

	log.Ops.Infof(ctx, "not found; will attempt auto-creation")

	log.Ops.Infof(ctx, "attempting to load CA cert: %s", caCertPath)
	// Neither service cert or key exist, attempt to load CA.
	sb.CACertificate, err = loadCertificateFile(caCertPath)
	if err == nil {
		// CA cert has been successfully loaded, attempt to load
		// CA key.
		log.Ops.Infof(ctx, "found; loading CA key: %s", caKeyPath)
		sb.CAKey, err = loadKeyFile(caKeyPath)
		if err != nil {
			return errors.Wrapf(
				err, "loaded service CA cert but failed to load service CA key file: %q", caKeyPath,
			)
		}
	} else if oserror.IsNotExist(err) {
		log.Ops.Infof(ctx, "not found; CA cert does not exist, auto-creating")
		// CA cert does not yet exist, create it and its key.
		if err := sb.createServiceCA(ctx, caCertPath, caKeyPath, initLifespan, serviceName); err != nil {
			return errors.Wrap(
				err, "failed to create Service CA",
			)
		}
	}
	// TODO(aaron-crl, knz): missing `else` case here.

	// CA cert and key should now be loaded, create service cert and key.
	var hostCert, hostKey []byte
	hostCert, hostKey, err = security.CreateServiceCertAndKey(
		ctx,
		log.Ops.Infof,
		initLifespan,
		commonName,
		serviceName,
		hostname,
		sb.CACertificate,
		sb.CAKey,
	)
	if err != nil {
		return errors.Wrap(
			err, "failed to create Service Cert and Key",
		)
	}

	log.Ops.Infof(ctx, "writing service cert: %s", serviceCertPath)
	if err := writeCertificateFile(serviceCertPath, hostCert); err != nil {
		return err
	}

	log.Ops.Infof(ctx, "writing service key: %s", serviceKeyPath)
	if err := writeKeyFile(serviceKeyPath, hostKey); err != nil {
		return err
	}

	return nil
}

// createServiceCA builds CA cert and key and populates them to
// ServiceCertificateBundle.
func (sb *ServiceCertificateBundle) createServiceCA(
	ctx context.Context,
	caCertPath string,
	caKeyPath string,
	initLifespan time.Duration,
	serviceName string,
) (err error) {
	ctx = logtags.AddTag(ctx, "auto-create-ca", nil)

	sb.CACertificate, sb.CAKey, err = security.CreateCACertAndKey(ctx, log.Ops.Infof, initLifespan, serviceName)
	if err != nil {
		return
	}

	log.Ops.Infof(ctx, "writing CA cert: %s", caCertPath)
	err = writeCertificateFile(caCertPath, sb.CACertificate)
	if err != nil {
		return
	}

	log.Ops.Infof(ctx, "writing CA key: %s", caKeyPath)
	err = writeKeyFile(caKeyPath, sb.CAKey)
	if err != nil {
		return
	}

	return
}

// Simple wrapper to make it easier to store certs somewhere else later.
// TODO (aaron-crl): Put validation checks here.
func loadCertificateFile(certPath string) (cert []byte, err error) {
	cert, err = ioutil.ReadFile(certPath)
	return
}

// Simple wrapper to make it easier to store certs somewhere else later.
// TODO (aaron-crl): Put validation checks here.
func loadKeyFile(keyPath string) (key []byte, err error) {
	key, err = ioutil.ReadFile(keyPath)
	return
}

// Simple wrapper to make it easier to store certs somewhere else later.
// This function will error if a file already exists at certPath.
func writeCertificateFile(certPath string, certPEM []byte) error {
	if _, err := os.Stat(certPath); err == nil {
		return errors.Newf("found existing certfile at: %q", certPath)
	} else if !oserror.IsNotExist(err) {
		return errors.Wrapf(err,
			"problem writing keyfile at: %q", certPath)
	}
	// TODO(aaron-crl): Add logging here.
	return ioutil.WriteFile(certPath, certPEM, 0600)
}

// Simple wrapper to make it easier to store certs somewhere else later.
// This function will error if a file alread exists at keyPath.
func writeKeyFile(keyPath string, keyPEM []byte) error {
	if _, err := os.Stat(keyPath); err == nil {
		return errors.Newf("found existing keyfile at: %q", keyPath)
	} else if !oserror.IsNotExist(err) {
		return errors.Wrapf(err,
			"problem writing keyfile at: %q", keyPath)
	}
	// TODO(aaron-crl): Add logging here.
	return ioutil.WriteFile(keyPath, keyPEM, 0600)
}

// InitializeFromConfig is called by the node creating certificates for the
// cluster. It uses or generates an InterNode CA to produce any missing
// unmanaged certificates. It does this base on the logic in:
// https://github.com/cockroachdb/cockroach/pull/51991
// N.B.: This function fast fails if an inter-node cert/key pair are present
// as this should _never_ happen.
func (b *CertificateBundle) InitializeFromConfig(ctx context.Context, c base.Config) error {
	cl := security.MakeCertsLocator(c.SSLCertsDir)

	// First check to see if host cert is already present
	// if it is, we should fail to initialize.
	if exists, err := cl.HasNodeCert(); err != nil {
		return err
	} else if exists {
		return errors.New("inter-node certificate already present")
	}

	rpcHostName, _, err := netutil.SplitHostPort(c.AdvertiseAddr, "0")
	if err != nil {
		return err
	}
	sqlHostName := rpcHostName
	if c.SplitListenSQL {
		sqlHostName, _, err = netutil.SplitHostPort(c.SQLAdvertiseAddr, "0")
		if err != nil {
			return err
		}
	}
	httpHostName, _, err := netutil.SplitHostPort(c.HTTPAdvertiseAddr, "0")
	if err != nil {
		return err
	}

	// Create the target directory if it does not exist yet.
	if err := cl.EnsureCertsDirectory(); err != nil {
		return err
	}

	// Start by loading or creating the InterNode certificates.
	if err := b.InterNode.loadOrCreateServiceCertificates(
		ctx,
		cl.NodeCertPath(),
		cl.NodeKeyPath(),
		cl.CACertPath(),
		cl.CAKeyPath(),
		initLifespan,
		security.NodeUser,
		"cockroach-node",
		rpcHostName,
	); err != nil {
		return errors.Wrap(err,
			"failed to load or create InterNode certificates")
	}

	// Initialize User auth certificates.
	// TODO(aaron-crl): Double check that we want to do this. It seems
	// like this is covered by the interface certificates?
	if err := b.UserAuth.loadOrCreateUserAuthCACertAndKey(
		ctx,
		cl.ClientCACertPath(),
		cl.ClientCAKeyPath(),
		initLifespan,
		"cockroach-client",
	); err != nil {
		return errors.Wrap(err,
			"failed to load or create User auth certificate(s)")
	}

	// Initialize SQLService Certs.
	if err := b.SQLService.loadOrCreateServiceCertificates(
		ctx,
		cl.SQLServiceCertPath(),
		cl.SQLServiceKeyPath(),
		cl.SQLServiceCACertPath(),
		cl.SQLServiceCAKeyPath(),
		initLifespan,
		security.NodeUser,
		"cockroach-sql",
		sqlHostName,
	); err != nil {
		return errors.Wrap(err,
			"failed to load or create SQL service certificate(s)")
	}

	// Initialize RPCService Certs.
	if err := b.RPCService.loadOrCreateServiceCertificates(
		ctx,
		cl.RPCServiceCertPath(),
		cl.RPCServiceKeyPath(),
		cl.RPCServiceCACertPath(),
		cl.RPCServiceCAKeyPath(),
		initLifespan,
		security.NodeUser,
		"cockroach-rpc",
		rpcHostName, // TODO(aaron-crl): Add RPC variable to config.
	); err != nil {
		return errors.Wrap(err,
			"failed to load or create RPC service certificate(s)")
	}

	// Initialize AdminUIService Certs.
	if err := b.AdminUIService.loadOrCreateServiceCertificates(
		ctx,
		cl.UICertPath(),
		cl.UIKeyPath(),
		cl.UICACertPath(),
		cl.UICAKeyPath(),
		initLifespan,
		httpHostName,
		"cockroach-http",
		httpHostName,
	); err != nil {
		return errors.Wrap(err,
			"failed to load or create Admin UI service certificate(s)")
	}

	return nil
}

// InitializeNodeFromBundle uses the contents of the CertificateBundle and
// details from the config object to write certs to disk and generate any
// missing host-specific certificates and keys
// It is assumed that a node receiving this has not has TLS initialized. If
// an inter-node certificate is found, this function will error.
func (b *CertificateBundle) InitializeNodeFromBundle(ctx context.Context, c base.Config) error {
	cl := security.MakeCertsLocator(c.SSLCertsDir)

	// First check to see if host cert is already present
	// if it is, we should fail to initialize.
	if exists, err := cl.HasNodeCert(); err != nil {
		return err
	} else if exists {
		return errors.New("inter-node certificate already present")
	}

	if err := cl.EnsureCertsDirectory(); err != nil {
		return err
	}

	// Write received CA's to disk. If any of them already exist, fail
	// and return an error.

	// Attempt to write InterNodeHostCA to disk first.
	if err := b.InterNode.writeCAOrFail(cl.CACertPath(), cl.CAKeyPath()); err != nil {
		return errors.Wrap(err, "failed to write InterNodeCA to disk")
	}

	// Attempt to write ClientCA to disk.
	if err := b.InterNode.writeCAOrFail(cl.ClientCACertPath(), cl.ClientCAKeyPath()); err != nil {
		return errors.Wrap(err, "failed to write ClientCA to disk")
	}

	// Attempt to write SQLServiceCA to disk.
	if err := b.InterNode.writeCAOrFail(cl.SQLServiceCACertPath(), cl.SQLServiceCAKeyPath()); err != nil {
		return errors.Wrap(err, "failed to write SQLServiceCA to disk")
	}

	// Attempt to write RPCServiceCA to disk.
	if err := b.InterNode.writeCAOrFail(cl.RPCServiceCACertPath(), cl.RPCServiceCAKeyPath()); err != nil {
		return errors.Wrap(err, "failed to write RPCServiceCA to disk")
	}

	// Attempt to write AdminUIServiceCA to disk.
	if err := b.InterNode.writeCAOrFail(cl.UICACertPath(), cl.UICAKeyPath()); err != nil {
		return errors.Wrap(err, "failed to write AdminUIServiceCA to disk")
	}

	// Once CAs are written call the same InitFromConfig function to create
	// host certificates.
	if err := b.InitializeFromConfig(ctx, c); err != nil {
		return errors.Wrap(
			err,
			"failed to initialize host certs after writing CAs to disk")
	}

	return nil
}

// writeCAOrFail will attempt to write a service certificate bundle to the
// specified paths on disk. It will ignore any missing certificate fields but
// error if it fails to write a file to disk.
func (sb *ServiceCertificateBundle) writeCAOrFail(certPath string, keyPath string) (err error) {
	if sb.CACertificate != nil {
		err = writeCertificateFile(certPath, sb.CACertificate)
		if err != nil {
			return
		}
	}

	if sb.CAKey != nil {
		err = writeKeyFile(keyPath, sb.CAKey)
		if err != nil {
			return
		}
	}

	return
}

// copyOnlyCAs is a helper function to only populate the CA portion of
// a ServiceCertificateBundle
func (sb *ServiceCertificateBundle) copyOnlyCAs(destBundle *ServiceCertificateBundle) {
	destBundle.CACertificate = sb.CACertificate
	destBundle.CAKey = sb.CAKey
}

// ToPeerInitBundle populates a bundle of initialization certificate CAs (only).
// This function is expected to serve any node providing a init bundle to a
// joining or starting peer.
func (b *CertificateBundle) ToPeerInitBundle() (pb CertificateBundle) {
	b.InterNode.copyOnlyCAs(&pb.InterNode)
	b.UserAuth.copyOnlyCAs(&pb.UserAuth)
	b.SQLService.copyOnlyCAs(&pb.SQLService)
	b.RPCService.copyOnlyCAs(&pb.RPCService)
	b.AdminUIService.copyOnlyCAs(&pb.AdminUIService)
	return
}
