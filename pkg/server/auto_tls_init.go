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
	"io/ioutil"
	"os"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
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
	caCertPath string, caKeyPath string, initLifespan time.Duration, serviceName string,
) (err error) {
	// Check if the service cert and key already exist.
	if _, err = os.Stat(caCertPath); oserror.IsNotExist(err) {
		// Cert DNE.
		if _, err = os.Stat(caKeyPath); !oserror.IsNotExist(err) {
			// Key exists but cert does not, this is an error.
			return errors.Wrapf(err,
				"found key but not certificate for user auth at: %s", caKeyPath)
		}

		// Create both cert and key for service CA.
		err = sb.createServiceCA(caCertPath, caKeyPath, initLifespan, serviceName)
		if err != nil {
			return err
		}

		return nil
	}

	// Load cert into ServiceCertificateBundle.
	sb.CACertificate, err = loadCertificateFile(caCertPath)
	if err != nil {
		return err
	}

	// Load the key only if it exists.
	if _, err = os.Stat(caKeyPath); !oserror.IsNotExist(err) {
		sb.CAKey, err = loadKeyFile(caKeyPath)
		if err != nil {
			return err
		}
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
	serviceCertPath string,
	serviceKeyPath string,
	caCertPath string,
	caKeyPath string,
	initLifespan time.Duration,
	serviceName string,
	hostname string,
) (err error) {
	// Check if the service cert and key already exist.
	if _, err = os.Stat(serviceCertPath); !oserror.IsNotExist(err) {
		// cert exists
		if _, err = os.Stat(serviceKeyPath); oserror.IsNotExist(err) {
			// cert exists but key doesn't, this is an error
			err = errors.Wrapf(err,
				"failed to load service certificate key for %s expected key at %s",
				serviceCertPath, serviceKeyPath)
			return
		}
	} else {
		// Niether service cert or key exist, attempt to load CA.
		// Check if the CA cert and key already exist.
		if _, err = os.Stat(caCertPath); !oserror.IsNotExist(err) {
			// cert exists
			if _, err = os.Stat(caKeyPath); oserror.IsNotExist(err) {
				// cert exists but key doesn't, this is an error
				err = errors.Wrapf(err,
					"failed to load service CA key for %s expected key at %s",
					caCertPath, caKeyPath)
				return
			}

			sb.CACertificate, err = loadCertificateFile(caCertPath)
			if err != nil {
				return errors.Wrapf(
					err, "failed to load certificate file: %s", caCertPath,
				)
			}

			sb.CAKey, err = loadKeyFile(caKeyPath)
			if err != nil {
				return errors.Wrapf(
					err, "failed to load key file: %s", caKeyPath,
				)
			}

		} else {
			// Build the CA cert and key.
			err = sb.createServiceCA(caCertPath, caKeyPath, initLifespan, serviceName)
			if err != nil {
				return errors.Wrap(
					err, "failed to create Service CA",
				)
			}

		}

		// Build service cert and key.
		var hostCert, hostKey []byte
		hostCert, hostKey, err = security.CreateServiceCertAndKey(
			initLifespan,
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

		err = writeCertificateFile(serviceCertPath, hostCert)
		if err != nil {
			return err
		}

		err = writeKeyFile(serviceKeyPath, hostKey)
		if err != nil {
			return err
		}

	}

	sb.HostCertificate, err = loadCertificateFile(serviceCertPath)
	if err != nil {
		return err
	}

	sb.HostKey, err = loadKeyFile(serviceKeyPath)
	if err != nil {
		return err
	}

	return nil
}

// createServiceCA builds CA cert and key and populates them to
// ServiceCertificateBundle.
func (sb *ServiceCertificateBundle) createServiceCA(
	caCertPath string, caKeyPath string, initLifespan time.Duration, serviceName string,
) (err error) {
	sb.CACertificate, sb.CAKey, err = security.CreateCACertAndKey(initLifespan, serviceName)
	if err != nil {
		return
	}

	err = writeCertificateFile(caCertPath, sb.CACertificate)
	if err != nil {
		return
	}

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

// simple wrapper to make it easier to store certs somewhere else later.
func writeCertificateFile(certPath string, certPEM []byte) error {
	return ioutil.WriteFile(certPath, certPEM, 0600)
}

// simple wrapper to make it easier to store certs somewhere else later.
func writeKeyFile(keyPath string, keyPEM []byte) error {
	return ioutil.WriteFile(keyPath, keyPEM, 0600)
}

// InitializeFromConfig is called by the node creating certificates for the
// cluster. It uses or generates an InterNode CA to produce any missing
// unmanaged certificates. It does this base on the logic in:
// https://github.com/cockroachdb/cockroach/pull/51991
// N.B.: This function fast fails if an InterNodeHost cert/key pair are present
// as this should _never_ happen.
func (b *CertificateBundle) InitializeFromConfig(c base.Config) (err error) {
	cl := security.MakeCertsLocator(c.SSLCertsDir)

	// First check to see if host cert is already present
	// if it is, we should fail to initialize.
	if _, err = os.Stat(cl.NodeCertPath()); !oserror.IsNotExist(err) {
		err = errors.New("interNodeHost certificate already present")
		return
	}

	// Start by loading or creating the InterNode certificates.
	err = b.InterNode.loadOrCreateServiceCertificates(
		cl.NodeCertPath(),
		cl.NodeKeyPath(),
		cl.CACertPath(),
		cl.CAKeyPath(),
		initLifespan,
		"InterNode Service",
		c.Addr,
	)
	if err != nil {
		err = errors.Wrap(err,
			"failed to load or create InterNode certificates")
		return
	}

	// Initialize User auth certificates.
	// TODO(aaron-crl): Double check that we want to do this. It seems
	// like this is covered by the interface certificates?
	err = b.UserAuth.loadOrCreateUserAuthCACertAndKey(
		cl.ClientCACertPath(),
		cl.ClientCAKeyPath(),
		initLifespan,
		"User Authentication",
	)
	if err != nil {
		err = errors.Wrap(err,
			"failed to load or create User auth certificate(s)")
		return
	}

	// Initialize SQLService Certs.
	err = b.SQLService.loadOrCreateServiceCertificates(
		cl.SQLServiceCertPath(),
		cl.SQLServiceKeyPath(),
		cl.SQLServiceCACertPath(),
		cl.SQLServiceCAKeyPath(),
		initLifespan,
		"SQL Service",
		c.SQLAddr,
	)
	if err != nil {
		err = errors.Wrap(err,
			"failed to load or create SQL service certificate(s)")
		return
	}

	// Initialize RPCService Certs.
	err = b.RPCService.loadOrCreateServiceCertificates(
		cl.RPCServiceCertPath(),
		cl.RPCServiceKeyPath(),
		cl.RPCServiceCACertPath(),
		cl.RPCServiceCAKeyPath(),
		initLifespan,
		"RPC Service",
		c.SQLAddr, // TODO(aaron-crl): Add RPC variable to config.
	)
	if err != nil {
		err = errors.Wrap(err,
			"failed to load or create RPC service certificate(s)")
		return
	}

	// Initialize AdminUIService Certs.
	err = b.AdminUIService.loadOrCreateServiceCertificates(
		cl.UICertPath(),
		cl.UIKeyPath(),
		cl.UICACertPath(),
		cl.UICAKeyPath(),
		initLifespan,
		"AdminUI Service",
		c.HTTPAddr,
	)
	if err != nil {
		err = errors.Wrap(err,
			"failed to load or create Admin UI service certificate(s)")
		return
	}

	return
}

// InitializeNodeFromBundle uses the contents of the CertificateBundle and
// details from the config object to write certs to disk and generate any
// missing host-specific certificates and keys
// It is assumed that a node receiving this has not has TLS initialized. If
// a interNodeHost certificate is found, this function will error.
func (b *CertificateBundle) InitializeNodeFromBundle(c base.Config) (err error) {
	cl := security.MakeCertsLocator(c.SSLCertsDir)

	// First check to see if host cert is already present
	// if it is, we should fail to initialize.
	if _, err = os.Stat(cl.NodeCertPath()); !oserror.IsNotExist(err) {
		err = errors.New("interNodeHost certificate already present")
		return
	}

	// Write received CA's to disk. If any of them already exist, fail
	// and return an error.

	// Attempt to write InterNodeHostCA to disk first.
	err = b.InterNode.writeCAOrFail(cl.CACertPath(), cl.CAKeyPath())
	if err != nil {
		err = errors.Wrap(err, "failed to write InterNodeCA to disk")
		return
	}

	// Attempt to write ClientCA to disk.
	err = b.InterNode.writeCAOrFail(cl.ClientCACertPath(), cl.ClientCAKeyPath())
	if err != nil {
		err = errors.Wrap(err, "failed to write ClientCA to disk")
		return
	}

	// Attempt to write SQLServiceCA to disk.
	err = b.InterNode.writeCAOrFail(cl.SQLServiceCACertPath(), cl.SQLServiceCAKeyPath())
	if err != nil {
		err = errors.Wrap(err, "failed to write SQLServiceCA to disk")
		return
	}

	// Attempt to write RPCServiceCA to disk.
	err = b.InterNode.writeCAOrFail(cl.RPCServiceCACertPath(), cl.RPCServiceCAKeyPath())
	if err != nil {
		err = errors.Wrap(err, "failed to write RPCServiceCA to disk")
		return
	}

	// Attempt to write AdminUIServiceCA to disk.
	err = b.InterNode.writeCAOrFail(cl.UICACertPath(), cl.UICAKeyPath())
	if err != nil {
		err = errors.Wrap(err, "failed to write AdminUIServiceCA to disk")
		return
	}

	// Once CAs are written call the same InitFromConfig function to create
	// host certificates.
	err = b.InitializeFromConfig(c)
	if err != nil {
		err = errors.Wrap(
			err,
			"failed to initialize host certs after writing CAs to disk")
		return
	}

	return
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
