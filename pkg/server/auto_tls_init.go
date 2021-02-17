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
	"errors"
	"io/ioutil"
	"os"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security"
)

// CertificateBundle manages the collection of certificates used by a
// CockroachDB node
type CertificateBundle struct {
	InterNode      ServiceCertificateBundle
	UserAuth       ServiceCertificateBundle
	SQLService     ServiceCertificateBundle
	RPCService     ServiceCertificateBundle
	AdminUIService ServiceCertificateBundle
}

// ServiceCertificateBundle is a container for the CA and host certs for a node
type ServiceCertificateBundle struct {
	CACertificate   []byte
	CAKey           []byte
	HostCertificate []byte // This will be blank if unused (as in the user case)
	HostKey         []byte // This will be blank if unused (as in the user case)
}

func (sb *ServiceCertificateBundle) loadServiceCertAndKey(certPath string, keyPath string) (err error) {
	// Load cert and key
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

func (sb *ServiceCertificateBundle) loadCACertAndKey(certPath string, keyPath string) (err error) {
	// Load cert and key
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

// LoadUserAuthCACertAndKey loads host certificate and key from disk or fails with error
func (sb *ServiceCertificateBundle) loadOrCreateUserAuthCACertAndKey(caCertPath string, caKeyPath string, initLifespan time.Duration, serviceName string) (err error) {
	// Check if the service cert and key already exist
	if _, err = os.Stat(caCertPath); os.IsNotExist(err) {
		// cert DNE
		if _, err = os.Stat(caKeyPath); !os.IsNotExist(err) {
			// key exist, this is an error
			err = errors.New("found key but not certificate for user auth at " + caKeyPath)
		}

		// (create both cert and key)
		err = sb.createServiceCA(caCertPath, caKeyPath, initLifespan, serviceName)
		if err != nil {
			return
		}

		return
	}

	// load cert
	sb.CACertificate, err = loadCertificateFile(caCertPath)
	if err != nil {
		return
	}

	// maybe load key (if it exists)
	if _, err = os.Stat(caKeyPath); !os.IsNotExist(err) {
		sb.CAKey, err = loadKeyFile(caKeyPath)
		if err != nil {
			return
		}
	}

	return
}

// loadOrCreateServiceCertificates will attempt to load the service cert/key
// into the service bundle.
// * If they do not exist:
//   It will attempt to load the service CA cert/key pair
//   * If they do not exist
//     It will generate the service CA cert/key pair
//     It will persist these to disk and store them
//       in the ServiceCertificateBundle
//   It will generate the service cert/key pair
//   It will persist these to disk and store them
//     in the ServiceCertificateBundle
func (sb *ServiceCertificateBundle) loadOrCreateServiceCertificates(
	serviceCertPath string,
	serviceKeyPath string,
	caCertPath string,
	caKeyPath string,
	initLifespan time.Duration,
	serviceName string,
	hostname string) (err error) {

	// Check if the service cert and key already exist
	if _, err = os.Stat(serviceCertPath); !os.IsNotExist(err) {
		// cert exists
		if _, err = os.Stat(serviceKeyPath); os.IsNotExist(err) {
			// cert exists but key doesn't, this is an error
			err = errors.New("failed to load service certificate key for " + serviceCertPath + "expected key at " + serviceKeyPath)
			return
		}
	} else {
		// niether service cert or key exist, attempt to load CA
		// Check if the CA cert and key already exist
		if _, err = os.Stat(caCertPath); !os.IsNotExist(err) {
			// cert exists
			if _, err = os.Stat(caKeyPath); os.IsNotExist(err) {
				// cert exists but key doesn't, this is an error
				err = errors.New("failed to load service CA key for " + caCertPath + "expected key at " + caKeyPath)
				return
			}

			sb.CACertificate, err = loadCertificateFile(caCertPath)
			sb.CAKey, err = loadKeyFile(caKeyPath)

		} else {
			// build CA cert and key
			err = sb.createServiceCA(caCertPath, caKeyPath, initLifespan, serviceName)
			if err != nil {
				return
			}

		}

		// build service cert and key
		var hostCert, hostKey []byte
		hostCert, hostKey, err = security.CreateServiceCertAndKey(
			initLifespan,
			serviceName,
			hostname,
			sb.CACertificate,
			sb.CAKey,
		)

		writeCertificateFile(serviceCertPath, hostCert)
		if err != nil {
			return
		}

		writeKeyFile(serviceKeyPath, hostKey)
		if err != nil {
			return
		}

	}

	sb.HostCertificate, err = loadCertificateFile(serviceCertPath)
	if err != nil {
		return
	}

	sb.HostKey, err = loadKeyFile(serviceKeyPath)
	if err != nil {
		return
	}

	return
}

func (sb *ServiceCertificateBundle) createServiceCA(caCertPath string, caKeyPath string, initLifespan time.Duration, serviceName string) (err error) {
	// build CA cert and key
	sb.CACertificate, sb.CAKey, err = security.CreateCACertAndKey(initLifespan, serviceName)
	if err != nil {
		return
	}

	writeCertificateFile(caCertPath, sb.CACertificate)
	if err != nil {
		return
	}

	writeKeyFile(caKeyPath, sb.CAKey)
	if err != nil {
		return
	}

	return
}

// simple wrapper to make it easier to store certs somewhere else later
// TODO (aaron-crl): Put validation checks here
func loadCertificateFile(certPath string) (cert []byte, err error) {
	cert, err = ioutil.ReadFile(certPath)
	return
}

// simple wrapper to make it easier to store certs somewhere else later
// TODO (aaron-crl): Put validation checks here
func loadKeyFile(keyPath string) (key []byte, err error) {
	key, err = ioutil.ReadFile(keyPath)
	return
}

// simple wrapper to make it easier to store certs somewhere else later
func writeCertificateFile(certPath string, certPEM []byte) error {
	return ioutil.WriteFile(certPath, certPEM, 0600)
}

// simple wrapper to make it easier to store certs somewhere else later
func writeKeyFile(keyPath string, keyPEM []byte) error {
	return ioutil.WriteFile(keyPath, keyPEM, 0600)
}

// InitializeFromConfig is called by the node creating certificates for the
// cluster. It uses or generates an InterNode CA to produce any missing
// unmanaged certificates. It does this base on the logic in:
// https://github.com/aaron-crl/cockroach/blob/rfc20200722_cert_free_secure_setup/docs/RFCS/20200722_certificate_free_secure_setup.md#initial-configuration-establishing-trust
// N.B.: This function fast fails if an InterNodeHost cert/key pair are present
// as this should _never_ happen.
func (b *CertificateBundle) InitializeFromConfig(c base.Config) (err error) {
	cl := security.MakeCertsLocator(c.SSLCertsDir)
	// TODO(aaron-crl): Put this in the config map
	initLifespan, err := time.ParseDuration("60m")

	// First check to see if host cert is already present
	// if it is, we should fail to initialize
	if _, err = os.Stat(cl.NodeCertPath()); !os.IsNotExist(err) {
		err = errors.New("InterNodeHost certificate already present")
		return
	}

	// Start by loading or creating the InterNode certificates
	b.InterNode.loadOrCreateServiceCertificates(
		cl.NodeCertPath(),
		cl.NodeKeyPath(),
		cl.CACertPath(),
		cl.CAKeyPath(),
		initLifespan,
		"InterNode Service",
		c.Addr,
	)

	b.UserAuth.loadOrCreateUserAuthCACertAndKey(
		cl.UICACertPath(),
		cl.UICAKeyPath(),
		initLifespan,
		"User Authentication",
	)

	// Initialize SQLService Certs
	b.SQLService.loadOrCreateServiceCertificates(
		cl.SQLServiceCertPath(),
		cl.SQLServiceKeyPath(),
		cl.SQLServiceCACertPath(),
		cl.SQLServiceCAKeyPath(),
		initLifespan,
		"SQL Service",
		c.SQLAddr,
	)

	// Initialize RPCService Certs
	b.RPCService.loadOrCreateServiceCertificates(
		cl.RPCServiceCertPath(),
		cl.RPCServiceKeyPath(),
		cl.RPCServiceCACertPath(),
		cl.RPCServiceCAKeyPath(),
		initLifespan,
		"RPC Service",
		c.SQLAddr, // TODO(aaron-crl): Add RPC variable to config
	)

	// Initialize AdminUIService Certs
	b.AdminUIService.loadOrCreateServiceCertificates(
		cl.UICertPath(),
		cl.UIKeyPath(),
		cl.UICACertPath(),
		cl.UICAKeyPath(),
		initLifespan,
		"AdminUI Service",
		c.HTTPAddr,
	)

	return
}
