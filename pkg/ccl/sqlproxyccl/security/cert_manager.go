// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package security

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
)

// CertManager is a collection of certificates that will be reloaded on
// SIGHUP signal or explicit Reload call.
type CertManager struct {
	syncutil.RWMutex
	ctx           context.Context
	monitorCancel context.CancelFunc
	certs         map[string]Cert
}

// NewCertManager creates a new certificate manager with empty certificate set.
func NewCertManager(ctx context.Context) *CertManager {
	cm := &CertManager{
		ctx:   ctx,
		certs: make(map[string]Cert),
	}
	return cm
}

// ManageCert will add the given cert to the certs managed by the manager.
func (cm *CertManager) ManageCert(id string, cert Cert) {
	cm.Lock()
	defer cm.Unlock()
	if len(cm.certs) == 0 {
		cm.startMonitorLocked()
	}
	cm.certs[id] = cert
}

// RemoveCert will remove the given cert from the certs managed by the manager.
func (cm *CertManager) RemoveCert(id string) {
	cm.Lock()
	defer cm.Unlock()
	delete(cm.certs, id)
	if len(cm.certs) == 0 {
		cm.stopMonitorLocked()
	}
}

// Cert will retrieve the managed cert with the give id if it exists.
// nil otherwise.
func (cm *CertManager) Cert(id string) Cert {
	cm.RLock()
	defer cm.RUnlock()
	return cm.certs[id]
}

// Registers a signal handler that triggers on SIGHUP and reloads the
// certificates. The handler will shutdown when the context is done.
func (cm *CertManager) startMonitorLocked() {
	ctx, cancel := context.WithCancel(cm.ctx)
	cm.monitorCancel = cancel
	refresh := sysutil.RefreshSignaledChan()

	go func() {
		select {
		case sig := <-refresh:
			log.Ops.Infof(ctx, "received signal %q, triggering certificate reload", sig)
			cm.Reload()
		case <-ctx.Done():
			return
		}
	}()
}

// Stop the running signal handler.
func (cm *CertManager) stopMonitorLocked() {
	cm.monitorCancel()
	cm.monitorCancel = nil
}

// Reload will verify and load/reload the managed certificates.
func (cm *CertManager) Reload() {
	cm.RLock()
	defer cm.RUnlock()

	errCount := 0
	for _, cert := range cm.certs {
		cert.Reload(cm.ctx)
		if cert.Err() != nil {
			errCount++
		}
	}
	if errCount > 0 {
		log.StructuredEvent(cm.ctx, &eventpb.CertsReload{
			Success: false,
			ErrorMessage: fmt.Sprintf(
				"%d certs (out of %d) failed to reload", errCount, len(cm.certs),
			)},
		)
	} else {
		log.StructuredEvent(cm.ctx, &eventpb.CertsReload{Success: true})
	}
}
