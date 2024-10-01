// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package security

import (
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/metric/aggmetric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const SQLUserLabel = "sql_user"

// Metrics is a metric.Struct for certificates.
type Metrics struct {
	// CA certificate expirations.
	CAExpiration       *metric.Gauge
	ClientCAExpiration *metric.Gauge
	UICAExpiration     *metric.Gauge
	TenantCAExpiration *metric.Gauge

	// Certificate expirations.
	NodeExpiration       *metric.Gauge
	NodeClientExpiration *metric.Gauge
	UIExpiration         *metric.Gauge
	TenantExpiration     *metric.Gauge
	// ClientExpiration is an aggregate metric, containing child metrics for all
	// users that have done cert auth with this node.
	// The children are labeled by SQL user.
	// The top-level aggregated value for this metric is not meaningful
	// (it sums up all the minimum expirations of all users).
	ClientExpiration *aggmetric.AggGauge

	// Below are TTL metrics which mirror the above expiration metrics.
	// Instead of returning the unix time in seconds however, they
	// return the number of seconds till expiration.
	CATTL         *metric.Gauge
	TenantTTL     *metric.Gauge
	TenantCATTL   *metric.Gauge
	UITTL         *metric.Gauge
	UICATTL       *metric.Gauge
	ClientCATTL   *metric.Gauge
	NodeTTL       *metric.Gauge
	NodeClientTTL *metric.Gauge
	ClientTTL     *aggmetric.AggGauge
}

var _ metric.Struct = (*Metrics)(nil)

// MetricStruct indicates that Metrics is a metric.Struct.
func (m *Metrics) MetricStruct() {}

var (
	metaCAExpiration = metric.Metadata{
		Name:        "security.certificate.expiration.ca",
		Help:        "Expiration for the CA certificate. 0 means no certificate or error.",
		Measurement: "Certificate Expiration",
		Unit:        metric.Unit_TIMESTAMP_SEC,
	}
	metaClientCAExpiration = metric.Metadata{
		Name:        "security.certificate.expiration.client-ca",
		Help:        "Expiration for the client CA certificate. 0 means no certificate or error.",
		Measurement: "Certificate Expiration",
		Unit:        metric.Unit_TIMESTAMP_SEC,
	}
	metaClientExpiration = metric.Metadata{
		Name: "security.certificate.expiration.client",
		Help: "Minimum expiration for client certificates, labeled by SQL user. 0 means no " +
			"certificate or error. ",
		Measurement: "Certificate Expiration",
		Unit:        metric.Unit_TIMESTAMP_SEC,
	}
	metaUICAExpiration = metric.Metadata{
		Name:        "security.certificate.expiration.ui-ca",
		Help:        "Expiration for the UI CA certificate. 0 means no certificate or error.",
		Measurement: "Certificate Expiration",
		Unit:        metric.Unit_TIMESTAMP_SEC,
	}
	metaNodeExpiration = metric.Metadata{
		Name:        "security.certificate.expiration.node",
		Help:        "Expiration for the node certificate. 0 means no certificate or error.",
		Measurement: "Certificate Expiration",
		Unit:        metric.Unit_TIMESTAMP_SEC,
	}
	metaNodeClientExpiration = metric.Metadata{
		Name:        "security.certificate.expiration.node-client",
		Help:        "Expiration for the node's client certificate. 0 means no certificate or error.",
		Measurement: "Certificate Expiration",
		Unit:        metric.Unit_TIMESTAMP_SEC,
	}
	metaUIExpiration = metric.Metadata{
		Name:        "security.certificate.expiration.ui",
		Help:        "Expiration for the UI certificate. 0 means no certificate or error.",
		Measurement: "Certificate Expiration",
		Unit:        metric.Unit_TIMESTAMP_SEC,
	}
	metaTenantCAExpiration = metric.Metadata{
		Name:        "security.certificate.expiration.ca-client-tenant",
		Help:        "Expiration for the Tenant Client CA certificate. 0 means no certificate or error.",
		Measurement: "Certificate Expiration",
		Unit:        metric.Unit_TIMESTAMP_SEC,
	}
	metaTenantExpiration = metric.Metadata{
		Name:        "security.certificate.expiration.client-tenant",
		Help:        "Expiration for the Tenant Client certificate. 0 means no certificate or error.",
		Measurement: "Certificate Expiration",
		Unit:        metric.Unit_TIMESTAMP_SEC,
	}

	metaCATTL = metric.Metadata{
		Name:        "security.certificate.ttl.ca",
		Help:        "Seconds till expiration for the CA certificate. 0 means expired, no certificate or error.",
		Measurement: "Certificate TTL",
		Unit:        metric.Unit_TIMESTAMP_SEC,
	}
	metaClientCATTL = metric.Metadata{
		Name:        "security.certificate.ttl.client-ca",
		Help:        "Seconds till expiration for the client CA certificate. 0 means expired, no certificate or error.",
		Measurement: "Certificate TTL",
		Unit:        metric.Unit_TIMESTAMP_SEC,
	}
	metaClientTTL = metric.Metadata{
		Name:        "security.certificate.ttl.client",
		Help:        "Seconds till expiration for the client certificates, labeled by SQL user. 0 means expired, no certificate or error.",
		Measurement: "Certificate TTL",
		Unit:        metric.Unit_TIMESTAMP_SEC,
	}
	metaUICATTL = metric.Metadata{
		Name:        "security.certificate.ttl.ui-ca",
		Help:        "Seconds till expiration for the UI CA certificate. 0 means expired, no certificate or error.",
		Measurement: "Certificate TTL",
		Unit:        metric.Unit_TIMESTAMP_SEC,
	}
	metaNodeTTL = metric.Metadata{
		Name:        "security.certificate.ttl.node",
		Help:        "Seconds till expiration for the node certificate. 0 means expired, no certificate or error.",
		Measurement: "Certificate TTL",
		Unit:        metric.Unit_TIMESTAMP_SEC,
	}
	metaNodeClientTTL = metric.Metadata{
		Name:        "security.certificate.ttl.node-client",
		Help:        "Seconds till expiration for the node's client certificate. 0 means expired, no certificate or error.",
		Measurement: "Certificate TTL",
		Unit:        metric.Unit_TIMESTAMP_SEC,
	}
	metaUITTL = metric.Metadata{
		Name:        "security.certificate.ttl.ui",
		Help:        "Seconds till expiration for the UI certificate. 0 means expired, no certificate or error.",
		Measurement: "Certificate TTL",
		Unit:        metric.Unit_TIMESTAMP_SEC,
	}
	metaTenantCATTL = metric.Metadata{
		Name:        "security.certificate.ttl.ca-client-tenant",
		Help:        "Seconds till expiration for the Tenant Client CA certificate. 0 means expired, no certificate or error.",
		Measurement: "Certificate TTL",
		Unit:        metric.Unit_TIMESTAMP_SEC,
	}
	metaTenantTTL = metric.Metadata{
		Name:        "security.certificate.ttl.client-tenant",
		Help:        "Seconds till expiration for the Tenant Client certificate. 0 means expired, no certificate or error.",
		Measurement: "Certificate TTL",
		Unit:        metric.Unit_TIMESTAMP_SEC,
	}
)

func expirationGauge(metadata metric.Metadata, ci *CertInfo) *metric.Gauge {
	return metric.NewFunctionalGauge(metadata, func() int64 {
		if ci != nil && ci.Error == nil {
			return ci.ExpirationTime.Unix()
		} else {
			return 0
		}
	})
}
func ttlGauge(metadata metric.Metadata, ci *CertInfo, ts timeutil.TimeSource) *metric.Gauge {
	return metric.NewFunctionalGauge(metadata, func() int64 {
		if ci != nil && ci.Error == nil {
			expiry := ci.ExpirationTime.Unix()
			sec := timeutil.Unix(expiry, 0).Sub(ts.Now()).Seconds()
			if sec < 0 {
				return 0
			}
			return int64(sec)
		} else {
			return 0
		}
	})
}

var defaultTimeSource = timeutil.DefaultTimeSource{}

// createMetricsLocked makes metrics using the certificate values on the manager.
// If the corresponding certificate is missing or invalid (Error != nil), we reset the
// metric to zero.
// cm.mu must be held to protect the certificates. Metrics do their own atomicity.
func (cm *CertificateManager) createMetricsLocked() Metrics {
	ts := cm.timeSource
	if ts == nil {
		ts = defaultTimeSource
	}
	b := aggmetric.MakeBuilder(SQLUserLabel)
	return Metrics{
		CAExpiration:         expirationGauge(metaCAExpiration, cm.caCert),
		TenantExpiration:     expirationGauge(metaTenantExpiration, cm.tenantCert),
		TenantCAExpiration:   expirationGauge(metaTenantCAExpiration, cm.tenantCACert),
		UIExpiration:         expirationGauge(metaUIExpiration, cm.uiCert),
		UICAExpiration:       expirationGauge(metaUICAExpiration, cm.uiCACert),
		ClientExpiration:     b.Gauge(metaClientExpiration),
		ClientCAExpiration:   expirationGauge(metaClientCAExpiration, cm.clientCACert),
		NodeExpiration:       expirationGauge(metaNodeExpiration, cm.nodeCert),
		NodeClientExpiration: expirationGauge(metaNodeClientExpiration, cm.nodeClientCert),

		CATTL:         ttlGauge(metaCATTL, cm.caCert, ts),
		TenantTTL:     ttlGauge(metaTenantTTL, cm.tenantCert, ts),
		TenantCATTL:   ttlGauge(metaTenantCATTL, cm.tenantCACert, ts),
		UITTL:         ttlGauge(metaUITTL, cm.uiCert, ts),
		UICATTL:       ttlGauge(metaUICATTL, cm.uiCACert, ts),
		ClientTTL:     b.Gauge(metaClientTTL),
		ClientCATTL:   ttlGauge(metaClientCATTL, cm.clientCACert, ts),
		NodeTTL:       ttlGauge(metaNodeTTL, cm.nodeCert, ts),
		NodeClientTTL: ttlGauge(metaNodeClientTTL, cm.nodeClientCert, ts),
	}
}
