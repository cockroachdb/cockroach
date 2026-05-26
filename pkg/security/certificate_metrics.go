// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package security

import (
	"math"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/metric/aggmetric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const SQLUserLabel = "sql_user"

// Metrics is a metric.Struct for certificates.
type Metrics struct {
	// CA certificate expirations.
	CAExpiration       *metric.FunctionalGauge
	ClientCAExpiration *metric.FunctionalGauge
	UICAExpiration     *metric.FunctionalGauge
	TenantCAExpiration *metric.FunctionalGauge

	// Certificate expirations.
	NodeExpiration       *metric.FunctionalGauge
	NodeClientExpiration *metric.FunctionalGauge
	UIExpiration         *metric.FunctionalGauge
	TenantExpiration     *metric.FunctionalGauge
	// ClientExpiration is an aggregate metric, containing child metrics for all
	// users that have done cert auth with this node.
	// The children are labeled by SQL user.
	// The top-level aggregated value for this metric is not meaningful
	// (it sums up all the minimum expirations of all users).
	ClientExpiration *aggmetric.AggGauge

	// Below are TTL metrics which mirror the above expiration metrics.
	// Instead of returning the unix time in seconds however, they
	// return the number of seconds till expiration.
	CATTL         *metric.FunctionalGauge
	TenantTTL     *metric.FunctionalGauge
	TenantCATTL   *metric.FunctionalGauge
	UITTL         *metric.FunctionalGauge
	UICATTL       *metric.FunctionalGauge
	ClientCATTL   *metric.FunctionalGauge
	NodeTTL       *metric.FunctionalGauge
	NodeClientTTL *metric.FunctionalGauge
	ClientTTL     *aggmetric.AggFunctionalGauge

	// Rotation timestamp metrics. Each records the unix timestamp (seconds)
	// of the last successful certificate reload where the certificate content
	// actually changed. 0 means never rotated since process start.
	CALastRotation         *metric.Gauge
	ClientCALastRotation   *metric.Gauge
	UICALastRotation       *metric.Gauge
	TenantCALastRotation   *metric.Gauge
	NodeLastRotation       *metric.Gauge
	NodeClientLastRotation *metric.Gauge
	UILastRotation         *metric.Gauge
	TenantLastRotation     *metric.Gauge

	// Days-until-expiry metrics. Each reports the number of days remaining
	// until the certificate expires. 0 means expired, missing, or error.
	CAExpiryDays         *metric.FunctionalGauge
	ClientCAExpiryDays   *metric.FunctionalGauge
	UICAExpiryDays       *metric.FunctionalGauge
	TenantCAExpiryDays   *metric.FunctionalGauge
	NodeExpiryDays       *metric.FunctionalGauge
	NodeClientExpiryDays *metric.FunctionalGauge
	UIExpiryDays         *metric.FunctionalGauge
	TenantExpiryDays     *metric.FunctionalGauge
}

var _ metric.Struct = (*Metrics)(nil)

// MetricStruct indicates that Metrics is a metric.Struct.
func (m *Metrics) MetricStruct() {}

var (
	metaCAExpiration = metric.Metadata{
		Name:         "security.certificate.expiration.ca",
		Help:         "Expiration for the CA certificate. 0 means no certificate or error.",
		Measurement:  "Certificate Expiration",
		Unit:         metric.Unit_TIMESTAMP_SEC,
		Visibility:   metric.Metadata_ESSENTIAL,
		Category:     metric.Metadata_EXPIRATIONS,
		HowToUse:     "See Description.",
		LabeledName:  "security.certificate.expiration",
		StaticLabels: metric.MakeLabelPairs(metric.LabelCertificateType, "ca"),
	}
	metaClientCAExpiration = metric.Metadata{
		Name:         "security.certificate.expiration.client-ca",
		Help:         "Expiration for the client CA certificate. 0 means no certificate or error.",
		Measurement:  "Certificate Expiration",
		Unit:         metric.Unit_TIMESTAMP_SEC,
		Visibility:   metric.Metadata_ESSENTIAL,
		Category:     metric.Metadata_EXPIRATIONS,
		HowToUse:     "See Description.",
		LabeledName:  "security.certificate.expiration",
		StaticLabels: metric.MakeLabelPairs(metric.LabelCertificateType, "client-ca"),
	}
	metaClientExpiration = metric.Metadata{
		Name: "security.certificate.expiration.client",
		Help: "Minimum expiration for client certificates, labeled by SQL user. 0 means no " +
			"certificate or error. ",
		Measurement:  "Certificate Expiration",
		Unit:         metric.Unit_TIMESTAMP_SEC,
		LabeledName:  "security.certificate.expiration",
		StaticLabels: metric.MakeLabelPairs(metric.LabelCertificateType, "client"),
	}
	metaUICAExpiration = metric.Metadata{
		Name:         "security.certificate.expiration.ui-ca",
		Help:         "Expiration for the UI CA certificate. 0 means no certificate or error.",
		Measurement:  "Certificate Expiration",
		Unit:         metric.Unit_TIMESTAMP_SEC,
		Visibility:   metric.Metadata_ESSENTIAL,
		Category:     metric.Metadata_EXPIRATIONS,
		HowToUse:     "See Description.",
		LabeledName:  "security.certificate.expiration",
		StaticLabels: metric.MakeLabelPairs(metric.LabelCertificateType, "ui-ca"),
	}
	metaNodeExpiration = metric.Metadata{
		Name:         "security.certificate.expiration.node",
		Help:         "Expiration for the node certificate. 0 means no certificate or error.",
		Measurement:  "Certificate Expiration",
		Unit:         metric.Unit_TIMESTAMP_SEC,
		Visibility:   metric.Metadata_ESSENTIAL,
		Category:     metric.Metadata_EXPIRATIONS,
		HowToUse:     "See Description.",
		LabeledName:  "security.certificate.expiration",
		StaticLabels: metric.MakeLabelPairs(metric.LabelCertificateType, "node"),
	}
	metaNodeClientExpiration = metric.Metadata{
		Name:         "security.certificate.expiration.node-client",
		Help:         "Expiration for the node's client certificate. 0 means no certificate or error.",
		Measurement:  "Certificate Expiration",
		Unit:         metric.Unit_TIMESTAMP_SEC,
		Visibility:   metric.Metadata_ESSENTIAL,
		Category:     metric.Metadata_EXPIRATIONS,
		HowToUse:     "See Description.",
		LabeledName:  "security.certificate.expiration",
		StaticLabels: metric.MakeLabelPairs(metric.LabelCertificateType, "node-client"),
	}
	metaUIExpiration = metric.Metadata{
		Name:         "security.certificate.expiration.ui",
		Help:         "Expiration for the UI certificate. 0 means no certificate or error.",
		Measurement:  "Certificate Expiration",
		Unit:         metric.Unit_TIMESTAMP_SEC,
		Visibility:   metric.Metadata_ESSENTIAL,
		Category:     metric.Metadata_EXPIRATIONS,
		HowToUse:     "See Description.",
		LabeledName:  "security.certificate.expiration",
		StaticLabels: metric.MakeLabelPairs(metric.LabelCertificateType, "ui"),
	}
	metaTenantCAExpiration = metric.Metadata{
		Name:         "security.certificate.expiration.ca-client-tenant",
		Help:         "Expiration for the Tenant Client CA certificate. 0 means no certificate or error.",
		Measurement:  "Certificate Expiration",
		Unit:         metric.Unit_TIMESTAMP_SEC,
		LabeledName:  "security.certificate.expiration",
		StaticLabels: metric.MakeLabelPairs(metric.LabelCertificateType, "ca-client-tenant"),
	}
	metaTenantExpiration = metric.Metadata{
		Name:         "security.certificate.expiration.client-tenant",
		Help:         "Expiration for the Tenant Client certificate. 0 means no certificate or error.",
		Measurement:  "Certificate Expiration",
		Unit:         metric.Unit_TIMESTAMP_SEC,
		LabeledName:  "security.certificate.expiration",
		StaticLabels: metric.MakeLabelPairs(metric.LabelCertificateType, "client-tenant"),
	}

	metaCATTL = metric.Metadata{
		Name:         "security.certificate.ttl.ca",
		Help:         "Seconds till expiration for the CA certificate. 0 means expired, no certificate or error.",
		Measurement:  "Certificate TTL",
		Unit:         metric.Unit_TIMESTAMP_SEC,
		LabeledName:  "security.certificate.ttl",
		StaticLabels: metric.MakeLabelPairs(metric.LabelCertificateType, "ca"),
	}
	metaClientCATTL = metric.Metadata{
		Name:         "security.certificate.ttl.client-ca",
		Help:         "Seconds till expiration for the client CA certificate. 0 means expired, no certificate or error.",
		Measurement:  "Certificate TTL",
		Unit:         metric.Unit_TIMESTAMP_SEC,
		LabeledName:  "security.certificate.ttl",
		StaticLabels: metric.MakeLabelPairs(metric.LabelCertificateType, "client-ca"),
	}
	metaClientTTL = metric.Metadata{
		Name:         "security.certificate.ttl.client",
		Help:         "Seconds till expiration for the client certificates, labeled by SQL user. 0 means expired, no certificate or error.",
		Measurement:  "Certificate TTL",
		Unit:         metric.Unit_TIMESTAMP_SEC,
		LabeledName:  "security.certificate.ttl",
		StaticLabels: metric.MakeLabelPairs(metric.LabelCertificateType, "client"),
	}
	metaUICATTL = metric.Metadata{
		Name:         "security.certificate.ttl.ui-ca",
		Help:         "Seconds till expiration for the UI CA certificate. 0 means expired, no certificate or error.",
		Measurement:  "Certificate TTL",
		Unit:         metric.Unit_TIMESTAMP_SEC,
		LabeledName:  "security.certificate.ttl",
		StaticLabels: metric.MakeLabelPairs(metric.LabelCertificateType, "ui-ca"),
	}
	metaNodeTTL = metric.Metadata{
		Name:         "security.certificate.ttl.node",
		Help:         "Seconds till expiration for the node certificate. 0 means expired, no certificate or error.",
		Measurement:  "Certificate TTL",
		Unit:         metric.Unit_TIMESTAMP_SEC,
		LabeledName:  "security.certificate.ttl",
		StaticLabels: metric.MakeLabelPairs(metric.LabelCertificateType, "node"),
	}
	metaNodeClientTTL = metric.Metadata{
		Name:         "security.certificate.ttl.node-client",
		Help:         "Seconds till expiration for the node's client certificate. 0 means expired, no certificate or error.",
		Measurement:  "Certificate TTL",
		Unit:         metric.Unit_TIMESTAMP_SEC,
		LabeledName:  "security.certificate.ttl",
		StaticLabels: metric.MakeLabelPairs(metric.LabelCertificateType, "node-client"),
	}
	metaUITTL = metric.Metadata{
		Name:         "security.certificate.ttl.ui",
		Help:         "Seconds till expiration for the UI certificate. 0 means expired, no certificate or error.",
		Measurement:  "Certificate TTL",
		Unit:         metric.Unit_TIMESTAMP_SEC,
		LabeledName:  "security.certificate.ttl",
		StaticLabels: metric.MakeLabelPairs(metric.LabelCertificateType, "ui"),
	}
	metaTenantCATTL = metric.Metadata{
		Name:         "security.certificate.ttl.ca-client-tenant",
		Help:         "Seconds till expiration for the Tenant Client CA certificate. 0 means expired, no certificate or error.",
		Measurement:  "Certificate TTL",
		Unit:         metric.Unit_TIMESTAMP_SEC,
		LabeledName:  "security.certificate.ttl",
		StaticLabels: metric.MakeLabelPairs(metric.LabelCertificateType, "ca-client-tenant"),
	}
	metaTenantTTL = metric.Metadata{
		Name:         "security.certificate.ttl.client-tenant",
		Help:         "Seconds till expiration for the Tenant Client certificate. 0 means expired, no certificate or error.",
		Measurement:  "Certificate TTL",
		Unit:         metric.Unit_TIMESTAMP_SEC,
		LabeledName:  "security.certificate.ttl",
		StaticLabels: metric.MakeLabelPairs(metric.LabelCertificateType, "client-tenant"),
	}

	// Rotation timestamp metadata.
	metaCALastRotation = metric.Metadata{
		Name:         "security.certificate.last_rotation.ca",
		Help:         "Unix timestamp of the last CA certificate rotation. 0 means no rotation since process start.",
		Measurement:  "Certificate Last Rotation",
		Unit:         metric.Unit_TIMESTAMP_SEC,
		Visibility:   metric.Metadata_ESSENTIAL,
		Category:     metric.Metadata_EXPIRATIONS,
		HowToUse:     "Use this metric to verify that CA certificate rotations are occurring as expected. A value of 0 indicates no rotation has happened since the process started. Compare with the corresponding expiry metric to ensure rotations happen well before expiration.",
		LabeledName:  "security.certificate.last_rotation",
		StaticLabels: metric.MakeLabelPairs(metric.LabelCertificateType, "ca"),
	}
	metaClientCALastRotation = metric.Metadata{
		Name:         "security.certificate.last_rotation.client-ca",
		Help:         "Unix timestamp of the last client CA certificate rotation. 0 means no rotation since process start.",
		Measurement:  "Certificate Last Rotation",
		Unit:         metric.Unit_TIMESTAMP_SEC,
		Visibility:   metric.Metadata_ESSENTIAL,
		Category:     metric.Metadata_EXPIRATIONS,
		HowToUse:     "Use this metric to verify that client CA certificate rotations are occurring as expected. A value of 0 indicates no rotation has happened since the process started. Compare with the corresponding expiry metric to ensure rotations happen well before expiration.",
		LabeledName:  "security.certificate.last_rotation",
		StaticLabels: metric.MakeLabelPairs(metric.LabelCertificateType, "client-ca"),
	}
	metaUICALastRotation = metric.Metadata{
		Name:         "security.certificate.last_rotation.ui-ca",
		Help:         "Unix timestamp of the last UI CA certificate rotation. 0 means no rotation since process start.",
		Measurement:  "Certificate Last Rotation",
		Unit:         metric.Unit_TIMESTAMP_SEC,
		Visibility:   metric.Metadata_ESSENTIAL,
		Category:     metric.Metadata_EXPIRATIONS,
		HowToUse:     "Use this metric to verify that UI CA certificate rotations are occurring as expected. A value of 0 indicates no rotation has happened since the process started. Compare with the corresponding expiry metric to ensure rotations happen well before expiration.",
		LabeledName:  "security.certificate.last_rotation",
		StaticLabels: metric.MakeLabelPairs(metric.LabelCertificateType, "ui-ca"),
	}
	metaTenantCALastRotation = metric.Metadata{
		Name:         "security.certificate.last_rotation.ca-client-tenant",
		Help:         "Unix timestamp of the last tenant client CA certificate rotation. 0 means no rotation since process start.",
		Measurement:  "Certificate Last Rotation",
		Unit:         metric.Unit_TIMESTAMP_SEC,
		Visibility:   metric.Metadata_ESSENTIAL,
		Category:     metric.Metadata_EXPIRATIONS,
		HowToUse:     "Use this metric to verify that tenant client CA certificate rotations are occurring as expected. A value of 0 indicates no rotation has happened since the process started. Compare with the corresponding expiry metric to ensure rotations happen well before expiration.",
		LabeledName:  "security.certificate.last_rotation",
		StaticLabels: metric.MakeLabelPairs(metric.LabelCertificateType, "ca-client-tenant"),
	}
	metaNodeLastRotation = metric.Metadata{
		Name:         "security.certificate.last_rotation.node",
		Help:         "Unix timestamp of the last node certificate rotation. 0 means no rotation since process start.",
		Measurement:  "Certificate Last Rotation",
		Unit:         metric.Unit_TIMESTAMP_SEC,
		Visibility:   metric.Metadata_ESSENTIAL,
		Category:     metric.Metadata_EXPIRATIONS,
		HowToUse:     "Use this metric to verify that node certificate rotations are occurring as expected. A value of 0 indicates no rotation has happened since the process started. Compare with the corresponding expiry metric to ensure rotations happen well before expiration.",
		LabeledName:  "security.certificate.last_rotation",
		StaticLabels: metric.MakeLabelPairs(metric.LabelCertificateType, "node"),
	}
	metaNodeClientLastRotation = metric.Metadata{
		Name:         "security.certificate.last_rotation.node-client",
		Help:         "Unix timestamp of the last node client certificate rotation. 0 means no rotation since process start.",
		Measurement:  "Certificate Last Rotation",
		Unit:         metric.Unit_TIMESTAMP_SEC,
		Visibility:   metric.Metadata_ESSENTIAL,
		Category:     metric.Metadata_EXPIRATIONS,
		HowToUse:     "Use this metric to verify that node client certificate rotations are occurring as expected. A value of 0 indicates no rotation has happened since the process started. Compare with the corresponding expiry metric to ensure rotations happen well before expiration.",
		LabeledName:  "security.certificate.last_rotation",
		StaticLabels: metric.MakeLabelPairs(metric.LabelCertificateType, "node-client"),
	}
	metaUILastRotation = metric.Metadata{
		Name:         "security.certificate.last_rotation.ui",
		Help:         "Unix timestamp of the last UI certificate rotation. 0 means no rotation since process start.",
		Measurement:  "Certificate Last Rotation",
		Unit:         metric.Unit_TIMESTAMP_SEC,
		Visibility:   metric.Metadata_ESSENTIAL,
		Category:     metric.Metadata_EXPIRATIONS,
		HowToUse:     "Use this metric to verify that UI certificate rotations are occurring as expected. A value of 0 indicates no rotation has happened since the process started. Compare with the corresponding expiry metric to ensure rotations happen well before expiration.",
		LabeledName:  "security.certificate.last_rotation",
		StaticLabels: metric.MakeLabelPairs(metric.LabelCertificateType, "ui"),
	}
	metaTenantLastRotation = metric.Metadata{
		Name:         "security.certificate.last_rotation.client-tenant",
		Help:         "Unix timestamp of the last tenant client certificate rotation. 0 means no rotation since process start.",
		Measurement:  "Certificate Last Rotation",
		Unit:         metric.Unit_TIMESTAMP_SEC,
		Visibility:   metric.Metadata_ESSENTIAL,
		Category:     metric.Metadata_EXPIRATIONS,
		HowToUse:     "Use this metric to verify that tenant client certificate rotations are occurring as expected. A value of 0 indicates no rotation has happened since the process started. Compare with the corresponding expiry metric to ensure rotations happen well before expiration.",
		LabeledName:  "security.certificate.last_rotation",
		StaticLabels: metric.MakeLabelPairs(metric.LabelCertificateType, "client-tenant"),
	}

	// Days-until-expiry metadata.
	metaCAExpiryDays = metric.Metadata{
		Name:         "security.certificate.expiry_days.ca",
		Help:         "Days until expiration for the CA certificate. 0 means expired, no certificate or error.",
		Measurement:  "Certificate Expiry Days",
		Unit:         metric.Unit_COUNT,
		Visibility:   metric.Metadata_ESSENTIAL,
		Category:     metric.Metadata_EXPIRATIONS,
		HowToUse:     "Alert when this value drops below your organization's rotation threshold (e.g. 30 days). A value of 0 means the certificate is expired, missing, or has an error. Renew the CA certificate and reload it before expiration to avoid cluster-wide TLS failures.",
		LabeledName:  "security.certificate.expiry_days",
		StaticLabels: metric.MakeLabelPairs(metric.LabelCertificateType, "ca"),
	}
	metaClientCAExpiryDays = metric.Metadata{
		Name:         "security.certificate.expiry_days.client-ca",
		Help:         "Days until expiration for the client CA certificate. 0 means expired, no certificate or error.",
		Measurement:  "Certificate Expiry Days",
		Unit:         metric.Unit_COUNT,
		Visibility:   metric.Metadata_ESSENTIAL,
		Category:     metric.Metadata_EXPIRATIONS,
		HowToUse:     "Alert when this value drops below your organization's rotation threshold (e.g. 30 days). A value of 0 means the certificate is expired, missing, or has an error. Renew the client CA certificate and reload it before expiration to avoid client authentication failures.",
		LabeledName:  "security.certificate.expiry_days",
		StaticLabels: metric.MakeLabelPairs(metric.LabelCertificateType, "client-ca"),
	}
	metaUICAExpiryDays = metric.Metadata{
		Name:         "security.certificate.expiry_days.ui-ca",
		Help:         "Days until expiration for the UI CA certificate. 0 means expired, no certificate or error.",
		Measurement:  "Certificate Expiry Days",
		Unit:         metric.Unit_COUNT,
		Visibility:   metric.Metadata_ESSENTIAL,
		Category:     metric.Metadata_EXPIRATIONS,
		HowToUse:     "Alert when this value drops below your organization's rotation threshold (e.g. 30 days). A value of 0 means the certificate is expired, missing, or has an error. Renew the UI CA certificate and reload it before expiration to avoid DB Console access failures.",
		LabeledName:  "security.certificate.expiry_days",
		StaticLabels: metric.MakeLabelPairs(metric.LabelCertificateType, "ui-ca"),
	}
	metaTenantCAExpiryDays = metric.Metadata{
		Name:         "security.certificate.expiry_days.ca-client-tenant",
		Help:         "Days until expiration for the tenant client CA certificate. 0 means expired, no certificate or error.",
		Measurement:  "Certificate Expiry Days",
		Unit:         metric.Unit_COUNT,
		Visibility:   metric.Metadata_ESSENTIAL,
		Category:     metric.Metadata_EXPIRATIONS,
		HowToUse:     "Alert when this value drops below your organization's rotation threshold (e.g. 30 days). A value of 0 means the certificate is expired, missing, or has an error. Renew the tenant client CA certificate and reload it before expiration to avoid tenant authentication failures.",
		LabeledName:  "security.certificate.expiry_days",
		StaticLabels: metric.MakeLabelPairs(metric.LabelCertificateType, "ca-client-tenant"),
	}
	metaNodeExpiryDays = metric.Metadata{
		Name:         "security.certificate.expiry_days.node",
		Help:         "Days until expiration for the node certificate. 0 means expired, no certificate or error.",
		Measurement:  "Certificate Expiry Days",
		Unit:         metric.Unit_COUNT,
		Visibility:   metric.Metadata_ESSENTIAL,
		Category:     metric.Metadata_EXPIRATIONS,
		HowToUse:     "Alert when this value drops below your organization's rotation threshold (e.g. 30 days). A value of 0 means the certificate is expired, missing, or has an error. Renew the node certificate and reload it before expiration to avoid inter-node communication failures.",
		LabeledName:  "security.certificate.expiry_days",
		StaticLabels: metric.MakeLabelPairs(metric.LabelCertificateType, "node"),
	}
	metaNodeClientExpiryDays = metric.Metadata{
		Name:         "security.certificate.expiry_days.node-client",
		Help:         "Days until expiration for the node client certificate. 0 means expired, no certificate or error.",
		Measurement:  "Certificate Expiry Days",
		Unit:         metric.Unit_COUNT,
		Visibility:   metric.Metadata_ESSENTIAL,
		Category:     metric.Metadata_EXPIRATIONS,
		HowToUse:     "Alert when this value drops below your organization's rotation threshold (e.g. 30 days). A value of 0 means the certificate is expired, missing, or has an error. Renew the node client certificate and reload it before expiration to avoid node-to-node RPC failures.",
		LabeledName:  "security.certificate.expiry_days",
		StaticLabels: metric.MakeLabelPairs(metric.LabelCertificateType, "node-client"),
	}
	metaUIExpiryDays = metric.Metadata{
		Name:         "security.certificate.expiry_days.ui",
		Help:         "Days until expiration for the UI certificate. 0 means expired, no certificate or error.",
		Measurement:  "Certificate Expiry Days",
		Unit:         metric.Unit_COUNT,
		Visibility:   metric.Metadata_ESSENTIAL,
		Category:     metric.Metadata_EXPIRATIONS,
		HowToUse:     "Alert when this value drops below your organization's rotation threshold (e.g. 30 days). A value of 0 means the certificate is expired, missing, or has an error. Renew the UI certificate and reload it before expiration to avoid DB Console HTTPS failures.",
		LabeledName:  "security.certificate.expiry_days",
		StaticLabels: metric.MakeLabelPairs(metric.LabelCertificateType, "ui"),
	}
	metaTenantExpiryDays = metric.Metadata{
		Name:         "security.certificate.expiry_days.client-tenant",
		Help:         "Days until expiration for the tenant client certificate. 0 means expired, no certificate or error.",
		Measurement:  "Certificate Expiry Days",
		Unit:         metric.Unit_COUNT,
		Visibility:   metric.Metadata_ESSENTIAL,
		Category:     metric.Metadata_EXPIRATIONS,
		HowToUse:     "Alert when this value drops below your organization's rotation threshold (e.g. 30 days). A value of 0 means the certificate is expired, missing, or has an error. Renew the tenant client certificate and reload it before expiration to avoid tenant-to-KV communication failures.",
		LabeledName:  "security.certificate.expiry_days",
		StaticLabels: metric.MakeLabelPairs(metric.LabelCertificateType, "client-tenant"),
	}
)

// certClosure defines a way to expose a certificate to the below metric types.
// Closures are used to expose certificates instead of direct references,
// because the references can become outdated if the system loads new
// certificates.
type certClosure func() *CertInfo

func expirationGauge(metadata metric.Metadata, certFunc certClosure) *metric.FunctionalGauge {
	return metric.NewFunctionalGauge(metadata, func() int64 {
		ci := certFunc()
		if ci != nil && ci.Error == nil {
			return ci.ExpirationTime.Unix()
		} else {
			return 0
		}
	})
}

func ttlGauge(
	metadata metric.Metadata, certFunc certClosure, ts timeutil.TimeSource,
) *metric.FunctionalGauge {
	return metric.NewFunctionalGauge(metadata, func() int64 {
		ci := certFunc()
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

const secondsPerDay = 86400

// expiryDaysGauge returns a FunctionalGauge that reports the number of whole
// days remaining until the certificate expires. math.Ceil is used so that any
// partial day is rounded up: a certificate with 1 second remaining reports 1
// (not 0), matching operator expectations that 0 means "already expired".
func expiryDaysGauge(
	metadata metric.Metadata, certFunc certClosure, ts timeutil.TimeSource,
) *metric.FunctionalGauge {
	return metric.NewFunctionalGauge(metadata, func() int64 {
		ci := certFunc()
		if ci != nil && ci.Error == nil {
			sec := ci.ExpirationTime.Sub(ts.Now()).Seconds()
			if sec < 0 {
				return 0
			}
			return int64(math.Ceil(sec / secondsPerDay))
		}
		return 0
	})
}

var defaultTimeSource = timeutil.DefaultTimeSource{}

// createMetrics makes metrics using the certificate values on the manager.
// If the corresponding certificate is missing or invalid (Error != nil), we reset the
// metric to zero.
// The returned FunctionalGauge closures acquire cm.mu internally when read.
func createMetrics(cm *CertificateManager) *Metrics {
	ts := cm.timeSource
	if ts == nil {
		ts = defaultTimeSource
	}
	b := aggmetric.MakeBuilder(SQLUserLabel)
	return &Metrics{
		CAExpiration:         expirationGauge(metaCAExpiration, cm.CACert),
		TenantExpiration:     expirationGauge(metaTenantExpiration, cm.TenantCert),
		TenantCAExpiration:   expirationGauge(metaTenantCAExpiration, cm.TenantCACert),
		UIExpiration:         expirationGauge(metaUIExpiration, cm.UICert),
		UICAExpiration:       expirationGauge(metaUICAExpiration, cm.UICACert),
		ClientExpiration:     b.Gauge(metaClientExpiration),
		ClientCAExpiration:   expirationGauge(metaClientCAExpiration, cm.ClientCACert),
		NodeExpiration:       expirationGauge(metaNodeExpiration, cm.NodeCert),
		NodeClientExpiration: expirationGauge(metaNodeClientExpiration, cm.NodeClientCert),

		CATTL:         ttlGauge(metaCATTL, cm.CACert, ts),
		TenantTTL:     ttlGauge(metaTenantTTL, cm.TenantCert, ts),
		TenantCATTL:   ttlGauge(metaTenantCATTL, cm.TenantCACert, ts),
		UITTL:         ttlGauge(metaUITTL, cm.UICert, ts),
		UICATTL:       ttlGauge(metaUICATTL, cm.UICACert, ts),
		ClientTTL:     b.FunctionalGauge(metaClientTTL, func(cvs []int64) int64 { return 0 }),
		ClientCATTL:   ttlGauge(metaClientCATTL, cm.ClientCACert, ts),
		NodeTTL:       ttlGauge(metaNodeTTL, cm.NodeCert, ts),
		NodeClientTTL: ttlGauge(metaNodeClientTTL, cm.NodeClientCert, ts),

		CALastRotation:         metric.NewGauge(metaCALastRotation),
		ClientCALastRotation:   metric.NewGauge(metaClientCALastRotation),
		UICALastRotation:       metric.NewGauge(metaUICALastRotation),
		TenantCALastRotation:   metric.NewGauge(metaTenantCALastRotation),
		NodeLastRotation:       metric.NewGauge(metaNodeLastRotation),
		NodeClientLastRotation: metric.NewGauge(metaNodeClientLastRotation),
		UILastRotation:         metric.NewGauge(metaUILastRotation),
		TenantLastRotation:     metric.NewGauge(metaTenantLastRotation),

		CAExpiryDays:         expiryDaysGauge(metaCAExpiryDays, cm.CACert, ts),
		ClientCAExpiryDays:   expiryDaysGauge(metaClientCAExpiryDays, cm.ClientCACert, ts),
		UICAExpiryDays:       expiryDaysGauge(metaUICAExpiryDays, cm.UICACert, ts),
		TenantCAExpiryDays:   expiryDaysGauge(metaTenantCAExpiryDays, cm.TenantCACert, ts),
		NodeExpiryDays:       expiryDaysGauge(metaNodeExpiryDays, cm.NodeCert, ts),
		NodeClientExpiryDays: expiryDaysGauge(metaNodeClientExpiryDays, cm.NodeClientCert, ts),
		UIExpiryDays:         expiryDaysGauge(metaUIExpiryDays, cm.UICert, ts),
		TenantExpiryDays:     expiryDaysGauge(metaTenantExpiryDays, cm.TenantCert, ts),
	}
}
