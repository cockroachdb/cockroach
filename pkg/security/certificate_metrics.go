// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package security

import (
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/metric/aggmetric"
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
)

func makeMetrics() Metrics {
	b := aggmetric.MakeBuilder(SQLUserLabel)
	m := Metrics{
		CAExpiration:         metric.NewGauge(metaCAExpiration),
		ClientCAExpiration:   metric.NewGauge(metaClientCAExpiration),
		TenantCAExpiration:   metric.NewGauge(metaTenantCAExpiration),
		UICAExpiration:       metric.NewGauge(metaUICAExpiration),
		ClientExpiration:     b.Gauge(metaClientExpiration),
		TenantExpiration:     metric.NewGauge(metaTenantExpiration),
		NodeExpiration:       metric.NewGauge(metaNodeExpiration),
		NodeClientExpiration: metric.NewGauge(metaNodeClientExpiration),
		UIExpiration:         metric.NewGauge(metaUIExpiration),
	}
	return m
}
