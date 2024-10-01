// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package serverrules

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/gogo/protobuf/proto"
)

const (
	nodeRestartRuleName      = "NodeRestart"
	nodeCACertExpiryRuleName = "NodeCACertExpiry"
	nodeCertExpiryRuleName   = "NodeCertExpiry"
)

// CreateAndAddRules initializes all server metric rules and adds them
// to the rule registry for tracking. All rules are exported in the
// YAML format.
func CreateAndAddRules(ctx context.Context, ruleRegistry *metric.RuleRegistry) {
	createAndRegisterNodeRestartRule(ctx, ruleRegistry)
	createAndRegisterNodeCACertExpiryRule(ctx, ruleRegistry)
	createAndRegisterNodeCertExpiryRule(ctx, ruleRegistry)
}

func createAndRegisterNodeRestartRule(ctx context.Context, ruleRegistry *metric.RuleRegistry) {
	expr := "resets(sys_uptime[10m]) > 5"
	annotations := []metric.LabelPair{{
		Name:  proto.String("summary"),
		Value: proto.String("Instance {{ $labels.instance }} restarted"),
	}, {
		Name:  proto.String("description"),
		Value: proto.String("{{ $labels.instance }} for cluster {{ $labels.cluster }} restarted {{ $value }} time(s) in 10m"),
	}}
	help := "Alert if a node restarts multiple times in a short span of time."
	nodeRestartRule, err := metric.NewAlertingRule(
		nodeRestartRuleName,
		expr,
		annotations,
		nil,
		time.Duration(0),
		help,
		false,
	)
	kvserver.MaybeAddRuleToRegistry(ctx, err, nodeRestartRuleName, nodeRestartRule, ruleRegistry)
}

func createAndRegisterNodeCACertExpiryRule(ctx context.Context, ruleRegistry *metric.RuleRegistry) {
	expr := "(security_certificate_expiration_ca > 0) and (security_certificate_expiration_ca - time()) < 86400 * 366"
	help := "Alert when the CA certificate on a node will expire in less than a year"
	annotations := []metric.LabelPair{{
		Name:  proto.String("summary"),
		Value: proto.String("CA certificate for {{ $labels.instance }} expires in less than a year"),
	}}
	labels := []metric.LabelPair{{
		Name:  proto.String("frequency"),
		Value: proto.String("daily"),
	}}
	nodeCACertExpiryRule, err := metric.NewAlertingRule(nodeCACertExpiryRuleName, expr, annotations, labels, time.Duration(0), help, false)
	kvserver.MaybeAddRuleToRegistry(ctx, err, nodeCACertExpiryRuleName, nodeCACertExpiryRule, ruleRegistry)
}

func createAndRegisterNodeCertExpiryRule(ctx context.Context, ruleRegistry *metric.RuleRegistry) {
	expr := "(security_certificate_expiration_node > 0) and (security_certificate_expiration_node - time()) < 86400 * 183"
	help := "Alert when a node certificate will expire in 6 months"
	annotations := []metric.LabelPair{{
		Name:  proto.String("summary"),
		Value: proto.String("Node certificate for {{ $labels.instance }} expires in less than 6 months"),
	}}
	labels := []metric.LabelPair{{
		Name:  proto.String("frequency"),
		Value: proto.String("daily"),
	}}
	nodeCertExpiryRule, err := metric.NewAlertingRule(nodeCertExpiryRuleName, expr, annotations, labels, time.Duration(0), help, false)
	kvserver.MaybeAddRuleToRegistry(ctx, err, nodeCertExpiryRuleName, nodeCertExpiryRule, ruleRegistry)
}
