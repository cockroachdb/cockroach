// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigstore

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/errors"
)

// systemSpanConfigStore is an in-memory data structure to store system span
// configurations. It is not safe for concurrent use.
type systemSpanConfigStore struct {
	store map[spanconfig.SystemTarget]roachpb.SpanConfig
}

// newSystemSpanConfigStore constructs and returns a new systemSpanConfigStore.
func newSystemSpanConfigStore() *systemSpanConfigStore {
	store := systemSpanConfigStore{}
	store.store = make(map[spanconfig.SystemTarget]roachpb.SpanConfig)
	return &store
}

// apply takes an incremental set of system span config updates and applies them
// to the underlying store. It returns a list of targets deleted/records added.
//
// TODO(arul): Even though this is called from spanconfig.Store.Apply, which
// accepts a dryrun field, we don't accept a a dry run option here because it's
// unused; instead, we plan on removing it entirely.
func (s *systemSpanConfigStore) apply(
	updates ...spanconfig.Update,
) (deleted []spanconfig.SystemTarget, added []spanconfig.Record, _ error) {
	if err := s.validateApplyArgs(updates); err != nil {
		return nil, nil, err
	}

	for _, update := range updates {
		if update.Addition() {
			conf, found := s.store[update.Target.GetSystemTarget()]
			if found {
				if conf.Equal(update.Config) {
					// no-op.
					continue
				}
				deleted = append(deleted, update.Target.GetSystemTarget())
			}

			s.store[update.Target.GetSystemTarget()] = update.Config
			added = append(added, spanconfig.Record(update))
		}

		if update.Deletion() {
			_, found := s.store[update.Target.GetSystemTarget()]
			if !found {
				continue // no-op
			}
			delete(s.store, update.Target.GetSystemTarget())
			deleted = append(deleted, update.Target.GetSystemTarget())
		}
	}

	return deleted, added, nil
}

// combine takes a key and an associated span configuration and combines it with
// all system span configs that apply to the given key.
func (s *systemSpanConfigStore) combine(
	key roachpb.RKey, config roachpb.SpanConfig,
) (roachpb.SpanConfig, error) {
	_, tenID, err := keys.DecodeTenantPrefix(roachpb.Key(key))
	if err != nil {
		return roachpb.SpanConfig{}, err
	}

	hostSetOnTenant, err := spanconfig.MakeTenantKeyspaceTarget(
		roachpb.SystemTenantID, tenID,
	)
	if err != nil {
		return roachpb.SpanConfig{}, err
	}
	// Construct a list of system targets that apply to the key given its tenant
	// prefix. These are:
	// 1. The system span config that applies over the entire keyspace.
	// 2. The system span config set by the host over the tenant's keyspace.
	// 3. The system span config set by the tenant itself over its own keyspace.
	targets := []spanconfig.SystemTarget{
		spanconfig.MakeEntireKeyspaceTarget(),
		hostSetOnTenant,
	}

	// We only need to do this for secondary tenants; we've already added this
	// target if tenID == system tenant.
	if tenID != roachpb.SystemTenantID {
		target, err := spanconfig.MakeTenantKeyspaceTarget(tenID, tenID)
		if err != nil {
			return roachpb.SpanConfig{}, err
		}
		targets = append(targets, target)
	}

	for _, target := range targets {
		systemSpanConfig, found := s.store[target]
		if found {
			config.GCPolicy.ProtectionPolicies = append(
				config.GCPolicy.ProtectionPolicies, systemSpanConfig.GCPolicy.ProtectionPolicies...,
			)
		}
	}
	return config, nil
}

// copy returns a copy of the system span config store.
func (s *systemSpanConfigStore) copy() *systemSpanConfigStore {
	clone := newSystemSpanConfigStore()
	for k := range s.store {
		clone.store[k] = s.store[k]
	}
	return clone
}

// iterate goes through all system span config entries in sorted order.
func (s *systemSpanConfigStore) iterate(f func(record spanconfig.Record) error) error {
	// Iterate through in sorted order.
	targets := make([]spanconfig.Target, 0, len(s.store))
	for k := range s.store {
		targets = append(targets, spanconfig.MakeTargetFromSystemTarget(k))
	}
	sort.Sort(spanconfig.Targets(targets))

	for _, target := range targets {
		if err := f(spanconfig.Record{
			Target: target,
			Config: s.store[target.GetSystemTarget()],
		}); err != nil {
			return err
		}
	}
	return nil
}

// validateApplyArgs ensures that updates can be applied to the system span
// config store. In particular, it checks that the updates target system targets
// and duplicate targets do not exist.
func (s *systemSpanConfigStore) validateApplyArgs(updates []spanconfig.Update) error {
	for _, update := range updates {
		if !update.Target.IsSystemTarget() {
			return errors.AssertionFailedf("expected update to system target update")
		}

		if update.Target.GetSystemTarget().IsReadOnly() {
			return errors.AssertionFailedf("invalid system target update")
		}
	}

	sorted := make([]spanconfig.Update, len(updates))
	copy(sorted, updates)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Target.Less(sorted[j].Target)
	})
	updates = sorted // re-use the same variable

	for i := range updates {
		if i == 0 {
			continue
		}

		if updates[i].Target.Equal(updates[i-1].Target) {
			return errors.Newf(
				"found duplicate updates %s and %s",
				updates[i-1].Target,
				updates[i].Target,
			)
		}
	}
	return nil
}
