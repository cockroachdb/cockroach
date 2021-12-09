// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"bytes"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// prefixRewrite holds information for a single []byte replacement of a prefix.
type prefixRewrite struct {
	OldPrefix []byte
	NewPrefix []byte
	noop      bool
}

// prefixRewriter is a matcher for an ordered list of pairs of byte prefix
// rewrite rules.
type prefixRewriter struct {
	rewrites []prefixRewrite
	last     int
}

// rewriteKey modifies key using the first matching rule and returns
// it. If no rules matched, returns false and the original input key.
func (p prefixRewriter) rewriteKey(key []byte) ([]byte, bool) {
	if len(p.rewrites) < 1 {
		return key, false
	}

	found := p.last
	if !bytes.HasPrefix(key, p.rewrites[found].OldPrefix) {
		// since prefixes are sorted, we can binary search to find where a matching
		// prefix would be. We use the predicate HasPrefix (what we want) or greater
		// (after what we want) to search.
		found = sort.Search(len(p.rewrites), func(i int) bool {
			return bytes.HasPrefix(key, p.rewrites[i].OldPrefix) || bytes.Compare(key, p.rewrites[i].OldPrefix) < 0
		})
		if found == len(p.rewrites) || !bytes.HasPrefix(key, p.rewrites[found].OldPrefix) {
			return key, false
		}
	}

	p.last = found
	rewrite := p.rewrites[found]
	if rewrite.noop {
		return key, true
	}
	if len(rewrite.OldPrefix) == len(rewrite.NewPrefix) {
		copy(key[:len(rewrite.OldPrefix)], rewrite.NewPrefix)
		return key, true
	}
	// TODO(dan): Special case when key's cap() is enough.
	newKey := make([]byte, 0, len(rewrite.NewPrefix)+len(key)-len(rewrite.OldPrefix))
	newKey = append(newKey, rewrite.NewPrefix...)
	newKey = append(newKey, key[len(rewrite.OldPrefix):]...)
	return newKey, true
}

// KeyRewriter rewrites old table IDs to new table IDs. It is able to descend
// into interleaved keys, and is able to function on partial keys for spans
// and splits.
type KeyRewriter struct {
	codec keys.SQLCodec

	// lastKeyTenant is the tenant ID and prefix for the most recent old key.
	lastKeyTenant struct {
		id     roachpb.TenantID
		prefix []byte
	}

	// fromSystemTenant is true if the backup was produced by a system tenant,
	// which is important ni that it means any tenant prefixed keys belong to a
	// backup of that tenant.
	fromSystemTenant bool

	prefixes prefixRewriter
	tenants  prefixRewriter
	descs    map[descpb.ID]catalog.TableDescriptor
}

// makeKeyRewriterFromRekeys makes a KeyRewriter from Rekey protos.
func makeKeyRewriterFromRekeys(
	codec keys.SQLCodec, rekeys []execinfrapb.TableRekey, tenants []execinfrapb.TenantRekey,
) (*KeyRewriter, error) {
	descs := make(map[descpb.ID]catalog.TableDescriptor)
	for _, rekey := range rekeys {
		// The coordinator may insert a known invalid rekey for id 0 that we ignore
		// here since we support tenant-aware rekeying, but would cause a proc which
		// would otherwise silently incorrectly handle that rekey to error out now.
		if rekey.OldID == 0 {
			continue
		}
		var desc descpb.Descriptor
		if err := protoutil.Unmarshal(rekey.NewDesc, &desc); err != nil {
			return nil, errors.Wrapf(err, "unmarshalling rekey descriptor for old table id %d", rekey.OldID)
		}
		table, _, _, _ := descpb.FromDescriptor(&desc)
		if table == nil {
			return nil, errors.New("expected a table descriptor")
		}
		descs[descpb.ID(rekey.OldID)] = tabledesc.NewBuilder(table).BuildImmutableTable()
	}

	return makeKeyRewriter(codec, descs, tenants)
}

// makeKeyRewriter makes a KeyRewriter from a map of descs keyed by original ID.
func makeKeyRewriter(
	codec keys.SQLCodec,
	descs map[descpb.ID]catalog.TableDescriptor,
	tenants []execinfrapb.TenantRekey,
) (*KeyRewriter, error) {
	var prefixes prefixRewriter
	var tenantPrefixes prefixRewriter
	tenantPrefixes.rewrites = make([]prefixRewrite, len(tenants))

	seenPrefixes := make(map[string]bool)
	for oldID, desc := range descs {
		// The PrefixEnd() of index 1 is the same as the prefix of index 2, so use a
		// map to avoid duplicating entries.

		for _, index := range desc.NonDropIndexes() {
			oldPrefix := roachpb.Key(MakeKeyRewriterPrefixIgnoringInterleaved(oldID, index.GetID()))
			newPrefix := roachpb.Key(MakeKeyRewriterPrefixIgnoringInterleaved(desc.GetID(), index.GetID()))
			if !seenPrefixes[string(oldPrefix)] {
				seenPrefixes[string(oldPrefix)] = true
				prefixes.rewrites = append(prefixes.rewrites, prefixRewrite{
					OldPrefix: oldPrefix,
					NewPrefix: newPrefix,
					noop:      bytes.Equal(oldPrefix, newPrefix),
				})
			}
			// All the encoded data for a index will have the prefix just added, but
			// if you need to translate a half-open range describing that prefix
			// (and we do), the prefix end needs to be in the map too.
			oldPrefix = oldPrefix.PrefixEnd()
			newPrefix = newPrefix.PrefixEnd()
			if !seenPrefixes[string(oldPrefix)] {
				seenPrefixes[string(oldPrefix)] = true
				prefixes.rewrites = append(prefixes.rewrites, prefixRewrite{
					OldPrefix: oldPrefix,
					NewPrefix: newPrefix,
					noop:      bytes.Equal(oldPrefix, newPrefix),
				})
			}
		}
	}
	sort.Slice(prefixes.rewrites, func(i, j int) bool {
		return bytes.Compare(prefixes.rewrites[i].OldPrefix, prefixes.rewrites[j].OldPrefix) < 0
	})
	fromSystemTenant := false
	for i := range tenants {
		if tenants[i].OldID.IsSystem() && tenants[i].NewID.IsSystem() {
			fromSystemTenant = true
			continue
		}
		tenantPrefixes.rewrites[i] = prefixRewrite{
			OldPrefix: keys.MakeSQLCodec(tenants[i].OldID).TenantPrefix(),
			NewPrefix: keys.MakeSQLCodec(tenants[i].NewID).TenantPrefix(),
		}
		tenantPrefixes.rewrites[i].noop = bytes.Equal(
			tenantPrefixes.rewrites[i].OldPrefix, tenantPrefixes.rewrites[i].NewPrefix,
		)
	}
	sort.Slice(tenantPrefixes.rewrites, func(i, j int) bool {
		return bytes.Compare(tenantPrefixes.rewrites[i].OldPrefix, tenantPrefixes.rewrites[j].OldPrefix) < 0
	})
	return &KeyRewriter{
		codec:            codec,
		prefixes:         prefixes,
		descs:            descs,
		tenants:          tenantPrefixes,
		fromSystemTenant: fromSystemTenant,
	}, nil
}

// MakeKeyRewriterPrefixIgnoringInterleaved creates a table/index prefix for
// the given table and index IDs. sqlbase.MakeIndexKeyPrefix is a similar
// function, but it takes into account interleaved ancestors, which we don't
// want here.
func MakeKeyRewriterPrefixIgnoringInterleaved(tableID descpb.ID, indexID descpb.IndexID) []byte {
	return keys.SystemSQLCodec.IndexPrefix(uint32(tableID), uint32(indexID))
}

// RewriteKey modifies key (possibly in place), changing all table IDs to their
// new value.
func (kr *KeyRewriter) RewriteKey(key []byte) ([]byte, bool, error) {
	// If we are a system tenant backup tenants and this key is a tenant key, then
	// we only restore it if we have a tenant rekey for it, i.e. we're restoring
	// that tenant.
	if kr.fromSystemTenant && bytes.HasPrefix(key, keys.TenantPrefix) {
		k, ok := kr.tenants.rewriteKey(key)
		return k, ok, nil
	}

	// At this point we know we're not restoring a tenant, however the keys we're
	// restoring from could still have tenant prefixes if they were backed up _by_
	// a tenant, so we'll remove the prefix if any, rekey, and then encode with
	// our own tenant prefix, if any.
	noTenantPrefix, oldTenantID, err := keys.DecodeTenantPrefix(key)
	if err != nil {
		return nil, false, err
	}

	rekeyed, ok, err := kr.rewriteTableKey(noTenantPrefix)
	if err != nil {
		return nil, false, err
	}

	if kr.lastKeyTenant.id != oldTenantID {
		kr.lastKeyTenant.id = oldTenantID
		kr.lastKeyTenant.prefix = keys.MakeSQLCodec(oldTenantID).TenantPrefix()
	}

	newTenantPrefix := kr.codec.TenantPrefix()
	if len(newTenantPrefix) == len(kr.lastKeyTenant.prefix) {
		keyTenantPrefix := key[:len(kr.lastKeyTenant.prefix)]
		copy(keyTenantPrefix, newTenantPrefix)
		rekeyed = append(keyTenantPrefix, rekeyed...)
	} else {
		rekeyed = append(newTenantPrefix, rekeyed...)
	}

	return rekeyed, ok, err
}

// rewriteTableKey rewrites the table IDs in the key.
// It assumes that any tenant ID has been stripped from the key so it operates
// with the system codec. It is the responsibility of the caller to either
// remap, or re-prepend any required tenant prefix.
func (kr *KeyRewriter) rewriteTableKey(key []byte) ([]byte, bool, error) {
	// Fetch the original table ID for descriptor lookup. Ignore errors because
	// they will be caught later on if tableID isn't in descs or kr doesn't
	// perform a rewrite.
	_, tableID, _ := keys.SystemSQLCodec.DecodeTablePrefix(key)
	// Rewrite the first table ID.
	key, ok := kr.prefixes.rewriteKey(key)
	if !ok {
		return nil, false, nil
	}
	desc := kr.descs[descpb.ID(tableID)]
	if desc == nil {
		return nil, false, errors.Errorf("missing descriptor for table %d", tableID)
	}
	return key, true, nil
}
