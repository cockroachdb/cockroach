// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
)

// storageVariantReg describes a test registration for a specific storage
// configuration. storageVariantRegs returns the base registration (default
// storage) as the first element, followed by one registration per applicable
// StorageVariant.
type storageVariantReg struct {
	// nameSuffix is appended to the base test name (e.g., "/vol=gp3").
	// Empty for the base registration.
	nameSuffix string
	// specOpts are additional spec.Option values to apply when constructing
	// the cluster spec for this variant.
	specOpts []spec.Option
	// clouds is the CompatibleClouds value for this variant.
	clouds registry.CloudSet
}

// storageVariantRegs returns the base registration plus one registration per
// applicable StorageVariant for the given base cloud set. Variants that are
// restricted to a cloud not in baseClouds are automatically excluded.
//
// skipLocalSSD should be set to true for tests that use DisableLocalSSD() or
// otherwise cannot run with local SSDs (e.g., tests requiring a specific
// VolumeSize). When true, local-SSD variants are excluded from the results.
//
// The first element is always the base registration (empty nameSuffix, no
// extra specOpts, original baseClouds). Subsequent elements are storage
// variants with their cloud set narrowed to the specific cloud.
func storageVariantRegs(baseClouds registry.CloudSet, skipLocalSSD bool) []storageVariantReg {
	regs := []storageVariantReg{
		{clouds: baseClouds},
	}
	for _, sv := range spec.StorageVariants() {
		if skipLocalSSD && sv.IsLocalSSD() {
			continue
		}
		if sv.Cloud != spec.AnyCloud && !baseClouds.Contains(sv.Cloud) {
			continue
		}
		variantClouds := baseClouds
		if sv.Cloud != spec.AnyCloud {
			variantClouds = registry.Clouds(sv.Cloud)
		}
		regs = append(regs, storageVariantReg{
			nameSuffix: "/" + sv.Name,
			specOpts:   sv.ClusterSpecOptions(),
			clouds:     variantClouds,
		})
	}
	return regs
}
