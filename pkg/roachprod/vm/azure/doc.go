// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package azure allows roachprod to create instances within the
// Microsoft Azure cloud.
//
// Much of the REST API code in this package is based off of
// https://github.com/Azure-Samples/azure-sdk-for-go-samples The API
// package is automatically generated from a REST API definition.
//
// Azure terminology differs somewhat from GCP and AWS. The top-level
// container for resources and billing is is a "Subscription". All
// computational resources (VMs, IPs, network configs) belong to a
// "Resource Group", which lives in a particular "Location", such as
// "eastus", and which are roughly equivalent to other cloud provider's
// regions. Individual availability zones don't surface directly unless
// replica sets are used. Roachprod uses resource groups for lifecycle
// management, allowing all resources used by a cluster to be extended
// or deleted at once.
//
// TODO(bob): Add support for deploying with replica sets.
//
// The following resources are created for each cluster:
//
//  Roachprod "commons"
//  | Resource Group (one per Location / Region)
//  |   VNet          (10.<offset>/16)
//  |     Subnet      (10.<offset>/18 range)
//
//  Per cluster
//  | Resource Group (one per Location / Region)
//  |   []IPAddress   (public IP address for each VM)
//  |   []NIC         (bound to IPAddress and to a common Subnet)
//  |   []VM          (bound to a NIC)
//  |     OSDisk      (100GB, standard SSD storage)
//
// Roachprod creates a "common" resource group, VNet, and Subnet for
// each location that clusters may be deployed into. Each NIC that is
// created will be bound to a common subnet. All of the managed VNets
// are peered together. This allows arbitrary connectivity between
// roachprod-managed clusters (e.g. to test cluster migration
// strategies).
package azure
