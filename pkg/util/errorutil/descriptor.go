// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package errorutil

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
)

const descriptorNotFound = "unable to look up descriptor for"

func IsDescriptorNotFoundError(err error) bool {
	return strings.HasPrefix(err.Error(), descriptorNotFound)
}

func NewNodeNotFoundError(nodeID roachpb.NodeID) error {
	return errors.Errorf("%s n%d", descriptorNotFound, nodeID)
}

func NewStoreNotFoundError(storeID roachpb.StoreID) error {
	return errors.Errorf("%s storeID %d", descriptorNotFound, storeID)
}
