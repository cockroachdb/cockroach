// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-06-07
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// http://www.apache.org/licenses/LICENSE-2.0

package cluster

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestVersionsAreValid(t *testing.T) {
	defer leaktest.AfterTest(t)()

	require.NoError(t, versionsSingleton.Validate())
}
