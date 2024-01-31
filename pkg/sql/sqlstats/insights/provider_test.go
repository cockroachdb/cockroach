// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package insights

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/stretchr/testify/require"
)

func TestProvider_WriterIgnoresInternalExecutorObservations(t *testing.T) {
	settings := cluster.MakeTestingClusterSettings()
	store := newStore(settings)
	ingester := newConcurrentBufferIngester(newRegistry(settings, &fakeDetector{stubEnabled: true}, store))
	provider := &defaultProvider{store: store, ingester: ingester}
	writer := provider.Writer(true /* internal */)
	writer.ObserveStatement(clusterunique.ID{}, &Statement{})
	writer.ObserveTransaction(clusterunique.ID{}, &Transaction{})
	require.Equal(t, event{}, ingester.guard.eventBuffer[0])
}
