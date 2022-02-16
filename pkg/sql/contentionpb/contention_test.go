// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package contentionpb

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestExtendedContentionEventHash(t *testing.T) {
	event1 := ExtendedContentionEvent{}
	event1.BlockingEvent.TxnMeta.ID = uuid.FastMakeV4()
	event1.WaitingTxnID = uuid.FastMakeV4()
	event1.CollectionTs = timeutil.Now()

	eventWithDifferentBlockingTxnID := event1
	eventWithDifferentBlockingTxnID.BlockingEvent.TxnMeta.ID = uuid.FastMakeV4()

	require.NotEqual(t, eventWithDifferentBlockingTxnID.Hash(), event1.Hash())

	eventWithDifferentWaitingTxnID := event1
	eventWithDifferentWaitingTxnID.WaitingTxnID = uuid.FastMakeV4()
	require.NotEqual(t, eventWithDifferentWaitingTxnID.Hash(), event1.Hash())

	eventWithDifferentCollectionTs := event1
	eventWithDifferentCollectionTs.CollectionTs = event1.CollectionTs.Add(time.Second)
	require.NotEqual(t, eventWithDifferentCollectionTs.Hash(), event1.Hash())
}
