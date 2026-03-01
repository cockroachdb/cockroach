// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ldrdecoder

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

type Transaction struct {
	Timestamp hlc.Timestamp
	WriteSet  []DecodedRow
}

type TxnDecoder struct {
	decoder tableDecoder
}

func NewTxnDecoder(
	ctx context.Context, descriptors descs.DB, settings *cluster.Settings, tables []TableMapping,
) (*TxnDecoder, error) {
	decoder, err := newTableDecoder(ctx, descriptors, settings, tables)
	if err != nil {
		return nil, err
	}
	return &TxnDecoder{
		decoder: decoder,
	}, nil
}

func (t *TxnDecoder) DecodeTxn(
	ctx context.Context, transaction []streampb.StreamEvent_KV,
) (Transaction, error) {
	if len(transaction) == 0 {
		return Transaction{}, errors.AssertionFailedf("empty transaction")
	}

	var result Transaction
	result.Timestamp = transaction[0].KeyValue.Value.Timestamp
	result.WriteSet = make([]DecodedRow, 0, len(transaction))

	for _, event := range transaction {
		decoded, _, err := t.decoder.decodeEvent(ctx, event)
		if err != nil {
			return Transaction{}, err
		}
		if decoded.RowTimestamp != result.Timestamp {
			return Transaction{}, errors.AssertionFailedf("inconsistent timestamps in transaction: got %s, expected %s",
				decoded.RowTimestamp, result.Timestamp)
		}
		result.WriteSet = append(result.WriteSet, decoded)
	}

	return result, nil
}
