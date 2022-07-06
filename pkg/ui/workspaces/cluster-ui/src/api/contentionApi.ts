// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { fetchData } from "./fetchData";

export type TransactionContentionEventsRequest =
  cockroach.server.serverpb.TransactionContentionEventsRequest;

export type TransactionContentionEventsResponse =
  cockroach.server.serverpb.TransactionContentionEventsResponse;

export type ITransactionContentionEventsResponse =
  cockroach.server.serverpb.ITransactionContentionEventsResponse;

const TRANSACTION_CONTENTION_PATH = "/_status/transactioncontentionevents";

export const getContentionTransactions =
  (): Promise<TransactionContentionEventsResponse> => {
    return fetchData(
      cockroach.server.serverpb.TransactionContentionEventsResponse,
      `${TRANSACTION_CONTENTION_PATH}`,
      null,
      null,
      "30M",
    );
  };
