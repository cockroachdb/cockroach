// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import React from "react";

import { fetchData } from "src/api";

type StatementsResponse = cockroach.server.serverpb.StatementsResponse;

export const DemoFetch: React.FC = () => {
  const [response, setResponse] = React.useState<StatementsResponse>(null);

  React.useEffect(() => {
    fetchData(
      cockroach.server.serverpb.StatementsResponse,
      "_status/statements",
    ).then(setResponse);
  }, []);

  return <code>{JSON.stringify(response).slice(0, 255)}...</code>;
};
