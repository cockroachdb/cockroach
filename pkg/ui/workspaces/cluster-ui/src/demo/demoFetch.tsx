// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
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
