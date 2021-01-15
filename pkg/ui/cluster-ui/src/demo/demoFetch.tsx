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
