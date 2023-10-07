// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import {
  ContentionResponseColumns,
  getContentionDetailsApi,
  formatContentionResponseRow,
} from "./contentionApi";
import * as sqlApi from "./sqlApi";
import {
  formatApiResult,
  SqlApiResponse,
  SqlExecutionResponse,
} from "./sqlApi";
import {
  ContentionDetails,
  InsightExecEnum,
  InsightNameEnum,
} from "../insights";
import moment from "moment-timezone";

function mockSqlResponse<T>(rows: T[]): SqlExecutionResponse<T> {
  return {
    execution: {
      retries: 0,
      txn_results: [
        {
          tag: "",
          start: "",
          end: "",
          rows_affected: 0,
          statement: 1,
          rows: [...rows],
        },
      ],
    },
  };
}

type ContentionApiTest = {
  name: string;
  contentionResp: SqlExecutionResponse<ContentionResponseColumns>;
  expected: SqlApiResponse<ContentionDetails[]>;
};

describe("test contention details api", () => {
  const waitingTxnID = "1a2a4828-5fc6-42d1-ab93-fadd4a514b69";
  const contentionDetailsMock: ContentionResponseColumns = {
    contention_duration: "00:00:00.00866",
    waiting_stmt_id: "17761e953a52c0300000000000000001",
    waiting_stmt_fingerprint_id: "b75264458f6e2ef3",
    schema_name: "public",
    database_name: "system",
    table_name: "namespace",
    index_name: "primary",
    key: `/NamespaceTable/30/1/0/0/"movr"/4/1`,
    collection_ts: "2023-07-28 19:25:36.434081+00",
    blocking_txn_id: "a13773b3-9bca-4019-9cfb-a376d6a4f412",
    blocking_txn_fingerprint_id: "4329ab5f4493f82d",
    waiting_txn_id: waitingTxnID,
    waiting_txn_fingerprint_id: "1831d909096f992c",
  };

  afterEach(() => {
    jest.resetAllMocks();
    jest.clearAllMocks();
  });

  const testCases: ContentionApiTest[] = [
    {
      name: "api response empty",
      contentionResp: mockSqlResponse([]),
      expected: { maxSizeReached: false, results: [] },
    },
    {
      name: "api response non-empty",
      contentionResp: mockSqlResponse([contentionDetailsMock]),
      expected: formatApiResult(
        [formatContentionResponseRow(contentionDetailsMock)],
        undefined,
        "",
      ),
    },
  ];

  test.each(testCases)("$name", async (tc: ContentionApiTest) => {
    await jest
      .spyOn(sqlApi, "executeInternalSql")
      .mockReturnValueOnce(Promise.resolve(tc.contentionResp));

    const res = await getContentionDetailsApi({
      waitingTxnID: waitingTxnID,
    });
    expect(res).toEqual(tc.expected);
  });
});
