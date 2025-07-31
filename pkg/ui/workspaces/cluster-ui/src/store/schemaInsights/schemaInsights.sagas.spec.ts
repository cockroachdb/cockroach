// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import moment from "moment-timezone";
import { expectSaga } from "redux-saga-test-plan";
import * as matchers from "redux-saga-test-plan/matchers";
import {
  EffectProviders,
  StaticProvider,
  throwError,
} from "redux-saga-test-plan/providers";

import { getSchemaInsights, SqlApiResponse } from "../../api";
import { InsightRecommendation } from "../../insights";

import {
  actions,
  reducer,
  SchemaInsightsState,
} from "./schemaInsights.reducer";
import {
  refreshSchemaInsightsSaga,
  requestSchemaInsightsSaga,
} from "./schemaInsights.sagas";

const lastUpdated = moment();

describe("SchemaInsights sagas", () => {
  let spy: jest.SpyInstance;
  beforeAll(() => {
    spy = jest.spyOn(moment, "utc").mockImplementation(() => lastUpdated);
  });

  afterAll(() => {
    spy.mockRestore();
  });

  const schemaInsights: InsightRecommendation[] = [
    {
      type: "DropIndex",
      database: "test_database",
      query: "DROP INDEX test_table@test_idx;",
      indexDetails: {
        table: "test_table",
        indexName: "test_idx",
        indexID: 1,
        schema: "public",
        lastUsed: "2022-08-22T22:30:02Z",
      },
    },
  ];

  const schemaInsightsResponse: SqlApiResponse<InsightRecommendation[]> = {
    maxSizeReached: false,
    results: schemaInsights,
  };

  const schemaInsightsAPIProvider: (EffectProviders | StaticProvider)[] = [
    [matchers.call.fn(getSchemaInsights), schemaInsightsResponse],
  ];

  describe("refreshSchemaInsightsSaga", () => {
    it("dispatches request Schema Insights action", () => {
      return expectSaga(refreshSchemaInsightsSaga, actions.request())
        .provide(schemaInsightsAPIProvider)
        .put(actions.request())
        .run();
    });
  });

  describe("requestSchemaInsightsSaga", () => {
    it("successfully requests schema insights", () => {
      return expectSaga(requestSchemaInsightsSaga, actions.request())
        .provide(schemaInsightsAPIProvider)
        .put(actions.received(schemaInsightsResponse))
        .withReducer(reducer)
        .hasFinalState<SchemaInsightsState>({
          data: schemaInsightsResponse,
          lastError: null,
          valid: true,
          lastUpdated,
        })
        .run();
    });

    it("returns error on failed request", () => {
      const error = new Error("Failed request");
      return expectSaga(requestSchemaInsightsSaga, actions.request())
        .provide([[matchers.call.fn(getSchemaInsights), throwError(error)]])
        .put(actions.failed(error))
        .withReducer(reducer)
        .hasFinalState<SchemaInsightsState>({
          data: null,
          lastError: error,
          valid: false,
          lastUpdated,
        })
        .run();
    });
  });
});
