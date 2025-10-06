// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { PayloadAction } from "@reduxjs/toolkit";
import moment from "moment";
import { expectSaga } from "redux-saga-test-plan";
import * as matchers from "redux-saga-test-plan/matchers";
import {
  EffectProviders,
  StaticProvider,
  throwError,
} from "redux-saga-test-plan/providers";

import {
  TableDetailsResponse,
  getTableDetails,
  SqlApiResponse,
  TableDetailsReqParams,
} from "../../api";
import { generateTableID, indexUnusedDuration } from "../../util";

import {
  actions,
  KeyedTableDetailsState,
  reducer,
} from "./tableDetails.reducer";
import {
  refreshTableDetailsSaga,
  requestTableDetailsSaga,
} from "./tableDetails.saga";

import ZoneConfig = cockroach.config.zonepb.ZoneConfig;
import ZoneConfigurationLevel = cockroach.server.serverpb.ZoneConfigurationLevel;

describe("TableDetails sagas", () => {
  const database = "test_db";
  const table = "test_table";
  const key = generateTableID(database, table);
  const requestAction: PayloadAction<TableDetailsReqParams> = {
    payload: { database, table, csIndexUnusedDuration: indexUnusedDuration },
    type: "request",
  };
  const tableDetailsResponse: SqlApiResponse<TableDetailsResponse> = {
    maxSizeReached: false,
    results: {
      idResp: { table_id: "mock_table_id" },
      createStmtResp: { create_statement: "CREATE TABLE test_table (num int)" },
      grantsResp: {
        grants: [
          {
            user: "test_user",
            privileges: ["privilege1", "privilege2", "privilege3"],
          },
          {
            user: "another_user",
            privileges: ["privilege1", "privilege4", "privilege7"],
          },
        ],
      },
      schemaDetails: {
        columns: ["col1", "col2", "col3"],
        indexes: ["idx1", "idx2", "idx3"],
      },
      zoneConfigResp: {
        configure_zone_statement: "",
        zone_config: new ZoneConfig({
          inherited_constraints: true,
          inherited_lease_preferences: true,
        }),
        zone_config_level: ZoneConfigurationLevel.CLUSTER,
      },
      heuristicsDetails: { stats_last_created_at: moment() },
      stats: {
        spanStats: {
          approximate_disk_bytes: 400,
          live_bytes: 30,
          total_bytes: 40,
          range_count: 50,
          live_percentage: 75,
        },
        replicaData: {
          storeIDs: [1, 2, 3],
          replicaCount: 3,
        },
        indexStats: {
          has_index_recommendations: false,
        },
      },
    },
  };

  const tableDetailsAPIProvider: (EffectProviders | StaticProvider)[] = [
    [matchers.call.fn(getTableDetails), tableDetailsResponse],
  ];

  describe("refreshTableDetailsSaga", () => {
    it("dispatches request TableDetails action", () => {
      return expectSaga(refreshTableDetailsSaga, requestAction)
        .put(actions.request(requestAction.payload))
        .run();
    });
  });

  describe("requestTableDetailsSaga", () => {
    it("successfully requests table details", () => {
      return expectSaga(requestTableDetailsSaga, requestAction)
        .provide(tableDetailsAPIProvider)
        .put(
          actions.received({
            tableDetailsResponse,
            key,
          }),
        )
        .withReducer(reducer)
        .hasFinalState<KeyedTableDetailsState>({
          [key]: {
            data: tableDetailsResponse,
            lastError: null,
            valid: true,
            inFlight: false,
          },
        })
        .run();
    });

    it("returns error on failed request", () => {
      const error = new Error("Failed request");
      return expectSaga(requestTableDetailsSaga, requestAction)
        .provide([[matchers.call.fn(getTableDetails), throwError(error)]])
        .put(
          actions.failed({
            err: error,
            key,
          }),
        )
        .withReducer(reducer)
        .hasFinalState<KeyedTableDetailsState>({
          [key]: {
            data: null,
            lastError: error,
            valid: false,
            inFlight: false,
          },
        })
        .run();
    });
  });
});
