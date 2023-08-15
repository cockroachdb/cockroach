// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { PayloadAction } from "@reduxjs/toolkit";
import {
  EffectProviders,
  StaticProvider,
  throwError,
} from "redux-saga-test-plan/providers";
import * as matchers from "redux-saga-test-plan/matchers";
import { expectSaga } from "redux-saga-test-plan";
import {
  DatabaseDetailsReqParams,
  DatabaseDetailsResponse,
  getDatabaseDetails,
  SqlApiResponse,
} from "../../api";
import ZoneConfig = cockroach.config.zonepb.ZoneConfig;
import ZoneConfigurationLevel = cockroach.server.serverpb.ZoneConfigurationLevel;
import {
  refreshDatabaseDetailsSaga,
  requestDatabaseDetailsSaga,
} from "./databaseDetails.saga";
import {
  actions,
  KeyedDatabaseDetailsState,
  reducer,
} from "./databaseDetails.reducer";
import { indexUnusedDuration } from "src/util/constants";

describe("DatabaseDetails sagas", () => {
  const database = "test_db";
  const csIndexUnusedDuration = indexUnusedDuration;
  const requestAction: PayloadAction<DatabaseDetailsReqParams> = {
    payload: { database, csIndexUnusedDuration },
    type: "request",
  };
  const databaseDetailsResponse: SqlApiResponse<DatabaseDetailsResponse> = {
    maxSizeReached: false,
    results: {
      idResp: { database_id: "mock_id" },
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
      tablesResp: { tables: ["yet", "another", "table"] },
      zoneConfigResp: {
        zone_config: new ZoneConfig({
          inherited_constraints: true,
          inherited_lease_preferences: true,
        }),
        zone_config_level: ZoneConfigurationLevel.CLUSTER,
      },
      stats: {
        spanStats: {
          approximate_disk_bytes: 1000,
          live_bytes: 100,
          total_bytes: 500,
          range_count: 20,
        },
        replicaData: {
          replicas: [1, 2, 3],
          regions: ["this", "is", "a", "region"],
        },
        indexStats: { num_index_recommendations: 4 },
      },
    },
  };
  const databaseDetailsAPIProvider: (EffectProviders | StaticProvider)[] = [
    [matchers.call.fn(getDatabaseDetails), databaseDetailsResponse],
  ];

  describe("refreshDatabaseDetailsSaga", () => {
    it("dispatches request DatabaseDetails action", () => {
      return expectSaga(refreshDatabaseDetailsSaga, requestAction)
        .put(actions.request(requestAction.payload))
        .run();
    });
  });

  describe("requestDatabaseDetailsSaga", () => {
    it("successfully requests database details", () => {
      return expectSaga(requestDatabaseDetailsSaga, requestAction)
        .provide(databaseDetailsAPIProvider)
        .put(
          actions.received({
            databaseDetailsResponse,
            key: database,
          }),
        )
        .withReducer(reducer)
        .hasFinalState<KeyedDatabaseDetailsState>({
          [database]: {
            data: databaseDetailsResponse,
            lastError: null,
            valid: true,
            inFlight: false,
          },
        })
        .run();
    });

    it("returns error on failed request", () => {
      const error = new Error("Failed request");
      return expectSaga(requestDatabaseDetailsSaga, requestAction)
        .provide([[matchers.call.fn(getDatabaseDetails), throwError(error)]])
        .put(
          actions.failed({
            err: error,
            key: database,
          }),
        )
        .withReducer(reducer)
        .hasFinalState<KeyedDatabaseDetailsState>({
          [database]: {
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
