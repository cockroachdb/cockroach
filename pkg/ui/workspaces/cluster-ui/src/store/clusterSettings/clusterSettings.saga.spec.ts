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
  actions,
  ClusterSettingsState,
  reducer,
} from "./clusterSettings.reducer";
import {
  getClusterSettings,
  SettingsRequestMessage,
} from "../../api/clusterSettingsApi";
import {
  refreshClusterSettingsSaga,
  requestClusterSettingsSaga,
} from "./clusterSettings.saga";

describe("ClusterSettings sagas", () => {
  const requestAction: PayloadAction<SettingsRequestMessage> = {
    payload: null,
    type: "request",
  };
  const clusterSettingsResponse =
    new cockroach.server.serverpb.SettingsResponse({
      key_values: {
        key: {
          value: "value",
          type: "string",
          description: "i am a cluster setting",
          public: false,
        },
        key2: {
          value: "value2",
          type: "string",
          description: "i am a public cluster setting",
          public: true,
        },
      },
    });
  const clusterSettingsAPIProvider: (EffectProviders | StaticProvider)[] = [
    [matchers.call.fn(getClusterSettings), clusterSettingsResponse],
  ];

  describe("refreshClusterSettingsSaga", () => {
    it("dispatches request ClusterSettings action", () => {
      return expectSaga(refreshClusterSettingsSaga, requestAction)
        .put(actions.request(requestAction.payload))
        .run();
    });
  });

  describe("requestClusterSettingsSaga", () => {
    it("successfully requests cluster settings", () => {
      return expectSaga(requestClusterSettingsSaga, requestAction)
        .provide(clusterSettingsAPIProvider)
        .put(actions.received(clusterSettingsResponse))
        .withReducer(reducer)
        .hasFinalState<ClusterSettingsState>({
          data: clusterSettingsResponse,
          lastError: null,
          valid: true,
          inFlight: false,
        })
        .run();
    });

    it("returns error on failed request", () => {
      const error = new Error("Failed request");
      return expectSaga(requestClusterSettingsSaga, requestAction)
        .provide([[matchers.call.fn(getClusterSettings), throwError(error)]])
        .put(actions.failed(error))
        .withReducer(reducer)
        .hasFinalState<ClusterSettingsState>({
          data: null,
          lastError: error,
          valid: false,
          inFlight: false,
        })
        .run();
    });
  });
});
