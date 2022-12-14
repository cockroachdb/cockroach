// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { createMemoryHistory } from "history";
import { bindActionCreators, Store } from "redux";
import Long from "long";
import {
  ContentionDebugDispatchProps,
  ContentionDebugStateProps,
} from "@cockroachlabs/cluster-ui";
import { AdminUIState, createAdminUIStore } from "oss/src/redux/state";
import * as fakeApi from "src/util/fakeApi";
import { mapDispatchToProps, mapStateToProps } from "./redux";
import * as protos from "@cockroachlabs/crdb-protobuf-client";

const timestamp = new protos.google.protobuf.Timestamp({
  seconds: new Long(Date.parse("Nov 26 2021 01:00:00 GMT") * 1e-3),
});

class TestDriver {
  private readonly actions: ContentionDebugDispatchProps;
  private readonly properties: () => ContentionDebugStateProps;

  constructor(store: Store<AdminUIState>) {
    this.actions = bindActionCreators(
      mapDispatchToProps,
      store.dispatch.bind(store),
    );
    this.properties = () => mapStateToProps(store.getState());
  }

  async refreshTxnContentionEvents() {
    return this.actions.refreshTxnContentionEvents();
  }

  assertproperties(expected: ContentionDebugStateProps) {
    expect(this.properties()).toEqual(expected);
  }
}

describe("Contention Visualizer Page", function () {
  let driver: TestDriver;

  beforeEach(function () {
    driver = new TestDriver(createAdminUIStore(createMemoryHistory()));
  });

  afterEach(function () {
    fakeApi.restore();
  });

  it("initializes the visualizer", async function () {
    fakeApi.stubClusterSettings({
      key_values: {
        "sql.stats.automatic_collection.enabled": { value: "true" },
      },
    });

    await driver.refreshTxnContentionEvents();

    driver.assertproperties({
      contentionEvents: [],
      contentionError: null,
    });
  });

  it("makes a txn contentions events request", async function () {
    fakeApi.stubContentionEvents();

    await driver.refreshTxnContentionEvents();

    driver.assertproperties({
      contentionEvents: [
        {
          blockingTxnExecutionID: "9b7d1e63-840b-481b-a9c0-e7ef6ca7b783",
          waitingTxnExecutionID: "c66f7c81-290d-42f5-b689-db893a70379b",
          collectedAt: String(timestamp),
          waitingTxnFingerprintID: "0000000000000000",
          contentionDuration: "200",
        },
        {
          blockingTxnExecutionID: "b45364c7-8cbe-4a35-b2a1-dcbdfc2f4f5c",
          waitingTxnExecutionID: "669bf5c3-23a9-4aba-a71b-0cef75221488",
          collectedAt: String(timestamp),
          waitingTxnFingerprintID: "0000000000000000",
          contentionDuration: "200",
        },
        {
          blockingTxnExecutionID: "9b7d1e63-840b-481b-a9c0-e7ef6ca7b783",
          waitingTxnExecutionID: "c66f7c81-290d-42f5-b689-db893a70379b",
          collectedAt: String(timestamp),
          waitingTxnFingerprintID: "0000000000000000",
          contentionDuration: "200",
        },
        {
          blockingTxnExecutionID: "b45364c7-8cbe-4a35-b2a1-dcbdfc2f4f5c",
          waitingTxnExecutionID: "669bf5c3-23a9-4aba-a71b-0cef75221488",
          collectedAt: String(timestamp),
          waitingTxnFingerprintID: "0000000000000000",
          contentionDuration: "200",
        },
        {
          blockingTxnExecutionID: "9b7d1e63-840b-481b-a9c0-e7ef6ca7b783",
          waitingTxnExecutionID: "c66f7c81-290d-42f5-b689-db893a70379b",
          collectedAt: String(timestamp),
          waitingTxnFingerprintID: "0000000000000000",
          contentionDuration: "200",
        }
      ],
      contentionError: null,
    });
  })
});
