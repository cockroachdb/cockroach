// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import _ from "lodash";
import React from "react";
import { assert } from "chai";
import { Action, Store } from "redux";
import { createMemoryHistory } from "react-router";
import { syncHistoryWithStore } from "react-router-redux";
import { mount, ReactWrapper } from "enzyme";

import "src/enzymeInit";
import { App } from "src/app";
import { AdminUIState, createAdminUIStore, History } from "src/redux/state";

import ClusterOverview from "src/views/cluster/containers/clusterOverview";
import NodeList from "src/views/clusterviz/containers/map/nodeList";
import { ClusterVisualization } from "src/views/clusterviz/containers/map";

describe("Routing to", () => {
  let history: History;
  let store: Store<AdminUIState, Action>;
  let appWrapper: ReactWrapper;

  beforeEach(() => {
    store = createAdminUIStore();
  });

  const initAppWithPath = (path: string) => {
    const memoryHistory = createMemoryHistory({
      entries: [path],
    });
    history = syncHistoryWithStore(memoryHistory, store);
    appWrapper = mount(<App history={history} store={store}/>);
  };

  describe("'/' path", () => {
    it("routes to <ClusterOverview> component", () => {
      initAppWithPath("/");
      assert.lengthOf(appWrapper.find(ClusterOverview), 1);
    });
  });

  describe("'/overview' path", () => {
    it("routes to <ClusterOverview> component", () => {
      initAppWithPath("/overview");
      assert.lengthOf(appWrapper.find(ClusterOverview), 1);
    });
  });

  describe("'/overview/list' path", () => {
    it("routes to <NodeList> component", () => {
      initAppWithPath("/overview");
      const clusterOverview = appWrapper.find(ClusterOverview);
      assert.lengthOf(clusterOverview, 1);
      const nodeList = clusterOverview.find(NodeList);
      assert.lengthOf(nodeList, 1);
    });
  });

  describe("'/overview/map' path", () => {
    it("routes to <ClusterViz> component", () => {
      initAppWithPath("/overview/map");
      const clusterOverview = appWrapper.find(ClusterOverview);
      const clusterViz = appWrapper.find(ClusterVisualization);
      assert.lengthOf(clusterOverview, 1);
      assert.lengthOf(clusterViz, 1);
    });
  });
});
