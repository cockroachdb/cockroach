// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { shallow } from "enzyme";
import { createMemoryHistory, History } from "history";
import React from "react";
import { match as Match } from "react-router-dom";

import { refreshCluster } from "src/redux/apiReducers";

import { Breadcrumbs } from "./breadcrumbs";

import { ClusterVisualization } from "./index";

describe("ClusterVisualization", () => {
  describe("parse tiers params from URL path", () => {
    let history: History;
    let match: Match;

    beforeEach(() => {
      history = createMemoryHistory();
      match = {
        path: "/overview/map",
        params: {},
        url: "http://localhost/overview/map",
        isExact: true,
      };
    });

    // parsed tiers from params are not stored in state and passed directly to <Breadcrumbs />
    // component so we can validate the parsed result by checking Breadcrumbs props.
    it("parses tiers as empty array for /overview/map path", () => {
      const wrapper = shallow(
        <ClusterVisualization
          history={history}
          location={history.location}
          clusterDataError={null}
          enterpriseEnabled={true}
          licenseDataExists={true}
          match={match}
          refreshCluster={refreshCluster}
        />,
      );
      history.push("/overview/map");
      wrapper.update();
      expect(wrapper.find(Breadcrumbs).prop("tiers").length).toBe(0);
    });

    it("parses multiple tiers in path for `/overview/map/region=us-west/az=a` path", () => {
      history.push("/overview/map/region=us-west/az=a");
      const wrapper = shallow(
        <ClusterVisualization
          history={history}
          location={history.location}
          clusterDataError={null}
          enterpriseEnabled={true}
          licenseDataExists={true}
          match={match}
          refreshCluster={refreshCluster}
        />,
      );

      wrapper.update();
      const expectedTiers = [
        { key: "region", value: "us-west" },
        { key: "az", value: "a" },
      ];
      expect(wrapper.find(Breadcrumbs).prop("tiers")).toEqual(expectedTiers);
    });
  });
});
