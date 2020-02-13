// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { assert } from "chai";
import { shallow } from "enzyme";
import { createMemoryHistory, History } from "history";
import { match as Match } from "react-router-dom";

import "src/enzymeInit";
import { ClusterVisualization } from "./index";
import { Breadcrumbs } from "./breadcrumbs";

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
        />,
      );
      history.push("/overview/map");
      wrapper.update();
      assert.lengthOf(wrapper.find(Breadcrumbs).prop("tiers"), 0);
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
        />,
      );

      wrapper.update();
      const expectedTiers = [
        { key: "region", value: "us-west"},
        { key: "az", value: "a"},
      ];
      assert.deepEqual(wrapper.find(Breadcrumbs).prop("tiers"), expectedTiers);
    });
  });
});
