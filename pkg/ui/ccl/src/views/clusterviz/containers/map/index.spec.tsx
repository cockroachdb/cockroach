// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

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
        { key: "region", value: "us-west" },
        { key: "az", value: "a" },
      ];
      assert.deepEqual(wrapper.find(Breadcrumbs).prop("tiers"), expectedTiers);
    });
  });
});
