// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Loading, useCluster } from "@cockroachlabs/cluster-ui";
import cn from "classnames";
import React, { useCallback } from "react";
import { useLocation, useRouteMatch } from "react-router";
import { useHistory } from "react-router-dom";

import { Dropdown } from "src/components/dropdown";
import { parseLocalityRoute } from "src/util/localities";
import { parseSplatParams } from "src/util/parseSplatParams";
import TimeScaleDropdown from "src/views/cluster/containers/timeScaleDropdownWithSearchParams";
import { Breadcrumbs } from "src/views/clusterviz/containers/map/breadcrumbs";
import NeedEnterpriseLicense from "src/views/clusterviz/containers/map/needEnterpriseLicense";
import NodeCanvasContainer from "src/views/clusterviz/containers/map/nodeCanvasContainer";
import swapByLicense from "src/views/shared/containers/licenseSwap";

const NodeCanvasContent = swapByLicense(
  NeedEnterpriseLicense,
  NodeCanvasContainer,
);

const items = [
  { value: "list", name: "Node List" },
  { value: "map", name: "Node Map" },
];

export default function ClusterVisualization() {
  const { data, error } = useCluster();
  const history = useHistory();
  const match = useRouteMatch();
  const location = useLocation();
  const handleMapTableToggle = useCallback(
    (value: string) => {
      history.push(`/overview/${value}`);
    },
    [history],
  );

  const splat = parseSplatParams(match, location);
  const tiers = parseLocalityRoute(splat);

  const licenseDataExists = !!data;
  const enterpriseEnabled = data?.enterprise_enabled ?? false;

  // TODO(couchand): integrate with license swapper
  const showingLicensePage = licenseDataExists && !enterpriseEnabled;

  const classes = cn("cluster-visualization-layout", {
    "cluster-visualization-layout--show-license": showingLicensePage,
  });

  const contentItemClasses = cn("cluster-visualization-layout__content-item", {
    "cluster-visualization-layout__content-item--show-license":
      showingLicensePage,
  });

  // TODO(vilterp): dedup with NodeList
  return (
    <div className={classes}>
      <div className="cluster-visualization-layout__content">
        <div className="cluster-visualization-layout__content-item">
          <Dropdown items={items} onChange={handleMapTableToggle}>
            Node Map
          </Dropdown>
        </div>
        <div className={contentItemClasses}>
          <Breadcrumbs tiers={tiers} />
        </div>
        <div className={contentItemClasses}>
          <TimeScaleDropdown />
        </div>
      </div>
      <Loading
        loading={!licenseDataExists}
        page={"containers"}
        error={error}
        render={() => <NodeCanvasContent tiers={tiers} />}
      />
    </div>
  );
}
