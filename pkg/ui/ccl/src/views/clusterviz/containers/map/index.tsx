// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

import React from "react";
import { InjectedRouter, RouterState } from "react-router";

import { Breadcrumbs } from "src/views/clusterviz/containers/map/breadcrumbs";
import NeedEnterpriseLicense from "src/views/clusterviz/containers/map/needEnterpriseLicense";
import NodeCanvasContainer from "src/views/clusterviz/containers/map/nodeCanvasContainer";
import TimeScaleDropdown from "src/views/cluster/containers/timescale";
import Dropdown, { DropdownOption } from "src/views/shared/components/dropdown";
import swapByLicense from "src/views/shared/containers/licenseSwap";
import { parseLocalityRoute } from "src/util/localities";
import Loading from "src/views/shared/components/loading";
import { connect } from "react-redux";
import { AdminUIState } from "src/redux/state";
import { selectEnterpriseEnabled } from "src/redux/license";
import "./tweaks.styl";

// tslint:disable-next-line:variable-name
const NodeCanvasContent = swapByLicense(NeedEnterpriseLicense, NodeCanvasContainer);

interface ClusterVisualizationProps {
  licenseDataExists: boolean;
  enterpriseEnabled: boolean;
  clusterDataError: Error | null;
}

class ClusterVisualization extends React.Component<ClusterVisualizationProps & RouterState & { router: InjectedRouter }> {
  handleMapTableToggle = (opt: DropdownOption) => {
    this.props.router.push(`/overview/${opt.value}`);
  }

  render() {
    const tiers = parseLocalityRoute(this.props.params.splat);
    const options: DropdownOption[] = [
      { value: "map", label: "Node Map" },
      { value: "list", label: "Node List" },
    ];

    // TODO(couchand): integrate with license swapper
    const showingLicensePage = this.props.licenseDataExists && !this.props.enterpriseEnabled;

    // TODO(vilterp): dedup with NodeList
    return (
      <div
        style={{
          width: "100%",
          height: showingLicensePage ? null : "100%",
          display: "flex",
          flexDirection: "column",
          overflow: "hidden",
          backgroundColor: "white",
        }}
        className="clusterviz"
      >
        <div style={{
          flex: "none",
          backgroundColor: "white",
          boxShadow: "0 0 4px 0 rgba(0, 0, 0, 0.2)",
          zIndex: 5,
          padding: "4px 12px",
        }}>
          <div style={{ float: "left" }}>
            <Dropdown
              title="View"
              selected="map"
              options={options}
              onChange={this.handleMapTableToggle}
            />
          </div>
          <div style={{ float: "right", display: showingLicensePage ? "none" : null }}><TimeScaleDropdown /></div>
          <div style={{ textAlign: "center", paddingTop: 4, display: showingLicensePage ? "none" : null }}><Breadcrumbs tiers={tiers} /></div>
        </div>
        <Loading
          loading={!this.props.licenseDataExists}
          error={this.props.clusterDataError}
          render={() => <NodeCanvasContent tiers={tiers} />}
        />
      </div>
    );
  }
}

function mapStateToProps(state: AdminUIState) {
  return {
    licenseDataExists: !!state.cachedData.cluster.data,
    enterpriseEnabled: selectEnterpriseEnabled(state),
    clusterDataError: state.cachedData.cluster.lastError,
  };
}

export default connect(mapStateToProps)(ClusterVisualization);
