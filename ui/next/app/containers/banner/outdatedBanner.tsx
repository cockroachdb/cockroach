/// <reference path="../../../typings/index.d.ts" />
import * as React from "react";
import { connect } from "react-redux";
import { createSelector } from "reselect";
import Banner from "./banner";
import { refreshVersion /*, VersionState */ } from "../../redux/version";
import { refreshCluster /*, ClusterState */ } from "../../redux/cluster";
import { refreshNodes /*, NodeStatusState */ } from "../../redux/nodes";
import { VERSION_DISMISSED_KEY, loadUIData, saveUIData } from "../../redux/uiData";
import { VersionList } from "../../interfaces/cockroachlabs";
import { NodeStatus } from "../../util/proto";

import _ = require("lodash");

const MILLISECONDS_IN_DAY = 1000 * 60 * 60 * 24;

class OutdatedBannerProps {
  clusterID: string;
  buildtag: string;
  versionsMatch: boolean;
  nodeCount: number;
  versionCount: number;
  newerVersions: VersionList;
  versionCheckDismissedFetched: boolean;
  versionCheckDismissed: number;

  refreshVersion: typeof refreshVersion;
  refreshCluster: typeof refreshCluster;
  refreshNodes: typeof refreshNodes;
  loadUIData: typeof loadUIData;
  saveUIData: typeof saveUIData;
}

class OutdatedBannerState {
  dismissed: boolean = false;
}

class OutdatedBanner extends React.Component<OutdatedBannerProps, OutdatedBannerState> {
  state = new OutdatedBannerState();

  getNecessaryInfo(props: OutdatedBannerProps = this.props) {
    props.refreshNodes();

    if (!props.versionCheckDismissedFetched) {
      props.loadUIData(VERSION_DISMISSED_KEY);
    }

    if (!props.clusterID) {
      props.refreshCluster();
    }

    if (props.clusterID && props.buildtag) {
      props.refreshVersion(props.clusterID, props.buildtag);
    }
  }

  componentWillMount() {
    this.getNecessaryInfo();
  }

  componentWillReceiveProps(props: OutdatedBannerProps) {
    this.getNecessaryInfo(props);
  }

  dismiss() {
    this.props.saveUIData({ key: VERSION_DISMISSED_KEY, value: Date.now() });
    this.setState({ dismissed: true });
  }

  bannerText() {
    if (!this.props.versionsMatch) {
      return `Node versions are mismatched. ${this.props.versionCount} versions were detected across ${this.props.nodeCount} nodes.`;
    } else if (this.props.newerVersions) {
      return `There is a newer version of CockroachDB available.`;
    }
  }

  visible() {
    // check if it was locally dismissed
    return !this.state.dismissed &&
      // check if the banner was dismissed in the last day
      (this.props.versionCheckDismissedFetched && !this.props.versionCheckDismissed || (Date.now() - this.props.versionCheckDismissed > MILLISECONDS_IN_DAY)) &&
      // check if it would show any text (only if there's a version mismatch or a newer version)
      !!this.bannerText();
  }

  render() {
    let visible: boolean = this.visible();
    return <Banner className="outdated" visible={visible} onclose={() => this.dismiss() }>
      <span className="icon-warning" />
      { this.bannerText() }
      <a href="https://www.cockroachlabs.com/docs/install-cockroachdb.html" target="_blank">
        <button>Update</button>
      </a>
    </Banner>;
  }
}

let nodeStatuses = (state: any): NodeStatus[] => state.nodes.statuses;
let newerVersions = (state: any): VersionList => state.version.valid ? state.version.data : null;
let clusterID = (state: any): string => state.cluster.data && state.cluster.data.cluster_id;

// TODO (maxlang): change the way uiData stores data so we don't need an added check to see if the value was fetched
// TODO (maxlang): determine whether they dismissed a node version mismatch or an upstream version check
let versionCheckDismissedFetched = (state: any): boolean => state.uiData.data && _.has(state.uiData.data, VERSION_DISMISSED_KEY);
let versionCheckDismissed = (state: any): number => state.uiData.data && state.uiData.data[VERSION_DISMISSED_KEY];

let versions = createSelector(
  nodeStatuses,
  (statuses: NodeStatus[]): string[] => statuses && _.uniq(_.map(statuses, (s: NodeStatus) => s.build_info.tag))
);

let nodeCount = createSelector(
  nodeStatuses,
  (statuses: NodeStatus[]) => statuses && statuses.length
);

let versionCount = createSelector(
  versions,
  (v: string[]) => v && v.length
);

let versionsMatch = createSelector(
  versions,
  (v: string[]) => v && v.length === 1
);

let buildtag = createSelector(
  versions,
  (v: string[]): string => v && v.length === 1 ? v[0] : null
);

// Connect the DisconnectedBanner class with our redux store.
let outdatedBannerConnected = connect(
  (state, ownProps) => {
    return {
      clusterID: clusterID(state),
      buildtag: buildtag(state),
      versionsMatch: versionsMatch(state),
      nodeCount: nodeCount(state),
      versionCount: versionCount(state),
      newerVersions: newerVersions(state),
      versionCheckDismissedFetched: versionCheckDismissedFetched(state),
      versionCheckDismissed: versionCheckDismissed(state),
    };
  },
  {
    refreshCluster,
    refreshVersion,
    refreshNodes,
    loadUIData,
    saveUIData,
  }
)(OutdatedBanner);

export default outdatedBannerConnected;
