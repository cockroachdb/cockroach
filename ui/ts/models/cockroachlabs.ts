/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />
/// <reference path="../../typings/browser.d.ts"/>
/// <reference path="../util/http.ts" />
/// <reference path="optinattributes.ts" />
/// <reference path="status.ts" />

module Models {
  "use strict";

  // CockroachLabs is the model used for interacting with the Cockroach Labs
  // servers for version checking or
  export module CockroachLabs {
    import MithrilPromise = _mithril.MithrilPromise;
    import NodeStatus = Models.Proto.NodeStatus;
    import MithrilDeferred = _mithril.MithrilDeferred;
    import Nodes = Models.Status.Nodes;
    import nodeStatusSingleton = Models.Status.nodeStatusSingleton;
    import GetUIDataResponse = Models.Proto.GetUIDataResponse;

    const DAY = 1000 * 60 * 60 * 24;

    const VERSION_CHECKED_KEY = "version_checked";

    export interface Version {
      version: string;
      detail: string;
    }

    export interface VersionList {
      details: Version[];
    }

    export interface VersionStatus {
      error: boolean;
      message: string;
      laterVersions: VersionList;
    }

    interface RegisterData {
      first_name: string;
      last_name: string;
      company: string;
      email: string;
    }

    export class CockroachLabs {
      clusterID: string;
      dismissed: boolean;
      // TODO: consider using querycache
      versionStatus: VersionStatus = {
        error: null,
        message: "Checking latest version...",
        laterVersions: null,
      };
      nodes: Nodes = nodeStatusSingleton;
      lastVersionCheck: number; // last version check, in milliseconds

      constructor(clusterID: string) {
        // TODO: get cluster ID from the node
        this.clusterID = "00000000-0000-0000-0000-000000000000";
        // TODO: get rid of double nodestatus refresh on cluster/nodes pages
        m.sync([this.nodes.refresh(), Models.API.getUIData(VERSION_CHECKED_KEY)]).then((data: [any, GetUIDataResponse]): void => {
          try {
            this.lastVersionCheck = data[1] && data[1].key_values && data[1].key_values[VERSION_CHECKED_KEY] && parseInt(atob(data[1].value), 10);
          } catch (e) {
            console.warn(e);
          }
          this.versionCheck().then((v: VersionStatus): void => {
            this.versionStatus = v;
          });
        });
      }

      // TODO: track when users dismiss the banner but show the prompt until then
      // versionCheck checks whether all the node versions match and, if so, if the current version is outdated
      // it only checks once per day
      versionCheck(): MithrilPromise<VersionStatus> {
        let nodeStatuses: NodeStatus[] = this.nodes.allStatuses();
        let versions: {[version: string]: number} = _.countBy<NodeStatus>(nodeStatuses, "build_info.tag");
        let d: MithrilDeferred<VersionStatus> = m.deferred();
        let versionCount: number = _.keys(versions).length;

        let setCheckedAndResolve: (v: VersionStatus) => void = (v: VersionStatus): void => {
          this.lastVersionCheck = Date.now();
          Models.API.setUIData({[VERSION_CHECKED_KEY]: btoa(JSON.stringify(this.lastVersionCheck))}).then(() => {
            d.resolve(v);
          });
        };

        // mismatching versions among nodes
        if (versionCount > 1) {
          setCheckedAndResolve({
            error: true,
            message: `Node versions are mismatched. ${versionCount} versions were detected across ${nodeStatuses.length} nodes.`,
            laterVersions: null,
          });
        } else if (versionCount < 1) { // This usually indicates we weren't able to retrieve the NodeStatus objects
          setCheckedAndResolve({
            error: null,
            message: "Unable to detect version.",
            laterVersions: null,
          });
        } else {
          if (Date.now() - this.lastVersionCheck > DAY) {
            // contact Cockroach Labs to check latest version
            m.request<VersionList>({
              url: `https://register.cockroachdb.com/api/clusters/updates?uuid=${this.clusterID}&version=${nodeStatuses[0].build_info.tag}`,
              config: Utils.Http.XHRConfig,
            }).then((laterVersions: VersionList) => {
              if (!_.isEmpty(laterVersions.details)) {
                let newerVersions: number = laterVersions.details.length;
                setCheckedAndResolve({
                  error: true,
                  message: `Found ${newerVersions} newer ${newerVersions === 1 ? "version" : "versions"} of CockroachDB.`,
                  laterVersions: laterVersions,
                });
              } else {
                setCheckedAndResolve({
                  error: false,
                  message: "You are using the latest version.",
                  laterVersions: null,
                });
              }
            });
          } else {
            d.resolve({
              error: null,
              message: "Already checked version in the past day.",
              laterVersions: null,
            });
          }
        }
        return d.promise;
      }
    }

    export let cockroachLabsSingleton = new CockroachLabs(null);
  }
}
