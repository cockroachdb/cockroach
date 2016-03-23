/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />
/// <reference path="../../typings/browser.d.ts"/>
/// <reference path="../util/http.ts" />
/// <reference path="optinattributes.ts" />
/// <reference path="status.ts" />

module Models {
  "use strict";

  // CockroachLabs is the model used for interacting with the Cockroach Labs servers
  // for version checking, registering cluster info, and sharing usage data
  export module CockroachLabs {
    import MithrilPromise = _mithril.MithrilPromise;
    import NodeStatus = Models.Proto.NodeStatus;
    import MithrilDeferred = _mithril.MithrilDeferred;
    import Nodes = Models.Status.Nodes;
    import nodeStatusSingleton = Models.Status.nodeStatusSingleton;
    import GetUIDataResponse = Models.Proto.GetUIDataResponse;

    // milliseconds in 1 day
    const DAY = 1000 * 60 * 60 * 24;

    // tracks when the version check was last dismissed
    const VERSION_DISMISSED_KEY = "version_dismissed";

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
      dismissedCheck: number;
      versionStatus: VersionStatus = {
        error: null,
        message: "Checking latest version...",
        laterVersions: null,
      };
      nodes: Nodes = nodeStatusSingleton;

      constructor(clusterID: string) {
        // TODO: get cluster ID from the node
        this.clusterID = "00000000-0000-0000-0000-000000000000";
        // TODO: get rid of double nodestatus refresh on cluster/nodes pages
        m.sync([this.nodes.refresh(), Models.API.getUIData([VERSION_DISMISSED_KEY])]).then((data: [any, GetUIDataResponse]): void => {
          try {
            let dismissedCheckRaw: string = _.get<string>(data, `1.key_values.${VERSION_DISMISSED_KEY}.value`);
            this.dismissedCheck = dismissedCheckRaw && parseInt(atob(dismissedCheckRaw), 10);
          } catch (e) {
            console.warn(e);
          }
          this.versionCheck();
        });
      }

      // versionCheck checks whether all the node versions match and, if so, if the current version is outdated
      // if the version banner is dismissed, it won't check again for another day
      versionCheck(): MithrilPromise<VersionStatus> {
        let nodeStatuses: NodeStatus[] = this.nodes.allStatuses();
        let versions: {[version: string]: number} = _.countBy<NodeStatus>(nodeStatuses, "build_info.tag");
        let d: MithrilDeferred<VersionStatus> = m.deferred();
        let versionCount: number = _.keys(versions).length;

        // TODO: consider using querycache
        // this sets the current version status to latest seen, then resolve the promise with that value
        let setVersionStatusAndResolve: (v: VersionStatus) => void = (v: VersionStatus): void => {
          this.versionStatus = v;
          d.resolve(v);
        };

        if (!this.dismissedCheck || (Date.now() - this.dismissedCheck > DAY)) {
          // mismatching versions among nodes
          if (versionCount > 1) {
            setVersionStatusAndResolve({
              error: true,
              message: `Node versions are mismatched. ${versionCount} versions were detected across ${nodeStatuses.length} nodes.`,
              laterVersions: null,
            });
          } else if (versionCount < 1) { // This usually indicates we weren't able to retrieve the NodeStatus objects
            setVersionStatusAndResolve({
              error: null,
              message: "Unable to detect version.",
              laterVersions: null,
            });
          } else {
            // contact Cockroach Labs to check latest version
            m.request<VersionList>({
              url: `https://register.cockroachdb.com/api/clusters/updates?uuid=${this.clusterID}&version=${nodeStatuses[0].build_info.tag}`,
              config: Utils.Http.XHRConfig,
            }).then((laterVersions: VersionList) => {
              if (!_.isEmpty(laterVersions.details)) {
                let newerVersions: number = laterVersions.details.length;
                setVersionStatusAndResolve({
                  error: true,
                  message: `Found ${newerVersions} newer ${newerVersions === 1 ? "version" : "versions"} of CockroachDB.`,
                  laterVersions: laterVersions,
                });
              } else {
                setVersionStatusAndResolve({
                  error: false,
                  message: "You are using the latest version.",
                  laterVersions: null,
                });
              }
            });
          }
        } else {
          setVersionStatusAndResolve({
            error: null,
            message: "Dismissed version check in the past day.",
            laterVersions: null,
          });
        }
        return d.promise;
      }

      dismissVersionCheck: () => void = (): void => {
        let now: number = Date.now();
        Models.API.setUIData({[VERSION_DISMISSED_KEY]: btoa(JSON.stringify(now))}).then(() => {
          this.dismissedCheck = now;
          this.versionStatus = {
            error: null,
            message: "Dismissed version check in the past day.",
            laterVersions: null,
          };
          m.redraw();
        });
      };
    }

    export let cockroachLabsSingleton = new CockroachLabs(null);
  }
}
