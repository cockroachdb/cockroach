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

    // tracks whether the latest registration data has been synchronized with the Cockroach Labs servers
    export const REGISTRATION_SYNCHRONIZED_KEY = "registration_synchronized";

    // Address of the Cockroach Labs servers.
    const COCKROACHLABS_ADDR = "https://register.cockroachdb.com";

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
      synchronized: boolean;

      loading: boolean = true;
      loadingPromise: MithrilPromise<any>;

      // TODO: create using a factory to better handle promises/errors
      constructor() {
        // TODO: get rid of double nodestatus refresh on cluster/nodes pages
        this.loadingPromise = m.sync([
          this.nodes.refresh(),
          Models.API.getUIData([VERSION_DISMISSED_KEY, REGISTRATION_SYNCHRONIZED_KEY]),
          Models.API.getClusterID(),
        ]).then((data: [any, GetUIDataResponse]): void => {

            // TODO: better error handling story, perhaps refactor error handling into Models.API.getUIData
            try {
              let dismissedCheckRaw: string = _.get<string>(data, `1.key_values.${VERSION_DISMISSED_KEY}.value`);
              this.dismissedCheck = dismissedCheckRaw && parseInt(atob(dismissedCheckRaw), 10);

              let synchronizedRaw: string = _.get<string>(data, `1.key_values.${REGISTRATION_SYNCHRONIZED_KEY}.value`);
              this.synchronized = synchronizedRaw && (atob(synchronizedRaw).toLowerCase() === "true") || false;
            } catch (e) {
              console.warn(e);
            }

            this.clusterID = data[2];

            this.loading = false;
            this.versionCheck();
          }).catch((e: Error) => {
            console.error(e);
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
              url: `${COCKROACHLABS_ADDR}/api/clusters/updates?uuid=${this.clusterID}&version=${nodeStatuses[0].build_info.tag}`,
              config: Utils.Http.XHRConfig,
              background: true,
            }).then((laterVersions: VersionList) => {
              if (!_.isEmpty(laterVersions.details)) {
                setVersionStatusAndResolve({
                  error: true,
                  message: `There is a newer version of CockroachDB available.`,
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

      // save saves the latest user optin values to the Cockroach Labs server.
      // If the user has opted in, it uses the registration endpoint.
      // If the user has not opted in or has opted out, it uses the unregistration endpoint.
      // It then saves the synchronization status to the current cluster.
      save(data: OptInAttributes): MithrilPromise<void> {
        let d: MithrilDeferred<any> = m.deferred();
        let p: MithrilPromise<any>;
        if (data && data.optin) {
          p = this.register({
            first_name: data.firstname,
            last_name: data.lastname,
            company: data.company,
            email: data.email,
          });
        } else {
          p = this.unregister();
        }

        p.then(() => {
            this.saveSync(true).then(() => d.resolve());
          })
          .catch((e: Error) => {
            console.warn("Error attempting to contact Cockroach Labs server.", e);
            this.saveSync(false).then(() => d.resolve());
          });

        return p;
      }

      register(data: RegisterData): MithrilPromise<any> {
        return m.request<VersionList>({
          url: `${COCKROACHLABS_ADDR}/api/clusters/register?uuid=${this.clusterID}`,
          method: "POST",
          data: data,
          config: Utils.Http.XHRConfig,
          background: true,
        });
      }

      unregister(): MithrilPromise<any> {
        return m.request<VersionList>({
          url: `${COCKROACHLABS_ADDR}/api/clusters/unregister?uuid=${this.clusterID}`,
          method: "DELETE",
          config: Utils.Http.XHRConfig,
          background: true,
        });
      }

      saveSync(synchronized: boolean): MithrilPromise<any> {
        this.synchronized = synchronized;
        return Models.API.setUIData({[REGISTRATION_SYNCHRONIZED_KEY]: btoa(JSON.stringify(synchronized))});
      }
    }

    export let cockroachLabsSingleton = new CockroachLabs();
  }
}
