// source: models/optinattributes.ts
/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />

module Models {
  "use strict";
  import MithrilPromise = _mithril.MithrilPromise;
  import MithrilDeferred = _mithril.MithrilDeferred;
  import GetUIDataResponse = Models.Proto.GetUIDataResponse;

  export module OptInAttributes {
    const KEY_HELPUS: string = "helpus";
    // The "server." prefix denotes that this key is shared with the server, so
    // changes to this key must be synchronized with the server code.
    const KEY_OPTIN: string = "server.optin-reporting";

    function getHelpUsData(): MithrilPromise<OptInAttributes> {
      // Query the optin and helpus uidata and merge them into OptInAttributes.
      let d: MithrilDeferred<OptInAttributes> = m.deferred();
      Models.API.getUIData([KEY_HELPUS]).then((response: GetUIDataResponse): void => {
        try {
          let attributes: OptInAttributes = <OptInAttributes>JSON.parse(atob(response.key_values[KEY_HELPUS].value));
          d.resolve(attributes);
        } catch (e) {
          d.reject(e);
        }
      }).catch((e: Error) => {
        d.reject(e);
      });
      return d.promise;
    }

    function setHelpUsData(attrs: OptInAttributes): MithrilPromise<any> {
      // Clone "optin" into a separate key, so that the server can query that
      // separately and cleanly. This is needed, because uidata is essentially
      // an opaque cookie jar for the admin UI. KEY_OPTIN is a special case,
      // because the server takes action based on the value for that key. So,
      // cloning "optin" allows the server to read the data from a well-defined
      // place while allowing the admin UI code to use uidata mostly
      // independently of the server.
      let keyValues: {[key: string]: string} = {
        [KEY_HELPUS]: btoa(JSON.stringify(attrs)),
        [KEY_OPTIN]: btoa(JSON.stringify(attrs.optin)),
      };
      return Models.API.setUIData(keyValues);
    }

    /**
     * OptInAttributes tracks the values the user has provided when opting in to usage reporting
     */
    export class OptInAttributes {
      email: string = "";
      optin: boolean = null; // Did the user opt in/out of reporting usage
      dismissed: number = null; // How many times did the user dismiss the banner/modal without opting in/out
      firstname: string = "";
      lastname: string = "";
      company: string = "";
      updates: boolean = null; // Did the user sign up for product/feature updates
    }

    // The values saved in the cluster
    export let savedAttributes: OptInAttributes = new OptInAttributes();
    // The current values in the UI
    export let currentAttributes: OptInAttributes = new OptInAttributes();

    /**
     * loaded is true once the original load completes
     */
    export let loaded: boolean = false;

    /**
     * loadPromise stores the original load request promise, so that any function can wait on the original load to complete
     */
    export let loadPromise: MithrilPromise<void> = load();

    // save saves the current attributes to the cluster
    export function save(): MithrilPromise<void> {
      // TODO: check timestamp on the backend to prevent overwriting data without loading first

      // Make sure we loaded the data first
      if (this.loaded) {
        // save the data both to the backend and the Cockroach Labs servers
        return setHelpUsData(currentAttributes)
          .then(() => {
            savedAttributes = _.clone(currentAttributes);
          });
      }
    }

    // load loads the latest attributes from the cluster
    export function load(): MithrilPromise<void> {
      let d: MithrilDeferred<any> = m.deferred();
      getHelpUsData()
        .then((attributes: OptInAttributes): void => {
          currentAttributes = attributes;
          loaded = true;
          savedAttributes = _.clone(currentAttributes);
          d.resolve();
        })
        // no helpus data found
        .catch(() => {
          loaded = true;
          currentAttributes = new OptInAttributes();
          savedAttributes = _.clone(currentAttributes);
          d.resolve();
        });
      return d.promise;
    }
  }
}
