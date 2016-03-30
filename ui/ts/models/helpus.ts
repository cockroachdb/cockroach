// source: pages/nodes.ts
/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />
/// <reference path="./optinattributes.ts" />
/// <reference path="./cockroachlabs.ts" />

module Models {
  "use strict";

  export module HelpUs {
    import MithrilPromise = _mithril.MithrilPromise;
    import MithrilAttributes = _mithril.MithrilAttributes;
    import GetUIDataResponse = Models.Proto.GetUIDataResponse;
    import MithrilDeferred = _mithril.MithrilDeferred;
    import OptInAttributes = Models.OptInAttributes;

    const KEY_HELPUS: string = "helpus";
    // The "server." prefix denotes that this key is shared with the server, so
    // changes to this key must be synchronized with the server code.
    const KEY_OPTIN: string = "server.optin-reporting";

    // Help Us flow is shown by default
    export function helpUsFlag(): boolean {
      return true;
    }

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

    export class UserOptIn {

      /**
       * savedAttributes are the values we originally received when we fetch SYSTEM.REPORTING.
       * They are updated when we save.
       */
      savedAttributes: OptInAttributes = new OptInAttributes();

      /**
       * attributes is populated with the values from SYSTEM.REPORTING.
       * They are updated whenever the user modifies form fields.
       */
      attributes: OptInAttributes = new OptInAttributes();

      /**
       * loaded is true once the original load completes
       */
      loaded: boolean = false;

      /**
       * loadPromise stores the original load request promise, so that any function can wait on the original load to complete
       */
      loadPromise: MithrilPromise<void> = null;

      /**
       * showRequired tracks whether there has been a failed submit, in which case we show the "required" indicator on any invalid inputs
       */
      showRequired: boolean = false;

      constructor() {
        this.loadPromise = this.load();

        this.loadPromise.then(() => {
          Models.CockroachLabs.cockroachLabsSingleton.loadingPromise.then(() => {
            if (!Models.CockroachLabs.cockroachLabsSingleton.synchronized) {
              Models.CockroachLabs.cockroachLabsSingleton.save(this.attributes);
            }
          });
        });
      }

      save(): MithrilPromise<void> {
        // Make sure we loaded the data first
        // TODO: check timestamp on the backend to prevent overwriting data without loading first
        if (this.loaded) {
          // save the data both to the backend and the Cockroach Labs servers
          return setHelpUsData(this.attributes)
          .then(() => {
            this.savedAttributes = _.clone(this.attributes);
            Models.CockroachLabs.cockroachLabsSingleton.save(this.attributes);
          });
        }
      }

      load(): MithrilPromise<void> {
        let d: MithrilDeferred<any> = m.deferred();
        getHelpUsData()
          .then((attributes: OptInAttributes): void => {
            this.attributes = attributes;
            this.loaded = true;
            this.savedAttributes = _.clone(this.attributes);
            d.resolve();
          })
          // no helpus data found
          .catch(() => {
            this.loaded = true;
            this.attributes = new OptInAttributes();
            this.savedAttributes = _.clone(this.attributes);
            d.resolve();
          });
        return d.promise;
      }

      /**
       * showHelpUs returns true if the user hasn't dismissed the banner/modal and if they haven't already opted in
       * @returns {boolean}
       */
      showHelpUs(): boolean {
        return (this.attributes.dismissed < 1) && !this.attributes.optin;
      }

      /**
       * optedIn returns true if the user has already opted in and provided their email
       * @returns {boolean}
       */
      optedIn(): boolean {
        return this.savedAttributes.optin && !!this.savedAttributes.email;
      }

      // Data binding helper function for form data.
      bindForm(): MithrilAttributes {
        return {
          onchange: (e: Event): void => {
            let target: HTMLInputElement = <HTMLInputElement>e.target;
            if (target.type !== "checkbox") {
              this.attributes[target.name] = target.value;
            } else {
              this.attributes[target.name] = target.checked;
              if (target.name === "optin") {
                this.attributes.dismissed = this.attributes.dismissed ? this.attributes.dismissed + 1 : 1;
              }
            }
          },
          onsubmit: (e: Event): boolean => {
            let target: HTMLButtonElement = <HTMLButtonElement>e.target;
            return !target.checkValidity();
          },
        };
      }
    }

    export let userOptInSingleton: UserOptIn = new UserOptIn();
  }
}
