// source: models/helpus.ts
/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />
/// <reference path="optinattributes.ts" />
/// <reference path="cockroachlabs.ts" />

module Models {
  "use strict";

  export module HelpUs {
    import MithrilPromise = _mithril.MithrilPromise;
    import MithrilDeferred = _mithril.MithrilDeferred;
    import MithrilAttributes = _mithril.MithrilAttributes;
    import OptInAttributes = Models.OptInAttributes.OptInAttributes;

    export class UserOptIn {

      constructor() {
        Models.OptInAttributes.loadPromise.then(() => {
          Models.CockroachLabs.cockroachLabsSingleton.loadingPromise.then(() => {
            if (!Models.CockroachLabs.cockroachLabsSingleton.synchronized) {
              Models.CockroachLabs.cockroachLabsSingleton.save(this.savedAttributes());
            }
          });
        });
      }

      // only show the "required" text after we attempt to submit
      showRequired: boolean = false;

      // attributes saved to the cluster
      savedAttributes(): OptInAttributes {
        return Models.OptInAttributes.savedAttributes;
      }

      // current attributes in the UI
      attributes(): OptInAttributes {
        return Models.OptInAttributes.currentAttributes;
      }

      // save opt in data and sync with cockroach labs server
      save(): MithrilPromise<any> {
        // Make sure we loaded the data first
        if (Models.OptInAttributes.loaded) {
          // save the data both to the backend and the Cockroach Labs servers
          return m.sync([/*Models.CockroachLabs.cockroachLabsSingleton.save(this.attributes()),*/ Models.OptInAttributes.save()]).then(() => {
            Models.CockroachLabs.cockroachLabsSingleton.uploadUsageStats();
            return;
          }
          );
        } else {
          let d: MithrilDeferred<any> = m.deferred();
          d.reject("Can't save until OptIn attributes have been loaded from the cluster.");
          return d.promise;
        }
      }

      /**
       * showHelpUs returns true if the user hasn't dismissed the banner/modal and if they haven't already opted in
       * @returns {boolean}
       */
      showHelpUs(): boolean {
        return (this.attributes().dismissed < 1) && !this.attributes().optin;
      }

      /**
       * optedIn returns true if the user has already opted in and provided their email
       * @returns {boolean}
       */
      optedIn(): boolean {
        return this.savedAttributes() && this.savedAttributes().optin && !!this.savedAttributes().email;
      }

      // Data binding helper function for form data.
      bindForm(): MithrilAttributes {
        return {
          onchange: (e: Event): void => {
            let target: HTMLInputElement = <HTMLInputElement>e.target;
            if (target.type !== "checkbox") {
              this.attributes()[target.name] = target.value;
            } else {
              this.attributes()[target.name] = target.checked;
              // If they interact with the opt-in checkbox, dismiss the banner
              if (target.name === "optin") {
                this.attributes().dismissed = this.attributes().dismissed ? this.attributes().dismissed + 1 : 1;
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
