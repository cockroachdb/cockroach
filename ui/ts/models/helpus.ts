// source: pages/nodes.ts
/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />

module Models {
  "use strict";

  export module HelpUs {
    import MithrilPromise = _mithril.MithrilPromise;
    import MithrilAttributes = _mithril.MithrilAttributes;
    import GetUIDataResponse = Models.Proto.GetUIDataResponse;
    import MithrilDeferred = _mithril.MithrilDeferred;

    export const OPTIN: string = "optin";
    export const DISMISSED: string = "dismissed";
    export const FIRSTNAME: string = "firstname";
    export const LASTNAME: string = "lastname";
    export const EMAIL: string = "email";
    export const COMPANY: string = "company";
    export const UPDATES: string = "updates";
    export const LASTUPDATED: string = "lastUpdated";

    // Help Us flow is shown by default
    export function helpUsFlag(): boolean {
      return true;
    }

    /**
     * OptInAttributes tracks the values we get from the system.ui table
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

    function getHelpUsData(): MithrilPromise<OptInAttributes> {
      let d: MithrilDeferred<OptInAttributes> = m.deferred();
      Models.API.getUIData("helpus").then((response: GetUIDataResponse): void => {
        try {
          let attributes: OptInAttributes = <OptInAttributes>JSON.parse(atob(response.value));
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
      return Models.API.setUIData({"helpus": btoa(JSON.stringify(attrs))});
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

      constructor() {
        this.loadPromise = this.load();
      }

      save(): MithrilPromise<void> {
        // Make sure we loaded the data first
        // TODO: check timestamp on the backend to prevent overwriting data without loading first
        if (this.loaded) {
          return setHelpUsData(this.attributes)
          .then(() => {
            this.savedAttributes = _.clone(this.attributes);
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
