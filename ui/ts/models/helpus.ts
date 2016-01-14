// source: pages/nodes.ts
/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />
/// <reference path="../models/sqlquery.ts" />

module Models {
  "use strict";

  export module HelpUs {
    import MithrilPromise = _mithril.MithrilPromise;
    import MithrilAttributes = _mithril.MithrilAttributes;

    export const OPTIN: string = "optin";
    export const DISMISSED: string = "dismissed";
    export const FIRSTNAME: string = "firstname";
    export const LASTNAME: string = "lastname";
    export const EMAIL: string = "email";
    export const COMPANY: string = "company";
    export const UPDATES: string = "updates";
    export const LASTUPDATED: string = "lastUpdated";

    // Currently the help us flow isn't shown unless the query attribute help-us=true in the URL
    export function helpUsFlag(): boolean {
      let helpUs: string = m.route.param("help-us");
      return (helpUs && helpUs.toString().toLowerCase() === "true");
    }

    /**
     * OptInAttributes tracks the values we get from the SYSTEM.REPORTING table
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
        if (this.loaded) {
          return Models.SQLQuery.runQuery(
            `BEGIN;
            DELETE FROM SYSTEM.REPORTING;
            INSERT INTO SYSTEM.REPORTING VALUES (
              '${this.attributes.email}',
              ${this.attributes.optin},
              ${this.attributes.dismissed},
              '${this.attributes.firstname}',
              '${this.attributes.lastname}',
              '${this.attributes.company}',
              ${this.attributes.updates},
              NOW()
             );
             COMMIT;`)
            .then(() => {
              this.savedAttributes = _.clone(this.attributes);
            });
        }
      }

      load(): MithrilPromise<void> {
        return Models.SQLQuery.runQuery(`SELECT * FROM SYSTEM.REPORTING ORDER BY ${LASTUPDATED} DESC LIMIT 1`, true)
          .then((r: Object): void => {
            if (r[0]) {
              _.each(_.keys(this.attributes), (attr: string): void => {
                this.attributes[attr] = r[0][attr];
              });
            }
            this.loaded = true;
            this.savedAttributes = _.clone(this.attributes);
          });
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
