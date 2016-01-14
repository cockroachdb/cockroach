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
    export function showHelpUs(): _mithril.MithrilPromise<boolean> {
      let helpUs: string = m.route.param("help-us");
      let deferred: _mithril.MithrilDeferred<boolean> = m.deferred();
      // TODO: remove this check to show the "Help Us" banner by default
      if (helpUs && helpUs.toString().toLowerCase() === "true") {
        // check if the user has already filled out the 'help us' info, or has opted out
        // TODO: also check if it's a dev cluster in which case we may not want to show the opt in banner
        Models.SQLQuery.runQuery(`SELECT ${OPTIN}, ${DISMISSED} FROM SYSTEM.REPORTING`, true).then(
          (r: Object): boolean => {
            return !!(deferred.resolve(!r[0] || (!_.isBoolean(r[0][OPTIN]) && !r[0][DISMISSED])));
          });
      } else {
        deferred.resolve(false);
      }
      return deferred.promise;
    }

    export class OptInAttributes {
      email: string = "";
      optin: boolean = false; // Did the user opt in/out of reporting usage
      dismissed: boolean = false; // Did the user dismiss the banner/modal without opting in/out
      firstname: string = "";
      lastname: string = "";
      company: string = "";
      updates: boolean = false; // Did the user sign up for product/feature updates
    }

    export class UserOptIn {

      attributes: OptInAttributes = new OptInAttributes();

      attributeList: string[] = _.keys(this.attributes);

      loaded: boolean = false;
      loadPromise: MithrilPromise<void> = null;

      constructor() {
        this.loadPromise = this.load();
      }

      save(): MithrilPromise<void> {
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
           COMMIT;`);
      }

      load(): MithrilPromise<void> {
        return Models.SQLQuery.runQuery("SELECT * FROM SYSTEM.REPORTING", true)
          .then((r: Object): void => {
            if (r[0]) {
              _.each(this.attributeList, (attr: string): void => {
                this.attributes[attr] = r[0][attr];
              });
            }
            this.loaded = true;
          });
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
  }
}
