module Models {
  "use strict";
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
}
