// source: components/outdatabanner.ts
/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />
/// <reference path="../models/cockroachlabs.ts" />
/// <reference path="banner.ts" />

module Components {
  "use strict";
  /**
   * OutdatedBanner is used to prompt the user to update to the latest Cockroach version
   */
  export module OutdatedBanner {

    export function controller(): any {
    }

    export function view(ctrl: any): _mithril.MithrilVirtualElement {
      if (Models.CockroachLabs.cockroachLabsSingleton.versionStatus.error) {
        return m.component(Components.Banner, {
          bannerClass: ".outdated",
          content: [
            m("span.icon-warning"),
            Models.CockroachLabs.cockroachLabsSingleton.versionStatus.message,
            m("a", {href: "https://www.cockroachlabs.com/docs/install-cockroachdb.html", target: "_blank"}, m("button", "Update")),
          ],
          onclose: Models.CockroachLabs.cockroachLabsSingleton.dismissVersionCheck,
        });
      }
      return m("");
    }
  }
}
