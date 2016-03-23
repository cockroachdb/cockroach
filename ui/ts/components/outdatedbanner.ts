// source: components/outdatabanner.ts
/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />
/// <reference path="banner.ts" />

module Components {
  "use strict";
  /**
   * HelpUs is used for the sign up flow to capture user information
   */
  export module OutdatedBanner {

    export function controller(): any {
    }

    export function view(ctrl: any): _mithril.MithrilVirtualElement {
      if (Models.CockroachLabs.cockroachLabsSingleton.versionStatus.error && !Models.CockroachLabs.cockroachLabsSingleton.dismissed) {
        return m.component(Components.Banner, {
          bannerClass: ".outdated",
          content: [
            m("span.icon-warning"),
            Models.CockroachLabs.cockroachLabsSingleton.versionStatus.message,
            m("a", {href: "https://www.cockroachlabs.com/docs/install-cockroachdb.html", target: "_blank"}, m("button", "Update")),
          ],
          onclose: (): void => { Models.CockroachLabs.cockroachLabsSingleton.dismissed = true; },
        });
      }
      return m("");
    }
  }
}
