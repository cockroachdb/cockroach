// source: components/disconnectedbanner.ts
/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />
/// <reference path="../models/health.ts" />
/// <reference path="banner.ts" />

module Components {
  "use strict";
  /**
   * DisconnectedBanner is used to indicate to the user that they are not connected to a node
   */
  export module DisconnectedBanner {

    interface Dismissable {
      dismissed: boolean;
    }

    let dismissedCtrl: Dismissable = {
      dismissed: false,
    };

    export function controller(): Dismissable {
      return dismissedCtrl;
    }

    export function view(ctrl: Dismissable): _mithril.MithrilVirtualElement {
      // Reset the banner dismiss if the connection becomes healthy again
      if (Models.Health.healthy) {
        ctrl.dismissed = false;
      }

      if (!Models.Health.healthy && !ctrl.dismissed) {
        return m.component(Components.Banner, {
          bannerClass: ".disconnected",
          content: [
            m("span.icon-warning"),
            "Connection to Cockroach node lost.",
          ],
          onclose: (): void => { ctrl.dismissed = true; },
        });
      }
      return m("");
    }
  }
}
