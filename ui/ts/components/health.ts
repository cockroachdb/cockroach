// source: components/health.ts
/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />
/// <reference path="../../typings/browser.d.ts" />
/// <reference path="../util/property.ts" />

// Author: Max Lang (max@cockroachlabs.com)

module Components {
  "use strict";

  /**
   * Health returns the health status as an icon
   */
  export module Health {

    import MithrilPromise = _mithril.MithrilPromise;
    export enum Health {
      GOOD, WARNING, BAD, UNKNOWN
    }

    class HealthController {
      health: string = "ok"; // Starts out OK because the page loaded
      refreshing: boolean = true;
      getHealth(): MithrilPromise<void> {
        let thiz = this;
        this.refreshing = true;
        m.redraw();
        return m.request({
          url: "/_admin/v1/health",
          deserialize: (d: any): any => { return d; },
          config: function(xhr: XMLHttpRequest): void { xhr.timeout = 2000; },
        })
        .then((r: any): void => {
          thiz.health = r.toString();
          thiz.refreshing = false;
        })
        .catch((r: any): void => {
          thiz.health = r.toString();
          thiz.refreshing = false;
        });
      }
    }

    export function controller(): HealthController {
      let hc: HealthController = new HealthController();
      // TODO: create global refresher
      setInterval(hc.getHealth.bind(hc), 2000);
      return hc;
    }

    export function view(ctrl: HealthController): _mithril.MithrilVirtualElement {
      console.log("ctrl.health", ctrl.health);
      let healthy: boolean = ctrl.health && ctrl.health.substring(0, 2) === "ok";
      if (healthy) {
        return m("div", [
          m("span.health-icon.icon-check-circle" + (ctrl.refreshing ? ".refreshing" : "")),
          m("span.refreshing-text", ctrl.refreshing ? " Refreshing..." : ""),
        ]);
      } else {
        return m("div", [
          m("span.health-icon.icon-x" + (ctrl.refreshing ? ".refreshing" : "")),
          m("span.unreachable-text", " Can't reach node. "),
          m("span.refreshing-text", ctrl.refreshing ? " Refreshing..." : ""),
        ]);
      }
    }
  }
}
