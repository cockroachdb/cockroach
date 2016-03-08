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

    class HealthController {
      healthy: boolean = true; // Starts out OK because the page loaded
      refreshing: boolean = true;
      interval: number;
      getHealth: () => MithrilPromise<void> = (): MithrilPromise<void> => {
        this.refreshing = true;
        m.redraw();
        return m.request({
          url: "/_admin/v1/health",
          deserialize: (d: any): any => { return d; },
          config: function(xhr: XMLHttpRequest): void { xhr.timeout = 2000; },
        })
        .then((r: any): void => {
          this.healthy = _.startsWith(r.toString(), "ok");
          this.refreshing = false;
        })
        .catch((r: any): void => {
          this.healthy = _.startsWith(r.toString(), "ok");
          this.refreshing = false;
        });
      };

      startRefresh(): void {
        this.interval = setInterval(this.getHealth, 2000);
      }

      onunload: () => void = (): void => {
        if (this.interval) {
          clearInterval(this.interval);
        }
      };
    }

    export function controller(): HealthController {
      let hc: HealthController = new HealthController();
      hc.startRefresh();
      // TODO: create global refresher
      return hc;
    }

    // TODO: add back refreshing indicators and some indicator of last refresh, and last _successful_ refresh
    // They are currently removed because the refresh interval for the health indicator runs at a different
    // time/frequency from the chart refreshes, so when the "refreshing..." text flashes it's confusing.
    // Once we coordinate refreshing globally across the UI (as mentioned in the controller) this can be re-added.
    export function view(ctrl: HealthController): _mithril.MithrilVirtualElement {
      if (ctrl.healthy) {
        return m("div", [
          m("span.health-icon.icon-check-circle" + (ctrl.refreshing ? ".refreshing" : "")),
          // m("span.refreshing-text", ctrl.refreshing ? " Refreshing..." : ""),
        ]);
      } else {
        return m("div", [
          m("span.health-icon.icon-x" + (ctrl.refreshing ? ".refreshing" : "")),
          m("span.unreachable-text", " Can't reach node. "),
          // m("span.refreshing-text", ctrl.refreshing ? " Refreshing..." : ""),
        ]);
      }
    }
  }
}
