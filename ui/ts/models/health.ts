// source: models/health.ts
/// <reference path="../util/http.ts" />
/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />

// Author: Max Lang (max@cockroachlabs.com)

module Models {
"use strict";
  import MithrilPromise = _mithril.MithrilPromise;

  /**
   * Health tracks the application health
   */
  export module Health {
    export let healthy: boolean = true; // Starts out OK because the page loaded
    let interval: number;
    function getHealth(): MithrilPromise<void> {
      m.redraw();
      return m.request({
        url: "/_admin/v1/health",
        deserialize: (d: any): any => { return d; },
        config: Utils.Http.XHRConfig,
      })
      .then((r: any): void => {
        healthy = _.startsWith(r.toString(), "ok");
      })
      .catch((r: any): void => {
        healthy = _.startsWith(r.toString(), "ok");
      });
    };

    function endRefresh(): void {
      if (interval) {
        clearInterval(interval);
      }
    }

    function startRefresh(): void {
      endRefresh();
      interval = setInterval(getHealth, 2000);
    }

    startRefresh();
  }
}
