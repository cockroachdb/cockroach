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
        config: Utils.Http.XHRConfig,
      })
      .then((r: any): void => {
        healthy = true;
      })
      .catch((r: any): void => {
        healthy = false;
      });
    };

    export function endRefresh(): void {
      if (interval) {
        clearInterval(interval);
      }
    }

    export function startRefresh(): void {
      endRefresh();
      interval = setInterval(getHealth, 2000);
    }
  }
}
