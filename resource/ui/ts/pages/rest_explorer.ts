// source: pages/rest_explorer.ts
/// <reference path="../typings/mithriljs/mithril.d.ts" />

// Author: Matt Tracy (matt@cockroachlabs.com)

/**
 * AdminViews is the primary module for Cockroaches administrative web
 * interface.
 */
module AdminViews {
  /**
   * RestExplorer is a very simple interface for exploring rest values. This is
   * a singleton Panel.
   */
  export module RestExplorer {
    /**
     * Model contains the static model for the RestExplorer panel.
     */
    module Model {
      /**
       * Properties for the model.
       */
      export var singleKey = m.prop("");
      export var singleValue = m.prop("");
      export var singleCounter = m.prop(0);
      export var rangeStart = m.prop("");
      export var rangeEnd = m.prop("");
      export var responseLog = m.prop([]);

      /**
       * logResponse is a terse function which decodes the response to an
       * XMLHttpRequest and generates a log messaged.
       */
      function logResponse(xhr: XMLHttpRequest, opts: _mithril.MithrilXHROptions):string {
        var data:string;
        if (xhr.responseType === "json") {
          data = JSON.stringify(xhr.response);
        } else {
          data = xhr.responseText;
        }
        data = data.length > 0 ? data : "(no response body)";
        data = ['[', opts.method, '] ', xhr.status, ' ', opts.url, ': ', data].join('');
        responseLog().push(data)
        return JSON.stringify(data)
      }

      /**
       * scan dispatches a scan request to the server based on the current
       * values of rangeStart and rangeEnd. The request will use the supplied
       * http method. The response will be logged to the response log.
       */
      export function scan(method: string):_mithril.MithrilPromise<any> {
        var endpoint = "/kv/rest/range?start=" + encodeURIComponent(rangeStart());
        if (!!rangeEnd()) {
          endpoint += '&end=' + encodeURIComponent(rangeEnd());
        }
        return m.request({
          method: method,
          url: endpoint,
          extract: logResponse,
        });
      }

      /**
       * entry dispatches a request targeting a single entry to the server, based
       * on the current values of singleKey and singleValue. The request will
       * use the supplied http method. The response will be logged to the
       * response log.
       */
      export function entry(method: string):_mithril.MithrilPromise<any> {
        var endpoint = "/kv/rest/entry/" + singleKey();
        var request:_mithril.MithrilXHROptions = {
          method: method,
          url: endpoint,
          extract: logResponse,
          serialize: function(data) { return data },
        };
        if (method === "POST") {
          request.config = function(xhr, opts) {
            xhr.setRequestHeader("Content-Type", "text/plain; charset=UTF-8") ;
            return xhr;
          }
          request.data = singleValue();
        }

        return m.request(request);
      }

      /**
       * counter dispatches a request targeting a single entry to the server, based
       * on the current values of singleKey and singleCounter. The request will
       * use the supplied http method. The response will be logged to the
       * response log.
       */
      export function counter(method: string):_mithril.MithrilPromise<any> {
        var endpoint = "/kv/rest/counter/" + singleKey();
        var request:_mithril.MithrilXHROptions = {
          method: method,
          url: endpoint,
          extract: logResponse,
          serialize: function(data) { return data },
        };
        if (method === "POST") {
          request.config = function(xhr, opts) {
            xhr.setRequestHeader("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8") ;
            return xhr;
          }
          request.data = singleCounter();
        }

        return m.request(request);
      }

      /**
       * clearLog clears the response log.
       */
      export function clearLog() {
        responseLog([]);
      };

    }

    /**
     * button is a helper function that generates a clickable button for the
     * RestExplorer page.
     */
    function button(text:string, onclick:()=>any, disabled:()=>boolean) {
      return m("input[type=button]", {
        value: text,
        disabled: disabled(),
        onclick: onclick,
      });
    }

    /**
     * stringField is a helper function that generates an input field for the
     * RestExplorer page.
     */
    function field<T>(text:string, value:()=>T, disabled:()=>boolean) {
      return m("input[type=text]", {
        placeholder: text,
        disabled: disabled(),
        value: value(),
        onchange: m.withAttr("value", value),
      });
    }

    module EntryComponent {
      class Controller {
        // Properties.
        responsePending = m.prop(false);
        key = Model.singleKey;
        val = Model.singleValue;

        // Functions.
        request(method:string) {
          this.responsePending(true);
          Model.entry(method).then(this.complete, this.complete);
        }

        // Bound functions which are used in callbacks.
        private complete = () => this.responsePending(false);
        get = () => this.request("GET");
        post = () => this.request("POST");
        head = () => this.request("HEAD");
        delete = () => this.request("DELETE");
      }

      export function controller():Controller {
        return new Controller();
      }

      export function view(ctrl:Controller) {
        return m("section.restExplorerControls-control", [
            m("h3", "K/V Pair"),
            m("form", [
              field("Key", ctrl.key, ctrl.responsePending),
              m.trust("&rarr;"),
              field("Value", ctrl.val, ctrl.responsePending),
              button("Get", ctrl.get, ctrl.responsePending),
              button("Head", ctrl.head, ctrl.responsePending),
              button("Put", ctrl.post, ctrl.responsePending),
              button("Delete", ctrl.delete, ctrl.responsePending),
            ])
        ]);
      }
    }

    module RangeComponent {
      class Controller {
        // Properties.
        responsePending = m.prop(false);
        rangeStart = Model.rangeStart;
        rangeEnd = Model.rangeEnd;

        // Functions.
        request(method:string) {
          this.responsePending(true);
          Model.scan(method).then(this.complete, this.complete);
        }

        // Bound functions which are used in callbacks.
        private complete = () => this.responsePending(false);
        get = () => this.request("GET");
        delete = () => this.request("DELETE");
      }

      export function controller():Controller {
        return new Controller();
      }

      export function view(ctrl:Controller) {
        return m("section.restExplorerControls-control", [
            m("h3", "Range"),
            m("form", [
              field("Start", ctrl.rangeStart, ctrl.responsePending),
              m.trust("&rarr;"),
              field("End", ctrl.rangeEnd, ctrl.responsePending),
              button("Get", ctrl.get, ctrl.responsePending),
              button("Delete", ctrl.delete, ctrl.responsePending),
            ])
        ]);
      }
    }

    module CounterComponent {
      class Controller {
        // Properties.
        responsePending = m.prop(false);
        key = Model.singleKey;
        val = Model.singleCounter;

        // Functions.
        private request(method:string) {
          this.responsePending(true);
          Model.counter(method).then(this.complete, this.complete);
        }

        // Bound functions which are used in callbacks.
        private complete = () => this.responsePending(false);
        get = () => this.request("GET");
        post = () => this.request("POST");
        head = () => this.request("HEAD");
        delete = () => this.request("DELETE");
      }

      export function controller():Controller {
        return new Controller();
      }

      export function view(ctrl:Controller) {
        return m("section.restExplorerControls-control", [
            m("h3", "Counter"),
            m("form", [
              field("Key", ctrl.key, ctrl.responsePending),
              m.trust("&rarr;"),
              field("Value", ctrl.val, ctrl.responsePending),
              button("Get", ctrl.get, ctrl.responsePending),
              button("Head", ctrl.head, ctrl.responsePending),
              button("Put", ctrl.post, ctrl.responsePending),
              button("Delete", ctrl.delete, ctrl.responsePending),
            ])
        ]);
      }
    }

    module LogComponent {
      interface Controller {
        log: _mithril.MithrilProperty<string[]>;
        clear: ()=>void;
      }

      export function controller():Controller {
        return {
          log: Model.responseLog,
          clear: Model.clearLog,
        }
      }

      export function view(ctrl:Controller) {
        return m(".restExplorerLog", [
            m("h3", "Console"),
            button("Clear", ctrl.clear, () => false),
            ctrl.log().map(function (str) {
              return m("", str)
            })
        ])
      }
    }

    export module Page {
      export function controller() {}
      export function view() {
        return m(".restExplorer", [
            m(".restExplorerControls", [
              EntryComponent,
              RangeComponent,
              CounterComponent,
            ]),
            LogComponent,
        ]);
      }
    }
  }
}
