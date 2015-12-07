/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />
/// <reference path="../../typings/tsd.d.ts" />
/// <reference path="../models/proto.ts" />

module Models {
  "use strict";
  export module SQLQuery {
    import Request = Models.Proto.Request;
    import Response = Models.Proto.Response;

    export function runQuery(q: string): _mithril.MithrilPromise<Response> {

      let data: Request = {user: "root", sql: q};

      let xhrConfig = function(xhr: XMLHttpRequest): XMLHttpRequest {
        xhr.setRequestHeader("Content-Type", "application/x-protobuf");
        xhr.setRequestHeader("Accept", "application/json");

        // TODO: receive data as protobuf instead of JSON
        // xhr.setRequestHeader("Accept", "application/x-protobuf");
        // xhr.responseType = "arraybuffer";
        return xhr;
      };

      return m.request({
        url: "/sql/Execute",
        config: xhrConfig,
        method: "POST",
        data: data,
      });
    }

  }
}
