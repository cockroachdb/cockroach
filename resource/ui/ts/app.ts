// source: app.ts
/// <reference path="typings/mithriljs/mithril.d.ts" />
/// <reference path="controllers/rest_explorer.ts" />
/// <reference path="controllers/monitor.ts" />

// Author: Andrew Bonventre (andybons@gmail.com)
// Author: Bram Gruneir (bramgruneir@gmail.com)

m.route.mode = "hash";
m.route(document.getElementById("root"), "/rest-explorer", {
    "/rest-explorer": AdminViews.RestExplorer.Page,
    "/monitor": AdminViews.Monitor.Page,
});
