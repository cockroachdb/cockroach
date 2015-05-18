// source: app.ts
/// <reference path="typings/angularjs/angular.d.ts" />
/// <reference path="typings/angularjs/angular-route.d.ts" />

// Author: Andrew Bonventre (andybons@gmail.com)
// Author: Bram Gruneir (bramgruneir@gmail.com)
var crApp = angular.module('cockroach', [
	'ngRoute',	
]);

crApp.config(['$routeProvider', function(routeProvider) {
  routeProvider.
  when('/rest-explorer', {
    controller:'RestExplorerCtrl',
    templateUrl:'/templates/rest_explorer.html'
  }).
    when('/monitor', {
	controller:'MonitorCtrl',
    templateUrl:'/templates/monitor.html'
  }).
  otherwise({
    redirectTo:'/'
  });
}]);
