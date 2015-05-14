// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Andrew Bonventre (andybons@gmail.com)
// Author: Bram Gruneir (bramgruneir@gmail.com)

/// <reference path="typings/angularjs/angular.d.ts" />
/// <reference path="typings/angularjs/angular-route.d.ts" />

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
