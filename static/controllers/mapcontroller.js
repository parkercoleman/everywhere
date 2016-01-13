(function() {
    var app = angular.module('otbp', ['ui.bootstrap']);

    app.controller('MapController', ['$http',  function($http){
        this.startLoc = undefined;
        this.endLoc = undefined;
    }]);
})();