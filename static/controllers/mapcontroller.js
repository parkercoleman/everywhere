(function() {
    var app = angular.module('otbp', ['ui.bootstrap']);

    app.controller('MapController', ['$http',  function($http){
        this.startLoc = undefined;
        this.endLoc = undefined;

        this.getCitiesFromPartial = function(cityName) {
            return $http.get('/graph/places/' + encodeURIComponent(cityName))
                .then(function(response){
                  return response.data;
            });
        };

        this.calculateRoute = function($http){
            $http.get('/calc_route/from/' + this.startLoc.gid + '/to/' + this.endLoc.gid)
                .then(function(response){

                });
        };
    }]);
})();
