(function() {
    var app = angular.module('otbp', ['ui.bootstrap']);

    app.controller('MapController', ['$scope', '$http',  function($scope, $http){
        this.startLoc = undefined;
        this.endLoc = undefined;
        this.currentRoute = undefined;

        this.getCitiesFromPartial = function(cityName) {
            return $http.get('/graph/places/' + encodeURIComponent(cityName))
                .then(function(response){
                  return response.data;
            });
        };

        this.calculateRoute = function(){
            var ctrl = this;
            $http.get('/graph/calc_route/from/' + this.startLoc.gid + '/to/' + this.endLoc.gid)
                .then(function(response){
                    ctrl.currentRoute = response.data;

                    var currentRouteLayer = L.tileLayer.wms("http://localhost:8080/geoserver/wms", {
                        layers: 'otbp:user_routes',
                        format: 'image/png',
                        'CQL_FILTER':"route_id='" + ctrl.currentRoute.route_id + "'",
                        attribution: "Off the Beatnik Path",
                        transparent: true
                    });

                    // Remove the old user_routes layer
                    map.eachLayer(function(layer) {
                        if(layer.wmsParams !== undefined && layer.wmsParams.layers === 'otbp:user_routes'){
                            map.removeLayer(layer);
                        }
                    });

                    // add the new one
                    map.addLayer(currentRouteLayer);
                });
        };
    }]);
})();
