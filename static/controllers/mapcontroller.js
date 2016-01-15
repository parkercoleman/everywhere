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
            $http.get('/graph/calc_route/from/' + this.startLoc.gid + '/to/' + this.endLoc.gid)
                .then(function(response){
                    this.currentRoute = response.data.route_id;

                    map.removeLayer(route);

                    var route = new ol.layer.Image({
                            source: new ol.source.ImageWMS({
                            url: 'http://localhost:8080/geoserver/wms',
                            params: {
                                'LAYERS': 'otbp:user_routes',
                                'CQL_FILTER':"route_id='" + this.currentRoute + "'"
                            },
                            serverType: 'geoserver'
                        })
                    });

                    map.addLayer(route);
                });
        };
    }]);
})();
