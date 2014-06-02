app.controller("TopicController", function ($http, $scope, $location, $routeParams, $interval) {
    $scope.graphData = {};
    $scope.graphData = {"mintime": null,
                        "maxtime": null,
                        "maxoffset": null,
                        "factors": {}};
    $scope.graph = {}
    $scope.exit = false;

    var maxPartitionCount = 0;
    $scope.maxPartitionCount = [];
    $http.get('/topics.json/' + $routeParams.name + '/' + $routeParams.cluster).success(function (data) {
        $scope.topic = data;
        angular.forEach($scope.topic, function (consumerGroup) {

            angular.forEach(consumerGroup.offsets, function (offset) {
                offset.partition = parseInt(offset.partition)
            })
        });
    });

    $http.get('/topics.json/' + $routeParams.name + '/' + $routeParams.cluster + '/range').success(function (data) {
        $scope.topicOffsets = {}

        angular.forEach(data, function (message) {
            if(message.consumerGroup == "Start") {
                $scope.startingOffsets = message;
            } else if(message.consumerGroup == "End") {
                $scope.endingOffsets = message;
                angular.forEach($scope.endingOffsets.offsets, function (offset) {
                    $scope.topicOffsets[offset.partition] = offset.offset;
                });

                $scope.maxPartitionCount = new Array(message.offsets.length);
            } else {
                console.log("Error");
            }
        });
    })

    $http.get('/topics.json/' + $routeParams.name + '/' + $routeParams.cluster + '/feed').success(function (data) {
        angular.forEach(data, function (message) {
            var well = angular.element('<div class="well well-sm"/>');
            well.text(message);
            $("#messages").append(well);
        });
    })

    var ws = new WebSocket('ws://' + $location.host() + ':' + $location.port() + '/topics.json/' + $routeParams.name + '/' + $routeParams.cluster + '/graph')
    ws.onmessage = function (message) {
        object = JSON.parse(message.data)
        addToGraphData($scope.graph, $scope.graphData, object);
    }
    initGraph($scope.graph);
    var redraw = d3.timer(function() {
        drawGraph($scope.graph, $scope.graphData);
        return $scope.exit;
    }); // refresh delay in ms
    $scope.$on('$destroy', function () {
        ws.close();
        $scope.exit = true;
    });
});