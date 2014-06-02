var app = angular.module('app', ['ngRoute'])
    .config(function ($routeProvider) {
        $routeProvider
            .when('/zookeepers', {
                controller: 'ZookeepersController',
                templateUrl: '/zookeepers'
            })
            .when('/topics', {
                controller: 'TopicsController',
                templateUrl: '/topics'
            })
            .when('/brokers', {
                controller: 'BrokersController',
                templateUrl: '/brokers'
            })
            .when('/topics/:name/:cluster', {
                controller: 'TopicController',
                templateUrl: function (params) {
                    return '/topics/' + params.name + '/' + params.cluster
                }
            })
            .otherwise({
                redirectTo: '/zookeepers'
            });
    });

app.run(function ($rootScope, $location) {
    $rootScope.isActive = function (route) {
        return route === $location.path();
    };
});

app.service('topicService', function () {
    var topic_ = "";
    var cluster = "";

    this.setTopic = function (topic) {
        topic_ = topic;
    };

    this.getTopic = function () {
        return topic_;
    };

    this.setCluster = function (cluster) {
        cluster_ = cluster;
    };

    this.getCluster = function () {
        return cluster_;
    };
});

app.filter('reverse', function () {
    return function (items) {
        return items.slice().reverse();
    };
});