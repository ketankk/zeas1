userApp
    .controller(
        'datasecurityCtrl',
        function(myService, $scope, $http, $templateCache, $location,
            $rootScope, $route, $upload, $filter, $modal, $interval, $window) {

            $scope.policyData = {};
            $scope.tab = 11;
            $scope.policyItems = [];
            $scope.denypolicyItems = [];

            $scope.setTab = function(tabId) {
                $scope.tab = tabId;

                if ($scope.tab == 11) {
                    listtagServices()
                }

                if ($scope.tab == 'createpolic') {
                    policyform()
                    getPolicyinfo()
                }

            };

            $scope.isSet = function(tabId) {
                return $scope.tab === tabId;
            };

            /*
            list tag services
            */

            $scope.method = 'GET';
            //$scope.url = 'rest/service/security/listTagServices' ;
            $scope.url = 'rest/service/governance/service/listService' ;
            $http({
                method: $scope.method,
                url: $scope.url,
                headers: headerObj
            }).success(function(data, status) {

                $scope.status = status;
                //console.log(data)
                if (status == 200) {
                    $scope.listTagServices = data.services
                }

            }).error(function(data, status) {
                console.log(status)

            });


            var listtagServices = function() {

                $scope.method = 'GET';
                //$scope.url = 'rest/service/security/listTagServices' ;
                $scope.url = 'rest/service/governance/service/listService' ;
                $http({
                    method: $scope.method,
                    url: $scope.url,
                    headers: headerObj
                }).success(function(data, status) {

                    $scope.status = status;
                    //console.log(data)
                    if (status == 200) {
                        $scope.listTagServices = data.services
                    }

                }).error(function(data, status) {
                    console.log(status)

                });
            }

            //list tag services ends here

            /*
            service function calls
            */

            //open service creation form
            $scope.cServiceform = function() {
                //console.log('helo')
                $scope.tab = 'createservice'
            }

            //creates the service
            $scope.cService = function(serData) {
                //console.log(serData)
                $scope.service = {};
                $scope.service.description = serData.serDescription
                $scope.service.name = serData.serviceName
                $scope.service.type = "tag"
                if (serData.serStatus == 'enable') {
                    $scope.service.isEnabled = true
                } else {
                    $scope.service.isEnabled = false
                }

                $scope.tab = 11;
                $scope.method = 'POST';
                $scope.url = 'rest/service/security/addTagService' ;
                $http({
                    method: $scope.method,
                    url: $scope.url,
                    headers: headerObj,
                    data: $scope.service
                }).success(function(data, status) {

                    $scope.status = status;

                    if (status == 200) {
                        //console.log(data)
                        $scope.listTagServices = data.services
                    }

                }).error(function(data, status) {
                    console.log(status)

                });

            }

            //delete the service
            $scope.deleteService = function(ids) {
                //console.log(ids)
                $scope.tab = 11;
                $scope.method = 'DELETE';
                $scope.url = 'rest/service/security/remove/' + ids + '/service' ;
                $http({
                    method: $scope.method,
                    url: $scope.url,
                    headers: headerObj,
                    data: $scope.service
                }).success(function(data, status) {

                    $scope.status = status;

                    if (status == 200) {
                        console.log(data)
                        $scope.listTagServices = data.services
                    }

                }).error(function(data, status) {
                    console.log(status)

                });
            }

            //service functions ends here



            /*
            policy functions
            */

            //list Service Policies
            $scope.getTags_service = function(ids, name) {
                //console.log('get tags listed under this service')
                //console.log(ids)
                $scope.serviceId = ids
                $scope.serviceName = name
                $scope.tab = 'getTags'
                $scope.method = 'GET';

                $scope.url = 'rest/service/security/listServicePolicies/' + ids ;
                $http({
                    method: $scope.method,
                    url: $scope.url,
                    headers: headerObj,
                    data: $scope.service
                }).success(function(data, status) {

                    $scope.status = status;

                    if (status == 200) {
                        console.log(data.policies)
                        $scope.listservicePolicies = data.policies
                    }

                }).error(function(data, status) {
                    console.log(status)

                });
            }

            //delete policy
            $scope.deletePolicy = function(idp) {
                console.log(idp)
                $scope.tab = 'getTags'
                $scope.method = 'DELETE';
                $scope.url = 'rest/service/security/remove/' + idp + '/policy/' + $scope.serviceId;
                $http({
                    method: $scope.method,
                    url: $scope.url,
                    headers: headerObj,
                    data: $scope.service
                }).success(function(data, status) {

                    $scope.status = status;

                    if (status == 200) {
                        console.log(data)
                        $scope.listservicePolicies = data.policies
                    }

                }).error(function(data, status) {
                    console.log(status)

                });
            }

            //policy form page
            var policyform = function() {

                $scope.policyItems.push({
                    'groups': "",
                    'users': "",
                    'conditions': "",
                    'accesses': "",
                });

                $scope.denypolicyItems.push({
                    'groups': "",
                    'users': "",
                    'conditions': "",
                    'accesses': "",
                });

            }

            //toggle policy enabling and audit enabling
            $scope.auditEnabled = true
            $scope.policyenabled = true
            $scope.auditLogging = function(checkBox) {
                $scope.checkBox1 = !$scope.checkBox1;
                if (checkBox) {
                    $scope.auditEnabled = true
                } else {

                    $scope.auditEnabled = false
                    console.log($scope.auditEnabled)
                }
            }

            $scope.policyEnabled = function(checkBox) {
                $scope.checkBox2 = !$scope.checkBox2;
                if (checkBox) {
                    $scope.policyenabled = true
                } else {
                    $scope.policyenabled = false
                    console.log($scope.policyenabled)
                }
            }

            //get data of requied fields
            var getPolicyinfo = function() {
                getlistofGroups()
                getlistofUsers()
                getAccesstypes()

            }

            //get access types
            var getAccesstypes = function() {
                $scope.accessitems = []
                nn = false;
                $scope.method = 'GET';
                $scope.url = 'rest/service/security/access' ;
                $http({
                    method: $scope.method,
                    url: $scope.url,
                    headers: headerObj
                }).success(function(data, status) {

                    $scope.status = status;

                    if (status == 200) {
                        //console.log(data.accessTypes)
                        $scope.accessData = data

                        angular.forEach($scope.accessData.accessTypes, function(v, k) {
                            angular.forEach($scope.accessitems, function(vv, kk) {
                                if (vv == v.name) {
                                    nn = true

                                } else {
                                    nn = false

                                }
                            })

                            if (nn == false) {
                                $scope.accessitems.push(v.name)

                            }

                        })
                        //console.log($scope.accessitems)
                    } else {
                        console.log(status)
                    }

                }).error(function(data, status) {
                    console.log(status)

                });

            }

            //get list of users in policy form
            var getlistofUsers = function() {
                $scope.masterUsers = []
                $scope.method = 'GET';
                $scope.url = 'rest/service/security/listOfUsers' ;
                $http({
                    method: $scope.method,
                    url: $scope.url,
                    headers: headerObj
                }).success(function(data, status) {

                    $scope.status = status;
                    if (status == 200) {
                        angular.forEach(data.vxUserList.vXUsers, function(v, k) {
                            $scope.masterUsers.push(v.name)
                        });
                    } else {
                        console.log(status)
                    }

                }).error(function(data, status) {
                    console.log(status)

                });
            }

            //get list of groups in policy form
            var getlistofGroups = function() {
                $scope.masterGroups = []
                $scope.method = 'GET';
                $scope.url = 'rest/service/security/listOfGroups' ;
                $http({
                    method: $scope.method,
                    url: $scope.url,
                    headers: headerObj
                }).success(function(data, status) {

                    $scope.status = status;
                    if (status == 200) {

                        angular.forEach(data.vxGroupList.vXGroups, function(v, k) {
                            $scope.masterGroups.push(v.name)
                        });
                    } else {
                        console.log(status)
                    }

                }).error(function(data, status) {
                    console.log(status)

                });
            }

            //get updated tag list to select 
            $scope.updateTags = function() {

                $scope.method = 'GET';
                $scope.url = 'rest/service/governance/types?type=' + 'TRAIT' ;
                $http({
                    method: $scope.method,
                    url: $scope.url,
                    headers: headerObj
                }).success(function(data, status) {

                    $scope.status = status;
                    //console.log(data)
                    if (status == 200) {
                        $scope.policytags = data
                    }

                }).error(function(data, status) {
                    console.log(status)

                });

            }

            //add  new row to the table
            $scope.addNew = function(key) {
                if (key == 'allowCond') {
                    $scope.policyItems.push({
                        'groups': "",
                        'users': "",
                        'conditions': "",
                        'accesses': "",
                    });
                    console.log($scope.policyItems)
                }

                if (key == 'denyCond') {
                    $scope.denypolicyItems.push({
                        'groups': "",
                        'users': "",
                        'conditions': "",
                        'accesses': "",
                    });
                    console.log($scope.denypolicyItems)
                }
            }

            //remove row from the table
            $scope.remove = function() {
                console.log('remove row')
                var newDataList = [];
                angular.forEach($scope.policyItems, function(selected) {
                    if (!selected.selected) {
                        newDataList.push(selected);
                    }
                });
                $scope.policyItems = newDataList;
            }



            //select groups for allowed conditions and denied conditions in policy creation form
            $scope.sGroups = []
            $scope.sdGroups = []
            $scope.selectedGroup = function(gName, index, alde) {

                //console.log(alde)
                if (alde == 'allowCond') {
                    rep = false
                    angular.forEach($scope.sGroups, function(v, k) {
                        if (v == gName) {
                            rep = true
                        }
                    })

                    if (rep != true) {
                        $scope.sGroups.push(gName)
                    }

                    angular.forEach($scope.policyItems, function(v, k) {

                        if (k == index) {

                            v.groups = $scope.sGroups
                        }

                    })
                    console.log($scope.policyItems)
                }


                if (alde == 'denyCond') {
                    rep1 = false
                    angular.forEach($scope.sdGroups, function(v, k) {
                        if (v == gName) {
                            rep1 = true
                        }
                    })

                    if (rep1 != true) {
                        $scope.sdGroups.push(gName)
                    }

                    angular.forEach($scope.denypolicyItems, function(v, k) {

                        if (k == index) {

                            v.groups = $scope.sdGroups
                        }

                    })
                    console.log($scope.denypolicyItems)
                }

            }

            //remove froup
            $scope.removeGroup = function(rGroupname) {
                console.log(rGroupname)
            }

            //select users for allowed conditions and denied conditions in policy creation form
            $scope.sUsers = []
            $scope.sdUsers = []
            $scope.selectedUser = function(uName, index, alde) {

                if (alde == 'allowCond') {
                    rep = false
                    angular.forEach($scope.sUsers, function(v, k) {
                        if (v == uName) {
                            rep = true
                        }
                    })

                    if (rep != true) {
                        $scope.sUsers.push(uName)
                    }

                    angular.forEach($scope.policyItems, function(v, k) {

                        if (k == index) {

                            v.users = $scope.sUsers
                        }

                    })

                    console.log($scope.policyItems)
                }

                if (alde == 'denyCond') {
                    rep1 = false
                    angular.forEach($scope.sdUsers, function(v, k) {
                        if (v == uName) {
                            rep1 = true
                        }
                    })

                    if (rep1 != true) {
                        $scope.sdUsers.push(uName)
                    }

                    angular.forEach($scope.denypolicyItems, function(v, k) {

                        if (k == index) {

                            v.users = $scope.sdUsers
                        }

                    })

                    console.log($scope.denypolicyItems)
                }


            }

            $scope.removeUser = function(rUsername) {
                console.log(rUsername)
            }

            //modal for policy conditions and component permision
            $scope.policyM = function(k, data, index) {

                if (k == 'pc') {
                    $scope.indp = index
                    $('#policyCondition').modal('show');

                }
                if (k == 'closePC') {

                    $('#policyCondition').modal('hide');
                    angular.forEach($scope.policyItems, function(v, k) {

                        if (k == $scope.indp) {
                            var val = []
                            val = data
                            var condition = []

                            condition.push({
                                'type': "accessed-after-expiry",
                                'value': val
                            });


                            v.conditions = condition
                        }
                        console.log($scope.policyItems)
                    })
                }
                if (k == 'cp') {
                    console.log(index)
                    $('#componentPermision').modal('show');
                }
                

            }

            //access components

            $scope.accessComps = []
            $scope.accessComp = function(comp) {
                $scope.apermissions = []
                $scope.comp = comp
                nn = false
                angular.forEach($scope.accessComps, function(v, k) {
                    if (comp == v.name) {
                        nn = true
                    }
                })
                if (nn == false) {
                    angular.forEach($scope.accessData.accessTypes, function(v, k) {
                        if (v.name == comp) {
                            $scope.apermissions.push(v.label)
                        }

                    })

                    $scope.accessComps.push({
                        'name': comp,
                        'permission': $scope.apermissions

                    });

                }


            }

            //cancel or submit permission
            $scope.cPermission =  function(obj){
                
                angular.forEach(obj,function(v,k){
                    console.log(v)
                    angular.forEach(v.permission,function(vv,kk){
                        if(vv.chkd){
                        console.log('submit permissions')
                    }
                    })
                    
                })
            }
            //submit policy form to create policy
            $scope.cPolicy = function() {
                console.log($scope.policyData)
                console.log($scope.policyItems)
                $scope.policyPayload = new Object();

                $scope.policyPayload.allowExceptions = [];
                $scope.policyPayload.denyExceptions = [];

                $scope.policyPayload.description = $scope.policyData.description
                $scope.policyPayload.isAuditEnabled = $scope.auditEnabled
                $scope.policyPayload.isEnabled = $scope.policyenabled
                $scope.policyPayload.name = $scope.policyData.policyName

                $scope.policyPayload.policyItems = $scope.policyItems
                $scope.policyPayload.denyPolicyItems = [];

                $scope.policyPayload.policyType = []
                $scope.policyPayload.service = $scope.serviceName;
                $scope.policyPayload.resources = {}

                console.log($scope.policyPayload)

            }


        })