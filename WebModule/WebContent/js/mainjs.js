google.setOnLoadCallback(function() {
	angular.bootstrap(document.body, [ 'userApp' ]);
});
google.load('visualization', '1', {
	packages : [ 'corechart' ]
});
var userApp = angular.module("userApp", [ "ngRoute", 'tableSort',
		'ui.bootstrap', "googlechart", 'flowChart', 'angularFileUpload',
		'scrollable-table' ]);
var proj_prefix = '/WebModule/';
userApp.config(function($routeProvider, $locationProvider) {
	$locationProvider.html5Mode(true).hashPrefix('!#');
	$routeProvider.when('/', {
		templateUrl : proj_prefix+'views/login.html'
	}).when('/DataSchema/', {
		templateUrl : proj_prefix+'views/dataschema.html',
		activetab : 'dataSchema'
	}).when('/dom/', {
		templateUrl : proj_prefix+'views/dom.html',
		activetab : 'dataSchema'
	}).when('/DataSchemaPreview/', {
		templateUrl : proj_prefix+'views/schemapreview.html',
		activetab : 'dataSchema'
	}).when('/ValidationRule/', {
		templateUrl : proj_prefix+'views/validationtable.html',
		reloadOnSearch : false,
		activetab : 'dataSchema'
	}).when('/IngestionSummary/', {
		templateUrl : proj_prefix+'views/ingestionsummary.html',
		activetab : 'dataSchema'
	}).when('/Streaming/', {
		templateUrl : proj_prefix+'views/streaming.html',
		activetab : 'dataSchema'
	}).when('/Driver/', {
		templateUrl : proj_prefix+'views/driver.html',
		activetab : 'dataSchema'
	}).when('/DataSource/', {
		templateUrl : proj_prefix+'views/datasourcer.html',
		activetab : 'DataSource'
	}).when('/userDetails/', {
		templateUrl : proj_prefix+'views/userdetails.html',
		activetab : 'admin'
	}).when('/archiveDetails/', {
		templateUrl : proj_prefix+'views/archiveddetails.html',
		activetab : 'admin'
	}).when('/DataSet/', {
		templateUrl : proj_prefix+'views/dataSet.html',
		controller : 'dataSet',
		activetab : 'dataSet'
	}).when('/DataIngestion/', {
		templateUrl : proj_prefix+'views/DataIngestion.html',
		activetab : 'DataIngestion'
	}).when('/GoogleGraph/', {
		templateUrl : proj_prefix+'views/gchart.html',
		activetab : 'GoogleGraph'
	}).when('/DatapipeWorkbench/', {
		templateUrl : proj_prefix+'views/DatapipeWorkbench.html',
		// controller : 'userApp.DatapipeWorkbench',
		activetab : 'DatapipeWorkbench'
	}).when('/QuerySelector/', {
		templateUrl : proj_prefix+'views/QuerySelector.html',
		controller : 'userApp.QuerySelector',
		activetab : 'QuerySelector'
	}).when('/PiplineResults/', {
		templateUrl : proj_prefix+'views/PiplineResults.html',
		controller : 'PiplineResults',
		activetab : 'PiplineResults'
	}).otherwise({
		redirectTo : '/'
	});

});

userApp.directive("contenteditable", function() {
	return {
		require : "ngModel",
		link : function(scope, element, attrs, ngModel) {

			function read() {
				ngModel.$setViewValue(element.html());
			}

			ngModel.$render = function() {
				element.html(ngModel.$viewValue || "");
			};

			element.bind("blur keyup change", function() {
				scope.$apply(read);
			});
		}
	};
});
userApp.factory('myService', function() {
	var savedData = {}
	function set(data) {
		savedData = data;
	}
	function get() {
		return savedData;
	}

	return {
		set : set,
		get : get
	}

});
function SortableCTRL($scope, $http, $templateCache, $location, $rootScope,
		$route) {

	// console.log($scope.userRole);
	$scope.model = new Object();
	$scope.model.link = 'Select';
	$scope.stramORDriver = function(linkname) {
		// alert(linkname);
		if (linkname == 'Streaming') {
			$location.path('/Streaming/');
		} else if (linkname == 'Driver') {
			$location.path('/Driver/');
		} else if (linkname == 'Profile') {
			$location.path('/DataSchema/');
		}
		// $scope.model.link = 'Select';
	}
	$scope.userRole = localStorage.getItem('itc.userRole');
	// console.log(localStorage.getItem('itc.dUsername'));
	$("#userDName").text(localStorage.getItem('itc.dUsername'));
	$scope.method = 'GET';
	$scope.url = 'rest/service/TabName';

	// $scope.url = 'https://jsonblob.com/api/54d5d294e4b0af9761b3a0c8';
	$http({
		method : $scope.method,
		url : $scope.url,
		cache : $templateCache,
		headers : {
			'X-Auth-Token' : localStorage.getItem('itc.authToken')
		}
	}).success(function(data, status) {
		$scope.status = status;
		var i = 0;
		$scope.dataTab = {}
		angular.forEach(data, function(attr) {
			// console.log(attr.jsonblob);
			$scope.dataTab[i] = angular.fromJson(attr.jsonblob);
			;
			// $scope.datasetArr[i] = attr.name;
			i++;
		});
		// console.log($scope.dataTab);
	}).error(function(data, status) {
		if (status == 401) {
			$location.path('/');
		}
		$scope.dataTab = data || "Request failed";
		$scope.status = status;
	});

	var sortableEle;
	$rootScope.Tab = 1;
	$scope.active = function() {
		return $scope.panes.filter(function(pane) {
			return pane.active;
		})[0];
	};
	$scope.sortableArray = [ 'One', 'Two', 'Three' ];
	/*
	 * $scope.progressArray =['Configure Data Schema','Configure Data Locations
	 * and Scheduler','Validation Rule Definition'];
	 */
	$scope.progressArray = [ {
		"tab" : "/DataSchemaPreview/",
		"title" : " Data Schema  "
	}, {
		"tab" : "/IngestionSummary/",
		"title" : " Data Source "
	}, {
		"tab" : "/ValidationRule/",
		"title" : " Data Quality "
	} ];
	// console.log($scope.progressArray[0].tab);
	$scope.$route = $route;
	$scope.pageLocation = $location.path();
	/*
	 * if($scope.pageLocation == '/DataSchema/'){ $scope.pageLocation1 =
	 * '/DataSchemaPreview/'; } else{ $scope.pageLocation1 = $location.path(); }
	 */

	// console.log($scope.progressArray);
	$scope.isActive = function(viewLocation) {
		var pageLocation = $location.path();
		if ($rootScope.Tab == 1) {
			pageLocation = '/DataSchema/';
		} else {
			pageLocation = '/admin/';
		}
		return viewLocation === pageLocation;
	};
	/*
	 * $scope.add = function() { $scope.sortableArray.push('Item:
	 * '+$scope.sortableArray.length);
	 * 
	 * sortableEle.refresh(); }
	 */
	$scope.dragStart = function(e, ui) {
		ui.item.data('start', ui.item.index());
	}
	$scope.dragEnd = function(e, ui) {
		var start = ui.item.data('start'), end = ui.item.index();

		$scope.sortableArray.splice(end, 0, $scope.sortableArray.splice(start,
				1)[0]);
		$scope.sortableArray = $scope.data;
		$scope.method = 'PUT';
		$scope.url = 'https://jsonblob.com/api/54d5d294e4b0af9761b3a0c8';
		;
		$http({
			method : $scope.method,
			url : $scope.url,
			data : $scope.sortableArray,
			cache : $templateCache
		}).success(function(data, status) {
			$scope.status = status;
		}).error(function(data, status) {
			if (status == 401) {
				$location.path('/');
			}
			$scope.data = data || "Request failed";
			$scope.status = status;
		});

		$scope.$apply();
	}

	/*
	 * sortableEle = $('#sortable').sortable({ start : $scope.dragStart, update :
	 * $scope.dragEnd });
	 */
}

PiplineResults = function($scope, $http, $templateCache, $location, $rootScope,
		$route) {
	/*
	 * if(localStorage.getItem('itc.authToken')){ var authToken =
	 * localStorage.getItem('itc.authToken'); $rootScope.authToken = authToken; }
	 */
	$scope.delete1 = function() {
		currentUser = $scope.Deluser;
		objDel = $scope.DelObj;
		// alert(currentUser)
		$scope.deleteRecord(currentUser, objDel);
	};
	$scope.showconfirm = function(id, obj) {
		$scope.Deluser = id;
		$scope.DelObj = obj;
		$('#deleteConfirmModal').modal('show');
	};
	$scope.method = 'GET';
	$scope.url = 'rest/service/pipeline/getMLAnalysis';

	// $scope.url = 'rest/service//pipeline/runMachineLearning';// set scope
	// data---post

	$http({
		method : $scope.method,
		url : $scope.url,
		headers : {
			'X-Auth-Token' : localStorage.getItem('itc.authToken')
		}
	}).success(function(dataMlAna, status) {
		$scope.status = status;
		$scope.dataMlAna = dataMlAna;
	}).error(function(dataMlAna, status) {
		$scope.dataMlAna = dataMlAna || "Request failed";
		$scope.status = status;
	});

	$scope.objrun = {};
	$scope.objrun.training = {};
	$scope.objrun.testing = {};
	/*$scope.algorithmArr = [ "Random Forest", "Logistic Regression",
			"R - Clustering" ];
	$scope.objrun.algorithm = $scope.algorithmArr[0];*/
	
	$scope.algorithmArr = [ "--Select Algorithm--","linear regression","Random Forest", "Logistic Regression",
	            			"R - Clustering" ];
	$scope.objrun.algorithm = $scope.algorithmArr[0];
   // $scope.datasetArray = [ "<--Select Dataset-->"];
   // $scope.objrun.training = $scope.datasetArray[0];
	$scope.method = 'GET';
	$scope.url = 'rest/service/pipeline/getPipelines';

	$http({
		method : $scope.method,
		url : $scope.url,
		headers : {
			'X-Auth-Token' : localStorage.getItem('itc.authToken')
		}
	}).success(function(dataSetAlg, status) {
		$scope.status = status;
		$scope.dataSetAlg = dataSetAlg;
		// console.log($scope.data);
		// delete $scope.data.dataType;
		// $scope.objrun.training = $scope.data[0].dataSet;
		var i = 1;
		$scope.datasetArr = new Array();
		angular.forEach($scope.data, function(attr) {
			$scope.datasetArr[0] =" select";
			$scope.datasetArr[i] = attr.dataSet;
			// $scope.datasetArr[i] = attr.name;
			i++;
		});
		//$scope.dataSetAlg[0]="<-- Select Dataset -->";
		$scope.objrun.training = $scope.datasetArr[0];
		//$scope.objrun.testing = $scope.dataSetAlg[1];
		//$scope.objrun.training.dataType = "Training Data";
		//$scope.objrun.testing.dataType = "Test Data";
		// console.log($scope.datasetArr)
	}).error(function(dataSetAlg, status) {
		$scope.data = dataSetAlg || "Request failed";
		$scope.status = status;
	});
	$scope.objRun = {};
	$scope.loader = {
		loading : false
	};

	$scope.columnName = new Object();
	$scope.tableName = new Array();
	$scope.columnNameByID = new Array();
	$scope.tableNameData = new Array();
	$scope.getColumnName = function(tablename1, tableNo) {
		//alert("hi");
		//alert(tablename1);
		var posTable = jQuery.inArray(tablename1, $scope.tableName);
		if (posTable == -1) {
			$scope.method = 'GET';
			$scope.url = 'rest/service/getColumns/' + tablename1;
			$http({
				method : $scope.method,
				url : $scope.url,
				// cache : $templateCache
				headers : {
					'X-Auth-Token' : localStorage.getItem('itc.authToken')
				}
			}).success(function(data, status) {
				$scope.data = data;
				// console.log($scope.data);
				$scope.columnName[tableNo] = new Object();

				$scope.columnName[tableNo] = $scope.data;
				$scope.tableNameData[tablename1] = $scope.data;
				$scope.columnNameByID = $scope.data;
				$scope.objrun.schema  = $scope.data;
				// console.log($scope.columnName);
				$scope.status = status;
				$scope.tableName[$scope.tableName.length] = tablename1;

			}).error(function(data, status) {
				if (status == 401) {
					$location.path('/');
				}
				$scope.data = data || "Request failed";
				$scope.status = status;
			});
		} else {
			var prevTable = $scope.tableName[posTable];
			$scope.columnName[tableNo] = new Object();

			$scope.columnName[tableNo] = $scope.tableNameData[prevTable];
		}
	};
	
	$scope.changeColumnFeature =function(value){
		$scope.objrun.feature = value;
	}
	
	$scope.changeColumnLabel =function(value){
		$scope.objrun.label = value;
	}
	
	$scope.runMachine = function(objRun) {
		//alert("heloo");
		//console.log(objRun);
		$scope.loader.loading = true;
		$('#runSucessID').html('');
		if (objRun.algorithm == "R - Clustering") {
			// delete objRun.dataTypetesting;
			delete objRun.testing;
			objRun.training.dataType = "Training Data";
		}

		$scope.objRun = objRun;
		$scope.method = 'POST';
		// console.log("Call");

		$scope.url = 'rest/service/pipeline/runMachineLearning';

		$http({
			method : $scope.method,
			url : $scope.url,
			data : $scope.objRun,
			headers : {
				'X-Auth-Token' : localStorage.getItem('itc.authToken')
			}
		})
				.success(function(data, status) {
					$scope.status = status;
					$scope.data = data;
					$scope.loader.loading = false;
					$route.reload();
				})
				.error(
						function(data, status) {
							$scope.method = 'GET';
							$scope.url = 'rest/service/pipeline/getMLAnalysis';

							// $scope.url =
							// 'rest/service//pipeline/runMachineLearning';//
							// set scope
							// data---post

							$http(
									{
										method : $scope.method,
										url : $scope.url,
										headers : {
											'X-Auth-Token' : localStorage
													.getItem('itc.authToken')
										}
									}).success(function(dataMlAna, status) {
								$scope.status = status;
								$scope.dataMlAna = dataMlAna;
							}).error(
									function(dataMlAna, status) {
										$scope.dataMlAna = dataMlAna
												|| "Request failed";
										$scope.status = status;
									});

							if (objRun.algorithm == "R - Clustering") {
								// $("Successfully executed 'R' script and
								// output has been stored at
								// <strong>/root/ZEAS/data/citi/ROutput</strong>").appendTo('#runSucessID');
								$('#runSucessID')
										.html(
												'Successfully executed \'R\' script and output has been stored at /root/ZEAS/data/citi/ROutput')
							}
							$scope.data = data || "Request failed";
							$scope.status = status;
							$scope.loader.loading = false;
							// $route.reload();
							// $scope.objRun.algorithm = 'R - Clustering';
						});

	};
	changeMClass('three');

};


userApp.getPipelines = function($scope, $http, $rootScope) {

	$scope.objrun = {};
	$scope.objrun.training = {};
	$scope.objrun.testing = {};
	$scope.algorithmArr = [ "Random Forest", "Logistic Regression",
			"R - Clustering" ];
	$scope.objrun.algorithm = $scope.algorithmArr[0];

	$scope.method = 'GET';
	$scope.url = 'rest/service/pipeline/getPipelines';

	$http({
		method : $scope.method,
		url : $scope.url,
		headers : {
			'X-Auth-Token' : localStorage.getItem('itc.authToken')
		}
	}).success(function(data, status) {
		$scope.status = status;
		$scope.data = data;
		// console.log($scope.data);
		// delete $scope.data.dataType;
		// $scope.objrun.training = $scope.data[0].dataSet;
		var i = 0;
		$scope.datasetArr = new Array();
		angular.forEach($scope.data, function(attr) {
			$scope.datasetArr[i] = attr.dataSet;
			// $scope.datasetArr[i] = attr.name;
			i++;
		});
		$scope.objrun.training = $scope.data[0];
		$scope.objrun.testing = $scope.data[1];
		$scope.objrun.training.dataType = "Training Data";
		$scope.objrun.testing.dataType = "Test Data";
		// console.log($scope.datasetArr)
	}).error(function(data, status) {
		if (status == 401) {
			$location.path('/');
		}
		$scope.data = data || "Request failed";
		$scope.status = status;
	});

}
userApp.getMLAnalysis = function($scope, $http, $rootScope, $route) {
	$scope.method = 'GET';
	$scope.url = 'rest/service/pipeline/getMLAnalysis';

	// $scope.url = 'rest/service//pipeline/runMachineLearning';// set scope
	// data---post

	$http({
		method : $scope.method,
		url : $scope.url,
		headers : {
			'X-Auth-Token' : localStorage.getItem('itc.authToken')
		}
	}).success(function(dataMlAna, status) {
		$scope.status = status;
		$scope.dataMlAna = dataMlAna;
	}).error(function(dataMlAna, status) {
		$scope.dataMlAna = dataMlAna || "Request failed";
		$scope.status = status;
	});

}
userApp.runMachineLearning = function($scope, $http, $rootScope, $route, $q) {
	$scope.objRun = {};
	$scope.loader = {
		loading : false
	};

	$scope.runMachine = function(objRun) {
		$scope.loader.loading = true;
		$('#runSucessID').html('');
		if (objRun.algorithm == "R - Clustering") {
			// delete objRun.dataTypetesting;
			delete objRun.testing;
			objRun.training.dataType = "Training Data";
		}

		$scope.objRun = objRun;
		$scope.method = 'POST';
		// console.log("Call");

		$scope.url = 'rest/service/pipeline/runMachineLearning';

		$http({
			method : $scope.method,
			url : $scope.url,
			data : $scope.objRun,
			headers : {
				'X-Auth-Token' : localStorage.getItem('itc.authToken')
			}
		})
				.success(function(data, status) {
					$scope.status = status;
					$scope.data = data;
					$scope.loader.loading = false;
					$route.reload();
				})
				.error(
						function(data, status) {
							$scope.method = 'GET';
							$scope.url = 'rest/service/pipeline/getMLAnalysis';

							// $scope.url =
							// 'rest/service//pipeline/runMachineLearning';//
							// set scope
							// data---post

							$http(
									{
										method : $scope.method,
										url : $scope.url,
										headers : {
											'X-Auth-Token' : localStorage
													.getItem('itc.authToken')
										}
									}).success(function(dataMlAna, status) {
								$scope.status = status;
								$scope.dataMlAna = {};
							}).error(
									function(dataMlAna, status) {
										$scope.dataMlAna = dataMlAna
												|| "Request failed";
										$scope.status = status;
									});
							if (objRun.algorithm == "R - Clustering") {
								// $("Successfully executed 'R' script and
								// output has been stored at
								// <strong>/root/ZEAS/data/citi/ROutput</strong>").appendTo('#runSucessID');
								$('#runSucessID')
										.html(
												'Successfully executed \'R\' script and output has been stored at /root/ZEAS/data/citi/ROutput')
							}
							$scope.data = data || "Request failed";
							$scope.status = status;
							$scope.loader.loading = false;
							// $route.reload();
							// $scope.objRun.algorithm = 'R - Clustering';
						});

	};
};

userApp.DatapipeWorkbench = function($scope, $location, $anchorScroll) {

}

userApp.controller('palleteCtrl', function($scope) {
	$scope.oneAtATime = true;

	$scope.status = {
		isFirstOpen : false,
		isFirstDisabled : false,
		isSecondOpen : false,
		isSecondDisabled : true
	};
	$scope.icon = {
		"false" : 'icon-plus-sign',
		"true" : 'icon-minus-sign'
	}
});
userApp.controller('PipelineCtrlTabs', function($scope, $window) {

});
userApp.service('uploadsService', function($http, $rootScope) {

	var code = '';
	var fileName = '';

	this.uploadFile = function(files, mapclass, reducer,dataSetName,stageName) {

		var fd = new FormData();

		// Take the first selected file
		fd.append("file", files[0]);
		fd.append("mapper", mapclass);
		fd.append("reducer", reducer);
		fd.append("dataSetName", dataSetName);
		fd.append("stageName", stageName);
		var promise = $http.post('rest/service/uploadAndTestRun/', fd, {
			withCredentials : true,
			headers : {
				'X-Auth-Token' : localStorage.getItem('itc.authToken'),
				'Content-Type' : undefined

			},

			transformRequest : angular.identity
		}).then(function(response) {

			code = response.data.code;
			fileName = response.data.fileName;

			return {
				code : function() {
					return code;
				},
				fileName : function() {
					return fileName;
				},
				response : response,
			};
		});
		return promise;
	};

});
selectedPipeId = '';
var chartDataModel = {};
// this.data = [];
//
// Code for the delete key.
//
var deleteKeyCode = 46;

//
// Code for control key.
//
var ctrlKeyCode = 65;

//
// Set to true when the ctrl key is down.
//
var ctrlDown = false;

//
// Code for A key.
//
var aKeyCode = 17;

//
// Code for esc key.
//
var escKeyCode = 27;
//
// Selects the next node id.
//
var nextNodeID = 10;

//
var PipelineCtrl = function(myService, $scope, $http, $templateCache,
		$location, $rootScope, $route, $interval, $upload, uploadsService) {

	$("#userDName").text(localStorage.getItem('itc.dUsername'));

	$scope.editData = new Object();
	$scope.editstageData = new Object();
	$scope.showinputData = false;
	// $scope.editstageData.inputDataset = 'Select';
	$scope.addsetStage = [ {
		"name" : "--Select--"
	}, {
		"name" : "Dataset"
	}, {
		"name" : "Stage"
	} ];
	$scope.inputdatasetlist = new Array();
	$scope.inputdatasetlist[0] = 'Select Table';
	$scope.outputdatasetlist = '';
	$scope.editTableData = new Object();
	$scope.editTableData.table1 = $scope.inputdatasetlist[0];
	$scope.joinArray = [ 'left', 'right', 'inner', 'outer' ];
	$scope.FilterArray = [ 'where' ];
	$scope.editTableData.filter = $scope.FilterArray[0];
	$scope.expressArray = [ '==', '!=', '>', '<' ];
	$scope.editstageData.join = $scope.joinArray[0];
	$scope.files = '';
	$scope.querymatches = [];
	// console.log($scope.addsetStage );
	$scope.editData1 = new Object();
	$scope.editData1.addStageSchema = "--Select--";
	$scope.graphShow = true;
	$scope.pipeEdit = false;
	$scope.pipeView = true;

	$scope.runstatushow = false;
	$scope.pipeDetailsshow = true;
	$scope.offsetHour = [ 'Minutes' ];
	$scope.offsetDay = [ 'Hour', 'Minutes' ];

	$scope.pipelineStart = false;
	$scope.logUpdate = true;
	$scope.pipeprevLog = new Object();
	changeMClass('two')

	$scope.resetoffset = function() {
		if ($scope.editData.frequency == 'One Time') {
			$scope.editData.offset = '';
			// console.log($scope.editData.startBatch)
		}

	}
	$scope.getScope = function(){
		//alert('hi');
		$(".dragMe").draggable({
	        helper: 'clone',
	        cursor: 'move',
	        tolerance: 'fit'
			});
	
//	jsPlumb.draggable(document.querySelectorAll(".window"), { grid: [20, 20] });
	//jsPlumb.fire("jsPlumbDemoLoaded", instance);
	}
	$scope.saveGraph =function(){
		$scope.data = {};
		$scope.data.type = 'DatapipeWorkbench';
		$scope.method = 'GET';
		$scope.url = 'rest/service/' + $scope.data.type+'/'+ selectedPipeId ;
		$http({
			method : $scope.method,
			url : $scope.url,
			headers : {
				'X-Auth-Token' : localStorage.getItem('itc.authToken')
			}
		}).success(
		function(data, status) {
			$scope.status = status;
			$scope.data=data;
			var jsonData=angular.fromJson($scope.data.jsonblob);
			var newJson = $('#jsonOutput').val();
			//console.log(newJson);
			if(newJson != ''){
				jsonData.ExecutionGraph= JSON.parse(newJson);;
			}
		
			$scope.data.type='DatapipeWorkbench';
			$scope.data.jsonblob = angular.toJson(jsonData);
			$scope.data.updatedBy = localStorage.getItem('itc.username');
			$scope.data.updatedDate = new Date();
			$scope.data.id = selectedPipeId;
			$scope.method="POST";
			$scope.url = 'rest/service/' + $scope.data.type + '/' + selectedPipeId;
			

			$http({
				method : $scope.method,
				url : $scope.url,
				data : $scope.data,
				headers : {
					'X-Auth-Token' : localStorage.getItem('itc.authToken')
				}
			}).success(
					function(data, status) {
						$scope.status = status;
					}).error(function(data, status) {
				$scope.data = data || "Request failed";
				scope.status = status;

			});

		
		
			}).error(function(data, status) {
		scope.data = data || "Request failed";
		scope.status = status;

	});

	}
	$scope.removeDataArr = function() {
		$scope.querymatches = [];
		$scope.showinputData = false;
	}
	$scope.createDataSet = function(hiveQuery) {
		var re = /(?:^|\W)\$(\w+)(?!\w)/g;
		var match;
		$scope.model = new Object();
		$scope.model['option1'] = new Object();
		$scope.model.option2 = 'Dataset'

		while (match = re.exec(hiveQuery)) {
			$scope.querymatches.push(match[1]);
			$scope.model['option1'][match[1]] = 'Select'
		}
		$scope.editstageData['param'] = new Object();
		$scope.editstageData['inputDataset'] = new Object();
	}
	$scope.document = {};

	$scope.setTitle = function(fileInput) {

		var file = fileInput.value;
		var filename = file.replace(/^.*[\\\/]/, '');
		var title = filename.substr(0, filename.lastIndexOf('.'));
		$("#title").val(title);
		$("#title").focus();
		$scope.document.title = title;
	};

	$scope.uploadFile = function(files) {

		/*
		 * uploadsService.uploadFile(files).then(function(promise){
		 * 
		 * $scope.code = promise.code(); //console.log($scope.code);
		 * $scope.fileName = promise.fileName(); });
		 */
		$scope.files = files;
	};

	$scope.callAtInterval = function() {
		// console.log("$scope.callAtInterval - Interval occurred");
		$scope.type = $location.path();
		$scope.type = $scope.type.replace(/\//g, '')
		// alert($scope.type);
		// alert($scope.pipeEdit);
		// alert($scope.pipelineStart);
		if ($scope.type == 'DatapipeWorkbench' && $scope.pipeEdit == true) {
			$scope.data = {};
			var errorC = 0;
			// alert('hi');
			// scope.data.name = editDatastage.stagename;
			$scope.data.type = 'listPipelineStageLogDetails';
			// scope.data.jsonblob = angular.toJson(editDatastage);
			$scope.method = 'GET';
			// console.log(authService.rootscope);
			/* console.log(newJson); */
			$scope.url = 'rest/service/' + $scope.data.type + '/'
					+ selectedPipeId;
			// console.log(scope.url);
			$http({
				method : $scope.method,
				url : $scope.url,
				// data : scope.data,
				headers : {
					'X-Auth-Token' : localStorage.getItem('itc.authToken')
				}
			})
					.success(
							function(data, status) {
								$scope.status = status;
								$scope.data = data;
								// console.log(data);
								var runstatusLog = '';

								// console.log($scope.data.length)
								if ($scope.data.length == 0) {
									$scope.logUpdate = false;

								} else {
									if ($scope.pipeprevLog.length != $scope.data.length) {
										$scope.logUpdate = true;
									} else if ($scope.pipeprevLog[$scope.pipeprevLog.length - 1].status != $scope.data[$scope.data.length - 1].status) {
										$scope.logUpdate = true;
									} else {
										$scope.logUpdate = false;
									}
								}

								if ($scope.logUpdate == true) {
									$('#pipeLog').html('');
									angular.forEach(data, function(gattr) {
										runstatusLog = gattr.stage + '  '
												+ gattr.status+'  '+gattr.startTime+'  '+gattr.endTime;
										// $('#runStatus').apend('<p>'+runstatusLog+'</p>');
										$('<p>' + runstatusLog + '</p>')
												.appendTo('#pipeLog');
									});
									$scope.pipeprevLog = $scope.data;
								}

								// $prevdata = $scope.runstatus;

								// $('#runStatus').after('<p>'+data+'</p>');

							}).error(function(data, status) {
								if (status == 401) {
									$location.path('/');
								}
						$scope.data = data || "Request failed";
						scope.status = status;

					});
		}
	}

	stopTime = $interval(function() {
		$scope.callAtInterval();
	}, 15000);
	$scope.$on('$destroy', function() {
		// console.log('STOP');
		$interval.cancel(stopTime);
	});
	// $scope.$on('$destroy', function () {
	// $interval.cancel($scope.callAtInterval()); });
	$scope.changeContent = function() {
		if ($scope.graphShow == true) {
			$scope.pipeEdit = true;
			$scope.graphShow = false;
			$scope.callAtInterval();
			// alert('Hiiiiiiiiii');
			// $('#graphHead').html('Pipeline Details');
			// $('#graphBottom').html('Pipeline Graph');
		} else {
			$scope.pipeEdit = false;
			$scope.graphShow = true;
			// $('#graphHead').html('Pipeline Graph');
			// $('#graphBottom').html('Pipeline Details');
		}
	}
	$scope.showHidePipe = function() {
		if ($scope.pipeView == true) {
			$scope.pipeView = false;
		} else {
			$scope.pipeView = true;
		}
	}
	$scope.clearForm = function(formName, datasetType) {
		// alert("form#"+formName)

		if (formName == 'Stage') {
			formName = 'stageForm1';
		} else if (formName == 'Dataset') {
			formName = 'datasetform';
		}
			
		$scope.editData1.addStageSchema = "--Select--";
		// alert("form#"+formName)
		// }
		// alert("form#"+formName)
		// $scope.addStageSchema
		if (formName == 'stageForm1') {
			// if(chartDataModel["nodes"].length > 0){
			$scope.querymatches = [];
			$scope.editstageData = new Object();
			$scope.editTableData = new Object();
			$scope.inputdatasetlist = new Array();
			$scope.inputdatasetlist[0] = 'Select Table';
			$scope.editTableData.table1 = $scope.inputdatasetlist[0];
			$scope.editTableData.table2 = $scope.inputdatasetlist[0];
			$scope.editTableData.table3 = $scope.inputdatasetlist[0];
			$scope.editTableData.table4 = $scope.inputdatasetlist[0];
			$scope.editTableData.table5 = $scope.inputdatasetlist[0];
			$scope.editTableData.table6 = $scope.inputdatasetlist[0];
			$scope.editTableData.table7 = $scope.inputdatasetlist[0];
			for (i = 1; i <= 7; i++) {
				$scope.columnName[i] = new Array();
				$scope.columnName[i][0] = 'Column';

			}
			$scope.editTableData.columnval1 = $scope.columnName[1][0];
			$scope.editTableData.columnval2 = $scope.columnName[2][0];
			$scope.editTableData.columnval3 = $scope.columnName[3][0];
			$scope.editTableData.columnval4 = $scope.columnName[4][0];
			$scope.editTableData.columnval5 = $scope.columnName[5][0];
			$scope.editTableData.columnval6 = $scope.columnName[6][0];
			$scope.editTableData.columnval7 = $scope.columnName[7][0];
			$scope.editTableData.filter = $scope.FilterArray[0];
			$scope.editTableData.join = $scope.joinArray[0];
			$scope.editTableData.expression = $scope.expressArray[0];
			$scope.outputdatasetlist = '';
			$scope.showinoutputData = false;
			$scope.showinputData = false;
			$scope.editstageData.seletType = $scope.stageType[1];
			$scope.editstageData.inputDataset = "Add New dataset";
			$scope.editstageData.outDataset = "Add New dataset";
			// var no = new ComboBox('cb_identifier');
			$('#AddStage').modal('show');
			/*
			 * } else{ alert('Please add dataset first in the canvas;'); return
			 * false; }
			 */
		} else if (formName == 'datasetform') {
			// if(chartDataModel["nodes"].length > 0){
			$scope.editdataData = new Object();
			$scope.datasetType = datasetType;
			// alert($scope.datasetType);
			$('#AddDataSet').modal('show');
			/*
			 * } else{ alert('Please add dataset first in the canvas;'); return
			 * false; }
			 */
		} else {
			$scope.editData = new Object();
			$scope.editData.frequency = 'Hourly';

		}
		/*
		 * $("form#" + formName).find("input[type=text],
		 * textarea,select").val("") $("form#" + formName).find('input:radio,
		 * input:checkbox').removeAttr( 'checked').removeAttr('selected');
		 */
		/*
		 * if (formName == 'stageForm1') { typeFormatFetch($scope, $http,
		 * $templateCache, $rootScope, $location, 'stageType');
		 * console.log($scope.stageType) $scope.editstageData = new Object();
		 * $scope.editstageData.seletType = $scope.stageType[0];
		 * console.log($scope.editstageData.seletType) }
		 */
	};

	$scope.addDataset = function(indataSet, datasetType) {
		// alert(datasetType)
		if (datasetType == 'output') {
			if (indataSet == 'Add New dataset') {
				$scope.clearForm('Dataset', datasetType);
			} else {
				if ($scope.outputdatasetlist.indexOf(indataSet) !== -1) {
					alert('Datadet already added');
				} else {
					$scope.outputdatasetlist = indataSet;
				}

				$scope.showinoutputData = true;
			}
		} else {
			if (indataSet == 'Add New dataset') {
				$scope.clearForm('Dataset', datasetType);
			} else {
				if ($scope.inputdatasetlist.length > 0) {
					if($scope.inputdatasetlist[0]== 'Select Table'){
						$scope.inputdatasetlist.pop();
					}
					if ($scope.inputdatasetlist.indexOf(indataSet) !== -1) {
						alert('Datadet already added');
					} else {
						$scope.inputdatasetlist.push(indataSet);
					}

					$scope.showinputData = true;
				} else {
					$scope.inputdatasetlist.push(indataSet);
					$scope.showinputData = true;
				}
			}
		}

		// console.log($scope.inputdatasetlist);
	};

	$scope.choicesSchema = {
		choices : [ {
			id : 1,
			text : "Expression builder",
			isUserAnswer : "false"
		}, {
			id : 2,
			text : "Hive Query",
			isUserAnswer : "true"
		} ]
	};
	$scope.schemaType = $scope.choicesSchema.choices[1].text;
	$scope.setChoiceForSchema = function(q, c) {
		angular.forEach(q.choices, function(c) {
			c.isUserAnswer = false;
		});

		c.isUserAnswer = true;
	};
	schemaSourceDetails(myService, $scope, $http, $templateCache, $rootScope,
			$location, 'DatapipeWorkbench');
	schemaSourceDetails(myService, $scope, $http, $templateCache, $rootScope,
			$location, 'DataSet');
	// console.log($scope.detaildataSet);
	// $scope.editstageData = myService.get();
	// console.log($scope.editstageData);
	schemaSourceDetails(myService, $scope, $http, $templateCache, $rootScope,
			$location, 'PipelineStage');

	// $scope.allStages;
	// getDataSet($scope, $http, $rootScope);
	$scope.startPipeLine = function() {
		$scope.data = {};
		var errorC = 0;
		// alert('hi');
		// scope.data.name = editDatastage.stagename;
		$scope.data.type = 'DatapipeWorkbench';
		// scope.data.jsonblob = angular.toJson(editDatastage);
		$scope.method = 'GET';
		// console.log(authService.rootscope);
		/* console.log(newJson); */
		$scope.url = 'rest/service/' + $scope.data.type + '/' + selectedPipeId;
		// console.log(scope.url);
		$http({
			method : $scope.method,
			url : $scope.url,
			// data : scope.data,
			headers : {
				'X-Auth-Token' : localStorage.getItem('itc.authToken')
			}
		}).success(
				function(data, status) {
					$scope.status = status;
					$scope.data = data;
					// alert(data);
					var jsonData = angular.fromJson($scope.data.jsonblob);
					var datasetC = 0;
					/*angular.forEach(jsonData.ExecutionGraph.nodes, function(
							gattr) {
						if (gattr.nodeSchema == 'dataSet') {
							datasetC++;
						}
					});
					if (datasetC == 0) {
						alert('please add dataset');
						// errorC++;
						return false;
					} else */if (jsonData.ExecutionGraph.connections == '') {
						alert('please add one connection');
						// errorC++;
						return false;
					}
					// alert(errorC);
					// if(errorC == 0){
					$scope.data = {};
					$scope.data.type = 'pipeline';
					$scope.method = 'GET';
					$scope.url = 'rest/service/' + $scope.data.type
							+ '/DatapipeWorkbench/' + selectedPipeId;
					// console.log(scope.url);
					$http(
							{
								method : $scope.method,
								url : $scope.url,
								// data : scope.data,
								headers : {
									'X-Auth-Token' : localStorage
											.getItem('itc.authToken')
								}
							}).success(function(data, status) {
						$scope.status = status;
						$scope.data = data;
						$prevdata = $scope.runstatus;
						// $scope.runstatus = $prevdata + data;
						// alert($scope.runstatus);
						// $('#runStatus').html(data);
						// $('#runStatus').apend('<p>'+data+'</p>');
						$('<p>' + data + '</p>').appendTo('#runStatus');
						$scope.graphShow = false;
						$scope.pipeEdit = true;
						$scope.pipelineStart = true;
						// $('#runstatusTab').addClass('active');
						// $('#pipeDetailsID').removeClass('active');
						// select();

						// console.log($scope.tabs);
						// $scope.tabs[1] = 'active';
					}).error(function(data, status) {
						if (status == 401) {
							$location.path('/');
						}
						$scope.data = data || "Request failed";
						$scope.status = status;

					});
					// }

				}).error(function(data, status) {
					if (status == 401) {
						$location.path('/');
					}
			scope.data = data || "Request failed";
			scope.status = status;

		});

	}
	$scope.editPipeline = function(id, type) {

		$scope.method = 'GET';

		$scope.type = $location.path();
		$scope.type = $scope.type.replace(/\//g, '')

		$scope.url = 'rest/service/' + $scope.type + '/' + id;

		$http({
			method : $scope.method,
			url : $scope.url,
			headers : {
				'X-Auth-Token' : localStorage.getItem('itc.authToken')
			}
		}).success(function(data, status) {
			// chartDataModel = {};
			$scope.status = status;
			$scope.editPipeLineData = new Object();
			$scope.editPipeLineData = angular.fromJson(data.jsonblob);
			$scope.editPipeLineData.id = data.id;
			$('#editPipeLine').modal('show');
			// alert('hi');

		}).error(function(data, status) {
			if (status == 401) {
				$location.path('/');
			}
			$scope.data = data || "Request failed";
			$scope.status = status;
		});

	}

	$scope.selectPipeline = function(id, type) {
		$('#flowchart-demo').val('');
		firsttime = 'true'
		/*var instance=window.instance;
		 instance.reset();*/
		 
		$scope.method = 'GET';
		if (type == undefined) {
			$scope.type = $location.path();
			$scope.type = $scope.type.replace(/\//g, '')
		} else {
			$scope.type = type;
		}

		$scope.url = 'rest/service/' + $scope.type + '/' + id;

		$http({
			method : $scope.method,
			url : $scope.url,
			headers : {
				'X-Auth-Token' : localStorage.getItem('itc.authToken')
			}
		})
				.success(
						function(data, status) {
							// chartDataModel = {};
							$scope.status = status;
							if (type == undefined) {
								$scope.editData = new Object();
								$scope.editData = angular
										.fromJson(data.jsonblob);
								// $scope.editData.id = data.id;

								// $scope.viewModel = new
								// flowchart.ChartViewModel(chartDataModel);
								// $scope.editData.ExecutionGraph =
								// $scope.editData.ExecutionGraph.replace('\"','')
								// console.log($scope.editData.ExecutionGraph);
								var graphTag = '<b>Pipeline : '
										+ $scope.editData.name + '</b>'
								// graphTag = graphTag.toUpperCase();
								$('#graphHead').html(graphTag);
							//	alert($scope.editData.ExecutionGraph);
								if ($scope.editData.ExecutionGraph != ''
										&& $scope.editData.ExecutionGraph != undefined) {
									var flowChart = $scope.editData.ExecutionGraph;
									var flowChartJson = JSON.stringify(flowChart);
									//console.log(flowChartJson);
									$('#jsonOutput').val(flowChartJson);
									//$("#saveall").trigger("click");
									//jsPlumb.loadFlowchart();
								}
								else{
									$('#jsonOutput').val('');
								}
								
							} else {
								$scope.editstageData = new Object();
								$scope.inputdatasetlist = new Array();
								$scope.inputdatasetlist[0] = 'Select Table';
								$scope.outputdatasetlist = '';
								$scope.editstageData = angular
										.fromJson(data.jsonblob);
								if ($scope.editstageData.inputDataset)
									$scope.inputdatasetlist = $scope.editstageData.inputDataset;
								if ($scope.editstageData.outDataset)
									$scope.outputdatasetlist = $scope.editstageData.outDataset;
								$scope.showinoutputData = true;
								$scope.showinputData = true;
								$scope.hiveQueryDiv = true;
								$scope.editstageData.id = data.id;
								$('#AddStage').modal('show');
							}
							if (type == undefined) {
								$("span").removeClass('ptagactive');
								// alert('#' + id);
								$('#' + id).addClass('ptagactive');
								$('#' + id).html(data.name);
								$('#runStatus').html('');
								$('#pipeLog').html('');
								$scope.pipeprevLog = new Object();
								$scope.pipelineStart = false;
								selectedPipeId = id;
								if(flowChartJson != '')
								$("#loadChat").trigger("click");
							}

						}).error(function(data, status) {
							if (status == 401) {
								$location.path('/');
							}
					$scope.data = data || "Request failed";
					$scope.status = status;
				});

	}
	// console.log(selectedPipeId)
	// $scope.selectPipeline(selectedPipeId);
	typeFormatFetch($scope, $http, $templateCache, $rootScope, $location,
			'Frequency');
	typeFormatFetch($scope, $http, $templateCache, $rootScope, $location,
			'stageType');
	/*
	 * $scope.addDatasetgraph = function() { schemaSourceDropDetails($scope,
	 * $http, $templateCache, $rootScope, $location, 'DataSet');
	 * $('#AddDataSet').modal('show'); } $scope.addSchemaToGraph =
	 * function(selectedSet){ $scope.addNewNode(selectedSet,'dataSet');
	 * $('#AddDataSet').modal('hide'); }
	 */
	// $scope.allStages = myService.get();
	// console.log($scope.editstageData);
	// console.log($scope.allStages);
	$scope.addDatasetgraph = function(selectedSet, selectedSetID) {
		// alert(selectedSetID);
		$scope.addNewNode(selectedSet, 'dataSet', selectedSetID);
	}
	$scope.saveAddUpdatePipeline = function(editDataPipe, oparationedit) {
		$('#flowchart-demo').val('');
		$scope.data = {};
		$scope.data.name = editDataPipe.name;
		$scope.data.type = $location.path();
		$scope.data.type = $scope.data.type.replace(/\//g, '')
		// $scope.editData.dataPipeName = editData.name;

		// console.log(editDataPipe)
		$scope.method = 'POST';
		if (oparationedit != undefined) {
			// editDataPipe.ExecutionGraph = '';
			$scope.data.jsonblob = angular.toJson(editDataPipe);
			$scope.data.updatedBy = localStorage.getItem('itc.username');
			$scope.data.updatedDate = new Date();
			$scope.data.id = editDataPipe.id;
			$scope.url = 'rest/service/' + $scope.data.type + '/'
					+ editDataPipe.id;

		} else {
			editDataPipe.ExecutionGraph = '';
			$scope.data.jsonblob = angular.toJson(editDataPipe);
			$scope.data.createdBy = localStorage.getItem('itc.username');
			$scope.data.updatedBy = localStorage.getItem('itc.username');
			$scope.data.createdDate = new Date();
			$scope.url = 'rest/service/addEntity/';

			$('#flowchart-demo').val('');
			$('#jsonOutput').val('');
			
			$('.ng-isolate-scope').val('');
			//$scope.chartViewModel = new Object();
			// $scope.viewModel = new flowchart.ChartViewModel(chartDataModel);
			// deleteAll();
			// $scope.chartViewModel = new
			// flowchart.ChartViewModel(chartDataModel);
		}

		$http({
			method : $scope.method,
			url : $scope.url,
			data : $scope.data,
			headers : {
				'X-Auth-Token' : localStorage.getItem('itc.authToken')
			}
		}).success(
				function(data, status) {
					$scope.status = status;
					if (oparationedit != undefined) {
						$('#editPipeLine').modal('hide');
						$scope.selectPipeline(editDataPipe.id);
					} else {
						schemaSourceDetails(myService, $scope, $http,
								$templateCache, $rootScope, $location);
						$('#AddModal').modal('hide');
						//$scope.getScope();
					}

					// $scope.addNewNode(data.name);

				}).error(function(data, status) {
					if (status == 401) {
						$location.path('/');
					}
			$scope.data = data || "Request failed";
			$scope.status = status;

		});

	}
	$scope.saveAddUpdateStage = function(editDatastage1, inputdatasetlist,outputdatasetlist) {
		// alert(selectedPipeId);
		// console.log(inputdatasetlist);
		$scope.data = {};
		$scope.data.name = editDatastage1.stagename;
		$scope.data.type = 'PipelineStage';
		// $scope.data.type = $scope.data.type.replace(/\//g, '')
		// $scope.editData.dataPipeName = editData.name;
		if (editDatastage1.seletType == 'Hive') {
			editDatastage1.inputDataset = inputdatasetlist.toString();
			//console.log(outputdatasetlist);
			if(outputdatasetlist == undefined){
				editDatastage1.outDataset = '';
			}
			else{
				editDatastage1.outDataset = outputdatasetlist;
			}
		}
		

		if (editDatastage1.seletType == 'MapReduce') {
			$('#mapLoader').show();
			$scope.mapclass = editDatastage1.mapclass;
			$scope.reduceclass = editDatastage1.reduceclass;
			$scope.dataSetName = editDatastage1.inputDataset;
			$scope.stageName = editDatastage1.stagename;
			uploadsService.uploadFile($scope.files, $scope.mapclass,
					$scope.reduceclass, $scope.dataSetName,$scope.stageName).then(
					function(promise) {

						$scope.code = promise.code();
						$('#mapLoader').hide();
						$scope.fileName = promise.fileName();
						if (promise.response.data == 'success') {
							alert('Test Run is Sucessfull');
						} else {
							alert('Test Run is Failed');
						}
						$('#AddStage').modal('hide');
					});
		}
	//	delete editDatastage1.seletType;
		$scope.data.jsonblob = angular.toJson(editDatastage1);
		$scope.method = 'POST';
		if (editDatastage1.id != undefined && editDatastage1.id != '') {
			// console.log(editDatastage1.id);
			$scope.data.updatedBy = localStorage.getItem('itc.username');
			$scope.data.updatedDate = new Date();
			$scope.data.id = editDatastage1.id;
			$scope.url = 'rest/service/' + $scope.data.type + '/'
					+ editDatastage1.id;

		} else {
			$scope.data.createdBy = localStorage.getItem('itc.username');
			$scope.data.updatedBy = localStorage.getItem('itc.username');
			$scope.data.createdDate = new Date();
			$scope.url = 'rest/service/addEntity/';
		}
		//if (editDatastage1.seletType != 'MapReduce') {
			$http({
				method : $scope.method,
				url : $scope.url,
				data : $scope.data,
				headers : {
					'X-Auth-Token' : localStorage.getItem('itc.authToken')
				}
			}).success(
					function(data, status) {
						$scope.status = status;
						editDatastage1.id = ''
						$('#AddStage').modal('hide');

						// schemaSourceDetails($scope, $http, $templateCache,
						// $rootScope, $location);
						//
						// console.log(data)
						// alert(selectedPipeId);
						// console.log('ID' + editDatastage1.id);

						if (editDatastage1.id != undefined
								&& editDatastage1.id != '') {
						//	var newStageID = editDatastage1.id;
							// alert(newStageID);
							//$scope.addNewNode(editDatastage1.stagename,
									//'stage', newStageID);
						}

						// $route.reload();
						schemaSourceDetails(myService, $scope, $http,
								$templateCache, $rootScope, $location,
								'PipelineStage');

					}).error(function(data, status) {
						if (status == 401) {
							$location.path('/');
						}
				$scope.data = data || "Request failed";
				$scope.status = status;

			});
	//	}

	}
	$scope.datasetType = '';
	$scope.saveAddUpdateDataSet = function(editData) {
		$scope.data = {};
		$scope.data.name = editData.name;
		$scope.data.type = 'DataSet';
		$scope.editData.description = editData.description;
		$scope.editData.dataIngestionId = editData.name;
		$scope.data.jsonblob = angular.toJson(editData);
		$scope.method = 'POST';
		if (editData.id != undefined && editData.id != null) {
			$scope.data.updatedBy = localStorage.getItem('itc.username');
			$scope.data.updatedDate = new Date();
			$scope.data.id = editData.id;
			$scope.url = 'rest/service/' + $scope.data.type + '/' + editData.id;

		} else {
			$scope.data.createdBy = localStorage.getItem('itc.username');
			$scope.data.updatedBy = localStorage.getItem('itc.username');
			$scope.data.createdDate = new Date();
			$scope.url = 'rest/service/addEntity/';
		}

		$http({
			method : $scope.method,
			url : $scope.url,
			data : $scope.data,
			headers : {
				'X-Auth-Token' : localStorage.getItem('itc.authToken')
			}
		}).success(
				function(data, status) {
					$scope.status = status;
					schemaSourceDetails(myService, $scope, $http,
							$templateCache, $rootScope, $location, 'DataSet');
					if ($scope.datasetType != '') {
						$scope.addDataset(editData.name, $scope.datasetType);
					}

					$('#AddDataSet').modal('hide');
				}).error(function(data, status) {
					if (status == 401) {
						$location.path('/');
					}
			$scope.data = data || "Request failed";
			$scope.status = status;

		});

	}
	$scope.delete1 = function() {
		currentUser = $scope.Deluser;
		objDel = $scope.DelObj;
		// alert(currentUser)
		$scope.deleteRecord(currentUser, objDel);
	};
	$scope.showconfirm = function(id, obj) {
		$scope.Deluser = id;
		$scope.DelObj = obj;
		$('#deleteConfirmModal').modal('show');
	};
	$scope.keyDown = function(evt) {

		if (evt.keyCode === ctrlKeyCode) {

			ctrlDown = true;
			evt.stopPropagation();
			evt.preventDefault();
		}
	};

	//
	// Event handler for key-up on the flowchart.
	//
	$scope.keyUp = function(evt) {

		if (evt.keyCode === deleteKeyCode) {
			//
			// Delete key.
			//
			$scope.chartViewModel.deleteSelected();
		}

		if (evt.keyCode == aKeyCode && ctrlDown) {
			// 
			// Ctrl + A
			//
			$scope.chartViewModel.selectAll();
		}

		if (evt.keyCode == escKeyCode) {
			// Escape.
			$scope.chartViewModel.deselectAll();
		}

		if (evt.keyCode === ctrlKeyCode) {
			ctrlDown = false;

			evt.stopPropagation();
			evt.preventDefault();
		}
	};

	
	$scope.deleteStage = function(id) {
		// alert(id);
		$scope.method = 'DELETE';
		$scope.url = 'rest/service/' + id;

		$http({
			method : $scope.method,
			url : $scope.url,
			data : $scope.data,
			cache : $templateCache,
			headers : {
				'X-Auth-Token' : localStorage.getItem('itc.authToken')
			}
		}).success(
				function(data, status) {
					$scope.status = status;
					$scope.data = data;
					schemaSourceDetails(myService, $scope, $http,
							$templateCache, $rootScope, $location,
							'PipelineStage');

				}).error(function(data, status) {
					if (status == 401) {
						$location.path('/');
					}
			$scope.data = data || "Request failed";
			$scope.status = status;
		});
	}

	//
	// Create the view-model for the chart and attach to the scope.
	//
	// $scope.chartViewModel = new flowchart.ChartViewModel(chartDataModel);
	$scope.columnName = new Object();
	$scope.tableName = new Array();
	$scope.tableNameData = new Array();
	$scope.getColumnName = function(tablename1, tableNo) {
		var posTable = jQuery.inArray(tablename1, $scope.tableName);
		if (posTable == -1) {
			$scope.method = 'GET';
			$scope.url = 'rest/service/getColumns/' + tablename1;
			$http({
				method : $scope.method,
				url : $scope.url,
				// cache : $templateCache
				headers : {
					'X-Auth-Token' : localStorage.getItem('itc.authToken')
				}
			}).success(function(data, status) {
				$scope.data = data;
				// console.log($scope.data);
				$scope.columnName[tableNo] = new Object();

				$scope.columnName[tableNo] = $scope.data;
				$scope.tableNameData[tablename1] = $scope.data;
				// console.log($scope.columnName);
				$scope.status = status;
				$scope.tableName[$scope.tableName.length] = tablename1;

			}).error(function(data, status) {
				if (status == 401) {
					$location.path('/');
				}
				$scope.data = data || "Request failed";
				$scope.status = status;
			});
		} else {
			var prevTable = $scope.tableName[posTable];
			$scope.columnName[tableNo] = new Object();

			$scope.columnName[tableNo] = $scope.tableNameData[prevTable];
		}
	}
	$scope.constructQuery = function(editTableData) {
		var joinQuery = ''
		var filterQuery = ''
		var groupQuery = ''
		var orderQuery = ''
		// console.log(editTableData);
		if (editTableData.table2 != 'Select Table' && editTableData.table3 != 'Select Table') {
			joinQuery = editTableData.join + ' join ' + editTableData.table2
					+ ' ON ' + ' ' + editTableData.table2 + '.'
					+ editTableData.columnval2 + '=' + editTableData.table3
					+ '.' + editTableData.columnval3;

		}
		if (editTableData.table4 != 'Select Table' && editTableData.table5 != 'Select Table') {
			filterQuery = editTableData.filter + ' ' + editTableData.table4
					+ '.' + editTableData.columnval4 + ' '
					+ editTableData.expression + ' ' + editTableData.table5
					+ '.' + editTableData.columnval5

		}
		if (editTableData.table6 != 'Select Table') {
			groupQuery = ' GROUP BY ' + editTableData.table6 + '.'
					+ editTableData.columnval6;

		}
		if (editTableData.table7 != 'Select Table') {
			if (groupQuery != '') {
				orderQuery = ' ,ORDER BY ' + editTableData.table7 + '.'
						+ editTableData.columnval7 + ' DESC';

			} else {
				orderQuery = ' ORDER BY ' + editTableData.table7 + '.'
						+ editTableData.columnval7 + ' DESC';

			}

		}
		$scope.editstageData.hiveQuery = 'SELECT ' + editTableData.columnval1
				+ ' FROM ' + editTableData.table1 + ' ' + joinQuery + ' '
				+ filterQuery + groupQuery + orderQuery;
		// console.log($scope.editstageData.hiveQuery);
		$scope.hiveQueryDiv = true
	}
}
// var pipeObj=new PipelineCtrl();
// ################################################################
// ###################DataSet Add/Update/Delete####################
// #################################################################
var getTypeList = function($scope, $http, $rootScope) {
	$scope.method = 'GET';
	$scope.url = 'rest/service/list/dataschema';
	$http({
		method : $scope.method,
		url : $scope.url,
		// cache : $templateCache
		headers : {
			'X-Auth-Token' : localStorage.getItem('itc.authToken')
		}
	}).success(function(data, status) {
		$scope.data = data;
		$scope.editdataData = new Object();
		$scope.editdataData.Schema = $scope.data[0];
		$scope.status = status;

	}).error(function(data, status) {
		if (status == 401) {
			$location.path('/');
		}
		$scope.data = data || "Request failed";
		$scope.status = status;
	});

}

var getDataSource = function($scope, $http, $rootScope) {
	$scope.method = 'GET';
	$scope.url = 'rest/service/list/DataSource';
	$http({
		method : $scope.method,
		url : $scope.url,
		// cache : $templateCache
		headers : {
			'X-Auth-Token' : localStorage.getItem('itc.authToken')
		}
	}).success(function(data, status) {
		$scope.data = data;
		$scope.status = status;
		$scope.editData.dataSource = $scope.data[0];
		// $scope.editData.destinationDataset = $scope.data[0];

	}).error(function(data, status) {
		if (status == 401) {
			$location.path('/');
		}
		$scope.data = data || "Request failed";
		$scope.status = status;
	});

}

var getDataSet = function($scope, $http, $rootScope) {
	$scope.method = 'GET';
	$scope.url = 'rest/service/list/dataset';
	$http({
		method : $scope.method,
		url : $scope.url,
		// cache : $templateCache
		headers : {
			'X-Auth-Token' : localStorage.getItem('itc.authToken')
		}
	}).success(function(data, status) {
		$scope.data = data;
		$scope.editData.destinationDataset = $scope.data[0];
		$scope.status = status;
		// $scope.editData.dataSet=$scope.data[0];

	}).error(function(data, status) {
		if (status == 401) {
			$location.path('/');
		}
		$scope.data = data || "Request failed";
		$scope.status = status;
	});

}

/* ********************************************************************* */
/* ***************************DATA INGESTION**************************** */
/* ********************************************************************* */
/*
 * var DataIngestion = function(myService,$scope, $http, $templateCache,
 * $location, $rootScope, $route, $interval) {
 * 
 * 
 * $scope.getIngestionLog = function(id) { $scope.method = 'GET'; $scope.url =
 * 'rest/service/listIngestionDetails/'+id; //console.log($scope.url);
 * 
 * $http({ method : $scope.method, url : $scope.url, headers : { 'X-Auth-Token' :
 * $rootScope.authToken } }).success(function(data, status) { $scope.status =
 * status; $scope.data = data; //console.log($scope.data)
 * }).error(function(data, status) { $scope.data = data || "Request failed";
 * $scope.status = status; }); //console.log("Heelloo"); }
 * 
 * $scope.getIngestionLog = function(id,direct) { $scope.getMyId = id; //
 * alert($('#dataIngestionLog').css('display'));
 * if($('#dataIngestionLog').css('display') == 'block' || direct != undefined){
 * 
 * //console.log($scope.getMyId); $scope.method = 'GET';
 * 
 * $scope.url = 'rest/service/listIngestionDetails/' + id;
 * 
 * $http({ method : $scope.method, url : $scope.url, headers : { 'X-Auth-Token' :
 * localStorage.getItem('itc.authToken') } }).success( function(data, status) {
 * $scope.status = status; $scope.dataIngLog = data;
 * 
 * $scope.modalStatus = angular.element("#dataIngestionLog")
 * .attr('aria-hidden'); }).error(function(data, status) { $scope.data = data ||
 * "Request failed"; $scope.status = status; }); } } $scope.runIngestionLog =
 * function(name,id) { $('#'+id+'loader').show();
 * 
 * $scope.method = 'POST';
 * 
 * $scope.url = 'rest/service/runScheduler/' + name;
 * 
 * $http({ method : $scope.method, url : $scope.url, headers : { 'X-Auth-Token' :
 * localStorage.getItem('itc.authToken') } }).success( function(data, status) {
 * $('#'+id+'loader').hide(); $('#dataIngestionLog').modal('show');
 * $scope.getIngestionLog(id,'direct'); $scope.status = status;
 * $scope.dataIngLog = data;
 * 
 * }).error(function(data, status) { $scope.data = data || "Request failed";
 * $scope.status = status; });
 *  }
 * 
 * var stopTime = $interval(function() {
 * 
 * $scope.getIngestionLog($scope.getMyId); }, 5000);
 * 
 * $scope.$on('$destroy', function() { // console.log('STOP');
 * $interval.cancel(stopTime); $scope.modalStatus = true; }); //
 * $scope.$watch('modalStatus', function() { if ($scope.modalStatus === true) {
 * var stopTime = $interval(function() { $scope.getIngestionLog($scope.getMyId); },
 * 5000); $scope.$on('$destroy', function() { // console.log('STOP');
 * $interval.cancel(stopTime); $scope.modalStatus = true; }) } else {
 * $scope.modalStatus = true; } // }, true);
 * 
 * $scope.patterField = {}; $scope.patterField.dateStructure =
 * "(?:19|20)[0-9]{2}-(?:(?:0[1-9]|1[0-2])-(?:0[1-9]|1[0-9]|2[0-9])|(?:(?!02)(?:0[1-9]|1[0-2])-(?:30))|(?:(?:0[13578]|1[02])-31))"
 * 
 * $scope.viewing = true; $scope.addNewBtn = true; $scope.editData = new
 * Object(); $scope.totalSourcer = 1; $scope.oldName = ''; $scope.Deluser = null
 * 
 * $scope.DelObj = {}; $scope.editData = {};
 * 
 * $scope.load = function(tpl) { $scope.tpl = tpl; }; $scope.load('tpl');
 * 
 * schemaSourceDetails(myService,$scope, $http, $templateCache, $rootScope,
 * $location); $scope.resetField=function(){ if($scope.editData.frequency ==
 * 'One Time'){ $scope.editData.startBatch=''; $scope.editData.endBatch='';
 * //console.log($scope.editData.startBatch) } } $scope.addUpdateDataSet =
 * function(id) { if (id == undefined) { getDataSource($scope, $http,
 * $rootScope); getDataSet($scope, $http, $rootScope); } $scope.frequencyArr = [
 * "One Time","Hourly", "Daily", "Weekly" ];
 * 
 * $scope.editData.id = null; if (id != undefined) { $scope.method = 'GET';
 * $scope.type = $location.path(); $scope.type = $scope.type.replace(/\//g, '')
 * $scope.url = 'rest/service/' + $scope.type + '/' + id; $scope.editData = new
 * Object(); $http({ method : $scope.method, url : $scope.url, headers : {
 * 'X-Auth-Token' : localStorage.getItem('itc.authToken') }
 * }).success(function(data, status) {
 * 
 * $scope.status = status;
 * 
 * $scope.editData = angular.fromJson(data.jsonblob); $scope.editData.id =
 * data.id;
 * 
 * }).error(function(data, status) { $scope.data = data || "Request failed";
 * $scope.status = status; }); } else { $scope.editData = {};
 * 
 * $scope.update = function(user) { $scope.editData = angular.copy(user); };
 * 
 * $scope.reset = function() { $scope.user = angular.copy($scope.editData); };
 * 
 * $scope.reset(); $scope.editData.Schema = "Int"; $scope.editData.frequency =
 * 'One Time'; } $scope.editing = true; $scope.viewing = false; $scope.addNewBtn =
 * false; } $scope.saveAddUpdateDataSet = function(editData) { $scope.data = {};
 * $scope.data.name = editData.name; $scope.data.type = $location.path();
 * $scope.data.type = $scope.data.type.replace(/\//g, '')
 * $scope.editData.description = editData.description;
 * $scope.editData.dataIngestionId = editData.name; $scope.data.jsonblob =
 * angular.toJson(editData); $scope.method = 'POST'; if (editData.id !=
 * undefined && editData.id != null) { $scope.data.updatedBy =
 * localStorage.getItem('itc.username'); $scope.data.updatedDate = new Date();
 * $scope.data.id = editData.id; $scope.url = 'rest/service/' + $scope.data.type +
 * '/' + editData.id; } else { $scope.data.createdBy =
 * localStorage.getItem('itc.username'); $scope.data.updatedBy =
 * localStorage.getItem('itc.username'); $scope.data.createdDate = new Date();
 * $scope.url = 'rest/service/addEntity/'; }
 * 
 * $http({ method : $scope.method, url : $scope.url, data : $scope.data, headers : {
 * 'X-Auth-Token' : localStorage.getItem('itc.authToken') } }).success(
 * function(data, status) { $scope.status = status;
 * schemaSourceDetails(myService,$scope, $http, $templateCache, $rootScope,
 * $location); $scope.editing = false; $scope.viewing = true; $scope.addNewBtn =
 * true;
 * 
 * }).error(function(data, status) { $scope.data = data || "Request failed";
 * $scope.status = status;
 * 
 * }); } $scope.cancelUpdate = function() { $scope.editing = false;
 * $scope.viewing = true; $scope.addNewBtn = true; } $scope.cancelADD =
 * function() { $scope.editing = false; $scope.viewing = true; $scope.addNewBtn =
 * true; }
 * 
 * $scope.delete1 = function() { currentUser = $scope.Deluser; objDel =
 * $scope.DelObj; // alert(currentUser) $scope.deleteRecord(currentUser,
 * objDel); }; $scope.capitaliseFirstLetter = function(string) { string =
 * string.toLowerCase(); return string.charAt(0).toUpperCase() +
 * string.slice(1); }
 * 
 * $scope.showconfirm = function(id, obj, name) { $scope.Deluser = id;
 * $scope.getDataSetName = $scope.capitaliseFirstLetter(name); $scope.DelObj =
 * obj; $('#deleteConfirmModal').modal('show'); }; }
 */
/* ##################################################################### */
/* ###############################DATA SET############################## */
/* ##################################################################### */

userApp.QuerySelector = function($scope, $http, $rootScope) {

	$scope.executeQueryBtn = function() {

		var queryString = $scope.queryInput;

		$scope.method = 'GET';
		$scope.url = 'rest/service/query/' + queryString;
		$http({
			method : $scope.method,
			url : $scope.url,
			data : $scope.data,
			// cache : $templateCache
			headers : {
				'X-Auth-Token' : localStorage.getItem('itc.authToken')
			}
		}).success(function(data, status) {
			$scope.status = status;
			$scope.data = data;
			// console.log($scope.data);
			if ($scope.data == "") {

				$scope.data = [ "Error in the Query" ];

			}

		}).error(function(data, status) {

			$scope.data = data || "Request failed";
			$scope.status = status;

		});
	};

}
/*
 * var dataSet = function(myService,$scope, $http, $templateCache, $location,
 * $rootScope, $route) {
 * 
 * $scope.viewing = true; $scope.editData = new Object(); $scope.addNewBtn =
 * true; $scope.totalSourcer = 1; $scope.oldName = ''; $scope.Deluser = null
 * 
 * $scope.DelObj = {}; $scope.newItem = function($event) { } var test =
 * window.location.pathname; var parts = window.location.pathname.split('/');
 * 
 * $scope.load = function(tpl) { $scope.tpl = tpl; }; $scope.load('tpl');
 * 
 * schemaSourceDetails(myService,$scope, $http, $templateCache, $rootScope,
 * $location);
 * 
 * $scope.addUpdateDataSet = function(id) { $scope.editData.id = null; if (id !=
 * undefined) { $scope.method = 'GET'; $scope.type = $location.path();
 * $scope.type = $scope.type.replace(/\//g, '') $scope.url = 'rest/service/' +
 * $scope.type + '/' + id; $scope.editData = new Object(); $http({ method :
 * $scope.method, url : $scope.url, // cache : $templateCache headers : {
 * 'X-Auth-Token' : localStorage.getItem('itc.authToken') }
 * }).success(function(data, status) {
 * 
 * $scope.status = status;
 * 
 * $scope.editData = angular.fromJson(data.jsonblob); $scope.editData.id =
 * data.id;
 * 
 * }).error(function(data, status) { $scope.data = data || "Request failed";
 * $scope.status = status; }); } else { $scope.editData = {};
 * 
 * $scope.update = function(user) { $scope.editData = angular.copy(user); };
 * 
 * $scope.reset = function() { $scope.user = angular.copy($scope.editData); };
 * 
 * $scope.reset(); } $scope.editing = true; $scope.viewing = false;
 * $scope.addNewBtn = false; }
 * 
 * $scope.saveAddUpdateDataSet = function(editData) { $scope.data = {};
 * $scope.data.name = editData.name; $scope.data.type = $location.path();
 * $scope.data.type = $scope.data.type.replace(/\//g, '')
 * $scope.editData.description = editData.description;
 * $scope.editData.dataSourcerId = editData.name; $scope.data.jsonblob =
 * angular.toJson(editData); $scope.method = 'POST'; if (editData.id !=
 * undefined && editData.id != null) { $scope.data.updatedBy =
 * localStorage.getItem('itc.username'); $scope.data.updatedDate = new Date();
 * $scope.data.id = editData.id; $scope.url = 'rest/service/' + $scope.data.type +
 * '/' + editData.id; } else { $scope.data.createdBy =
 * localStorage.getItem('itc.username'); $scope.data.updatedBy =
 * localStorage.getItem('itc.username'); $scope.data.createdDate = new Date();
 * $scope.url = 'rest/service/addEntity/'; }
 * 
 * $http({ method : $scope.method, url : $scope.url, data : $scope.data, //
 * cache : $templateCache headers : { 'X-Auth-Token' :
 * localStorage.getItem('itc.authToken') } }).success( function(data, status) {
 * $scope.status = status; schemaSourceDetails(myService,$scope, $http,
 * $templateCache, $rootScope, $location); $scope.editing = false;
 * $scope.viewing = true; $scope.addNewBtn = true;
 * 
 * }).error(function(data, status) { $scope.data = data || "Request failed";
 * $scope.status = status;
 * 
 * }); } $scope.cancelUpdate = function() { $scope.editing = false;
 * $scope.viewing = true; $scope.addNewBtn = true; } $scope.cancelADD =
 * function() { $scope.editing = false; $scope.viewing = true; $scope.addNewBtn =
 * true; }
 * 
 * $scope.delete1 = function() { currentUser = $scope.Deluser; objDel =
 * $scope.DelObj; // alert(currentUser) $scope.deleteRecord(currentUser,
 * objDel); }; $scope.capitaliseFirstLetter = function(string) { string =
 * string.toLowerCase(); return string.charAt(0).toUpperCase() +
 * string.slice(1); }
 * 
 * $scope.showconfirm = function(id, obj, name) { $scope.Deluser = id;
 * $scope.getDataSetName = $scope.capitaliseFirstLetter(name); $scope.DelObj =
 * obj; $('#deleteConfirmModal').modal('show'); // console.log(name); }; }
 */
// ++++++++++++++++++++++++++++++++++++++ Date Picker Starts Here
// ++++++++++++++++++++++++++++++++++++++++++++++++
userApp
		.controller('DatepickerDemoCtrl',
				function($scope) {
					$scope.today = function() {
						$scope.dt = new Date();
					};
					$scope.today();

					$scope.clear = function() {
						$scope.dt = null;
					};

					// Disable weekend selection
					$scope.disabled = function(date, mode) {
						return (mode === 'day' && (date.getDay() === 0 || date
								.getDay() === 6));
					};

					$scope.toggleMin = function() {
						$scope.minDate = $scope.minDate ? null : new Date();
					};
					$scope.toggleMin();

					$scope.open = function($event) {
						$event.preventDefault();
						$event.stopPropagation();

						$scope.opened = true;
					};

					$scope.dateOptions = {
						formatYear : 'yy',
						startingDay : 1
					};

					$scope.formats = [ 'dd-MMMM-yyyy', 'yyyy/MM/dd',
							'dd.MM.yyyy', 'shortDate' ];
					$scope.format = $scope.formats[0];
				});
// --------------------------- Date Picker Ends Here ---------------------------
var googleChartCtrl = function($scope, $http, $templateCache, $rootScope,
		$location) {

	var chart1 = {};
	chart1.type = "ColumnChart";
	chart1.cssStyle = "height:200px; width:500px;";
	$scope.method = 'GET';
	$scope.type = $location.path();
	$scope.type = $scope.type.replace(/\//g, '');
	$scope.url = 'rest/service/' + $scope.type;// 'http://jsonblob.com/api/54215e4ee4b00ad1f05ed73d';//http://jsonblob.com/api/541aa950e4b0ad15b49f3cfd
	// alert($scope.url)
	$http({
		method : $scope.method,
		url : $scope.url,
		// cache : $templateCache
		headers : {
			'X-Auth-Token' : localStorage.getItem('itc.authToken')
		}
	}).success(function(data, status) {

		$scope.status = status;
		$scope.data = data;

		$scope.detaildata1 = {};
		$scope.detaildata = angular.fromJson($scope.data);
		// console.log($scope.detaildata);
		var i = 0;
		chart1.data = $scope.detaildata[0].jsonblob;
		// console.log(chart1.data);
	}).error(function(data, status) {
		if (status == 401) {
			$location.path('/');
		}
		$scope.data = data || "Request failed";
		$scope.status = status;
	});

	/*
	 * chart1.data = {"cols": [ {id: "month", label: "Month", type: "string"},
	 * {id: "laptop-id", label: "Laptop", type: "number"}, {id: "desktop-id",
	 * label: "Desktop", type: "number"}, {id: "server-id", label: "Server",
	 * type: "number"}, {id: "cost-id", label: "Shipping", type: "number"} ],
	 * "rows": [ {c: [ {v: "January"}, {v: 19, f: "42 items"}, {v: 12, f: "Ony
	 * 12 items"}, {v: 7, f: "7 servers"}, {v: 4} ]}, {c: [ {v: "February"}, {v:
	 * 13}, {v: 1, f: "1 unit (Out of stock this month)"}, {v: 12}, {v: 2} ]},
	 * {c: [ {v: "March"}, {v: 24}, {v: 0}, {v: 11}, {v: 6} ]} ]};
	 */

	chart1.options = {
		"title" : "Sales per month",
		"isStacked" : "true",
		"legend" : {
			"position" : 'bottom'
		},
		"fill" : 20,
		"displayExactValues" : true,
		"vAxis" : {
			"title" : "Sales unit",
			"gridlines" : {
				"count" : 10
			}
		},
		"hAxis" : {
			"title" : "Date"
		}
	};

	chart1.formatters = {};

	$scope.chart = chart1;
}
userApp.filter('orderObjectBy', function() {
	return function(items, field, reverse) {
		var filtered = [];
		angular.forEach(items, function(item) {
			filtered.push(item);
		});
		filtered.sort(function(a, b) {
			if (a[field] > b[field])
				return 1;
			if (a[field] < b[field])
				return -1;
			return 0;
		});
		if (reverse)
			filtered.reverse();
		return filtered;
	};
});
var schemanameCheck = function(schemaName, $scope, $http, $templateCache,
		$rootScope, $location, $tabType) {
	if (schemaName != undefined) {
		$scope.schemaName = new Object();
		$scope.method = 'POST';
		$scope.url = 'rest/service/schemaNameCheck/';
		$scope.schemaName.name = schemaName;

		$http({
			method : $scope.method,
			url : $scope.url,
			data : $scope.schemaName,
			// cache : $templateCache
			headers : {
				'X-Auth-Token' : localStorage.getItem('itc.authToken')
			}
		}).success(function(data, status) {
			$scope.status = status;
			if (data == 'true') {
				$scope.schemanamenotok = true;
			} else if (data == 'false') {
				$scope.schemanamenotok = false;
			}
			// datapreview = data
		}).error(function(data, status) {
			if (status == 401) {
				$location.path('/');
			}
		});
	}

}

var schemaSourceDetails = function(myService, $scope, $http, $templateCache,
		$rootScope, $location, $tabType, $filter) {

	// $scope.itemsPerPage = 2;
	// $scope.currentPage = 1;
	/*
	 * if(localStorage.getItem('itc.authToken')){ var authToken =
	 * localStorage.getItem('itc.authToken'); $rootScope.authToken = authToken; }
	 */
	// console.log($rootScope.authToken);
	$scope.method = 'GET';
	$scope.type = $location.path();
	$scope.type = $scope.type.replace(/\//g, '');
	if ($tabType != undefined) {
		$scope.type = $tabType;
	} else {
		$tabType = $scope.type;
	}
	if ($tabType == 'DataSchema') {
		$scope.url = 'rest/service/profiles';
	} else if ($tabType == 'driver') {
		$scope.url = 'rest/service/getStreamDrivers';
	} else {
		$scope.url = 'rest/service/' + $scope.type;
	}
	// 'http://jsonblob.com/api/54215e4ee4b00ad1f05ed73d';//http://jsonblob.com/api/541aa950e4b0ad15b49f3cfd
	// alert($scope.url)
	$http({
		method : $scope.method,
		url : $scope.url,
		// cache : $templateCache
		headers : {
			'X-Auth-Token' : localStorage.getItem('itc.authToken')
		}
	})
			.success(
					function(data, status) {

						$scope.status = status;
						$scope.data = data;
						if ($tabType == 'DataSchema') {
							$scope.detaildataSchema = {};
							$scope.editData.addSchemaMed = "---Select---";
						} else if ($tabType == 'DataSource') {
							$scope.detaildataSource = {};
						} else if ($tabType == 'DataSet') {
							$scope.detaildataSet = {};
						} else if ($tabType == 'DataIngestion') {
							$scope.detaildataIngestion = {};
						} else if ($tabType == 'DatapipeWorkbench') {
							$scope.detaildataPipe = {};
						} else if ($tabType == 'PipelineStage') {
							$scope.allStages = {};
							$scope.detaildataPipeStageMap = new Object();
							// console.log($scope.detaildataPipeStageMap.length);
							$scope.detaildataPipeStageHive = new Object();
							$scope.detaildataPipeStagePig = new Object();
							$scope.detaildataPipeStageSpark = new Object();
						} else if ($tabType == 'Streaming') {
							$scope.detailStreaming = {};
						} else if ($tabType == 'driver') {
							$scope.detailDriver = {};
						}
						$scope.totalItems = $scope.data.length;
						// console.log('total'+$scope.totalItems)
						/*
						 * $scope.pageCount = function () { return
						 * Math.ceil($scope.data.length / $scope.itemsPerPage); };
						 */

						// $scope.data.$promise.then(function () {
						/*
						 * $scope.$watch('currentPage + itemsPerPage',
						 * function() { var begin = (($scope.currentPage - 1) *
						 * $scope.itemsPerPage), end = begin +
						 * $scope.itemsPerPage; // alert(end);
						 * $scope.filtereddetaildata1 = $scope.data.slice(begin,
						 * end);
						 */

						$scope.detaildata = angular.fromJson($scope.data);
						if ($tabType == 'DataSchema') {
							$scope.detaildataSchema = $scope.detaildata;
							
						}//schemaJsonBlob
						// console.log($scope.detaildata);
						var i = 0;
						angular
								.forEach(
										$scope.detaildata,
										function(attr) {

											if ($tabType == 'DataSource') {
												$scope.detaildataSource[i] = angular
														.fromJson(attr.jsonblob);
												$scope.detaildataSource[i].id = attr.id;
											}else if ($tabType == 'DataSchema') {
												$scope.editData[i] = new Object();
												$scope.editData[i] = angular
														.fromJson(attr.schemaJsonBlob);
												if ($scope.editData[i].fileName != undefined) {
													$scope.detaildataSchema[i].fileName = $scope.editData[i].fileName;
												}

												$scope.detaildataSchema[i].lastModified = getDateFormat(attr.schemaModificationDate);
												$scope.detaildataSchema[i].dataSchemaType = $scope.editData[i].dataSchemaType;
												$scope.detaildataSchema[i].profile = true;
											} 
											else if ($tabType == 'DataSet') {
												$scope.detaildataSet[i] = angular
														.fromJson(attr.jsonblob);
												$scope.detaildataSet[i].id = attr.id;

											} else if ($tabType == 'DataIngestion') {
												$scope.detaildataIngestion[i] = angular
														.fromJson(attr.jsonblob);
												$scope.detaildataIngestion[i].id = attr.id;
											} else if ($tabType == 'DatapipeWorkbench') {
												$scope.detaildataPipe[i] = angular
														.fromJson(attr.jsonblob);
												$scope.detaildataPipe[i].id = attr.id;
											} else if ($tabType == 'Streaming') {
												$scope.detailStreaming[i] = angular
														.fromJson(attr.jsonblob);
												$scope.detailStreaming[i].id = attr.id;
												// $scope.detailStreaming = {};
											} else if ($tabType == 'driver') {
												$scope.detailDriver[i] = attr;
												$scope.detailDriver[i].id = attr.driverId;
												// $scope.detailStreaming = {};
											} else if ($tabType == 'PipelineStage') {
												var stageArray = angular
														.fromJson(attr.jsonblob);
												$scope.allStages[i] = stageArray;
												$scope.allStages[i].id = attr.id;
												// myService.set($scope.allStages);
												if (stageArray.seletType == 'MapReduce') {
													$scope.detaildataPipeStageMap[i] = stageArray;
													$scope.detaildataPipeStageMap[i].id = attr.id;
												} else if (stageArray.seletType == 'Hive') {
													$scope.detaildataPipeStageHive[i] = stageArray;
													$scope.detaildataPipeStageHive[i].id = attr.id;

												} else if (stageArray.seletType == 'Pig') {
													$scope.detaildataPipeStagePig[i] = stageArray;
													$scope.detaildataPipeStagePig[i].id = attr.id;

												} else if (stageArray.seletType == 'Spark') {
													$scope.detaildataPipeStageSpark[i] = angular
															.fromJson(attr.jsonblob);
													$scope.detaildataPipeStageSpark[i].id = attr.id;
													// alert($scope.detaildataPipeStageSpark[i].id);

												}

											}

											i++;
										});
						if ($tabType == 'DatapipeWorkbench') {
							if (selectedPipeId == '') {
								if($scope.detaildata[0].id)
								selectedPipeId = $scope.detaildata[0].id;
							} else {
								var selId = Object.keys($scope.detaildataPipe).length - 1;

								selectedPipeId = $scope.detaildataPipe[selId].id;

							}
							// alert(selectedPipeId)
							$scope.selectPipeline(selectedPipeId);
							
						}
						if ($tabType == 'DataSet') {
							$scope.editstageData.inputDataset = $scope.detaildataSet[0].name;
							$scope.editstageData.outDataset = $scope.detaildataSet[0].name;
							// console.log($scope.editstageData);

						}
						// });
						if ($tabType == 'PipelineStage') {
							$scope.mapLength = Object
									.keys($scope.detaildataPipeStageMap).length;
							$scope.hiveLength = Object
									.keys($scope.detaildataPipeStageHive).length;
							$scope.pigLength = Object
									.keys($scope.detaildataPipeStagePig).length;
							$scope.sparkLength = Object
									.keys($scope.detaildataPipeStageSpark).length;
							//
						}
						//$scope.getScope();
						//$scope.toggleOpen()
						$scope.totalSourcer = $scope.detaildata.length;
						$scope.editing = false;
						$scope.viewing = true;
						$scope.addSchema = true;
					}).error(function(data, status) {
						if (status == 401) {
							$location.path('/');
						}
				$scope.data = data || "Request failed";
				$scope.status = status;
			});
}
var schemaSourceDropDetails = function($scope, $http, $templateCache,
		$rootScope, $location, $tabType) {

	// $scope.itemsPerPage = 2;
	// $scope.currentPage = 1;

	$scope.method = 'GET';
	$scope.schemaDetail = {};
	$scope.type = $tabType;

	$scope.url = 'rest/service/list/' + $scope.type;// 'http://jsonblob.com/api/54215e4ee4b00ad1f05ed73d';//http://jsonblob.com/api/541aa950e4b0ad15b49f3cfd
	// alert($scope.url)
	$http({
		method : $scope.method,
		url : $scope.url,
		// cache : $templateCache
		headers : {
			'X-Auth-Token' : localStorage.getItem('itc.authToken')
		}
	}).success(function(data, status) {

		$scope.status = status;
		$scope.schemaDetail = data;
		$scope.editData.schema = $scope.schemaDetail[0];
	}).error(function(data, status) {
		if (status == 401) {
			$location.path('/');
		}
		$scope.data = data || "Request failed";
		$scope.status = status;
	});

}
var typeFormatFetch = function($scope, $http, $templateCache, $rootScope,
		$location, dataType, pageurl) {

	// $scope.itemsPerPage = 2;
	// $scope.currentPage = 1;

	$scope.method = 'GET';

	$scope.type = $location.path();
	$scope.type = $scope.type.replace(/\//g, '');
	if (pageurl != undefined) {
		$scope.type = pageurl;
	}
	$scope.url = 'rest/service/list/' + $scope.type + '/' + dataType;// 'http://jsonblob.com/api/54215e4ee4b00ad1f05ed73d';//http://jsonblob.com/api/541aa950e4b0ad15b49f3cfd
	// alert($scope.url)
	$http({
		method : $scope.method,
		url : $scope.url,
		// cache : $templateCache
		headers : {
			'X-Auth-Token' : localStorage.getItem('itc.authToken')
		}
	}).success(function(data, status) {

		$scope.status = status;
		if (dataType == 'Format') {

			$scope.sourceFormat = {};
			$scope.sourceFormat = data;
			if ($scope.editData.format == undefined)
				$scope.editData.format = $scope.sourceFormat[0];
		} else if (dataType == 'Type') {
			$scope.sourceType = {};
			$scope.sourceType = data;
			if (pageurl == undefined) {
				$scope.editData.sourcerType = $scope.sourceType[0];
			}
			// $scope.dd.dataType = $scope.sourceType[0];
		} else if (dataType == 'Frequency') {
			$scope.sourceFreq = {};
			$scope.sourceFreq = data;
			$scope.editData.frequency = $scope.sourceFreq[0];
			// $scope.dd.dataType = $scope.sourceType[0];
		} else if (dataType == 'stageType') {
			$scope.stageType = {};
			$scope.stageType = data;
			$scope.editstageData.seletType = $scope.stageType[1];
			// console.log($scope.editData.seletType);
		}

	}).error(function(data, status) {
		if (status == 401) {
			$location.path('/');
		}
		$scope.data = data || "Request failed";
		$scope.status = status;
	});

}

userApp.directive('focusMe', function($timeout, $parse) {
	return {
		link : function(scope, element, attrs) {
			var model = $parse(attrs.focusMe);
			// console.log(model);
			scope.$watch(model, function(value) {
				// console.log('value=',value);
				// if(value === true) {
				$timeout(function() {
					element[0].focus();
				});
				// }
			});
			element.bind('blur', function() {
				// console.log('blur')
				// scope.$apply(model.assign(scope, false));
			})
		}
	};
});

var datapreview = new Array();

userApp.controller('ModalInstanceCtrl', function(myService, $scope,
		$modalInstance, $http, $templateCache, $location, $rootScope, $route,
		$upload, $filter, $modal) {

	$scope.databaseType = [ 'mysql', 'Oracle' ];
	$scope.porArr = [ "3306", "1521" ];
	$scope.delimiterArr = ["tab","space",,"comma","underscore","slash","Control-A","Control-B","Control-C","newline","carriage return"];
	$scope.editData = {};
	$scope.errorCode = '';
	// $scope.editData.filepath = '';
	$scope.editData.dbType = $scope.databaseType[0];
	$scope.editData.port = $scope.porArr[0];
	$scope.editData.rowDeli = $scope.delimiterArr[0];
	$scope.editData.colDeli = $scope.delimiterArr[0];
	$scope.fileSys = 'file';
	typeFormatFetch($scope, $http, $templateCache,
			$rootScope, $location, 'Format','DataSource');
	$scope.closeModal = function() {
		$modalInstance.dismiss('cancel');

	};
	$scope.getName = function(s) {
		return s.replace(/^.*[\\\/]/, '');
	};
	$scope.selectPort = function(dbType) {
		if (dbType == $scope.databaseType[0]) {
			$scope.editData.port = $scope.porArr[0];
		} else {
			$scope.editData.port = $scope.porArr[1];
		}
	}
	$scope.upload = function(filetype) {
		$scope.errorCode = '';
		$scope.filedata = new Object();
		$rootScope.fileSys = filetype;
		if (filetype == 'file') {
			var fileName = $scope.editData.filepath;
			$rootScope.fileName = fileName;
			if (fileName == undefined || fileName == '') {
				alert('Please enter a valid file path');
				return false;
			}
			$('#uploadLoader').show();
			newSchemaName = $scope.getName(fileName)
			newSchemaName = newSchemaName.split('.');
			newSchemaName = newSchemaName[0];
			$scope.filedata.fileName = fileName;
			$scope.filedata.fileType = $scope.editData.format;
			if($scope.editData.format == 'Fixed Width'){
				$scope.filedata.noOfColumn = $scope.editData.noofColumn;
				$scope.filedata.fixedValues = $scope.editData.fixedValues;
			}
			else if($scope.editData.format == 'Delimited'){
				$scope.filedata.rowDeli = $scope.editData.rowDeli;
				$scope.filedata.colDeli = $scope.editData.colDeli;
			}
			
		} else {
			newSchemaName = ''
			delete $scope.editData.filepath;
			delete $rootScope.fileName
			$('#connectLoader').show();
			$scope.filedata = $scope.editData;
		}
		myService.set($scope.filedata);
		datapreview = new Array();

		// alert(newSchemaName);
		$scope.method = 'POST';
		// fileName = encodeURIComponent(fileName)
		// fileName = fileName.replace(/\./g, '\.');
		// alert(fileName);

		$scope.url = 'rest/service/getSchemaAuto';
		$http({
			method : $scope.method,
			url : $scope.url,
			data : $scope.filedata,
			// cache : $templateCache
			headers : {
				'X-Auth-Token' : localStorage.getItem('itc.authToken')
			}
		}).success(function(data, status) {

			$scope.status = status;
			// alert("data :"+data);
			datapreview = data;// data;
			$scope.closeModal();
			$location.path("/DataSchemaPreview/");

			// datapreview = data

		}).error(function(data, status) {
			if (status == 401) {
				$location.path('/');
			}
			if (filetype == 'file') {
				$('#uploadLoader').hide();
			} else {
				$('#connectLoader').hide();
			}
			$scope.errorCode = data;// data;
			// console.log($scope.errorCode);
			// $scope.closeModal();
			// $location.path("/DataSchemaPreview/");
			/*
			 * $scope.data = data || "Request failed"; $scope.status = status;
			 */
		});

	};
	$scope.submitStream = function(editData) {
		$('#submitLoader').show();
		$scope.data = new Object();
		// console.log(editData);
		$scope.data.name = editData.Consumer_Name;
		$scope.data.type = 'Streaming';
		// editData.dataSourcerId = editData.Consumer_Name;
		$scope.data.jsonblob = angular.toJson(editData);
		$scope.method = 'POST';
		$scope.data.createdBy = localStorage.getItem('itc.username');
		$scope.data.updatedBy = localStorage.getItem('itc.username');
		$scope.data.createdDate = new Date();
		$scope.url = 'rest/service/addEntity/';
		$http({
			method : $scope.method,
			url : $scope.url,
			data : $scope.data,
			headers : {
				'X-Auth-Token' : localStorage.getItem('itc.authToken')
			}
		})
				.success(
						function(data, status) {
							$scope.status = status;
							$('#submitLoader').hide();
							$scope.closeModal();
							schemaSourceDetails(myService, $scope, $http,
									$templateCache, $rootScope, $location,
									'Streaming');
							$location.path('/Streaming/');
						}).error(function(data, status) {
							if (status == 401) {
								$location.path('/');
							}
					$scope.data = data || "Request failed";
					$scope.status = status;

				});

	}

});
userApp
		.controller('driveCtrl',
				function(myService, $scope, $http, $templateCache, $location,
						$rootScope, $route, $upload, $filter, $modal) {
					schemaSourceDetails(myService, $scope, $http,
							$templateCache, $rootScope, $location, 'driver');
					$scope.delete1 = function() {
						currentUser = $scope.Deluser;
						objDel = $scope.DelObj;
						// alert(currentUser)
						$scope.stopDriver($scope.Deluser);
					};
					$scope.showconfirm = function(id, obj) {
						// alert(id);
						$scope.Deluser = id;
						$scope.DelObj = obj;
						$('#deleteConfirmModal').modal('show');
					};
					$scope.stopDriver = function(id) {
						$scope.method = 'POST';
						$scope.url = 'rest/service/stopStreamDriver';
						$scope.stopData = new Object();
						$scope.stopData.driverId = id;
						$scope.stopData.stopBy = localStorage
								.getItem('itc.username');
						$http(
								{
									method : $scope.method,
									url : $scope.url,
									data : $scope.stopData,
									headers : {
										'X-Auth-Token' : localStorage
												.getItem('itc.authToken')
									}
								}).success(
								function(data, status) {
									$scope.status = status;
									$('#deleteConfirmModal').modal('hide');
									schemaSourceDetails(myService, $scope,
											$http, $templateCache, $rootScope,
											$location, 'driver');
									$scope.editing = false;
									$scope.viewing = true;
									$scope.addNewBtn = true;

								}).error(function(data, status) {
									if (status == 401) {
										$location.path('/');
									}
							$scope.data = data || "Request failed";
							$scope.status = status;

						});
					};

				});

userApp.controller('streamingCtrl',
		function(myService, $scope, $http, $templateCache, $location,
				$rootScope, $route, $upload, $filter, $modal) {
			schemaSourceDetails(myService, $scope, $http, $templateCache,
					$rootScope, $location, 'Streaming');
			$scope.delete1 = function() {
				currentUser = $scope.Deluser;
				objDel = $scope.DelObj;
				//alert(currentUser)
				$scope.deleteRecord(currentUser, objDel);
			};
			$scope.allconsumer=false;
			$scope.addStream = function(){
				
				$('#addStream').modal('show');
			};
			$scope.showconfirm = function(id, obj) {
			 if(obj.count >0){
				 //alert("Cannot Delete ! first Stop the Running Job");
				 $('#alertMessage').modal('show');
			 }
				
			 if(obj.count ==0){
					$scope.Deluser = id;
					$scope.DelObj = obj;
				 $('#deleteConfirmModal').modal('show');
				}
			};
			
			$scope.submitStream = function(editData) {

				$scope.data = new Object();
				// console.log(editData);
				$scope.data.name = editData.Consumer_Name;
				$scope.data.type = 'Streaming';
				// editData.dataSourcerId = editData.Consumer_Name;
				$scope.data.jsonblob = angular.toJson(editData);
				$scope.method = 'POST';
				$scope.data.createdBy = localStorage.getItem('itc.username');
				$scope.data.updatedBy = localStorage.getItem('itc.username');
				$scope.data.createdDate = new Date();
				$scope.url = 'rest/service/addStreamEntity/';
				$http({
					method : $scope.method,
					url : $scope.url,
					data : $scope.data,
					headers : {
						'X-Auth-Token' : localStorage.getItem('itc.authToken')
					}
				})
						.success(
								function(data, status) {
									$scope.status = status;
									$('#addStream').hide();
									$('#submitLoader').hide();
									//$scope.closeModal();
									schemaSourceDetails(myService, $scope, $http,
											$templateCache, $rootScope, $location,
											'Streaming');
									$location.path('/Streaming/');
								}).error(function(data, status) {
									if (status == 401) {
										$location.path('/');
									}
							$scope.data = data || "Request failed";
							$scope.status = status;

						});

			};
			
			
			
			
			$scope.addUpdateStream = function(id) {
				if (id != undefined) {
					$scope.method = 'GET';
					$scope.type = $location.path();
					$scope.type = $scope.type.replace(/\//g, '');
					$scope.url = 'rest/service/' + $scope.type + '/' + id;
					$scope.editData = new Object();
					$http(
							{
								method : $scope.method,
								url : $scope.url,
								headers : {
									'X-Auth-Token' : localStorage
											.getItem('itc.authToken')
								}
							}).success(function(data, status) {

						$scope.status = status;

						$scope.editData = angular.fromJson(data.jsonblob);
						$scope.editData.id = data.id;

					}).error(function(data, status) {
						if (status == 401) {
							$location.path('/');
						}
						$scope.data = data || "Request failed";
						$scope.status = status;
					});
				} else {
					$scope.editData = {};

					$scope.update = function(user) {
						$scope.editData = angular.copy(user);
					};

					$scope.reset = function() {
						$scope.user = angular.copy($scope.editData);
					};

					$scope.reset();
					/* $scope.editData.Schema = "Int"; */
					$scope.editData.frequency = 'One Time';

				}
				$scope.editing = true;
				$scope.viewing = false;
				$scope.addNewBtn = false;

			}
			$scope.saveAddUpdateDataSet = function(editData) {
				$scope.data = {};
				$scope.data.name = editData.Consumer_Name
				$scope.data.type = $location.path();
				$scope.data.type = $scope.data.type.replace(/\//g, '')
				$scope.editData.dataIngestionId = editData.Consumer_Name;
				$scope.data.jsonblob = angular.toJson(editData);
				$scope.method = 'POST';
				if (editData.id != undefined && editData.id != null) {
					$scope.data.updatedBy = localStorage
							.getItem('itc.username');
					$scope.data.updatedDate = new Date();
					$scope.data.id = editData.id;
					$scope.url = 'rest/service/' + $scope.data.type + '/'
							+ editData.id;

				} else {
					$scope.data.createdBy = localStorage
							.getItem('itc.username');
					$scope.data.updatedBy = localStorage
							.getItem('itc.username');
					$scope.data.createdDate = new Date();
					$scope.url = 'rest/service/addEntity/';
				}

				$http({
					method : $scope.method,
					url : $scope.url,
					data : $scope.data,
					headers : {
						'X-Auth-Token' : localStorage.getItem('itc.authToken')
					}
				}).success(
						function(data, status) {
							$scope.status = status;
							schemaSourceDetails(myService, $scope, $http,
									$templateCache, $rootScope, $location);
							$scope.editing = false;
							$scope.viewing = true;
							$scope.addNewBtn = true;

						}).error(function(data, status) {
							if (status == 401) {
								$location.path('/');
							}
					$scope.data = data || "Request failed";
					$scope.status = status;

				});

			}
			$scope.cancelUpdate = function() {
				$scope.editing = false;
				$scope.viewing = true;
				$scope.addNewBtn = true;
			}
			$scope.cancelADD = function() {
				$scope.editing = false;
				$scope.viewing = true;
				$scope.addNewBtn = true;
			}
			$scope.runStream = function(editData) {
				// alert('#'+editData.id+'loader')
				$('#' + editData.id + 'loader').show();
				$scope.data = new Object();
				// console.log(editData);
				$scope.data.name = editData.Consumer_Name;
				$scope.data.type = 'Streaming';
				// editData.dataSourcerId = editData.Consumer_Name;
				$scope.data.jsonblob = angular.toJson(editData);
				$scope.method = 'POST';
				$scope.data.createdBy = localStorage.getItem('itc.username');
				$scope.data.updatedBy = localStorage.getItem('itc.username');
				$scope.method = 'POST';
				$scope.data.createdBy = localStorage.getItem('itc.username');
				$scope.data.updatedBy = localStorage.getItem('itc.username');
				$scope.data.createdDate = new Date();
				$scope.url = 'rest/service/startStream/';
				$http({
					method : $scope.method,
					url : $scope.url,
					data : $scope.data,
					headers : {
						'X-Auth-Token' : localStorage.getItem('itc.authToken')
					}
				}).success(
						function(data, status) {
							$('#' + editData.id + 'loader').hide();
							$scope.status = status;
							schemaSourceDetails(myService, $scope, $http,
									$templateCache, $rootScope, $location,
									'Streaming');
							// console.log($scope.detailStreaming);
							// $location.path('/Streaming/');

						}).error(function(data, status) {
							if (status == 401) {
								$location.path('/');
							}
					$scope.data = data || "Request failed";
					$scope.status = status;
				});
			};
			
			$scope.showDrivers = function(consumer) {
				//alert("helooooo:::"+consumer);
				//stramORDriver('Driver')
				$scope.allconsumer=false;
				//$('#StreamDrivers').modal('show');
				$scope.data = new Object();
				$scope.data.name = consumer;
				$scope.method = 'POST';
				$scope.url = 'rest/service/getStreamDrivers';

				$http(
						{
							method : $scope.method,
							url : $scope.url,
							data : $scope.data,
							//cache : $templateCache,
							headers : {
								'X-Auth-Token' : localStorage
										.getItem('itc.authToken')
							}
						})
						.success(
								function(data, status) {
									$scope.status = status;
									$scope.data = data;
									
									
									$scope.detailDriver = {};
									$scope.totalItems = $scope.data.length;
									$scope.detaildata = angular.fromJson($scope.data);
								//	alert($scope.detaildata.length)
									var i = 0;
									angular.forEach(
											$scope.detaildata,
											function(attr) {
												$scope.detailDriver[i] = attr;
												$scope.detailDriver[i].id = attr.driverId;
											//	alert(attr.driverId);
												// $scope.detailStreaming = {};
												i++;
											});



									$scope.totalSourcer = $scope.detaildata.length;
									$scope.editing = false;
									$scope.viewing = true;
									$scope.addSchema = true;
																								
									$('#StreamDrivers').modal('show');
								})
						.error(
								function(data, status) {
									$scope.data = data
											|| "Request failed";
									$scope.status = status;
								});
				
				
			};
			
			
			$scope.showRunningJobs = function() {
				$scope.allconsumer=true;
				//$('#StreamDrivers').modal('show');
				$scope.data = new Object();
				//$scope.data.name = consumer;
				$scope.method = 'POST';
				$scope.url = 'rest/service/getStreamDrivers';

				$http(
						{
							method : $scope.method,
							url : $scope.url,
							data : $scope.data,
							//cache : $templateCache,
							headers : {
								'X-Auth-Token' : localStorage
										.getItem('itc.authToken')
							}
						})
						.success(
								function(data, status) {
									$scope.status = status;
									$scope.data = data;
									
									
									$scope.detailDriver = {};
									$scope.totalItems = $scope.data.length;
									$scope.detaildata = angular.fromJson($scope.data);
								//	alert($scope.detaildata.length)
									var i = 0;
									angular.forEach(
											$scope.detaildata,
											function(attr) {
												$scope.detailDriver[i] = attr;
												$scope.detailDriver[i].id = attr.driverId;
											//	alert(attr.driverId);
												// $scope.detailStreaming = {};
												i++;
											});



									$scope.totalSourcer = $scope.detaildata.length;
									$scope.editing = false;
									$scope.viewing = true;
									$scope.addSchema = true;
																								
									$('#StreamDrivers').modal('show');
								})
						.error(
								function(data, status) {
									$scope.data = data
											|| "Request failed";
									$scope.status = status;
								});
				
				
			};
			
			$scope.stopDriver = function(id,con_name) {
				//alert("con_name length"+con_name.length);
				$('#' + id + 'loader').show();
				$('#' + con_name + 'loader').show();	
				$scope.method = 'POST';
				$scope.url = 'rest/service/stopStreamDriver';
				$scope.stopData = new Object();
				$scope.stopData.driverId = id;
				$scope.stopData.stopBy = localStorage
						.getItem('itc.username');
				$http(
						{
							method : $scope.method,
							url : $scope.url,
							data : $scope.stopData,
							headers : {
								'X-Auth-Token' : localStorage
										.getItem('itc.authToken')
							}
						}).success(
						function(data, status) {
							$('#' + con_name+ 'loader').hide();
							$('#' + id + 'loader').hide();
							$scope.status = status;
							if(con_name.length !=0){
							$scope.showDrivers(con_name);
							}else
							$scope.showRunningJobs();
							schemaSourceDetails(myService, $scope, $http, $templateCache,
									$rootScope, $location, 'Streaming');
						}).error(function(data, status) {
							if (status == 401) {
								$location.path('/');
							}
					$scope.data = data || "Request failed";
					$scope.status = status;

				});
			};
			

		});


var getDateFormat = function(timemodi) {
	var lastModified = new Date(timemodi);
	var year = lastModified.getFullYear();
	var month = lastModified.getMonth();
	var date = lastModified.getDate();
	var hour = lastModified.getHours();
	var min = lastModified.getMinutes();
	var sec = lastModified.getSeconds();
	return date + '/' + month + '/' + year + ' ' + hour + ':' + min + ':' + sec;
}

userApp
		.controller(
				'dataSchemaSource',
				function(myService, $scope, $http, $templateCache, $location,
						$rootScope, $route, $upload, $filter, $modal) {

					/*
					 * if(localStorage.getItem('itc.authToken')){ var authToken =
					 * localStorage.getItem('itc.authToken');
					 * $rootScope.authToken = authToken; }
					 */

					// $scope.dataSrc.test = [0, 1];
					$scope.frequencyArr = [ "One Time", "Hourly", "Daily",
											"Weekly" ];
					$scope.filterRow = false;
					$scope.query = {}
				    $scope.queryBy = '$'
				 //   $scope.query['schedulerFrequency'] = 'Select Frequency';
					$scope.viewing = true;
					$scope.addSchema = true;
					$scope.editData = new Object();
					$scope.totalSourcer = 1;
					$scope.oldName = '';
					$scope.Deluser = null
					$scope.DelObj = {};
					$scope.pipeEdit = false;
					$scope.savebutton = true;
					$scope.xmlEndTag = '';
					// $scope.editData.frequency = $scope.frequencyArr[0];
					typeFormatFetch($scope, $http, $templateCache,
							$rootScope, $location, 'Format','DataSource')
					// prevMenu = '';
					changeMClass('one')
					// $scope.sourceType = ['file'];
					// $scope.editData.sourcerType = $scope.sourceType[0];
					$scope.newItem = function($event) {
						$scope.ddObject = new Object();
						$scope.ddObject.active = true;
						$scope.ddArray.push($scope.ddObject);
					}
					
					$scope.schemanamenotok = false;
					$scope.chkSchemaName = function(schemaName) {
						schemanameCheck(schemaName, $scope, $http,
								$templateCache, $rootScope, $location,
								'dataschema');
					}
					var test = window.location.pathname;
					var parts = window.location.pathname.split('/');
					$("#userDName").text(localStorage.getItem('itc.dUsername'));

					$scope.load = function(tpl) {
						$scope.tpl = tpl;
					};
					$scope.load('tpl');
					$scope.method = 'GET';
					$scope.type = $location.path();
					$scope.type = $scope.type.replace(/\//g, '');
					if ($scope.type == 'DataSource') {
						schemaSourceDropDetails($scope, $http, $templateCache,
								$rootScope, $location, 'DataSchema');
						typeFormatFetch($scope, $http, $templateCache,
								$rootScope, $location, 'Type')
						typeFormatFetch($scope, $http, $templateCache,
								$rootScope, $location, 'Format')

					}
					if ($scope.type == 'DataSchema') {
						typeFormatFetch($scope, $http, $templateCache,
								$rootScope, $location, 'Type')
						$scope.valRule = 'No';
						$scope.valRule = [ 'Yes', 'No' ];
						$scope.pIIRule = [ 'Obfuscate', 'Remove' ];
						$scope.addSchemaMed = [ {
							"name" : "---Select---"
						}, {
							"name" : "Manual"
						}, {
							"name" : "Automatic"
						} ];
						$scope.editData = new Object();
						$scope.editData.addSchemaMed = "---Select---";
						schemaSourceDetails(myService,$scope, $http,$templateCache, $rootScope,$location, 'DataSchema');
						

						/*$scope.method = 'GET';
						$scope.type = $location.path();
						$scope.type = $scope.type.replace(/\//g, '')
						$scope.url = 'rest/service/profiles';

						$http(
								{
									method : $scope.method,
									url : $scope.url,
									// cache : $templateCache
									headers : {
										'X-Auth-Token' : localStorage
												.getItem('itc.authToken')
									}
								})
								.success(
										function(data, status) {

											$scope.status = status;
											// console.log($scope.status);
											$scope.detaildataSchema = data;

											var i = 0;
											angular
													.forEach(
															data,
															function(attr) {
																$scope.editData[i] = new Object();
																$scope.editData[i] = angular
																		.fromJson(attr.schemaJsonBlob);
																if ($scope.editData[i].fileName != undefined) {
																	$scope.detaildataSchema[i].fileName = $scope.editData[i].fileName;
																}

																$scope.detaildataSchema[i].lastModified = getDateFormat(attr.schemaModificationDate);
																$scope.detaildataSchema[i].dataSchemaType = $scope.editData[i].dataSchemaType;
																$scope.detaildataSchema[i].profile = true;
																// console.log($scope.detaildataSchema[i].fileName);
																// console.log($scope.detaildataSchema[i].dataSchemaType);
																i++;
															});

										}).error(function(data, status) {
									$scope.data = data || "Request failed";
									$scope.status = status;
								});
*/
						// console.log($scope.addSchemaMed)
					} else {
						schemaSourceDetails(myService, $scope, $http,
								$templateCache, $rootScope, $location);
					}
					$scope.getSouceinfo = function(sourceId) {
						schemaSourceDetails(myService, $scope, $http,
								$templateCache, $rootScope, $location,
								'DataSource');
						// console.log($scope.detaildataSource);
					}

					$scope.addUpdateSchema = function(id, dbdetails) {
						// alert(sourcePath);
						// console.log($scope.editData1.id);

						if (id != undefined) {
							$scope.method = 'GET';
							$scope.type = $location.path();
							$scope.type = $scope.type.replace(/\//g, '')

							$scope.url = 'rest/service/' + $scope.type + '/'
									+ id;
							if (dbdetails != undefined) {
								// $scope.type = 'DataSource';
								$scope.url = 'rest/service/DataSource/' + id;
							}
							$scope.editData1 = new Object();
							$http(
									{
										method : $scope.method,
										url : $scope.url,
										// cache : $templateCache
										headers : {
											'X-Auth-Token' : localStorage
													.getItem('itc.authToken')
										}
									})
									.success(
											function(data, status) {

												$scope.status = status;

												$scope.editData1 = angular
														.fromJson(data.jsonblob);
												// console.log(data);
												$scope.editData1.id = data.id;
												if ($scope.editData1.xmlEndTag) {
													$scope.xmlEndTag = $scope.editData1.xmlEndTag;
													// console.log($scope.editData.xmlEndTag)
												}
												// $scope.editData.xmlEndTag =
												// data.xmlEndTag;
												if (dbdetails != undefined) {
													$scope.dbDetails = new Object();
													$scope.dbDetails = angular
															.fromJson($scope.editData1.fileData);
													// console.log($scope.dbDetails[0].dbName);
													$('#detailDb')
															.modal('show');
												}
												$scope.ddArray = $scope.editData1.dataAttribute;
												var i = 0;
												angular
														.forEach(
																$scope.ddArray,
																function(attr) {
																	$scope.ddArray[i].active = false;

																	i++;
																});
												// $scope.ddArray[0].active =
												// true;

											}).error(function(data, status) {
												if (status == 401) {
													$location.path('/');
												}
										$scope.data = data || "Request failed";
										$scope.status = status;
									});
						} else {
							$("form#schemaForm").find(
									"input[type=text], textarea,select")
									.val("")
							$('form#schemaForm').find(
									'input:radio, input:checkbox').removeAttr(
									'checked').removeAttr('selected');
							$scope.editData1 = new Object();
							$scope.ddArray = new Array();
							$scope.ddObject1 = new Object();
							$scope.ddObject1.active = true;
							$scope.ddObject2 = new Object();
							$scope.ddArray.push($scope.ddObject1);
							$scope.ddArray.push($scope.ddObject2);

						}

						if (dbdetails == undefined) {
							if ($scope.type == 'DataSchema' && id == undefined) {
								$scope.viewing = false;

								if ($scope.editData.addSchemaMed == 'Manual') {
									$scope.editing = true;
									$scope.schemaupload = false;
									$scope.addSchema = false;
								} else if ($scope.editData.addSchemaMed == 'Automatic') {

									// Modal for Automatic

									// console.log("Automatic");

									$scope.showModal = function() {

										var modalInstance = $modal
												.open({
													templateUrl : 'myModalContent.html',
													controller : 'ModalInstanceCtrl'
												});
										// $scope.editData.addSchemaMed = '';

										modalInstance.result.then(function() {
											// $scope.selected = selectedItem;
										}, function() {
											// console.log('Modal dismissed at:
											// ' + new Date());
										});

									}();

									$scope.editing = false;
									// $scope.addSchema = false;
									$scope.viewing = true;
									$('#filepath').focus();
								} else {
									$scope.viewing = true;
									$scope.editing = false;
									$scope.schemaupload = false;
								}
								$scope.editData.addSchemaMed = "---Select---";
							} else {
								$scope.editing = true;
								$scope.addSchema = false;
								$scope.viewing = false;
							}

						}

					}
					$scope.saveAddUpdateDataSource = function(editData) {
						$scope.data = {};
						$scope.data.name = editData.name;
						$scope.data.type = $location.path();
						$scope.data.type = $scope.data.type.replace(/\//g, '')
						$scope.editData.schema = editData.schema;
						$scope.editData.format = editData.format;
						$scope.editData.location = editData.location;
						$scope.editData.dataSource = editData.dataSource;
						$scope.editData.dataSourcerId = editData.name;
						$scope.data.jsonblob = angular.toJson(editData);
						$scope.method = 'POST';
						if (editData.id != undefined && editData.id != '') {
							$scope.data.updatedBy = localStorage
									.getItem('itc.username');
							$scope.data.updatedDate = new Date();
							$scope.data.id = editData.id;
							$scope.url = 'rest/service/' + $scope.data.type
									+ '/' + editData.id;

						} else {
							$scope.data.createdBy = localStorage
									.getItem('itc.username');
							$scope.data.updatedBy = localStorage
									.getItem('itc.username');
							$scope.data.createdDate = new Date();
							$scope.url = 'rest/service/addEntity/';
						}

						$http(
								{
									method : $scope.method,
									url : $scope.url,
									data : $scope.data,
									headers : {
										'X-Auth-Token' : localStorage
												.getItem('itc.authToken')
									}
								}).success(
								function(data, status) {
									$scope.status = status;

									schemaSourceDetails(myService, $scope,
											$http, $templateCache, $rootScope,
											$location);
									$scope.editing = false;
									$scope.viewing = true;

								}).error(function(data, status) {
									if (status == 401) {
										$location.path('/');
									}
							$scope.data = data || "Request failed";
							$scope.status = status;

						});

					}

					$scope.saveAddUpdateSchema = function(editData, dd, isQuit) {
						$scope.data = new Object();
						$scope.editData = new Object();
						// console.log(dd);
						$scope.data.name = editData.name;
						$scope.data.type = $location.path();
						$scope.data.type = $scope.data.type.replace(/\//g, '')
						$scope.editData.description = editData.description;
						// console.log($scope.editData.dataSchemaType)
						$scope.editData.dataSchemaType = editData.dataSchemaType
						$scope.editData.dataSourcerId = editData.name;
						$scope.editData.name = editData.name;
						$scope.editData.dataAttribute = dd;
						if ($scope.xmlEndTag != '')
							$scope.editData.xmlEndTag = $scope.xmlEndTag;
						$scope.data.jsonblob = angular.toJson($scope.editData);
						// console.log($scope.editData);
						$scope.method = 'POST';
						if (editData.id != undefined && editData.id != '') {
							$scope.data.updatedBy = localStorage
									.getItem('itc.username');
							$scope.data.id = editData.id;
							$scope.url = 'rest/service/' + $scope.data.type
									+ '/' + editData.id;// 'http://jsonblob.com/api/54215e4ee4b00ad1f05ed73d';
							$http(
									{
										method : $scope.method,
										url : $scope.url,
										data : $scope.data,
										headers : {
											'X-Auth-Token' : localStorage
													.getItem('itc.authToken')
										}
									}).success(
									function(data, status) {
										$scope.status = status;
										// var fromAutoSchema = false;
										// $rootScope.fromAutoSchema = false;
										$scope.data = data;
										$scope.dataschema = $scope.data
										$scope.editSchema = true;
										// console.log(isQuit)
										if (isQuit != undefined) {
											schemaSourceDetails(myService,
													$scope, $http,
													$templateCache, $rootScope,
													$location, 'DataSchema');

										} else {
											myService.set($scope);
											$location.path('/ValidationRule/');
										}

										/*
										 * schemaSourceDetails(myService,$scope,
										 * $http, $templateCache, $rootScope,
										 * $location);
										 */
									}).error(function(data, status) {
										if (status == 401) {
											$location.path('/');
										}
								$scope.data = data || "Request failed";
								$scope.status = status;
							});
						} else {
							$scope.data.createdBy = localStorage
									.getItem('itc.username');
							$scope.data.updatedBy = localStorage
									.getItem('itc.username');
							$scope.url = 'rest/service/addEntity/';
							$scope.data.dataSchemaType = 'Manual';
							// console.log($scope.data);
							myService.set($scope.data);

							$location.path("/IngestionSummary/");
						}

					}
					$scope.canceloparation = function() {
						$scope.editing = false;
						$scope.schemaupload = false;
						$scope.viewing = true;
						$scope.addSchema = true;
						$scope.schemanamenotok = false;
						// $scope.editData.addSchemaMed = '';
					}

					$scope.delete1 = function() {
						currentUser = $scope.Deluser;
						objDel = $scope.DelObj;
						// alert(currentUser)
						$scope.deleteRecord(currentUser, objDel);
					};
					$scope.showconfirm = function(id, obj) {
						$scope.Deluser = id;
						$scope.DelObj = obj;
						$('#deleteConfirmModal').modal('show');
					};
					$scope.getName = function(s) {
						return s.replace(/^.*[\\\/]/, '');
					}

					$scope.runScheduler = function(scedulerID, schedulerName,
							schemaid) {
						$('#' + schemaid + 'loader').show();
						// console.log(schemaid);
						$scope.method = 'POST'
						$scope.filedata = new Object();
						$scope.url = 'rest/service/runScheduler/'
								+ schedulerName;
						$http(
								{
									method : $scope.method,
									url : $scope.url,
									// cache : $templateCache
									headers : {
										'X-Auth-Token' : localStorage
												.getItem('itc.authToken')
									}
								}).success(function(data, status) {
							$scope.status = status;
							// datapreview = data
						}).error(function(data, status) {
							if (status == 401) {
								$location.path('/');
							}
							$scope.data = data || "Request failed";
							$scope.status = status;
							$('#' + schemaid + 'loader').hide();
							$('#runFN').html(data.fileName);
							$('#runBT').html(data.fileSize);
							$('#runTT').html(data.timeTaken);
							$('#confirmRun').modal('show');
						});

					}
					$scope.chartData = new Object();
					$scope.getIngestionLog = function(id) {
						$scope.getMyId = id;

						// alert($('#dataIngestionLog').css('display'));
						/*
						 * if($('#dataIngestionLog').css('display') == 'block' ||
						 * direct != undefined){
						 */

						// console.log($scope.getMyId);
						$scope.method = 'GET';

						$scope.url = 'rest/service/listIngestionDetails/' + id;

						$http(
								{
									method : $scope.method,
									url : $scope.url,
									headers : {
										'X-Auth-Token' : localStorage
												.getItem('itc.authToken')
									}
								})
								.success(
										function(data, status) {
											$scope.status = status;
											$scope.dataIngLog = data;
											if ($scope.dataIngLog.ingestionStart != null) {
												$scope.dataIngLog.ingestionStartArr = $scope.dataIngLog.ingestionStart
														.split('|');
											}

											if ($scope.dataIngLog.ingestionFails != null) {
												$scope.dataIngLog.ingestionFailsArr = $scope.dataIngLog.ingestionFails
														.split('|');
											}
											if ($scope.dataIngLog.ingestionComplete != null) {
												$scope.dataIngLog.ingestionCompleteArr = $scope.dataIngLog.ingestionComplete
														.split('|');
											}

											if ($scope.dataIngLog.validationStart != null) {
												$scope.dataIngLog.validationStartArr = $scope.dataIngLog.validationStart
														.split('|');
											}
											// console.log($scope.dataIngLog.validationStartArr[0]);
											if ($scope.dataIngLog.validationComplete != null) {
												$scope.dataIngLog.validationCompleteArr = $scope.dataIngLog.validationComplete
														.split('|');
											}
											// console.log($scope.dataIngLog.ingestionStartArr[0]);
											if ($scope.dataIngLog.noOfRecords != 0) {
												$scope.chartData = [
														[
																'No Of Cleansed',
																$scope.dataIngLog.noOfCleansed ],
														[
																'No Of Range Fails',
																$scope.dataIngLog.validationLogDetails.noOfRangeFails ],
														[
																'No Of Regex Fails',
																$scope.dataIngLog.validationLogDetails.noOfRegexFails ],
														[
																'No Of Fixed Length Fails',
																$scope.dataIngLog.validationLogDetails.noOfFixedlLengthFails ],
														[
																'No Of WhiteList Fails',
																$scope.dataIngLog.validationLogDetails.noOfWhiteListFails ],
														[
																'No Of BlackList Fails',
																$scope.dataIngLog.validationLogDetails.noOfBlackListFails ],
														[
																'No Of Mandatory Fails',
																$scope.dataIngLog.validationLogDetails.noOfMandatoryFails ],
														[
																'No Of DataTypeMismatch Fails',
																$scope.dataIngLog.validationLogDetails.noOfDataTypeMismatchFails ],
														[
																'No Of ColumnMismatch Fails',
																$scope.dataIngLog.validationLogDetails.noOfColumnMismatchFails ],
														[
																'No Of other Fails',
																$scope.dataIngLog.validationLogDetails.noOfotherFails ] ];
											} else {
												$scope.chartData = [];
											}

											// $scope.charDraw($scope.dataIngLog);
											// console.log($scope.dataIngLog);
											$('#dataIngestionLog')
													.modal('show')
										}).error(function(data, status) {
											if (status == 401) {
												$location.path('/');
											}
									$scope.data = data || "Request failed";
									$scope.status = status;
								});
						/* } */

					}
					$scope.chartTitle = "Ingestion Log Chart";
					$scope.chartWidth = 500;
					$scope.chartHeight = 200;
					/*
					 * $scope.chartData = [ ['Work', 11], ['Eat', 2],
					 * ['Commute', 2], ['Watch TV', 2], ['Sleep', 7] ];
					 */

				});
userApp.directive('pieChart', function($timeout) {
	return {
		restrict : 'EA',
		scope : {
			title : '@title',
			width : '@width',
			height : '@height',
			data : '=data',
			selectFn : '&select'
		},
		link : function($scope, $elm, $attr) {

			// Create the data table and instantiate the chart
			var data = new google.visualization.DataTable();
			data.addColumn('string', 'Label');
			data.addColumn('number', 'Value');
			var chart = new google.visualization.PieChart($elm[0]);

			draw();

			// Watches, to refresh the chart when its data, title or dimensions
			// change
			$scope.$watch('data', function() {
				draw();
			}, true); // true is for deep object equality checking
			$scope.$watch('title', function() {
				draw();
			});
			$scope.$watch('width', function() {
				draw();
			});
			$scope.$watch('height', function() {
				draw();
			});

			// Chart selection handler
			google.visualization.events.addListener(chart, 'select',
					function() {
						var selectedItem = chart.getSelection()[0];
						// console.log(selectedItem)
						if (selectedItem) {
							$scope.$apply(function() {
								$scope.selectFn({
									selectedRowIndex : selectedItem.row
								});
							});
						}
					});

			function draw() {
				if (!draw.triggered) {
					draw.triggered = true;
					$timeout(function() {
						draw.triggered = false;
						var label, value;
						data.removeRows(0, data.getNumberOfRows());
						angular.forEach($scope.data, function(row) {
							label = row[0];
							value = parseFloat(row[1], 10);
							if (!isNaN(value)) {
								data.addRow([ row[0], value ]);
							}
						});
						var options = {
							'title' : $scope.title,
							'width' : $scope.width,
							'height' : $scope.height,
							// is3D: true,
							// legend: 'none',
							pieSliceText : 'none',
						};
						chart.draw(data, options);
						// No raw selected
						$scope.selectFn({
							selectedRowIndex : undefined
						});
					}, 0, true);
				}
			}
		}
	};
});

var newSchemaName;
var dataSchemapreview = function(myService, $scope, $http, $templateCache,
		$location, $rootScope, $route, $upload, $filter) {
	$scope.filetempPath = '';
	$scope.datasetPath = ''
	$scope.tableview = true;
	// $scope.ingeationView = false;
	$scope.editdata = new Object();
	$scope.editdata = new Object();
	$scope.editdata.newSchemaName = '';
	$scope.editdata.newSchemaName = newSchemaName;
	typeFormatFetch($scope, $http, $templateCache, $rootScope, $location,
			'Type', 'DataSchema')
	// console.log($scope.sourceType);
	// alert("datapreview :"+$scope.sourceType);
	$scope.schemanamenotok = false;
	$scope.chkSchemaName = function(schemaName) {
		schemanameCheck(schemaName, $scope, $http, $templateCache, $rootScope,
				$location, 'dataschemapreview');
	}
	$scope.datapreview = datapreview;
	$scope.column = {};
	var limit = 0;

	// console.log(datapreview);
	$scope.columns = {
		header : $scope.datapreview[0]
	};
	// alert("$scope.columns :"+$scope.datapreview[0]);
	// console.log(datapreview);
	$scope.columnsdata = {
		datatype : $scope.datapreview[1]
	};
	$scope.columnName = new Array();
	$scope.columnType = new Array();
	var i = 0;
	angular.forEach($scope.columns.header, function(attr) {
		$scope.columnName[i] = attr;
		i++;
	});
	var i = 0;
	angular.forEach($scope.columnsdata.datatype, function(attr) {
		$scope.columnType[i] = attr;
		i++;
	});
	$scope.piiRule = $scope.datapreview[2];
	// console.log($scope.columnName);
	// console.log($scope.piiRule);
	$scope.datapreview.shift();
	$scope.datapreview.shift();
	$scope.datapreview.shift();
	$scope.fileDBData = myService.get();
	if ($scope.fileDBData.fileName != undefined) {
		var re = /(?:\.([^.]+))?$/;
		var format = re.exec($scope.fileDBData.fileName)[1];
		var fileext = format.toUpperCase();
		if (fileext == 'XML') {
			var xmlEndTag = $scope.datapreview[0];
			$scope.editdata.xmlEndTag = xmlEndTag[0]
			$scope.datapreview.shift();
		}
	}
	// console.log($scope.fileDBData);
	$scope.ingestionSummaryPage = function(editdata, columnName, columnType) {
		// console.log(columnType);
		$scope.data = new Object();
		// $scope.hdfsPathAcess = false;
		// alert("hey you");
		$scope.editdata.newSchemaName = editdata.newSchemaName;
		$scope.editdata.dataAttribute = new Array();
		$scope.editdata.fileData = new Array();
		$scope.editdata.dataAttribute.Name = new Object();
		$scope.editdata.dataAttribute.dataType = new Object();
		$scope.data.name = $scope.editdata.newSchemaName;
		$scope.editdata.name = $scope.editdata.newSchemaName;
		if ($scope.editdata.xmlEndTag != undefined)
			$scope.editdata.xmlEndTag = $scope.editdata.xmlEndTag;
		$scope.data.type = 'DataSchema';
		// $scope.editdata.description = newSchemaName +' description';
		$scope.editdata.dataSourcerId = 'NewSchema';
		$scope.editdata.dataSchemaType = 'Automatic';
		$scope.editdata.fileName = $rootScope.fileName;
		// console.log(columnName.length);
		for ( var j = 0; j < columnName.length; j++) {
			$scope.columnData = new Object();
			$scope.columnData.Name = new Object();
			$scope.columnData.dataType = new Object();

			$scope.columnData.Name = columnName[j]
			$scope.columnData.dataType = columnType[j];
			// console.log($scope.piiRule[j]);
			if ($scope.piiRule[j] == 'Y') {
				$scope.columnData.piirule = $scope.piiRule[j];
			}
			$scope.editdata.dataAttribute.push($scope.columnData);
		}
		$scope.editdata.fileData.push($scope.fileDBData);
		$scope.editdata.dataSchemaType = 'Automatic';
		myService.set($scope.editdata);
		// console.log($scope.editdata);
		$location.path("/IngestionSummary/");
	}
	$scope.resetField = function() {
		if ($scope.editData.frequency == 'One Time') {
			$scope.editData.startBatch = '';
			$scope.editData.endBatch = '';
			// console.log($scope.editData.startBatch)
		}

	}
	$scope.editData = new Object();
	$scope.frequencyArr = [ "One Time", "Hourly", "Daily", "Weekly" ];
	$scope.editData.frequency = $scope.frequencyArr[0];
	$scope.resetField = function() {
		if ($scope.editData.frequency == 'One Time') {
			$scope.editData.startBatch = '';
			$scope.editData.endBatch = '';
			// console.log($scope.editData.startBatch)
		}

	}

	$scope.saveNewSchema = function(columnName, columnData) {

		$scope.data = new Object();
		// alert("hey you");
		$scope.editdata.dataAttribute = new Array();
		$scope.editdata.dataAttribute.Name = new Object();
		$scope.editdata.dataAttribute.dataType = new Object();
		$scope.data.name = $scope.editdata.newSchemaName;
		$scope.editdata.name = $scope.editdata.newSchemaName;
		$scope.data.type = 'DataSchema';
		// $scope.editdata.description = newSchemaName +' description';
		$scope.editdata.dataSourcerId = 'NewSchema';
		$scope.editdata.dataSchemaType = 'Automatic';
		$scope.editdata.fileName = $rootScope.fileName;
		i = 0;
		angular.forEach(columnName.header,
				function(attr) {
					$scope.columnData = new Object();
					$scope.columnData.Name = new Object();
					$scope.columnData.dataType = new Object();
					$scope.columnData.valRule = new Object();
					$scope.columnData.Name = $('#colName' + i).text().trim();
					$scope.columnData.dataType = $(
							'#dataType' + i + ' option:selected').text();
					$scope.columnData.valRule = "No";
					$scope.editdata.dataAttribute.push($scope.columnData);
					i++;
				});
		/*
		 * i=0; angular.forEach(columnData.datatype, function(attr) {
		 * $scope.editdata.dataAttribute[i]['dataType = attr;
		 * //$scope.datasetArr[i] = attr.name; i++; });
		 */
		// console.log($scope.editdata.dataAttribute)
		// $scope.data.dataAttribute = dd;
		$scope.data.jsonblob = angular.toJson($scope.editdata);

		$scope.method = 'POST';
		$scope.data.createdBy = localStorage.getItem('itc.username');
		$scope.data.updatedBy = localStorage.getItem('itc.username');
		$scope.url = 'rest/service/addEntity/';
		$http({
			method : $scope.method,
			url : $scope.url,
			data : $scope.data,
			headers : {
				'X-Auth-Token' : localStorage.getItem('itc.authToken')
			}
		}).success(
				function(data, status) {
					$scope.status = status;
					schemaSourceDetails(myService, $scope, $http,
							$templateCache, $rootScope, $location);
					var fromAutoSchema = true;
					$rootScope.fromAutoSchema = fromAutoSchema;
					$location.path("/DataSchema/");

				}).error(function(data, status) {
					if (status == 401) {
						$location.path('/');
					}
			$scope.data = data || "Request failed";
			$scope.status = status;

		});

	}

	var z = {};
	$scope.tdClicks = function() {
		// alert(tdID);
		var x = "", y = ""
		$("td span").click(function() {
			z = $(this);
			x = $(this).text().trim();
			var len = $(this).val();
			if (!x) {
				x = "";
			}
			// alert(x);
			$(this).html("<input type='text' size='30' value='" + x + "' />");
			$(this).find("input[type='text']").focus();
			$(this).find("input[type='text']").caretToEnd();
			$("td span").unbind("click");
			$(this).find("input[type='text']").bind("blur", function() {
				// alert($('#text'+tdID).val());
				$scope.catchme($(this).val());
				$scope.tdClicks();
			});
		});
	}

	$scope.catchme = function(wht) {
		// alert(wht);
		$(z).text(wht);
	}
	$scope.canceloparation = function() {
		$location.path("/DataSchema/");
	}
}

var ingesstionsummary = function(myService, $scope, $http, $templateCache,
		$location, $rootScope, $route, $upload, $filter) {
	$scope.desiredLocation = myService.get();
	// console.log($scope.desiredLocation.xmlEndTag)
	$scope.editData = new Object();
	// $scope.editData.sourcePath = $scope.desiredLocation.fileName;
	// $scope.editData.setPath = $scope.desiredLocation.setPath;
	$scope.sourcenamenotok = false;
	$scope.hdfsPathAcess = false;
	$scope.dataError = '';
	// $scope.sourcenameOk = false;
	// console.log($scope.sourcenamenotok);
	$scope.editData.dataSchemaType = $scope.desiredLocation.dataSchemaType;
	$scope.frequencyArr = [ "One Time", "Hourly", "Daily", "Weekly" ];
	typeFormatFetch($scope, $http, $templateCache, $rootScope, $location,
			'Format', 'DataSource')
	// console.log($scope.desiredLocation.fileName);
	$scope.chksourcePath = function(sourcePath) {
		// alert(sourcePath);

		if (sourcePath != undefined) {
			$scope.method = 'POST'
			$scope.sourcePath = new Object();
			$scope.url = 'rest/service/sourceLocationCheck/';
			$scope.sourcePath.name = sourcePath;
			$http({
				method : $scope.method,
				url : $scope.url,
				data : $scope.sourcePath,
				// cache : $templateCache
				headers : {
					'X-Auth-Token' : localStorage.getItem('itc.authToken')
				}
			}).success(function(data, status) {
				$scope.status = status;
				if (data == 'true') {
					$scope.sourcenamenotok = true;
				} else if (data == 'false') {
					$scope.sourcenamenotok = false;
				}
				// datapreview = data
			}).error(function(data, status) {
				if (status == 401) {
					$location.path('/');
				}
			});

		}
	}
	$scope.chkTargetHDFSPathAcess = function(hdfsPath, editData) {
		// alert(hdfsPath);
		$scope.dataError = '';
		if (hdfsPath != undefined) {
			$scope.method = 'POST'
			$scope.hdfsPath = new Object();
			$scope.url = 'rest/service/verifyTargetHDFSPathAcess/';
			$scope.hdfsPath.location = hdfsPath;
			$http({
				method : $scope.method,
				url : $scope.url,
				data : $scope.hdfsPath,
				// cache : $templateCache
				headers : {
					'X-Auth-Token' : localStorage.getItem('itc.authToken')
				}
			}).success(function(data, status) {
				$scope.status = status;
				// console.log(data);
				$scope.hdfsPathAcess = false;
				$scope.savefullIngenstion(editData);
				// datapreview = data
			}).error(function(data, status) {
				if (status == 401) {
					$location.path('/');
				}
				$scope.hdfsPathAcess = true;
				$scope.dataError = data;
				// $scope.savefullIngenstion(editData);
				// console.log($scope.dataError);

			});

		}
	}
	if ($scope.desiredLocation.dataSchemaType == 'Automatic') {
		if ($scope.desiredLocation.fileName != undefined) {
			var re = /(?:\.([^.]+))?$/;
			var format = re.exec($scope.desiredLocation.fileName)[1];
			format = format.toUpperCase();
			$scope.editData.format = format;

			var urlstr = $scope.desiredLocation.fileName;
			var r = /[^\/]*$/;
			urlstr = urlstr.replace(r, ''); // '/this/is/a/folder/'
			// console.log(urlstr);
			$scope.editData.sourcePath = urlstr;

		} else {
			// console.log('ENTER');
			$scope.editData.fileData = new Object();
			$scope.editData.fileData = angular
					.fromJson($scope.desiredLocation.fileData);
			// console.log($scope.editData.fileData[0].dbType)
			var format = $scope.editData.fileData[0].dbType;
			format = format.toUpperCase();
			$scope.editData.format = format;
			if ($scope.editData.format == 'MYSQL') {
				$scope.editData.format = 'MySQL'
			}
			// console.log($scope.editData.format)
			$scope.editData.sourcePath = 'RDBMS';
		}
	}
	$scope.editData.frequency = $scope.frequencyArr[0];

	$scope.resetField = function() {
		if ($scope.editData.frequency == 'One Time') {
			$scope.editData.startBatch = '';
			$scope.editData.endBatch = '';
			// console.log($scope.editData.startBatch)
		}

	}
	$scope.cancelIngestion = function() {
		$location.path('/DataSchema/');
	}
	$scope.savefullIngenstion = function(editData) {
		// alert('hiii');
		// $scope.chkTargetHDFSPathAcess(editData.setPath);
		$scope.dataschema = new Object();
		$scope.editdataschema = new Object();
		$scope.editdataschema.dataAttribute = new Array();
		$scope.editdataschema.dataAttribute.Name = new Object();
		$scope.editdataschema.dataSchemaType = $scope.desiredLocation.dataSchemaType;
		// console.log($scope.editdataschema.dataSchemaType);

		console.log($scope.dataError);

		$scope.editdataschema.description = $scope.desiredLocation.description;
		if ($scope.desiredLocation.dataSchemaType == 'Manual') {
			$scope.editdataschema.jsonblob = angular
					.fromJson($scope.desiredLocation.jsonblob);
			$scope.editdataschema.jsonblob.dataSchemaType = $scope.desiredLocation.dataSchemaType;
			$scope.editdataschema.jsonblob.name = $scope.desiredLocation.name;
			$scope.desiredLocation.newSchemaName = $scope.desiredLocation.name;
			$scope.dataschema.jsonblob = angular
					.toJson($scope.editdataschema.jsonblob);
		} else if ($scope.desiredLocation.dataSchemaType == 'Automatic') {
			// console.log($scope.desiredLocation.fileData);
			$scope.editdataschema.dataAttribute.dataType = new Object()
			$scope.editdataschema.dataAttribute = $scope.desiredLocation.dataAttribute;
			$scope.editdataschema.name = $scope.desiredLocation.newSchemaName;
			$scope.editdataschema.dataSourcerId = 'NewSchema';
			if ($scope.desiredLocation.xmlEndTag != undefined)
				$scope.editdataschema.xmlEndTag = $scope.desiredLocation.xmlEndTag;
			$scope.dataschema.jsonblob = angular.toJson($scope.editdataschema);
		}
		//if ($scope.dataError == '') {
			$scope.dataschema.name = $scope.desiredLocation.newSchemaName;
			$scope.dataschema.type = 'DataSchema';
			$scope.method = 'POST';
			$scope.dataschema.createdBy = localStorage.getItem('itc.username');
			$scope.dataschema.updatedBy = localStorage.getItem('itc.username');
			$scope.url = 'rest/service/addEntity/';
			$http({
				method : $scope.method,
				url : $scope.url,
				data : $scope.dataschema,
				headers : {
					'X-Auth-Token' : localStorage.getItem('itc.authToken')
				}
			})
					.success(
							function(data, status) {
								$scope.status = status;
								/*
								 * schemaSourceDetails($scope, $http,
								 * $templateCache, $rootScope, $location);
								 */
								$scope.dataschema.id = data.id;
								if ($scope.desiredLocation.dataSchemaType == 'Automatic') {
									$scope.editdataschema.fileData = $scope.desiredLocation.fileData;
									// jData = angular.fromJson(jdata.fileData);

									if ($rootScope.fileSys == 'file') {
										jData1 = angular
												.fromJson($scope.editdataschema.fileData);
										$scope.filetempPath = jData1[0].fileName;
									} else {
										jData1 = angular
												.fromJson($scope.editdataschema.fileData);
										$scope.filetempPath = jData1[0].tableName
												+ '.database';
									}
									// $scope.dataschema.filetempPath = new
									// Object();
									// $scope.dataschema.filetempPath =
									// $scope.filetempPath;
									// console.log(jData1[0].fileName)
									var fromAutoSchema = true;
								} else {
									var fromAutoSchema = false;
								}

								$rootScope.fromAutoSchema = fromAutoSchema;
								$scope.dataSource = {};
								$scope.editDataSource = {};
								$scope.editDataSet = {};
								$scope.editDataSchedular = {};
								/*
								 * $scope.editDataSource.fileData = new Array();
								 * if($scope.desiredLocation.dataSchemaType ==
								 * 'Automatic'){ var re = /(?:\.([^.]+))?$/; var
								 * format = re.exec($scope.filetempPath)[1];
								 * format = format.toUpperCase();
								 * $scope.dataSource.format = format;
								 * $scope.editDataSource.format = format; }
								 */
								$scope.editDataSource.format = editData.format;
								$scope.dataSource.name = $scope.desiredLocation.newSchemaName
										+ '_Source';
								$scope.dataSource.type = 'DataSource';
								$scope.editDataSource.schema = $scope.desiredLocation.newSchemaName;
								$scope.editDataSource.fileData = $scope.editdataschema.fileData
								$scope.editDataSource.location = editData.sourcePath;
								$scope.editDataSource.dataSource = $scope.desiredLocation.newSchemaName
										+ '_Source';
								$scope.editDataSource.name = $scope.desiredLocation.newSchemaName
										+ '_Source';
								;
								$scope.editDataSource.dataSourcerId = $scope.desiredLocation.newSchemaName
										+ '_Source';
								if ($rootScope.fileSys == 'file') {
									$scope.editDataSource.sourcerType = 'File';
								} else {
									$scope.editDataSource.sourcerType = 'RDBMS';
								}
								$scope.dataSource.jsonblob = angular
										.toJson($scope.editDataSource);
								$scope.method = 'POST';
								$scope.dataSource.createdBy = localStorage
										.getItem('itc.username');
								$scope.dataSource.updatedBy = localStorage
										.getItem('itc.username');
								$scope.url = 'rest/service/addEntity/';

								$http(
										{
											method : $scope.method,
											url : $scope.url,
											data : $scope.dataSource,
											headers : {
												'X-Auth-Token' : localStorage
														.getItem('itc.authToken')
											}
										}).success(function(data, status) {
									$scope.status = status;
									/*
									 * schemaSourceDetails($scope, $http,
									 * $templateCache, $rootScope, $location);
									 */
									$scope.editing = false;
									$scope.viewing = true;

								}).error(function(data, status) {
									if (status == 401) {
										$location.path('/');
									}
									$scope.data = data || "Request failed";
									$scope.status = status;

								});
								$scope.dataSet = {};
								$scope.dataSet.name = $scope.desiredLocation.newSchemaName
										+ '_DataSet';
								$scope.dataSet.type = 'DataSet';
								$scope.editDataSet.dataIngestionId = $scope.desiredLocation.newSchemaName
										+ '_DataSet';
								$scope.editDataSet.name = $scope.desiredLocation.newSchemaName
										+ '_DataSet';
								$scope.editDataSet.Schema = $scope.desiredLocation.newSchemaName;
								$scope.editDataSet.location = editData.setPath;
								$scope.dataSet.jsonblob = angular
										.toJson($scope.editDataSet);
								$scope.method = 'POST';

								$scope.dataSet.createdBy = localStorage
										.getItem('itc.username');
								$scope.dataSet.updatedBy = localStorage
										.getItem('itc.username');
								$scope.dataSet.createdDate = new Date();
								$scope.url = 'rest/service/addEntity/';

								$http(
										{
											method : $scope.method,
											url : $scope.url,
											data : $scope.dataSet,
											headers : {
												'X-Auth-Token' : localStorage
														.getItem('itc.authToken')
											}
										})
										.success(
												function(data, status) {
													$scope.status = status;
													/*
													 * schemaSourceDetails($scope,
													 * $http, $templateCache,
													 * $rootScope, $location);
													 */
													$scope.dataScheduler = {};
													$scope.dataScheduler.name = $scope.desiredLocation.newSchemaName
															+ '_Schedular';
													$scope.dataScheduler.type = 'DataIngestion';
													$scope.editDataSchedular.dataIngestionId = $scope.desiredLocation.newSchemaName
															+ '_Schedular';
													$scope.editDataSchedular.name = $scope.desiredLocation.newSchemaName
															+ '_Schedular';
													$scope.editDataSchedular.dataSource = $scope.desiredLocation.newSchemaName
															+ '_Source';
													$scope.editDataSchedular.destinationDataset = $scope.desiredLocation.newSchemaName
															+ '_DataSet';
													$scope.editDataSchedular.frequency = editData.frequency;
													$scope.editDataSchedular.startBatch = editData.startBatch;
													$scope.editDataSchedular.endBatch = editData.endBatch;
													$scope.datasetPath = editData.setPath;
													$scope.dataScheduler.jsonblob = angular
															.toJson($scope.editDataSchedular);
													$scope.method = 'POST';

													$scope.dataScheduler.createdBy = localStorage
															.getItem('itc.username');
													$scope.dataScheduler.updatedBy = localStorage
															.getItem('itc.username');
													$scope.dataScheduler.createdDate = new Date();
													$scope.url = 'rest/service/addEntity/';

													$http(
															{
																method : $scope.method,
																url : $scope.url,
																data : $scope.dataScheduler,
																headers : {
																	'X-Auth-Token' : localStorage
																			.getItem('itc.authToken')
																}
															})
															.success(
																	function(
																			data,
																			status) {
																		$scope.status = status;
																		/*
																		 * schemaSourceDetails($scope,
																		 * $http,
																		 * $templateCache,
																		 * $rootScope,
																		 * $location);
																		 * $location.path("/DataIngestion/");
																		 */
																		/*
																		 * var
																		 * testRunHtml =
																		 * 'Sucessfully
																		 * created
																		 * schema:
																		 * '+$scope.desiredLocation.newSchemaName+ '
																		 * <br/>Source:
																		 * '+$scope.desiredLocation.newSchemaName+'_Source'+'
																		 * <br/>Dataset:'
																		 * +$scope.desiredLocation.newSchemaName+'_DataSet'+'
																		 * <br/>Scheduler:
																		 * '+$scope.desiredLocation.newSchemaName+'_Schedular'+'<br/>';
																		 * $('#finalmodalBODY').html(testRunHtml);
																		 * $('#finalModel').modal('show');
																		 */
																		myService
																				.set($scope);
																		$location
																				.path("/ValidationRule/");

																	})
															.error(
																	function(
																			data,
																			status) {
																		$scope.data = data
																				|| "Request failed";
																		$scope.status = status;

																	});

												})
										.error(
												function(data, status) {
													$scope.data = data
															|| "Request failed";
													$scope.status = status;

												});

							}).error(function(data, status) {
								if (status == 401) {
									$location.path('/');
								}
						$scope.data = data || "Request failed";
						$scope.status = status;

					});
		//}
	}
	$scope.runautoSchema = function() {

		$('#finalModel').modal('hide');
		$scope.method = 'POST'
		$scope.filedata = new Object();
		$scope.url = 'rest/service/testRunIngestion';
		if ($rootScope.fromAutoSchema == true) {
			$scope.filedata.fileName = $scope.filetempPath;
		}
		$scope.filedata.targetPath = $scope.datasetPath;
		$http({
			method : $scope.method,
			url : $scope.url,
			data : $scope.filedata,
			// cache : $templateCache
			headers : {
				'X-Auth-Token' : localStorage.getItem('itc.authToken')
			}
		}).success(function(data, status) {
			$scope.status = status;

			// datapreview = data
		}).error(function(data, status) {
			if (status == 401) {
				$location.path('/');
			}
			$scope.data = data || "Request failed";
			$scope.status = status;
			$('#runFN').html(data.fileName);
			$('#runBT').html(data.fileSize);
			$('#runTT').html(data.timeTaken);
			$('#confirmRun').modal('show');
		});

	}

	$scope.canceloparation = function() {
		$location.path("/DataSchema/");
	}
}
var validaionTable = function(myService, $scope, $http, $templateCache,
		$location, $rootScope, $route, $upload, $filter, $anchorScroll) {

	$scope.validRuleName = [ 'Regex', 'Range', 'White List', 'Black List',
			'Fixed Length', 'Confidential', 'Mandatory' ];
	$scope.validRuleinfo = [
			'It must be a valid regular expression pattern with proper syntax. e.g. [a-b]* or abc.*',
			'Range validation is applicable for  string, date and numeric types of data. It must be a valid numeric range separated by comma(,). e.g. 4,6 or <5 or >5 or <=5 or >=5 or 2012/01/12,2014/13/15',
			'WhiteList validator can be applicable for any types of data. In this validation, the list of the validator matching with data present in the actual DataSet will be included .List of validator value must be separated by comma(,). e.g. 4,5,7 or john,smith,lobo',
			'BlackList validator can be applicable for any type of data.In this validation, the list of the validator matching with data present in the actual DataSet will be excluded.List of validator value must be separated by comma(,). e.g. 4,5,7 or john,smith,lobo',
			'FixedLength validation is applicable for both string and numeric types of data except Double. Input should be a valid numeric value. e.g. 4',
			'Choose the appropriate action to make your confidential data safe',
			'By choosing "YES" you can put a restriction that data must be present in the particular field.(i.e. field should not contain any NULL value, N/A value or empty)' ];
	$scope.pIIRule = [ 'Obfuscate', 'Remove', 'No' ];
	$scope.mandatoryArr = [ 'Yes', 'No' ];
	$scope.piidef = 'Obfuscate';
	$scope1 = myService.get();
	$scope.dataschemaTableblob = new Object();
	$scope.editSchema = $scope1.editSchema;
	$scope.correctArr = new Array();
	$scope.errorArr = new Array();
	$scope.valArray = new Array();
	$scope.title = new Array();
	$scope.dataschemaTableblob = angular.fromJson($scope1.dataschema.jsonblob);
	// console.log($scope.dataschemaTableblob);//piirule
	$scope.dataschemaTable = angular
			.fromJson($scope.dataschemaTableblob.dataAttribute);
	$scope.dataschemaTable1 = $scope.dataschemaTable;
	// console.log($scope.dataschemaTable)
	$scope.canceloparation = function(modalID) {
		$("#" + modalID).on('hidden.bs.modal', function() {
			$location.path("/DataSchema/");
			$scope.$apply();
		});
		/*
		 * $('#finalModel').modal('hide'); $location.path("/DataSchema/");
		 */
	}

	$scope.ruleComb = new Array();

	for ( var i = 0; i < $scope.dataschemaTable.length; i++) {
		var column = "'" + $scope.dataschemaTable[i]['Name'] + "'";
		$scope.valArray[column] = new Array();
		$scope.ruleComb[i] = new Array();
		$scope.errorArr[i] = new Array();
		$scope.correctArr[i] = new Array();
		for ( var j = 0; j < $scope.validRuleName.length; j++) {
			if ($scope.dataschemaTable[i][$scope.validRuleName[j]] == undefined) {
				if ($scope.validRuleName[j] == 'Confidential') {
					if ($scope.dataschemaTable[i]['piirule'] == 'Y') {
						$scope.ruleComb[i][j] = $scope.piidef;
					} else {
						$scope.ruleComb[i][j] = "No";
					}

				} else if ($scope.validRuleName[j] == 'Mandatory') {
					$scope.ruleComb[i][j] = "No";
				} else {
					$scope.ruleComb[i][j] = "N/A";
				}
			} else {
				$scope.ruleComb[i][j] = $scope.dataschemaTable[i][$scope.validRuleName[j]];
			}
			var rule = "'" + $scope.validRuleName[j] + "'"
			$scope.valArray[column][rule] = i + ',' + j;
			$scope.errorArr[i][j] = false;
			$scope.correctArr[i][j] = false;
		}
	}

	$scope.editdata = {};
	$scope.dataschema = new Object();
	$scope.dataschema = $scope1.dataschema;
	$scope.editdata.name = $scope.dataschema.name;
	$scope.editdata.type = 'DataSchema';
	$scope.saveValRule = function(totalDD) {
		// console.log(totalDD);
		var i = 0;
		$scope.editdata.dataAttribute = new Array();
		for ( var i = 0; i < $scope.dataschemaTable.length; i++) {
			$scope.columnData = new Object();
			$scope.columnData.Name = new Object();
			$scope.columnData.dataType = new Object();
			$scope.columnData.Name = $scope.dataschemaTable[i].Name;
			$scope.columnData.dataType = $scope.dataschemaTable[i].dataType;
			$scope.columnData.description = $scope.dataschemaTable[i].description;
			for ( var j = 0; j < $scope.validRuleName.length; j++) {
				var newRule = $scope.validRuleName[j];
				var tdVal = $scope.ruleComb[i][j];
				if ($scope.validRuleName[j] == 'Confidential'
						|| $scope.validRuleName[j] == 'Mandatory') {
					if (tdVal == 'No') {
						tdVal = 'N/A';
					}
				}
				var tdValcase = tdVal.toUpperCase();
				if (tdValcase != 'N/A' && tdValcase != '') {
					$scope.columnData[newRule] = tdVal;
				}
				$scope.errorArr[i][j] = false
				$scope.correctArr[i][j] = true;

			}
			$scope.editdata.dataAttribute.push($scope.columnData);

		}
		$scope.editdata.dataSchemaType = $scope.dataschemaTableblob.dataSchemaType;
		if ($scope.dataschemaTableblob.xmlEndTag)
			$scope.editdata.xmlEndTag = $scope.dataschemaTableblob.xmlEndTag;
		// console.log($scope.editdata.dataSchemaType);
		$scope.editdata.description = $scope.dataschemaTableblob.description;
		$scope.dataschema.jsonblob = angular.toJson($scope.editdata);
		$scope.method = 'POST';
		$scope.dataschema.updatedBy = localStorage.getItem('itc.username');
		$scope.dataschema.updatedDate = new Date().getTime();

		$scope.url = 'rest/service/validatorValidation';
		$http({
			method : $scope.method,
			url : $scope.url,
			data : $scope.dataschema,
			headers : {
				'X-Auth-Token' : localStorage.getItem('itc.authToken')
			}
		})
				.success(
						function(data, status) {
							$scope.status = status;
							$scope.resultsError = new Array();
							$scope.resultsError = data;
							// console.log($scope.resultsError.length)
							if ($scope.resultsError.length > 0) {
								/*
								 * for(var i=0;i<$scope.dataschemaTable.length;i++) {
								 * 
								 * for(var j=0;j<$scope.resultsError.length;j++) {
								 * eachError =
								 * $scope.resultsError[i].split('|'); if(){ } } }
								 */
								var j = 0;
								for (i = 0; i < $scope.resultsError.length; i++) {
									eachError = new Array();
									eachError = $scope.resultsError[i]
											.split('|');
									var rule = "'" + eachError[0] + "'";
									var column = "'" + eachError[1] + "'";
									var title = "'" + eachError[2] + "'";

									if ($scope.valArray[column][rule]) {
										// console.log($scope.valArray[column][rule])
										var indexs = $scope.valArray[column][rule];
										indexsArr = new Array();
										indexsArr = indexs.split(',');
										// alert(indexsArr[0] + indexsArr[1]);
										if (j == 0) {
											$location.hash(indexsArr[0]
													+ indexsArr[1]);
											$anchorScroll();
											j++;
										}

										$scope.errorArr[indexsArr[0]][indexsArr[1]] = true
										$scope.correctArr[indexsArr[0]][indexsArr[1]] = false;
										eachError = $scope.resultsError[i]
												.split('|');
										var rule = "'" + eachError[0] + "'";
										var column = "'" + eachError[1] + "'";
										var title = eachError[2];
										$scope.title[indexsArr[0]] = new Array();
										$scope.title[indexsArr[0]][indexsArr[1]] = '';
										$scope.title[indexsArr[0]][indexsArr[1]] = title;

										// console.log(title);
										// console.log($scope.title[indexsArr[0]][indexsArr[1]]);
									}

									// console.log(eachError);
								}
							} else {
								$scope.editdata.dataSchemaType = $scope.dataschemaTableblob.dataSchemaType;
								if ($scope.dataschemaTableblob.xmlEndTag)
									$scope.editdata.xmlEndTag = $scope.dataschemaTableblob.xmlEndTag;
								// console.log($scope.editdata.dataSchemaType);
								$scope.editdata.description = $scope.dataschemaTableblob.description;
								$scope.dataschema.jsonblob = angular
										.toJson($scope.editdata);
								$scope.method = 'POST';
								$scope.dataschema.updatedBy = localStorage
										.getItem('itc.username');
								$scope.dataschema.updatedDate = new Date();

								$scope.method = 'POST';
								$scope.dataschema.updatedBy = localStorage
										.getItem('itc.username');
								$scope.dataschema.updatedDate = new Date();

								$scope.url = 'rest/service/'
										+ $scope.dataschema.type + '/'
										+ $scope.dataschema.id;
								$http(
										{
											method : $scope.method,
											url : $scope.url,
											data : $scope.dataschema,
											headers : {
												'X-Auth-Token' : localStorage
														.getItem('itc.authToken')
											}
										}).success(function(data, status) {
									$scope.status = status;

								}).error(function(data, status) {
									if (status == 401) {
										$location.path('/');
									}
									$scope.data = data || "Request failed";
									$scope.status = status;

								});
								if ($scope.dataschemaTableblob.dataSchemaType == 'Manual') {
									$location.path('/DataSchema/');
								} else {
									if ($scope1.editSchema == true) {
										$location.path('/DataSchema/');
									} else {
										var testRunHtml = 'Sucessfully created <br/><table><tr><td><b>schema :</b> </td><td>&nbsp;'
												+ $scope.dataschema.name
												+ ' </td></tr><tr><td><b>Source :</b></td><td>&nbsp;'
												+ $scope.dataschema.name
												+ '_Source'
												+ '</td></tr><tr><td><b>Dataset :</b></td><td>&nbsp;'
												+ $scope.dataschema.name
												+ '_DataSet'
												+ '</td></tr><tr><td><b>Scheduler : </b></td><td>&nbsp;'
												+ $scope.dataschema.name
												+ '_Schedular'
												+ '</td></tr></table>';
										$('#finalmodalBODY').html(testRunHtml);
										$('#finalModel').modal('show');
									}
								}
							}

						}).error(function(data, status) {
							if (status == 401) {
								$location.path('/');
							}
					$scope.data = data || "Request failed";
					$scope.status = status;

				});
	}
	$scope.skipValRule = function(totalDD) {
		if ($scope.dataschemaTableblob.dataSchemaType == 'Manual') {
			$location.path('/DataSchema/');
		} else {
			if ($scope1.editSchema == true) {
				$location.path('/DataSchema/');
			} else {
				$scope.dataschema = new Object();
				$scope.dataschema = $scope1.dataschema;
				var testRunHtml = 'Sucessfully created <br/><table><tr><td><b>schema :</b> </td><td>&nbsp;'
						+ $scope.dataschema.name
						+ ' </td></tr><tr><td><b>Source :</b></td><td>&nbsp;'
						+ $scope.dataschema.name
						+ '_Source'
						+ '</td></tr><tr><td><b>Dataset :</b></td><td>&nbsp;'
						+ $scope.dataschema.name
						+ '_DataSet'
						+ '</td></tr><tr><td><b>Scheduler : </b></td><td>&nbsp;'
						+ $scope.dataschema.name
						+ '_Schedular'
						+ '</td></tr></table>';
				$('#finalmodalBODY').html(testRunHtml);
				$('#finalModel').modal('show');
			}
		}
	}
	$scope.runautoSchema = function() {

		$('#TestRunLoader').show();
		$scope.method = 'POST'
		$scope.filedata = new Object();
		$scope.url = 'rest/service/testRunIngestion';
		if ($rootScope.fromAutoSchema == true) {
			$scope.filedata.fileName = $scope1.filetempPath;
		}
		$scope.filedata.targetPath = $scope1.datasetPath;
		$http({
			method : $scope.method,
			url : $scope.url,
			data : $scope.filedata,
			// cache : $templateCache
			headers : {
				'X-Auth-Token' : localStorage.getItem('itc.authToken')
			}
		}).success(function(data, status) {
			$scope.status = status;
			$('#runFN').html(data.fileName);
			$('#runBT').html(data.fileSize);
			$('#runTT').html(data.timeTaken);
			$('#TestRunLoader').hide();
			$('#finalModel').modal('hide');
			$('#confirmRun').modal('show');
			// datapreview = data
		}).error(function(data, status) {
			if (status == 401) {
				$location.path('/');
			}
			$scope.data = data || "Request failed";
			$scope.status = status;

		});

	}

}
userApp.directive('fixedTableHeaders', [ '$timeout', fixedTableHeaders ]);

function fixedTableHeaders($timeout) {
	return {
		restrict : 'A',
		link : link
	};

	function link(scope, element, attrs) {

		$timeout(function() {
			element.stickyTableHeaders();
		}, 0);
	}
}
userApp
		.directive(
				'myDirective',
				function(myService, $rootScope, $http, $templateCache,
						$location) {
					return function(scope, element, attrs) {
						scope.fetchDetails = function(id, userdetails) {
							if (userdetails != undefined) {
								scope.method = 'GET';
								scope.url = 'rest/service/getUser/' + id;

								$http(
										{
											method : scope.method,
											url : scope.url,
											data : scope.data,
											cache : $templateCache,
											headers : {
												'X-Auth-Token' : localStorage
														.getItem('itc.authToken')
											}
										})
										.success(
												function(data, status) {
													scope.status = status;
													scope.data = data;
													if (scope.data.displayName != undefined) {
														var dispName = scope.data.displayName;
														var dispNameArray = dispName
																.split(' ');
														scope.data.firstName = dispNameArray[0];
														scope.data.lastName = dispNameArray[1];

													}
												})
										.error(
												function(data, status) {
													scope.data = data
															|| "Request failed";
													scope.status = status;
												});
							} else {
								scope.method = 'GET';
								scope.url = 'rest/service/' + scope.type + '/'
										+ id;

								$http(
										{
											method : scope.method,
											url : scope.url,
											data : scope.data,
											cache : $templateCache,
											headers : {
												'X-Auth-Token' : localStorage
														.getItem('itc.authToken')
											}
										})
										.success(
												function(data, status) {
													scope.status = status;
													scope.data = data;
													scope.jData = angular
															.fromJson(scope.data.jsonblob);
													scope.data.createdDate = getDateFormat(scope.data.createdDate);
													scope.data.updatedDate = getDateFormat(scope.data.updatedDate);
												})
										.error(
												function(data, status) {
													scope.data = data
															|| "Request failed";
													scope.status = status;
												});
							}

						};

						scope.deleteRecord = function(id, odjDel) {
							scope.method = 'DELETE';
							// console.log(odjDel.userprofile);
							if (odjDel.profile != true
									&& odjDel.userprofile != true) {
								scope.url = 'rest/service/' + id;
								$http(
										{
											method : scope.method,
											url : scope.url,
											data : scope.data,
											cache : $templateCache,
											headers : {
												'X-Auth-Token' : localStorage
														.getItem('itc.authToken')
											}
										}).success(
										function(data, status) {
											scope.status = status;
											scope.data = data;
											schemaSourceDetails(myService,
													scope, $http,
													$templateCache, $rootScope,
													$location);
											schemaSourceDetails(myService,
													scope, $http,
													$templateCache, $rootScope,
													$location, 'DataSet');
											odjDel.isRowHidden = true
											$('#deleteConfirmModal').modal(
													'hide');

										}).error(function(data, status) {
											if (status == 401) {
												$location.path('/');
											}
									scope.data = data || "Request failed";
									scope.status = status;
								});
							} else if (odjDel.userprofile == true) {
								scope.url = 'rest/service/deleteUser/' + id;
								$http(
										{
											method : scope.method,
											url : scope.url,
											data : scope.data,
											cache : $templateCache,
											headers : {
												'X-Auth-Token' : localStorage
														.getItem('itc.authToken')
											}
										}).success(
										function(data, status) {
											scope.status = status;
											scope.data = data;
											schemaSourceDetails(myService,
													scope, $http,
													$templateCache, $rootScope,
													$location);
											odjDel.isRowHidden = true
											$('#deleteConfirmModal').modal(
													'hide');

										}).error(function(data, status) {
											if (status == 401) {
												$location.path('/');
											}
									scope.data = data || "Request failed";
									scope.status = status;
								});
							} else {
								scope.method = 'POST';
								scope.url = 'rest/service/moveToArchive';
								scope.profileDelete = new Object();
								scope.profileDelete.jsonblob = odjDel.datasetID+','+odjDel.datasourceid+','+odjDel.schemaId+','+id;
								scope.profileDelete.location = odjDel.dataSetTargetPath;
								scope.profileDelete.name = odjDel.schemaName;
								scope.profileDelete.createdBy = odjDel.user;
								$http(
										{
											method : scope.method,
											url : scope.url,
											data : scope.profileDelete,
											cache : $templateCache,
											headers : {
												'X-Auth-Token' : localStorage
														.getItem('itc.authToken')
											}
										}).success(function(data, status) {
									scope.status = status;
									scope.data = data;
									odjDel.isRowHidden = true
									$('#deleteConfirmModal').modal('hide');

								}).error(function(data, status) {
									if (status == 401) {
										$location.path('/');
									}
									scope.data = data || "Request failed";
									scope.status = status;
								});
								
							}

						}
					};
				});

var mainController = function($scope, $http, $templateCache, $location,
		$rootScope, $route) {

	$scope.pipeEdit = false;
	$scope.pipelineStart = false;

	var getViewId = function() {
		var test = window.location.pathname;
		var parts = window.location.pathname.split('/');
		if (parts.length <= 3) {
			return true;
		}
		return false;

	};

	$scope.isLoginPage = function() {
		var viewId = getViewId();

		return viewId;

	}
	$scope.logOutPage = function() {
		$rootScope.authToken = '';
		localStorage.clear();
		// console.log(localStorage.getItem('itc.username'));
		// prevMenu = 'one';
		// $location.path("/");

	}
	$scope.userdetails = function() {
		$location.path("/userDetails/");

	}
	$scope.getDDRecord = function() {
		$scope.method = 'GET';
		$scope.url = 'rest/service/listsourcer';

		$http({
			method : $scope.method,
			url : $scope.url,
			headers : {
				'X-Auth-Token' : localStorage.getItem('itc.authToken')
			}
		}).success(function(data, status) {

			$scope.status = status;
			$scope.data = data;
			$scope.detaildata = angular.fromJson($scope.data);

		}).error(function(data, status) {
			if (status == 401) {
				$location.path('/');
			}
			$scope.data = data || "Request failed";
			$scope.status = status;
		});

		$location.path("/DataSchema/");

	}
	$scope.resetUserId = function(userinfo) {
		// alert('hi');
		if (userinfo.newpass == userinfo.curpass) {
			alert('New password and Current can\'t be same!');
			return false;
		} else if (userinfo.newpass != userinfo.conpass) {
			alert('New password and confirm password not matched!');
			return false;
		}
		data = new Object;

		data.password = userinfo.curpass;
		data.newpassword = userinfo.newpass;
		data.retypepassword = userinfo.conpass
		$scope.method = 'POST';
		var userName = localStorage.getItem('itc.username');
		$scope.url = 'rest/service/updatePassword/' + userName;
		$scope.data = data;
		$http({
			method : $scope.method,
			url : $scope.url,
			data : $scope.data,
			headers : {
				'X-Auth-Token' : localStorage.getItem('itc.authToken')
			}
		}).success(function(data, status) {

			$scope.status = status;
			$scope.data = data;
			$("#resetodal").modal('hide')
			$scope.conMail = true;

		}).error(function(data, status) {
			if (status == 401) {
				$location.path('/');
			}
			$("#forgetModal").modal('hide')
			$scope.data = data || "Request failed";
			$scope.status = status;
		});

	}
}
/*
 * var userRole = function($scope) {
 * 
 * $scope.role = [ "admin", "user", "abc" ]; };
 */
var showuserdetails = function($scope, $rootScope, $location, $http) {
	delete $scope.edituserData;
	$scope.method = 'GET';
	$scope.url = 'rest/service/listUsers/';
	$scope.userListDetails = new Object();
	$scope.edituserData = new Object();
	$scope.role = [ "admin", "user", "abc" ];
	$scope.edituserData.role = $scope.role[0];
	$http({
		method : $scope.method,
		url : $scope.url,
		// cache : $templateCache
		headers : {
			'X-Auth-Token' : localStorage.getItem('itc.authToken')
		}
	}).success(function(data, status) {

		$scope.status = status;
		$scope.userListDetails = data;
		var i = 0;
		angular.forEach(data, function(attr) {
			$scope.userListDetails[i].userprofile = true;

			i++;
		});
	}).error(function(data, status) {
		if (status == 401) {
			$location.path('/');
		}
		$scope.data = data || "Request failed";
		$scope.status = status;
	});
}
var archiveCtrl = function($scope, $rootScope, $location, $http) {
	
	$("#userDName").text(localStorage.getItem('itc.dUsername'));
	$('#schemarunID').html('');
	$scope.listArchivedSchema = function(){
		$scope.method = 'GET';
		$scope.url = 'rest/service/ListArchiveProfiles/';
		$scope.edituserData = new Object();
		// console.log(userRole);

		$http({
			method : $scope.method,
			url : $scope.url,
			// cache : $templateCache
			headers : {
				'X-Auth-Token' : localStorage.getItem('itc.authToken')
			}
		}).success(function(data, status) {

			$scope.status = status;
			//data = new Object();
			$scope.editArchiveData = data;
			$scope.archivelen = Object.keys($scope.editArchiveData).length
			//console.log(Object.keys($scope.editArchiveData).length);
		}).error(function(data, status) {
			$scope.restoreData = data;
			$('#schemarunID').html($scope.restoreData);
			if (status == 401) {
				$location.path('/');
			}
			$scope.data = data || "Request failed";
			$scope.status = status;
		});
	}
	$scope.listArchivedSchema();
	$scope.restoreSchema = function(schemaID){
		$scope.method = 'GET';
		$scope.url = 'rest/service/restoreArchivedData/'+schemaID;
		$http({
			method : $scope.method,
			url : $scope.url,
			//data : schemaID,
			// cache : $templateCache
			headers : {
				'X-Auth-Token' : localStorage.getItem('itc.authToken')
			}
		}).success(function(data, status) {
			//console.log(data);
			$scope.listArchivedSchema();
		}).error(function(data, status) {
			$scope.restoreData = data;
			$('#schemarunID').html($scope.restoreData);
			if (status == 401) {
				$location.path('/');
			}
			$scope.data = data || "Request failed";
			$scope.status = status;
		});
	}
}
var userDetailsCtrl = function($scope, $rootScope, $location, $http) {

	$scope.usernamenotok = false
	$scope.usernameOk = false
	showuserdetails($scope, $rootScope, $location, $http);
	$scope.clearuser = function() {
		$scope.usernamenotok = false
		$scope.usernameOk = false
		delete $scope.edituserData;
		$scope.edituserData = new Object();
		$scope.edituserData.role = $scope.role[0];
	}
	$scope.chkuser = function(edituserName) {
		$scope.usernamenotok = false
		$scope.usernameOk = false
		$scope.method = 'POST';
		$scope.url = 'rest/service/CheckUserNameAvailability/' + edituserName;
		$http({
			method : $scope.method,
			url : $scope.url,
			// cache : $templateCache
			headers : {
				'X-Auth-Token' : localStorage.getItem('itc.authToken')
			}
		}).success(function(data, status) {
			if (data == 'true') {
				$scope.usernamenotok = true
				$scope.usernameOk = false
			} else {
				$scope.usernamenotok = false
				$scope.usernameOk = true
			}
		}).error(function(data, status) {
			if (status == 401) {
				$location.path('/');
			}
			$scope.data = data || "Request failed";
			$scope.status = status;
		});
	}

	$scope.getUser = function(userId) {
		$scope.method = 'GET';
		$scope.url = 'rest/service/getUser/' + userId;
		$scope.edituserData = new Object();
		// console.log(userRole);

		$http({
			method : $scope.method,
			url : $scope.url,
			// cache : $templateCache
			headers : {
				'X-Auth-Token' : localStorage.getItem('itc.authToken')
			}
		}).success(function(data, status) {

			$scope.status = status;
			$scope.edituserData = data;
			$scope.edituserData.role = $scope.edituserData.roles.rolesName;
			if ($scope.edituserData.displayName != undefined) {
				var dispName = $scope.edituserData.displayName;
				var dispNameArray = dispName.split(' ');
				$scope.edituserData.firstName = dispNameArray[0];
				$scope.edituserData.lastName = dispNameArray[1];
				delete $scope.edituserData.displayName;
			}
			$('#userAddMethod').modal('show');
		}).error(function(data, status) {
			if (status == 401) {
				$location.path('/');
			}
			$scope.data = data || "Request failed";
			$scope.status = status;
		});
	}
	$scope.saveuserDetails = function(edituserData) {
		// console.log(edituserData);
		$scope.edituserData.roles = new Object();
		$scope.edituserData.displayName = '';
		if (edituserData.firstName != undefined) {
			edituserData.displayName = $scope.edituserData.firstName
		}
		if (edituserData.lastName != undefined) {
			edituserData.displayName = edituserData.displayName + ' '
					+ edituserData.lastName;
		}
		// delete $scope.edituserData.firstName;
		// delete $scope.edituserData.lastName;
		edituserData.roles.rolesName = edituserData.role;
		delete edituserData.role;
		// delete $scope.edituserData.password;
		if (edituserData.userId != undefined) {
			$scope.method = 'POST';
			$scope.url = 'rest/service/updateUser/' + edituserData.userId;
			$http({
				method : $scope.method,
				url : $scope.url,
				data : edituserData,
				// cache : $templateCache
				headers : {
					'X-Auth-Token' : localStorage.getItem('itc.authToken')
				}
			}).success(function(data, status) {
				$scope.status = status;
				showuserdetails($scope, $rootScope, $location, $http);
				$('#userAddMethod').modal('hide');
			}).error(function(data, status) {
				if (status == 401) {
					$location.path('/');
				}
				$scope.data = data || "Request failed";
				$scope.status = status;
			});
		} else {
			$scope.method = 'POST';
			$scope.url = 'rest/service/addUser';

			$http({
				method : $scope.method,
				url : $scope.url,
				data : edituserData,
				// cache : $templateCache
				headers : {
					'X-Auth-Token' : localStorage.getItem('itc.authToken')
				}
			}).success(function(data, status) {
				$scope.status = status;
				showuserdetails($scope, $rootScope, $location, $http);
				$('#userAddMethod').modal('hide');
			}).error(function(data, status) {
				if (status == 401) {
					$location.path('/');
				}
				$scope.data = data || "Request failed";
				$scope.status = status;
			});

		}

	}
	$scope.delete1 = function() {
		currentUser = $scope.Deluser;
		objDel = $scope.DelObj;
		// alert(currentUser)
		$scope.deleteRecord(currentUser, objDel);
	};
	$scope.showconfirm = function(id, obj) {
		$scope.Deluser = id;
		$scope.DelObj = obj;
		$('#deleteConfirmModal').modal('show');
	};

}
var userController = function($scope, $rootScope, $location, $http) {
	$scope.forgtPass = false;
	$scope.loginForm = true;
	$scope.conMail = false;
	$scope.resetPass = false;
	$scope.resPass = false;
	$scope.pipeEdit = false;
	var fromAutoSchema = false;
	$rootScope.fromAutoSchema = fromAutoSchema;

	$scope.submitUserDetails = function() {
		$scope.domains = [ 'hotmail.com', 'gmail.com', 'aol.com' ];
		$scope.topLevelDomains = [ "com", "net", "org" ];
		$scope.data = 'name=' + $scope.username + '&amp;passwd='
				+ $scope.password;

		var data = {
			username : $scope.username,
			password : $scope.password
		}
		var itc = {};
		itc.username = '';

		$scope.method = 'POST';
		$scope.url = 'rest/user/authenticate';
		$http({
			method : $scope.method,
			url : $scope.url,
			params : data
		}).success(function(data, status) {

			$scope.status = status;
			$scope.data = data;
			$scope.detaildata = angular.fromJson($scope.data);
			var authToken = data.token;
			$rootScope.authToken = authToken;
			$scope.username = data.token.split(":")[0];
			$scope.dUsername = data.token.split(":")[1];
			$rootScope.userRole = data.token.split(":")[4];
			// console.log($scope.dUsername);
			localStorage.setItem('itc.authToken', $rootScope.authToken);
			localStorage.setItem('itc.username', $scope.username);
			localStorage.setItem('itc.dUsername', $scope.dUsername);
			localStorage.setItem('itc.userRole', $scope.userRole);

			if ($scope.rememberMe) {
				$cookieStore.put('authToken', authToken);
			}

			$location.path("/DataSchema/");

		}).error(function(data, status) {

			if (status == '401') {
				$scope.wrgPass = true;
			}
			if (status == '405') {
				$scope.wrgPass = true;
			}
			$scope.data = data || "Request failed";
			$scope.status = status;
		});

		/*
		 * $http.post('rest/user/authenticate', data).success( function(data,
		 * status, headers, config) { $scope.user = data; var loginAuth = false;
		 * for (key = 0; key < (data.length); key++) { // if(data[key].name ==
		 * $scope.username && // data[key].password == $scope.password){
		 * 
		 * $location.path("/allSources/"); loginAuth = true; // } if (loginAuth
		 * === false) { $scope.conMail = false; $scope.resPass = false;
		 * $scope.wrgPass = true; } } }).error(function(data, status, headers,
		 * config) { alert("failure message: " + JSON.stringify({ data : data
		 * })); });
		 */

	};
	$scope.forgetPass = function() {

		// $scope.loginForm = false;
		// $scope.resPass = false;
		// $scope.forgtPass = true;
		$("#forgetModal").modal('show')
	}
	$scope.closeBox = function(pname) {

		$scope.loginForm = true;
		// $scope.forgtPass = false;
		$("#loginModal").modal('hide')
		if (pname == 'reset') {
			$("#password_modal").modal('hide')
			$scope.resetpassTab = false;
		}
	}
	$scope.loadLoginConfirm = function(forgetUser) {
		$scope.wrgPass = false;
		$scope.loginForm = true;
		// $scope.forgtPass = false;
		$scope.resPass = false;
		$scope.method = 'POST';
		$scope.url = 'rest/user/forgetPassword/' + forgetUser;
		$http({
			method : $scope.method,
			url : $scope.url,
		}).success(function(data, status) {

			$scope.status = status;
			$scope.data = data;
			$("#forgetModal").modal('hide')
			$scope.conMail = true;

		}).error(function(data, status) {
			if (status == 401) {
				$location.path('/');
			}
			$("#forgetModal").modal('hide')
			$scope.data = data || "Request failed";
			$scope.status = status;
		});

	}

}
// ************ Flow Chart ***********************

userApp.service('modalService', [ '$modal', function($modal) {

	var modalDefaults = {
		backdrop : true,
		keyboard : true,
		modalFade : true,
		templateUrl : '/app/partials/modal.html'
	};

	var modalOptions = {
		closeButtonText : 'Close',
		actionButtonText : 'OK',
		headerText : 'Proceed?',
		bodyText : 'Perform this action?'
	};

	this.showModal = function(customModalDefaults, customModalOptions) {
		if (!customModalDefaults)
			customModalDefaults = {};
		customModalDefaults.backdrop = 'static';
		return this.show(customModalDefaults, customModalOptions);
	};

	this.show = function(customModalDefaults, customModalOptions) {
		// Create temp objects to work with since we're in a singleton service
		var tempModalDefaults = {};
		var tempModalOptions = {};

		// Map angular-ui modal custom defaults to modal defaults defined in
		// service
		angular.extend(tempModalDefaults, modalDefaults, customModalDefaults);

		// Map modal.html $scope custom properties to defaults defined in
		// service
		angular.extend(tempModalOptions, modalOptions, customModalOptions);

		if (!tempModalDefaults.controller) {
			tempModalDefaults.controller = function($scope, $modalInstance) {
				$scope.modalOptions = tempModalOptions;
				$scope.modalOptions.ok = function(result) {
					$modalInstance.close(result);
				};
				$scope.modalOptions.close = function(result) {
					$modalInstance.dismiss('cancel');
				};
			}
		}

		return $modal.open(tempModalDefaults).result;
	};

} ]);

userApp.controller('AppCtrl', [ '$scope', 'modalService',
		function AppCtrl($scope, prompt) {

			// Setup the data-model for the chart.
			//
			/*
			 * var chartDataModel = {
			 * 
			 * nodes: [ { name: "Node 1", id: 0, x: 0, y: 0, inputConnectors: [ {
			 * name: "A", }, ], outputConnectors: [ { name: "A", }, ], }, {
			 * name: "Node 2", id: 1, x: 300, y: 200, inputConnectors: [ { name:
			 * "A", }, ], outputConnectors: [ { name: "A", }, ], }, ],
			 * 
			 * connections: [ { source: { nodeID: 0, connectorIndex: 0, },
			 * 
			 * dest: { nodeID: 1, connectorIndex: 0, }, },
			 *  ] };
			 */

			//
			// Event handler for key-down on the flowchart.
			//
		} ]);

if (prevMenu == undefined) {
	var prevMenu = 'one';
}
var prevClass = '';
function changeMClass(menuID) {
	// alert(prevMenu);
	// alert(menuID)
	// $('#'+menuID).addClass($('#'+menuID).attr('class')+"selected");
	if (prevMenu != undefined) {
		if ($('#' + prevMenu)) {
			prevClass = $('#' + prevMenu).attr('class')
			// alert(prevClass);
			if (prevClass) {
				var newprevClass = prevClass.replace('selected', '');
				// alert(newprevClass);
				$('#' + prevMenu).removeClass(prevClass).addClass(newprevClass);
			}

		}

	}

	var newMenuClass = $('#' + menuID).attr('class');
	if (newMenuClass != undefined) {
		var newMenuClass1 = newMenuClass.replace('selected', '');
		// alert(newMenuClass1);
		$('#' + menuID).removeClass(newMenuClass).addClass(
				newMenuClass1 + "selected");
	}

	prevMenu = menuID;
}
