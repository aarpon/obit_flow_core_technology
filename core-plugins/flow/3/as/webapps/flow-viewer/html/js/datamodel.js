/**
 * DataModel class
 *
 * @author Aaron Ponti
 */

define(["openbis",
        "as/dto/sample/search/SampleSearchCriteria",
        "as/dto/sample/fetchoptions/SampleFetchOptions",
        "as/dto/dataset/search/DataSetSearchCriteria",
        "dss/dto/datasetfile/search/DataSetFileSearchCriteria",
        "dss/dto/datasetfile/fetchoptions/DataSetFileFetchOptions",
        "as/dto/service/search/AggregationServiceSearchCriteria",
        "as/dto/service/fetchoptions/AggregationServiceFetchOptions",
        "as/dto/service/execute/AggregationServiceExecutionOptions",
        "js/dataviewer",
        "js/util"
    ],
    function (openbis,
              SampleSearchCriteria,
              SampleFetchOptions,
              DataSetSearchCriteria,
              DataSetFileSearchCriteria,
              DataSetFileFetchOptions,
              AggregationServiceSearchCriteria,
              AggregationServiceFetchOptions,
              AggregationServiceExecutionOptions,
              DataViewer,
              naturalSort) {

        "use strict";

        /**
         * DataModel class
         **/
        var DataModel = function () {

            if (!(this instanceof DataModel)) {
                throw new TypeError("DataModel constructor cannot be called as a function.");
            }

            /**
             * Server-side services
             */

            this.retrieveFCSEventsService = null;

            /**
             * Properties
             */

            // Store a reference to the DataViewer as a global
            // variable for easier reach from callbacks.
            if (! window.DATAVIEWER) {
                window.DATAVIEWER = new DataViewer();
            }
            this.dataViewer = window.DATAVIEWER;

            // A map to keep track of which experiments
            // support plate acquisitions.
            this.SUPPORTS_PLATE_ACQ = {
                "LSR_FORTESSA_EXPERIMENT": true,
                "FACS_ARIA_EXPERIMENT": false,
                "INFLUX_EXPERIMENT": false,
                "S3E_EXPERIMENT": false,
                "MOFLO_XDP_EXPERIMENT": false
            };

            // Instantiate openBIS V3 API
            this.openbisV3 = new openbis();

            // Current experiment version
            this.EXPERIMENT_LATEST_VERSION = 2;

            // Use the context to log in
            this.openbisV3.loginFromContext();

            // Retrieve information from the context
            var webappcontext = this.openbisV3.getWebAppContext();

            // {...}_EXPERIMENT sample identifier
            this.experimentSampleId = webappcontext.getEntityIdentifier();

            // {...}_EXPERIMENT sample type
            this.experimentSampleType = webappcontext.getEntityType();

            this.EXPERIMENT_PREFIX = this.experimentSampleType.substring(0,
                this.experimentSampleType.indexOf("_EXPERIMENT"));

            // {...}_EXPERIMENT sample (entity returned from the context)
            this.experimentSample = null;

            // Experiment name
            this.expName = "";

            // Tree mode;
            this.treeModel = null;

            // Retrieve the {...}_EXPERIMENT data with all relevant information
            this.getFlowExperimentSampleAndDisplay();

        };

        /**
         * Methods
         */

        DataModel.prototype = {

            constructor: DataModel,

            /**
             * Get current {...}_EXPERIMENT sample.
             *
             * All relevant information is retrieved along with the sample.
             */
            getFlowExperimentSampleAndDisplay: function () {

                // Search for the sample of type and given perm id
                var criteria = new SampleSearchCriteria();
                criteria.withType().withCode().thatEquals(this.experimentSampleType);
                criteria.withCode().thatEquals(this.experimentSampleId);
                var fetchOptions = new SampleFetchOptions();
                fetchOptions.withType();
                fetchOptions.withProperties();
                fetchOptions.withDataSets().withType();

                var parentFetchOptions = new SampleFetchOptions();
                parentFetchOptions.withType();
                parentFetchOptions.withProperties();
                fetchOptions.withParentsUsing(parentFetchOptions);

                // Keep a reference to this object (for the callback)
                var dataModelObj = this;

                // Query the server
                this.openbisV3.searchSamples(criteria, fetchOptions).done(function (result) {

                    // Store the {...}_EXPERIMENT sample object
                    dataModelObj.experimentSample = result.getObjects()[0];

                    // Store the {...}_EXPERIMENT_NAME property
                    dataModelObj.expName = dataModelObj.experimentSample.properties["$NAME"];

                    // Initialize the tree
                    dataModelObj.initializeTreeModel();

                    // Draw it
                    dataModelObj.dataViewer.drawTree(dataModelObj.treeModel);

                    // Display the experiment summary
                    dataModelObj.dataViewer.displayExperimentInfo(dataModelObj.experimentSample);
                });
            },

            /**
             * Build the initial tree for the DynaTree library
             *
             * Further tree manipulation is performed via the DynaTree library
             */
            initializeTreeModel: function () {

                // Alias
                var dataModelObj = this;

                // First-level children
                var first_level_children = [];
                if (dataModelObj.SUPPORTS_PLATE_ACQ[dataModelObj.experimentSampleType]) {

                    first_level_children = [
                        {
                            title: "Plates",
                            element: null,
                            icon: "folder.png",
                            isFolder: true,
                            isLazy: true,
                            expCode: dataModelObj.experimentSample.code,
                            type: 'plate_container'
                        },
                        {
                            title: "Tubesets",
                            element: null,
                            icon: "folder.png",
                            isFolder: true,
                            isLazy: true,
                            expCode: dataModelObj.experimentSample.code,
                            type: 'tubesets'
                        }];

                } else {

                    first_level_children = [
                        {
                            title: "Tubesets",
                            element: null,
                            icon: "folder.png",
                            isFolder: true,
                            isLazy: true,
                            expCode: dataModelObj.experimentSample.code,
                            type: 'tubesets'
                        }];
                }

                // Build the initial tree (root)
                dataModelObj.treeModel =
                    {
                        persist: false,
                        imagePath: "./third-party/dynatree/skin/",
                        minExpandLevel: 2,
                        selectMode: 1,
                        autoFocus: false,
                        keyboard: true,
                        strings: {
                            loading: "Retrieving...",
                            loadError: "Sorry, could not retrieve data."
                        },
                        debugLevel: 0,
                        onActivate: function (node) {
                            // Display
                            DATAVIEWER.displayDetailsAndActions(node);
                        },
                        onLazyRead: function (node) {
                            // Important: fetch only!
                            dataModelObj.fetch(node);
                        },
                        children: [
                            {
                                title: dataModelObj.expName,
                                expCode: dataModelObj.experimentSample.code,
                                element: dataModelObj.experimentSample,
                                icon: "experiment.png",
                                isFolder: true,
                                isLazy: false,
                                expand: true,
                                children: first_level_children
                            }]
                    };
            },

            /**
             * Fetch data for current node
             *
             * @param   node    A DynaTree node
             */
            fetch: function (node) {

                // Get the data and update the tree
                switch (node.data.type) {

                    // Extract and display the plates
                    case "plate_container":
                        this.getPlates(node);
                        break;

                    // Extract and display the tubes
                    // (and corresponding specimens)
                    case "tubesets":
                        this.getTubes(node);
                        break;

                    // Extract and display the wells in current plate
                    // (and corresponding specimens)
                    case "wells":

                        this.getWells(node);
                        break;

                    // Extract and displays dataset and FCS file
                    // associated to either a well or a tube
                    case "fcs":
                        this.getDataSets(node);
                        break;

                    default:
                        // Unknown node!
                        node.setLazyNodeStatus(DTNodeStatus_Error,
                            {
                                tooltip: "Type was: " + node.data.type,
                                info: "Unknown type for the selected node!"
                            });
                        break;
                }

            },

            /**
             * Get the dataset associated to a given sample
             * @param   sample  A sample
             * @param   node    The DynaTree (parent) node to which the generated
             *                  nodes will be appended.
             * @return  dataset The dataset associated to the sample
             */
            getDataSets: function (node) {

                // Alias
                var dataModelObj = this;

                // Get the sample
                var sample = node.data.element;

                // Get the dataset
                var fetchOptions = new SampleFetchOptions();
                fetchOptions.withProperties();
                fetchOptions.withDataSets().withType();
                fetchOptions.withDataSets().withProperties();
                fetchOptions.withType();

                this.openbisV3.getSamples([sample.permId], fetchOptions).done(function (map) {

                    if (!(sample.permId in map)) {

                        // Sample not found: set node status accordingly
                        node.setLazyNodeStatus(DTNodeStatus_Error,
                            {
                                tooltip: response.error.data.exceptionTypeName,
                                info: "Error retrieving information."
                            });

                    } else {

                        // Get the updated sample
                        var updated_sample = map[sample.permId];

                        // Get the dataset
                        var datasets = updated_sample.getDataSets();
                        var dataset = datasets[0];

                        // Get the file
                        var criteria = new DataSetFileSearchCriteria();
                        var dataSetCriteria = criteria.withDataSet().withOrOperator();
                        dataSetCriteria.withPermId().thatEquals(dataset.permId.permId);

                        var fetchOptions = new DataSetFileFetchOptions();

                        // Query the server
                        dataModelObj.openbisV3.getDataStoreFacade().searchFiles(criteria, fetchOptions).done(function (result) {

                            // Extract the files
                            var datasetFiles = result.getObjects();

                            // Find the only fcs file and add its name and URL to the DynaTree
                            datasetFiles.forEach(function (f) {

                                if (!f.isDirectory() &&
                                    f.getPath().toLowerCase().indexOf(".fcs") !== -1) {

                                    // Append the file name to the dataset object for
                                    // use in addToTreeModel()
                                    var filename = '';
                                    var indx = f.getPath().lastIndexOf("/");
                                    if (indx === -1 || indx === f.getPath().length - 1) {
                                        // This should not happen, but we build in
                                        // a fallback anyway
                                        filename = f.getPath();
                                    } else {
                                        filename = f.getPath().substr(indx + 1);
                                    }
                                    dataset.filename = filename;

                                    // Build the download URL
                                    var url = f.getDataStore().getDownloadUrl() + "datastore_server/" +
                                        f.permId.dataSetId.permId + "/" + f.getPath() + "?sessionID=" +
                                        dataModelObj.openbisV3.getWebAppContext().sessionId;

                                    // Store it in the dataset object
                                    var eUrl = encodeURI(url);
                                    eUrl = eUrl.replace('+', '%2B');
                                    dataset.url = eUrl;

                                    // Add the results to the tree
                                    var result = [];
                                    result.push(dataset);
                                    dataModelObj.addToTreeModel(result, node);
                                }
                            });

                        });
                    }
                });
            },

            /**
             * Get the Plates for current experiment
             * @param node Plate_container node to which the Plates are to be added.
             */
            getPlates: function (node) {

                // Alias
                var dataModelObj = this;

                // Required arguments
                var plateSampleType = dataModelObj.EXPERIMENT_PREFIX + "_PLATE";

                // Search for the sample of type {...}_TUBESET and given perm id
                var criteria = new SampleSearchCriteria();
                criteria.withType().withCode().thatEquals(plateSampleType);
                criteria.withParents().withCode().thatEquals(this.experimentSampleId);
                criteria.withParents().withType().withCode().thatEquals(this.experimentSampleType);

                var fetchOptions = new SampleFetchOptions();
                fetchOptions.withType();
                fetchOptions.withProperties();

                // Search
                this.openbisV3.searchSamples(criteria, fetchOptions).done(function (result) {

                    // Retrieve the plates
                    var plates = result.getObjects();

                    if (plates.length === 0) {
                        dataModelObj.addToTreeModel([], node);
                        return;
                    }

                    // Pass the children of the tubeset to the addToTreeModel() method
                    dataModelObj.addToTreeModel(plates, node);

                });

            },

            /**
             * Get the Tubes for current experiment
             * @param node Tubeset node to which the Tubes are to be added.
             */
            getTubes: function (node) {

                // Alias
                var dataModelObj = this;

                // Required arguments
                var tubesetSampleType = dataModelObj.EXPERIMENT_PREFIX + "_TUBESET";

                // Search for the sample of type {...}_TUBESET and given perm id
                var criteria = new SampleSearchCriteria();
                criteria.withType().withCode().thatEquals(tubesetSampleType);
                criteria.withParents().withCode().thatEquals(this.experimentSampleId);
                criteria.withParents().withType().withCode().thatEquals(this.experimentSampleType);

                var fetchOptions = new SampleFetchOptions();
                fetchOptions.withProperties();
                fetchOptions.withType();
                fetchOptions.withChildren().withType();
                fetchOptions.withChildren().withProperties();

                var parentFetchOptions = new SampleFetchOptions();
                parentFetchOptions.withType();
                parentFetchOptions.withProperties();
                parentFetchOptions.withChildren().withParents().withType();
                parentFetchOptions.withChildren().withParents().withProperties();
                fetchOptions.withParentsUsing(parentFetchOptions);

                // Query the server
                this.openbisV3.searchSamples(criteria, fetchOptions).done(function (result) {

                    // Are there tubesets?
                    if (result.length === 0) {
                        dataModelObj.addToTreeModel(result, node);
                        return;
                    }

                    // Retrieve the sample
                    var tubesetSample = result.objects[0];

                    // Pass the children of the tubeset to the addToTreeModel() method
                    dataModelObj.addToTreeModel(tubesetSample.children, node);
                });
            },

            /**
             * Get the Wells for current plate
             * @param node Platee node to which the Wells are to be added.
             */
            getWells: function (node) {

                // Alias
                var dataModelObj = this;

                // Retrieve the plate (sample) from the node
                var plateSample = node.data.element;

                var fetchOptions = new SampleFetchOptions();
                fetchOptions.withChildren();
                fetchOptions.withChildren().withType();
                fetchOptions.withChildren().withProperties();

                var parentFetchOptions = new SampleFetchOptions();
                parentFetchOptions.withType();
                parentFetchOptions.withProperties();
                parentFetchOptions.withChildren().withParents().withType();
                parentFetchOptions.withChildren().withParents().withProperties();
                fetchOptions.withParentsUsing(parentFetchOptions);

                this.openbisV3.getSamples([plateSample.permId], fetchOptions).done(function (map) {

                    if (!(plateSample.permId in map)) {

                        // Sample not found: set node status accordingly
                        node.setLazyNodeStatus(DTNodeStatus_Error,
                            {
                                tooltip: response.error.data.exceptionTypeName,
                                info: "Error retrieving information."
                            });

                    } else {

                        // Get the sample (plate)
                        var sample = map[plateSample.permId];

                        // Get the wells
                        var wells = sample.children;

                        // Pass the children of the tubeset to the addToTreeModel() method
                        dataModelObj.addToTreeModel(wells, node);
                    }
                });
            },

            /**
             * Converts the results returned by openBIS to an array of nodes to be
             * used with DynaTree
             *
             * @param result  Array of objects to be added to the tree model.
             * @param node    The parent DynaTree node
             */
            addToTreeModel: function (result, node) {

                // Alias
                var dataModelObj = this;

                // Server returned an error condition: set node status accordingly
                if (result == null || result.hasOwnProperty("error")) {
                    node.setLazyNodeStatus(DTNodeStatus_Error,
                        {
                            tooltip: response.error.data.exceptionTypeName,
                            info: "Error retrieving information."
                        });
                    return;
                }

                // No objects found
                if (result.length === 0) {

                    // PWS status OK
                    node.setLazyNodeStatus(DTNodeStatus_Ok);

                    // Customize the node
                    if (node && node.data && node.data.type &&
                        (node.data.type === "plate_container" || node.data.type === "tubesets")) {
                        node.addChild({
                            title: "<i>none</i>",
                            icon: "empty.png",
                            isFolder: false,
                            isLazy: false,
                            unselectable: true
                        });
                    }
                    return;
                }

                // We create the nodes to add to the DynaTree: each node contains
                // the openBIS sample to be used later. In case of wells and tubes,
                // we actually create an array of specimens, each of which has tubes
                // or wells as children.
                var res = [];
                $.each(result, function (index, sample) {

                    // Declare some variables
                    var i = 0;
                    var parent = null;
                    var specimenNode = null;
                    var specimenSample = null;
                    var specimenName = null;
                    var indx = -1;
                    var specimenType = dataModelObj.EXPERIMENT_PREFIX + "_SPECIMEN";

                    // Get element type
                    var elementType = sample.getType().code;

                    switch (elementType) {

                        case (dataModelObj.EXPERIMENT_PREFIX + "_PLATE"):

                            res.push({
                                title: sample.properties[dataModelObj.EXPERIMENT_PREFIX + "_PLATE_NAME"],
                                icon: "plate.png",
                                isFolder: true,
                                isLazy: true,
                                expCode: node.data.expCode,
                                type: "wells",
                                element: sample
                            });
                            break;

                        case (dataModelObj.EXPERIMENT_PREFIX + "_WELL"):

                            // Specimen sample
                            specimenSample = null;

                            i = 0;
                            while (specimenSample === null) {
                                parent = sample.parents[i];

                                if (parent.getType().code === specimenType) {
                                    specimenSample = parent;
                                    break;
                                }

                                i++;
                            }

                            // The tube is a child of a specimen
                            specimenNode = null;

                            // Specimen name
                            specimenName = specimenSample.properties["$NAME"];

                            // Do we already have a Specimen node?
                            indx = dataModelObj.findIn(res, specimenName);

                            // Do we already have a Specimen node?
                            if (indx === -1) {

                                // Create a new Specimen node and add it to the array
                                specimenNode = {
                                    title: specimenName,
                                    icon: "specimen.png",
                                    isFolder: true,
                                    isLazy: false,
                                    expCode: node.data.expCode,
                                    type: "specimen",
                                    element: null,
                                    children: []
                                };

                            } else {

                                // We get the node from the array
                                specimenNode = res[indx];

                            }

                            // Now we create the well node and add it as a child of
                            // the correct specimen node
                            var wellNode = {
                                title: sample.properties[dataModelObj.EXPERIMENT_PREFIX + "_WELL_NAME"],
                                icon: "well.png",
                                isFolder: true,
                                isLazy: true,
                                expCode: node.data.expCode,
                                type: "fcs",
                                element: sample
                            };

                            // And finally we add it to the specimen node and store
                            // the specimen into the res array
                            specimenNode.children.push(wellNode);
                            if (indx === -1) {
                                res.push(specimenNode);
                            } else {
                                res[indx] = specimenNode;
                            }
                            break;

                        case (dataModelObj.EXPERIMENT_PREFIX + "_TUBESET"):

                            // Nothing to do
                            break;


                        case (dataModelObj.EXPERIMENT_PREFIX + "_TUBE"):

                            // Specimen sample
                            specimenSample = null;

                            i = 0;
                            while (specimenSample === null) {
                                parent = sample.parents[i];

                                if (parent.getType().code === specimenType) {
                                    specimenSample = parent;
                                    break;
                                }

                                i++;
                            }

                            // The tube is a child of a specimen
                            specimenNode = null;

                            // Specimen name
                            specimenName = specimenSample.properties["$NAME"];

                            // Do we already have a Specimen node?
                            indx = dataModelObj.findIn(res, specimenName);

                            if (indx === -1) {

                                // Create a new Specimen node and add it to the array
                                specimenNode = {
                                    title: specimenName,
                                    icon: "specimen.png",
                                    isFolder: true,
                                    isLazy: false,
                                    expCode: node.data.expCode,
                                    type: "specimen",
                                    element: null,
                                    children: []
                                };

                            } else {

                                // We get the node from the array
                                specimenNode = res[indx];

                            }

                            // Now we create the tube node and add it as a child of
                            // the correct specimen node
                            var tubeNode = {
                                title: sample.properties[dataModelObj.EXPERIMENT_PREFIX + "_TUBE_NAME"],
                                icon: "tube.png",
                                isFolder: true,
                                isLazy: true,
                                expCode: node.data.expCode,
                                type: "fcs",
                                element: sample
                            };

                            // And finally we add it to the specimen node and store
                            // the specimen into the tubesetNode children
                            specimenNode.children.push(tubeNode);
                            if (indx === -1) {
                                res.push(specimenNode);
                            } else {
                                res[indx] = specimenNode;
                            }
                            break;

                        case (dataModelObj.EXPERIMENT_PREFIX + "_FCSFILE"):

                            // File name
                            res.push({
                                title: sample.filename,
                                icon: "fcs.png",
                                isFolder: false,
                                isLazy: false,
                                expCode: node.data.expCode,
                                type: "leaf",
                                element: sample
                            });
                            break;

                        default:

                            // Unexpected sample type!
                            node.setLazyNodeStatus(DTNodeStatus_Error,
                                {
                                    tooltip: elementType,
                                    info: "Unexpected element type!"
                                });
                            return;

                    }

                });

                // PWS status OK
                node.setLazyNodeStatus(DTNodeStatus_Ok);
                node.addChild(res);

                // Natural-sort children nodes
                node.sortChildren(this.customSort, true);
            },

            /**
             * Check if a DynaTree node already exists in a simple 1-D array
             * (not already in the tree!)
             *
             * @param res   A simple, 1-D array of DynaTree nodes
             * @param title The title of the node to be searched for.
             * @return number of the node in the array or -1 if not found.
             */
            findIn: function (res, title) {

                if (res.length === 0) {
                    return -1;
                }
                for (var i = 0; i < res.length; i++) {
                    if (res[i].title === title) {
                        return i;
                    }
                }
                return -1;
            },

            /**
             * Add parameter information to the FCS nodes
             * @param node An FCS file DynaTree node
             * @param action Function that renders the node.
             */
            getAndAddParameterInfoForDatasets: function (node, action) {

                // Consistency check on the node

                if (node.data.element.getType().code !== (this.EXPERIMENT_PREFIX + "_FCSFILE")) {
                    console.log("The node is not of the expected type!")
                    return;
                }

                // Old experiments might not have anything stored in {prefix}_FCSFILE_PARAMETERS.
                if (!node.data.element.properties[this.EXPERIMENT_PREFIX + "_FCSFILE_PARAMETERS"]) {
                    return;
                }

                // If the node already contain the processed parameter information, we can go straight to
                // the rendering.
                if (!node.data.parameterInfo) {

                    // Retrieve parameter information
                    var parametersXML = $.parseXML(node.data.element.properties[this.EXPERIMENT_PREFIX + "_FCSFILE_PARAMETERS"]);
                    var parameters = parametersXML.childNodes[0];

                    var numParameters = parameters.getAttribute("numParameters");
                    var numEvents = parameters.getAttribute("numEvents");

                    var names = [];
                    var compositeNames = [];
                    var display = [];

                    // Parameter numbering starts at 1
                    var parametersToDisplay = 0;
                    for (var i = 1; i <= numParameters; i++) {

                        // If the parameter contains the PnCHANNELTYPE attribute (BD Influx Cell Sorter),
                        // we only add it if the channel type is 6.
                        var channelType = parameters.getAttribute("P" + i + "CHANNELTYPE");
                        if (channelType != null && channelType !== 6) {
                            continue;
                        }

                        // Store the parameter name
                        var name = parameters.getAttribute("P" + i + "N");
                        names.push(name);

                        // Store the composite name
                        var pStr = parameters.getAttribute("P" + i + "S");
                        var composite = name;
                        if (pStr !== "") {
                            composite = name + " (" + pStr + ")";
                        }
                        compositeNames.push(composite);

                        // Store the display scale
                        var displ = parameters.getAttribute("P" + i + "DISPLAY");
                        display.push(displ);

                        // Update the count of parameters to display
                        parametersToDisplay++;
                    }

                    // Store the parameter info
                    node.data.parameterInfo = {
                        "numParameters": parametersToDisplay,
                        "numEvents": numEvents,
                        "names": names,
                        "compositeNames": compositeNames,
                        "display": display
                    }

                }

                // Now render
                action(node);

            },

            /**
             * Update an outdated experiment.
             * @param expPermId string Experiment perm identifier.
             */
            upgradeExperiment: function (expPermId) {

                // Check that this is an experiment node

                // Parameters for the aggregation service
                var parameters = {
                    expPermId: expPermId
                };

                // Message
                var unexpected = "Sorry, unexpected feedback from server " +
                    "obtained. Please contact your administrator.";

                // Returned parameters
                var r_Success;
                var r_ErrorMessage;

                // Inform the user that we are about to process the request
                DATAVIEWER.displayStatus("Please wait while processing your request. This might take a while...", "info");

                // Must use global object
                DATAMODEL.openbisServer.createReportFromAggregationService(
                    CONFIG.dataStoreServer,
                    "upgrade_experiment",
                    parameters,
                    function (response) {

                        var status;
                        var level;
                        var row;

                        if (response.error) {
                            status = "Sorry, could not process request.";
                            level = "danger";
                            r_Success = "0";
                        } else {
                            status = "";
                            if (response.result.rows.length !== 1) {
                                status = unexpected;
                                level = "danger";
                            } else {
                                row = response.result.rows[0];
                                if (row.length !== 2) {
                                    status = unexpected;
                                    level = "danger";
                                } else {

                                    // Extract returned values for clarity
                                    r_Success = row[0].value;
                                    r_ErrorMessage = row[1].value;

                                    if (r_Success === "1") {
                                        status = r_ErrorMessage;
                                        level = "success";

                                    } else {
                                        status = "Sorry, there was an error: \"" +
                                            r_ErrorMessage + "\".";
                                        level = "danger";
                                    }
                                }
                            }
                        }
                        // We only display errors
                        DATAVIEWER.displayStatus(status, level);

                        // Reload the page after a short delay
                        if (r_Success === "1") {
                            setTimeout(function () {
                                window.location.reload();
                            }, 1000);
                        }

                    });
            },

            /**
             * Process the results returned from the retrieveFCSEvents() server-side plug-in
             * @param response JSON object
             */
            processResultsFromRetrieveFCSEventsServerSidePlugin: function (table) {

                // Did we get the expected result?
                if (! table.rows || table.rows.length !== 1) {
                    DATAVIEWER.displayStatus(unexpected, "danger");
                    return;
                }

                // Get the row of results
                var row = table.rows[0];

                // Retrieve the uid
                var r_UID = row[0].value;

                // Is the process completed?
                var r_Completed = row[1].value;

                if (r_Completed === 0) {

                    // Call the plug-in
                    setTimeout(function () {

                            // We only need the UID of the job
                            var parameters = {};
                            parameters["uid"] = r_UID;

                            // Now call the service
                            var options = new AggregationServiceExecutionOptions();
                            options.withParameter("uid", r_UID);

                            DATAMODEL.openbisV3.executeAggregationService(
                                DATAMODEL.retrieveFCSEventsService.getPermId(),
                                options).then(function(result) {
                                DATAMODEL.processResultsFromRetrieveFCSEventsServerSidePlugin(result);
                            })},
                        parseInt(CONFIG['queryPluginStatusInterval']));

                    // Return here
                    return;

                }

                // We completed the call and we can process the result

                // Returned parameters
                var r_Success = row[2].value;
                var r_ErrorMessage = row[3].value;
                var r_Data = row[4].value;
                var r_Code = row[5].value;
                var r_ParamX = row[6].value;
                var r_ParamY = row[7].value;
                var r_DisplayX = row[8].value;
                var r_DisplayY = row[9].value;
                var r_NumEvents = row[10].value;   // Currently not used
                var r_MaxNumEvents = row[11].value;
                var r_SamplingMethod = row[12].value;
                var r_NodeKey = row[13].value;

                var level;
                if (r_Success === 1) {

                    // Error message and level
                    status = r_ErrorMessage;
                    level = "success";

                    // Plot the data
                    DATAVIEWER.plotFCSData(r_Data, r_ParamX, r_ParamY, r_DisplayX, r_DisplayY);

                    // Cache the plotted data
                    var dataKey = r_Code + "_" + r_ParamX + "_" + r_ParamY + "_" + r_MaxNumEvents.toString() +
                        "_" + r_DisplayX + "_" + r_DisplayY + "_" + r_SamplingMethod.toString();
                    DATAVIEWER.cacheFCSData(r_NodeKey, dataKey, r_Data);

                } else {
                    status = "Sorry, there was an error: \"" + r_ErrorMessage + "\".";
                    level = "danger";
                }

                // We only display errors
                if (r_Success === 0) {
                    DATAVIEWER.displayStatus(status, level);
                } else {
                    DATAVIEWER.hideStatus();
                }

                return table;

            },

            /**
             * Generate a scatter plot for FCS file of given code and parameters.
             * @param node DynaTree node Node from the experiment structure tree.
             * @param code string openBIS code of the FCS file
             * @param paramX string Name of the parameter for the X axis
             * @param paramY string Name of the parameter for the Y axis
             * @param displayX string Display type of the parameter for the X axis ("LIN" or "LOG)
             * @param displayY string Display type of the parameter for the Y axis ("LIN" or "LOG)
             * @param maxNumEvents int Maximum number of events to be retrieved from the server.
             * @param samplingMethod
             */
            generateFCSPlot: function (node, code, paramX, paramY, displayX, displayY, maxNumEvents, samplingMethod) {

                // Check whether the data for the plot is already cached
                if (node.data.cached) {
                    var key = code + "_" + paramX + "_" + paramY + "_" + maxNumEvents.toString() +
                        "_" + displayX + "_" + displayY + "_" + samplingMethod.toString();
                    if (node.data.cached.hasOwnProperty(key)) {

                        // Plot the cached data
                        DATAVIEWER.plotFCSData(
                            node.data.cached[key],
                            paramX,
                            paramY,
                            displayX,
                            displayY);

                        // Return immediately
                        return;
                    }
                }

                // Parameters for the aggregation service
                var parameters = {
                    code: code,
                    paramX: paramX,
                    paramY: paramY,
                    displayX: displayX,
                    displayY: displayY,
                    numEvents: node.data.parameterInfo['numEvents'],
                    maxNumEvents: maxNumEvents,
                    samplingMethod: samplingMethod,
                    nodeKey: node.data.key
                };

                // Inform the user that we are about to process the request
                DATAVIEWER.displayStatus("Please wait while processing your request. This might take a while...", "info");

                // Call service
                if (null === DATAMODEL.retrieveFCSEventsService) {
                    var criteria = new AggregationServiceSearchCriteria();
                    criteria.withName().thatEquals("retrieve_fcs_events");
                    var fetchOptions = new AggregationServiceFetchOptions();
                    DATAMODEL.openbisV3.searchAggregationServices(criteria, fetchOptions).then(function(result) {
                            if (undefined === result.objects) {
                                console.log("Could not retrieve the server-side aggregation service!")
                                return;
                            }
                            DATAMODEL.retrieveFCSEventsService = result.getObjects()[0];

                            // Now call the service
                            var options = new AggregationServiceExecutionOptions();
                            for (var key in parameters) {
                                options.withParameter(key, parameters[key]);
                            }
                            DATAMODEL.openbisV3.executeAggregationService(
                                DATAMODEL.retrieveFCSEventsService.getPermId(),
                                options).then(function(result) {
                                    DATAMODEL.processResultsFromRetrieveFCSEventsServerSidePlugin(result);
                            });
                        });
                } else {
                    // Call the service
                    var options = new AggregationServiceExecutionOptions();
                    for (var key in parameters) {
                        options.withParameter(key, parameters[key]);
                    }
                    DATAMODEL.openbisV3.executeAggregationService(
                        DATAMODEL.retrieveFCSEventsService.getPermId(),
                        options).then(function(result) {
                        DATAMODEL.processResultsFromRetrieveFCSEventsServerSidePlugin(result);
                    });
                }
            },

            /**
             * Process the results returned from the exportDatasets() server-side plug-in
             * @param response JSON object
             */
            processResultsFromExportDatasetsServerSidePlugin: function (response) {

                var status;
                var unexpected = "Sorry, unexpected feedback from server " +
                    "obtained. Please contact your administrator.";
                var level = "";
                var row;

                // Returned parameters
                var r_UID;
                var r_Completed;
                var r_Success;
                var r_ErrorMessage;
                var r_NCopiedFiles;
                var r_RelativeExpFolder;
                var r_ZipArchiveFileName;
                var r_Mode;

                // First check if we have an error
                if (response.error) {

                    status = "Sorry, could not process request.";
                    level = "danger";
                    r_Success = "0";

                } else {

                    // No obvious errors. Retrieve the results.
                    status = "";
                    if (response.result.rows.length !== 1) {

                        // Unexpected number of rows returned
                        status = unexpected;
                        level = "danger";

                    } else {

                        // We have a potentially valid result
                        row = response.result.rows[0];

                        // Retrieve the uid
                        r_UID = row[0].value;

                        // Retrieve the 'completed' status
                        r_Completed = row[1].value;

                        // If the processing is not completed, we wait a few seconds and trigger the
                        // server-side plug-in again. The interval is defined by the admin.
                        if (r_Completed === "0") {

                            // We only need the UID of the job
                            var parameters = {};
                            parameters["uid"] = r_UID;

                            // Call the plug-in
                            setTimeout(function () {
                                    DATAMODEL.openbisServer.createReportFromAggregationService(
                                        CONFIG['dataStoreServer'], "export_flow_datasets",
                                        parameters, DATAMODEL.processResultsFromExportDatasetsServerSidePlugin)
                                },
                                parseInt(CONFIG['queryPluginStatusInterval']));

                            // Return here
                            return;

                        } else {

                            if (row.length !== 8) {

                                // Again, something is wrong with the returned results
                                status = unexpected;
                                level = "error";

                            } else {

                                // Extract returned values for clarity
                                r_Success = row[2].value;
                                r_ErrorMessage = row[3].value;
                                r_NCopiedFiles = row[4].value;
                                r_RelativeExpFolder = row[5].value;
                                r_ZipArchiveFileName = row[6].value;
                                r_Mode = row[7].value;

                                if (r_Success === "1") {
                                    var snip = "<b>Congratulations!</b>&nbsp;";
                                    if (r_NCopiedFiles === 1) {
                                        snip = snip +
                                            "<span class=\"badge\">1</span> file was ";
                                    } else {
                                        snip = snip +
                                            "<span class=\"badge\">" +
                                            r_NCopiedFiles + "</span> files were ";
                                    }
                                    if (r_Mode === "normal") {
                                        status = snip + "successfully exported to " +
                                            "{...}/" + r_RelativeExpFolder + ".";
                                    } else {
                                        // Add a placeholder to store the download URL.
                                        status = snip + "successfully packaged. <span id=\"download_url_span\"></span>";
                                    }
                                    level = "success";
                                } else {
                                    if (r_Mode === "normal") {
                                        status = "Sorry, there was an error exporting " +
                                            "to your user folder:<br /><br />\"" +
                                            r_ErrorMessage + "\".";
                                    } else {
                                        status = "Sorry, there was an error packaging your files for download!";
                                    }
                                    level = "error";
                                }
                            }
                        }
                    }
                }
                DATAVIEWER.displayStatus(status, level);

                // Retrieve the URL (asynchronously)
                if (r_Success === "1" && r_Mode === "zip") {
                    DATAMODEL.openbisServer.createSessionWorkspaceDownloadUrl(r_ZipArchiveFileName,
                        function (url) {
                            var downloadString =
                                '<img src="img/download.png" heigth="32" width="32"/>&nbsp;<a href="' + url + '">Download</a>!';
                            $("#download_url_span").html(downloadString);
                        });
                }
            },

            /**
             * Call an aggregation plug-in to copy the datasets associated to selected
             * node to the user folder.
             * @param experimentId string Experiment identifier
             * @param experimentType string Type of the experiment (LSR_FORTESSA_EXPERIMENT or FACS_ARIA_EXPERIMENT)
             * @param type string Type of the element to process
             * @param identifier string Identified of the element to process
             * @param specimen string Specimen name or ""
             * @param mode string One of "normal" or "zip"
             */
            exportDatasets: function (experimentId, experimentType, type, identifier, specimen, mode) {

                // Parameters for the aggregation service
                var parameters = {
                    experimentId: experimentId,
                    experimentType: experimentType,
                    entityType: type,
                    entityId: identifier,
                    specimen: specimen,
                    mode: mode
                };

                // Inform the user that we are about to process the request
                DATAVIEWER.displayStatus("Please wait while processing your request. This might take a while...", "info");

                // Must use global object
                DATAMODEL.openbisServer.createReportFromAggregationService(
                    CONFIG['dataStoreServer'], "export_flow_datasets",
                    parameters, DATAMODEL.processResultsFromExportDatasetsServerSidePlugin);
            },

            /**
             *
             * Custom sorter for DynaTree::sortChildren. It just passes the node
             * titles on to the natural sort algorithm by Jim Palmer.
             *
             * @param item1 First node
             * @param item2 Second node
             * @return int 1 if item1 > item2, 0 if item 1 == item2, -1 if item1 < item2
             *
             * @see naturalSort
             *
             */
            customSort: function (item1, item2) {
                return naturalSort(item1.data.title, item2.data.title);
            }
        };

        // Return the DataModel function
        return DataModel;
    });
