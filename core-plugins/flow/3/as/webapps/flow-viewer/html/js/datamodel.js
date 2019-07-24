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
            this.exportDatasetsService = null;
            this.upgradeExperimentService = null;

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
            let webappcontext = this.openbisV3.getWebAppContext();

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
             * Converts the results returned by openBIS to an array of nodes to be
             * used with DynaTree
             *
             * @param result  Array of objects to be added to the tree model.
             * @param parentNode    The parent DynaTree node
             */
            addToTreeModel: function (result, parentNode) {

                // Alias
                const dataModelObj = this;

                // Server returned an error condition: set node status accordingly
                if (result == null || result.hasOwnProperty("error")) {
                    parentNode.setLazyNodeStatus(DTNodeStatus_Error,
                        {
                            tooltip: response.error.data.exceptionTypeName,
                            info: "Error retrieving information."
                        });
                    return;
                }

                // No objects found
                if (result.length === 0) {

                    // PWS status OK
                    parentNode.setLazyNodeStatus(DTNodeStatus_Ok);

                    // Customize the node
                    if (parentNode && parentNode.data && parentNode.data.type &&
                        (parentNode.data.type === "ALL_PLATES" || parentNode.data.type === "TUBESET")) {
                        parentNode.addChild({
                            title: "<i>none</i>",
                            icon: "empty.png",
                            isFolder: false,
                            isLazy: false,
                            unselectable: true,
                            type: "EMPTY"
                        });
                    }
                    return;
                }

                // We create the nodes to add to the DynaTree: each node contains
                // the openBIS sample to be used later. In case of wells and tubes,
                // we actually create an array of specimens, each of which has tubes
                // or wells as children.
                let res = [];
                $.each(result, function (index, sample) {

                    // Declare some variables
                    let i = 0;
                    let parent = null;
                    let specimenNode = null;
                    let specimenSample = null;
                    let specimenName = null;
                    let indx = -1;
                    const specimenType = dataModelObj.EXPERIMENT_PREFIX + "_SPECIMEN";

                    // Get element type
                    let elementType = sample.getType().code;

                    switch (elementType) {

                        case (dataModelObj.EXPERIMENT_PREFIX + "_PLATE"):

                            res.push({
                                title: sample.properties["$NAME"],
                                icon: "plate.png",
                                isFolder: true,
                                isLazy: true,
                                expCode: parentNode.data.expCode,
                                type: "PLATE",
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
                            specimenName = "";
                            if (null !== specimenSample) {
                                specimenName = specimenSample.properties["$NAME"];
                            }

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
                                    expCode: parentNode.data.expCode,
                                    type: "SPECIMEN",
                                    element: null,
                                    children: []
                                };

                            } else {

                                // We get the node from the array
                                specimenNode = res[indx];

                            }

                            // Now we create the well node and add it as a child of
                            // the correct specimen node
                            let wellNode = {
                                title: sample.properties["$NAME"],
                                icon: "well.png",
                                isFolder: true,
                                isLazy: true,
                                expCode: parentNode.data.expCode,
                                type: "WELL",
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
                            specimenName = "";
                            if (null !== specimenSample) {
                                specimenName = specimenSample.properties["$NAME"];
                            }

                            // Do we already have a Specimen node?
                            indx = dataModelObj.findIn(res, specimenName);

                            if (indx === -1) {

                                // Create a new Specimen node and add it to the array
                                specimenNode = {
                                    title: specimenName,
                                    icon: "specimen.png",
                                    isFolder: true,
                                    isLazy: false,
                                    expCode: parentNode.data.expCode,
                                    type: "SPECIMEN",
                                    element: null,
                                    children: []
                                };

                            } else {

                                // We get the node from the array
                                specimenNode = res[indx];

                            }

                            // Now we create the tube node and add it as a child of
                            // the correct specimen node
                            let tubeNode = {
                                title: sample.properties["$NAME"],
                                icon: "tube.png",
                                isFolder: true,
                                isLazy: true,
                                expCode: parentNode.data.expCode,
                                type: "TUBE",
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
                                expCode: parentNode.data.expCode,
                                type: "FCS",
                                element: sample
                            });
                            break;

                        default:

                            // Unexpected sample type!
                            parentNode.setLazyNodeStatus(DTNodeStatus_Error,
                                {
                                    tooltip: elementType,
                                    info: "Unexpected element type!"
                                });
                            return;

                    }

                });

                // PWS status OK
                parentNode.setLazyNodeStatus(DTNodeStatus_Ok);
                parentNode.addChild(res);

                // Natural-sort children nodes
                parentNode.sortChildren(this.customSort, true);
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
            },

            /**
             * Call an aggregation plug-in to copy the datasets associated to selected
             * node to the user folder.
             * @param task Helper argument to define what to export. One of:
             *             EXPERIMENT_SAMPLE,  ALL_PLATES, PLATE, ALL_TUBES
             * @param collectionId string Collection identifier
             * @param collectionType string Collection type
             * @param experimentSampleId string Identifier of the experiment (sample)
             * @param experimentSampleType string Type of the experiment (sample)
             * @param entityId string Identifier of the element to process
             * @param entityType string Type of the element to process
             * @param mode string One of "normal" or "zip"
             */
            callServerSidePluginExportDataSets: function (task,
                                                          collectionId,
                                                          collectionType,
                                                          experimentSampleId,
                                                          experimentSamplePermId,
                                                          experimentSampleType,
                                                          platePermId,
                                                          plateType,
                                                          mode) {

                // Parameters for the aggregation service
                let parameters = {
                    task: task,
                    collectionId: collectionId,
                    collectionType: collectionType,
                    expSampleId: experimentSampleId,
                    expSamplePermId: experimentSamplePermId,
                    expSampleType: experimentSampleType,
                    platePermId: platePermId,
                    plateType: plateType,
                    mode: mode
                };

                // Inform the user that we are about to process the request
                DATAVIEWER.displayStatus("Please wait while processing your request. This might take a while...", "info");

                // Call service
                if (null === DATAMODEL.exportDatasetsService) {
                    let criteria = new AggregationServiceSearchCriteria();
                    criteria.withName().thatEquals("export_flow_datasets");
                    let fetchOptions = new AggregationServiceFetchOptions();
                    DATAMODEL.openbisV3.searchAggregationServices(criteria, fetchOptions).then(function(result) {
                        if (undefined === result.objects) {
                            console.log("Could not retrieve the server-side aggregation service!")
                            return;
                        }
                        DATAMODEL.exportDatasetsService = result.getObjects()[0];

                        // Now call the service
                        let options = new AggregationServiceExecutionOptions();
                        for (let key in parameters) {
                            options.withParameter(key, parameters[key]);
                        }
                        DATAMODEL.openbisV3.executeAggregationService(
                            DATAMODEL.exportDatasetsService.getPermId(),
                            options).then(function(result) {
                            DATAMODEL.processResultsFromExportDataSetsServerSidePlugin(result);
                        });
                    });
                } else {
                    // Call the service
                    let options = new AggregationServiceExecutionOptions();
                    for (let k in parameters) {
                        options.withParameter(k, parameters[k]);
                    }
                    DATAMODEL.openbisV3.executeAggregationService(
                        DATAMODEL.exportDatasetsService.getPermId(),
                        options).then(function(result) {
                        DATAMODEL.processResultsFromExportDataSetsServerSidePlugin(result);
                    });
                }
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
            callServerSidePluginGenerateFCSPlot: function (node, code, paramX, paramY, displayX, displayY, maxNumEvents, samplingMethod) {

                // Check whether the data for the plot is already cached
                if (node.data.cached) {
                    let key = code + "_" + paramX + "_" + paramY + "_" + maxNumEvents.toString() +
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
                let parameters = {
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
                DATAVIEWER.displayStatus("Please wait while processing your request. This might take a while...",
                    "info");

                // Call service
                if (null === DATAMODEL.retrieveFCSEventsService) {
                    let criteria = new AggregationServiceSearchCriteria();
                    criteria.withName().thatEquals("retrieve_fcs_events");
                    let fetchOptions = new AggregationServiceFetchOptions();
                    DATAMODEL.openbisV3.searchAggregationServices(criteria, fetchOptions).then(function(result) {
                        if (undefined === result.objects) {
                            console.log("Could not retrieve the server-side aggregation service!");
                            return;
                        }
                        DATAMODEL.retrieveFCSEventsService = result.getObjects()[0];

                        // Now call the service
                        let options = new AggregationServiceExecutionOptions();
                        for (let key in parameters) {
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
                    let options = new AggregationServiceExecutionOptions();
                    for (let k in parameters) {
                        options.withParameter(k, parameters[k]);
                    }
                    DATAMODEL.openbisV3.executeAggregationService(
                        DATAMODEL.retrieveFCSEventsService.getPermId(),
                        options).then(function(result) {
                        DATAMODEL.processResultsFromRetrieveFCSEventsServerSidePlugin(result);
                    });
                }
            },

            /**
             * Update an outdated experiment.
             * @param expPermId string Experiment perm identifier.
             */
            callServerSidePluginUpgradeExperiment: function (collectionPermId, expSamplePermId) {

                // Retrieve the service if necessary
                if (null === DATAMODEL.upgradeExperimentService) {
                    let criteria = new AggregationServiceSearchCriteria();
                    criteria.withName().thatEquals("upgrade_experiment");
                    let fetchOptions = new AggregationServiceFetchOptions();
                    DATAMODEL.openbisV3.searchAggregationServices(criteria, fetchOptions).then(function(result) {
                        if (undefined === result.objects) {
                            console.log("Could not retrieve the server-side aggregation service!");
                            return;
                        }
                        DATAMODEL.upgradeExperimentService = result.getObjects()[0];

                        // Now call the service
                        let options = new AggregationServiceExecutionOptions();
                        options.withParameter("collectionPermId", collectionPermId);
                        options.withParameter("expSamplePermId", expSamplePermId);
                        DATAMODEL.openbisV3.executeAggregationService(
                            DATAMODEL.upgradeExperimentService.getPermId(),
                            options).then(function(result) {
                            DATAMODEL.processResultsFromUpgradeExperimentServerSidePlugin(result);
                        });
                    });
                } else {
                    // Call the service
                    let options = new AggregationServiceExecutionOptions();
                    options.withParameter("collectionPermId", collectionPermId);
                    options.withParameter("expSamplePermId", expSamplePermId);
                    DATAMODEL.openbisV3.executeAggregationService(
                        DATAMODEL.upgradeExperimentService.getPermId(),
                        options).then(function(result) {
                        DATAMODEL.processResultsFromUpgradeExperimentServerSidePlugin(result);
                    });
                }
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
                    case "ALL_PLATES":
                        this.getPlates(node);
                        break;

                    // Extract and display the tubes
                    // (and corresponding specimens)
                    case "TUBESET":
                        this.getTubes(node);
                        break;

                    // Extract and display the wells in current plate
                    // (and corresponding specimens)
                    case "PLATE":

                        this.getWells(node);
                        break;

                    // Extract and displays dataset and FCS file
                    // associated to a well
                    case "WELL":
                        this.getDataSets(node);
                        break;

                    // Extract and displays dataset and FCS file
                    // associated to a tube
                    case "TUBE":
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
                for (let i = 0; i < res.length; i++) {
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
            getAndAddParameterInfoForDataSets: function (node, action) {

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
                    const parametersXML = $.parseXML(node.data.element.properties[this.EXPERIMENT_PREFIX + "_FCSFILE_PARAMETERS"]);
                    const parameters = parametersXML.childNodes[0];

                    const numParameters = parameters.getAttribute("numParameters");
                    const numEvents = parameters.getAttribute("numEvents");

                    let names = [];
                    let compositeNames = [];
                    let display = [];

                    // Parameter numbering starts at 1
                    let parametersToDisplay = 0;
                    for (let i = 1; i <= numParameters; i++) {

                        // If the parameter contains the PnCHANNELTYPE attribute (BD Influx Cell Sorter),
                        // we only add it if the channel type is 6.
                        let channelType = parameters.getAttribute("P" + i + "CHANNELTYPE");
                        if (channelType != null && channelType !== 6) {
                            continue;
                        }

                        // Store the parameter name
                        let name = parameters.getAttribute("P" + i + "N");
                        names.push(name);

                        // Store the composite name
                        let pStr = parameters.getAttribute("P" + i + "S");
                        let composite = name;
                        if (pStr !== "") {
                            composite = name + " (" + pStr + ")";
                        }
                        compositeNames.push(composite);

                        // Store the display scale
                        let displ = parameters.getAttribute("P" + i + "DISPLAY");
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
             * Get current {...}_EXPERIMENT sample.
             *
             * All relevant information is retrieved along with the sample.
             */
            getFlowExperimentSampleAndDisplay: function () {

                // Search for the sample of type and given perm id
                let criteria = new SampleSearchCriteria();
                criteria.withType().withCode().thatEquals(this.experimentSampleType);
                criteria.withIdentifier().thatEquals(this.experimentSampleId);
                let fetchOptions = new SampleFetchOptions();
                fetchOptions.withType();
                fetchOptions.withProperties();
                fetchOptions.withDataSets().withType();
                fetchOptions.withExperiment();
                fetchOptions.withExperiment().withType();

                let parentFetchOptions = new SampleFetchOptions();
                parentFetchOptions.withType();
                parentFetchOptions.withProperties();
                fetchOptions.withParentsUsing(parentFetchOptions);

                // Keep a reference to this object (for the callback)
                let dataModelObj = this;

                // Query the server
                this.openbisV3.searchSamples(criteria, fetchOptions).done(function (result) {

                    // Store the {...}_EXPERIMENT sample object
                    dataModelObj.experimentSample = result.getObjects()[0];

                    // Store the $NAME property
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
             * Get the dataset associated to a given sample
             * @param   node    The DynaTree (parent) node to which the generated
             *                  nodes will be appended.
             * @return  dataset The dataset associated to the sample
             */
            getDataSets: function (node) {

                // Alias
                const dataModelObj = this;

                // Get the sample
                let sample = node.data.element;

                // Get the dataset
                let fetchOptions = new SampleFetchOptions();
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
                        let updated_sample = map[sample.permId];

                        // Get the dataset
                        let datasets = updated_sample.getDataSets();
                        let dataset = datasets[0];

                        // Get the file
                        let criteria = new DataSetFileSearchCriteria();
                        let dataSetCriteria = criteria.withDataSet().withOrOperator();
                        dataSetCriteria.withPermId().thatEquals(dataset.permId.permId);

                        let fetchOptions = new DataSetFileFetchOptions();

                        // Query the server
                        dataModelObj.openbisV3.getDataStoreFacade().searchFiles(criteria, fetchOptions).done(function (result) {

                            // Extract the files
                            let datasetFiles = result.getObjects();

                            // Find the only fcs file and add its name and URL to the DynaTree
                            datasetFiles.forEach(function (f) {

                                if (!f.isDirectory() &&
                                    f.getPath().toLowerCase().indexOf(".fcs") !== -1) {

                                    // Append the file name to the dataset object for
                                    // use in addToTreeModel()
                                    let filename = '';
                                    let indx = f.getPath().lastIndexOf("/");
                                    if (indx === -1 || indx === f.getPath().length - 1) {
                                        // This should not happen, but we build in
                                        // a fallback anyway
                                        filename = f.getPath();
                                    } else {
                                        filename = f.getPath().substr(indx + 1);
                                    }
                                    dataset.filename = filename;

                                    // Build the download URL
                                    let url = f.getDataStore().getDownloadUrl() + "/datastore_server/" +
                                        f.permId.dataSetId.permId + "/" + f.getPath() + "?sessionID=" +
                                        dataModelObj.openbisV3.getWebAppContext().sessionId;

                                    // Store it in the dataset object
                                    let eUrl = encodeURI(url);
                                    eUrl = eUrl.replace('+', '%2B');
                                    dataset.url = eUrl;

                                    // Add the results to the tree
                                    let result = [];
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
             * @param node ALL_PLATES node to which the Plates are to be added.
             */
            getPlates: function (node) {

                // Alias
                const dataModelObj = this;

                // Required arguments
                const plateSampleType = dataModelObj.EXPERIMENT_PREFIX + "_PLATE";

                // Search for the sample of type {...}_TUBESET and given perm id
                let criteria = new SampleSearchCriteria();
                criteria.withType().withCode().thatEquals(plateSampleType);
                criteria.withParents().withPermId().thatEquals(this.experimentSample.permId.permId);
                criteria.withParents().withType().withCode().thatEquals(this.experimentSampleType);

                let fetchOptions = new SampleFetchOptions();
                fetchOptions.withType();
                fetchOptions.withProperties();

                // Search
                this.openbisV3.searchSamples(criteria, fetchOptions).done(function (result) {

                    // Retrieve the plates
                    let plates = result.getObjects();

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
                const dataModelObj = this;

                // Required arguments
                const tubesetSampleType = dataModelObj.EXPERIMENT_PREFIX + "_TUBESET";

                // Search for the sample of type {...}_TUBESET and given perm id
                let criteria = new SampleSearchCriteria();
                criteria.withType().withCode().thatEquals(tubesetSampleType);
                criteria.withParents().withPermId().thatEquals(this.experimentSample.permId.permId);
                criteria.withParents().withType().withCode().thatEquals(this.experimentSampleType);

                let fetchOptions = new SampleFetchOptions();
                fetchOptions.withProperties();
                fetchOptions.withType();
                fetchOptions.withChildren().withType();
                fetchOptions.withChildren().withProperties();

                let parentFetchOptions = new SampleFetchOptions();
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
                    let tubesetSample = result.objects[0];

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
                const dataModelObj = this;

                // Retrieve the plate (sample) from the node
                let plateSample = node.data.element;

                let fetchOptions = new SampleFetchOptions();
                fetchOptions.withChildren();
                fetchOptions.withChildren().withType();
                fetchOptions.withChildren().withProperties();

                let parentFetchOptions = new SampleFetchOptions();
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
                        let sample = map[plateSample.permId];

                        // Get the wells
                        let wells = sample.children;

                        // Pass the children of the tubeset to the addToTreeModel() method
                        dataModelObj.addToTreeModel(wells, node);
                    }
                });
            },

            /**
             * Build the initial tree for the DynaTree library
             *
             * Further tree manipulation is performed via the DynaTree library
             */
            initializeTreeModel: function () {

                // Alias
                const dataModelObj = this;

                // First-level children
                let first_level_children = [];
                if (dataModelObj.SUPPORTS_PLATE_ACQ[dataModelObj.experimentSampleType]) {

                    first_level_children = [
                        {
                            title: "Plates",
                            element: null,
                            icon: "folder.png",
                            isFolder: true,
                            isLazy: true,
                            expCode: dataModelObj.experimentSample.code,
                            type: 'ALL_PLATES'
                        },
                        {
                            title: "Tubes",
                            element: null,
                            icon: "folder.png",
                            isFolder: true,
                            isLazy: true,
                            expCode: dataModelObj.experimentSample.code,
                            type: 'TUBESET'
                        }];

                } else {

                    first_level_children = [
                        {
                            title: "Tubes",
                            element: null,
                            icon: "folder.png",
                            isFolder: true,
                            isLazy: true,
                            expCode: dataModelObj.experimentSample.code,
                            type: 'TUBESET'
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
                        onSelect: function (node) {
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
                                type: "EXPERIMENT_SAMPLE",
                                icon: "experiment.png",
                                isFolder: true,
                                isLazy: false,
                                expand: true,
                                children: first_level_children
                            }]
                    };
            },

            /**
             * Process the results returned from the callServerSidePluginExportDataSets() server-side plug-in
             * @param table Result table
             */
            processResultsFromExportDataSetsServerSidePlugin: function (table) {

                // Did we get the expected result?
                if (!table.rows || table.rows.length !== 1) {
                    DATAVIEWER.displayStatus(
                        "There was an error exporting the data!",
                        "danger");
                    return;
                }

                // Get the row of results
                let row = table.rows[0];

                // Retrieve the uid
                let r_UID = row[0].value;

                // Is the process completed?
                let r_Completed = row[1].value;

                if (r_Completed === 0) {

                    // Call the plug-in
                    setTimeout(function () {

                            // We only need the UID of the job
                            let parameters = {};
                            parameters["uid"] = r_UID;

                            // Now call the service
                            let options = new AggregationServiceExecutionOptions();
                            options.withParameter("uid", r_UID);

                            DATAMODEL.openbisV3.executeAggregationService(
                                DATAMODEL.exportDatasetsService.getPermId(),
                                options).then(function (result) {
                                DATAMODEL.processResultsFromExportDataSetsServerSidePlugin(result);
                            })
                        },
                        parseInt(CONFIG['queryPluginStatusInterval']));

                    // Return here
                    return;

                }

                // The service completed. We can now process the results.

                // Level of the message
                let level = "";

                // Returned parameters
                let r_Success = row[2].value;
                let r_ErrorMessage = row[3].value;
                let r_NCopiedFiles = row[4].value;
                let r_RelativeExpFolder = row[5].value;
                let r_ZipArchiveFileName = row[6].value;
                let r_Mode = row[7].value;

                if (r_Success === 1) {
                    let snip = "<b>Congratulations!</b>&nbsp;";
                    if (r_NCopiedFiles === 1) {
                        snip = snip + "<span class=\"badge\">1</span> file was ";
                    } else {
                        snip = snip + "<span class=\"badge\">" + r_NCopiedFiles + "</span> files were ";
                    }
                    if (r_Mode === "normal") {
                        status = snip + "successfully exported to {...}/" + r_RelativeExpFolder + ".";
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

                DATAVIEWER.displayStatus(status, level);

                if (r_Success === 1 && r_Mode === "zip") {

                    // Build the download URL with a little hack
                    DATAMODEL.openbisV3.getDataStoreFacade().createDataSetUpload("dummy").then(function(result) {
                        let url = result.getUrl();
                        let indx = url.indexOf("/datastore_server/store_share_file_upload");
                        if (indx !== -1) {
                            let dssUrl = url.substring(0, indx);
                            let downloadUrl = encodeURI(
                                dssUrl + "/datastore_server/session_workspace_file_download?" +
                                "sessionID=" + DATAMODEL.openbisV3.getWebAppContext().sessionId + "&filePath=" +
                                r_ZipArchiveFileName);

                            let downloadString =
                                '<img src="img/download.png" heigth="32" width="32"/>&nbsp;<a href="' +
                                downloadUrl + '">Download</a>!';
                            $("#download_url_span").html(downloadString);

                        }
                    });
                }
            },

            /**
             * Process the results returned from the retrieveFCSEvents() server-side plug-in
             * @param table Result table
             */
            processResultsFromRetrieveFCSEventsServerSidePlugin: function (table) {

                // Did we get the expected result?
                if (! table.rows || table.rows.length !== 1) {
                    DATAVIEWER.displayStatus(
                        "There was an error retrieving the data to plot!",
                        "danger");
                    return;
                }

                // Get the row of results
                let row = table.rows[0];

                // Retrieve the uid
                let r_UID = row[0].value;

                // Is the process completed?
                let r_Completed = row[1].value;

                if (r_Completed === 0) {

                    // Call the plug-in
                    setTimeout(function () {

                            // We only need the UID of the job
                            let parameters = {};
                            parameters["uid"] = r_UID;

                            // Now call the service
                            let options = new AggregationServiceExecutionOptions();
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
                let r_Success = row[2].value;
                let r_ErrorMessage = row[3].value;
                let r_Data = row[4].value;
                let r_Code = row[5].value;
                let r_ParamX = row[6].value;
                let r_ParamY = row[7].value;
                let r_DisplayX = row[8].value;
                let r_DisplayY = row[9].value;
                let r_NumEvents = row[10].value;   // Currently not used
                let r_MaxNumEvents = row[11].value;
                let r_SamplingMethod = row[12].value;
                let r_NodeKey = row[13].value;

                let level;
                if (r_Success === 1) {

                    // Error message and level
                    status = r_ErrorMessage;
                    level = "success";

                    // Plot the data
                    DATAVIEWER.plotFCSData(r_Data, r_ParamX, r_ParamY, r_DisplayX, r_DisplayY);

                    // Cache the plotted data
                    let dataKey = r_Code + "_" + r_ParamX + "_" + r_ParamY + "_" + r_MaxNumEvents.toString() +
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
             * Process the results returned from the retrieveFCSEvents() server-side plug-in
             * @param table Result table
             */
            processResultsFromUpgradeExperimentServerSidePlugin: function (table) {

                // Did we get the expected result?
                if (! table.rows || table.rows.length !== 1) {
                    DATAVIEWER.displayStatus(
                        "There was an error retrieving the data to plot!",
                        "danger");
                    return;
                }

                // Get the row of results
                let row = table.rows[0];

                // Extract returned values for clarity
                let r_Success = row[0].value;
                let r_Message = row[1].value;

                let level;
                if (r_Success === 1) {
                    status = r_Message;
                    level = "success";
                } else {
                    status = "Sorry, there was an error: \"" +
                        r_Message + "\".";
                    level = "danger";
                }

                // Display the message
                DATAVIEWER.displayStatus(status, level);

                // Reload the page after a short delay
                if (r_Success === 1) {
                    setTimeout(function () {
                        window.location.reload();
                    }, 1000);
                }
            },
        };

        // Return the DataModel function
        return DataModel;
    });
