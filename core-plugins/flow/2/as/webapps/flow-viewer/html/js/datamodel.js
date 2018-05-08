/**
 * DataModel class
 * 
 * @author Aaron Ponti
 *
 */


/**
 * Define a model class to hold all experiment nodes. The tree model
 * used is DynaTree (http://code.google.com/p/dynatree/).
 */
function DataModel() {

    "use strict";

    // Current experiment version
    this.EXPERIMENT_LATEST_VERSION = 1;

    // Create a context object to access the context information
    this.context = new openbisWebAppContext();

    // Create an OpenBIS facade to call JSON RPC services
    this.openbisServer = new openbis("/openbis");

    // Reuse the current sessionId that we received in the context for
    // all the facade calls
    this.openbisServer.useSession(this.context.getSessionId());  

    // Experiment identifier
    this.expId = this.context.getEntityIdentifier();

    // Experiment type
    this.experimentType = this.context.getEntityType();

    this.EXPERIMENT_PREFIX = this.experimentType.substring(0,
        this.experimentType.indexOf("_EXPERIMENT"));

    // Experiment object and name
    this.exp = null;
    this.expName = "";

    // Attachments
    this.attachments = null;

    // Tree mode;
    this.treeModel = null;

    // Alias
    var dataModelObj = this;

    // Get the experiment object for given ID and update the model
    this.getExperiment(function(response) {

        if (response.hasOwnProperty("error")) {
            // Server returned an error
            dataModelObj.exp = null;
            dataModelObj.expName = "Error: could not retrieve experiment!";
        } else {
            dataModelObj.exp = response.result[0];
            dataModelObj.expName = dataModelObj.exp.properties[dataModelObj.EXPERIMENT_PREFIX + "_EXPERIMENT_NAME"];
        }

        // Initialize the tree
        dataModelObj.buildInitialTree();

        // Draw it
        DATAVIEWER.drawTree(dataModelObj.treeModel);

        // Display the experiment summary
        DATAVIEWER.displayExperimentInfo(dataModelObj.exp);

        // Retrieve and display attachment list
        dataModelObj.retrieveAndDisplayAttachments();

    });
}

/**
 * Build the initial tree for the DynaTree library
 * 
 * Further tree manipulation is performed via the DynaTree library
 */
DataModel.prototype.buildInitialTree = function() {

    // Alias
    var dataModelObj = this;

    // First-level children
    var first_level_children = [];
    if (this.supportsPlateAcq(dataModelObj.experimentType)) {

        first_level_children = [
            {
                title: "Plates",
                element: null,
                icon: "folder.png",
                isFolder: true,
                isLazy: true,
                expCode: dataModelObj.exp.code,
                type: 'plate_container'
            },
            {
                title: "Tubesets",
                element: null,
                icon: "folder.png",
                isFolder: true,
                isLazy: true,
                expCode: dataModelObj.exp.code,
                type: 'tubesets'
            } ];

    } else {

        first_level_children = [
            {
                title: "Tubesets",
                element: null,
                icon: "folder.png",
                isFolder: true,
                isLazy: true,
                expCode: dataModelObj.exp.code,
                type: 'tubesets'
            } ];
    }

    // Build the initial tree (root)
    this.treeModel = 
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
            onActivate: function(node) {
                // Display
                DATAVIEWER.displayDetailsAndActions(node);
            },            
            onLazyRead: function(node) {
                // Important: fetch only!
                dataModelObj.fetch(node);
            },
            children: [
                { 
                    title: dataModelObj.expName,
                    expCode: dataModelObj.exp.code,
                    element: dataModelObj.exp,
                    icon: "experiment.png",
                    isFolder: true, 
                    isLazy: false,
                    expand: true,
                    children: first_level_children
                } ]
        };
};

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
DataModel.prototype.customSort = function(item1, item2) {

    return naturalSort(item1.data.title, item2.data.title);

};

/**
 * Fetch data for current node
 *
 * @param   node    A DynaTree node
 */
DataModel.prototype.fetch = function(node) {

    // Alias
    var dataModelObj = this;

    // Get the data and update the tree
    switch (node.data.type) {

        // This gets and displays the plates in the experiment
        case "plate_container":
            this.getSamplesOfType(dataModelObj.EXPERIMENT_PREFIX + "_PLATE",
                node.data.expCode, function(response) {
                    dataModelObj.toDynaTree(response, node);
                });
            break;

        // This gets and displays the tubes in the experiment by first
        // getting the Tubeset and then the tubes it contains. Since the
        // Tubeset is a 'virtual' object generated in openBIS just to
        // make sure that the tubes are contained somewhere, we do not
        // display it at all.
        case "tubesets":
            this.getTubes(node.data.expCode, node);
            break;

        // This displays the wells contained in a given plate
        case "wells":
            this.getContainedSamples(node.data.element.code, function(response) {
                dataModelObj.toDynaTree(response, node);
            });
            break;

        // This displays the FCS file associated to either a well or a tube
        case "fcs":
            this.getAndAddDatasetForTubeOrWell(node.data.element, node);
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

};

/**
 * Check if a DynaTree node already exists in a simple 1-D array
 * (not already in the tree!)
 *
 * @param res   A simple, 1-D array of DynaTree nodes
 * @param title The title of the node to be searched for.
 * @return number of the node in the array or -1 if not found.
 */
DataModel.prototype.findIn = function(res, title) {

    if (res.length === 0) {
        return -1;
    }
    for (var i = 0; i < res.length; i++) {
        if (res[i].title === title) {
            return i;
        }
    }
    return -1;
};

/**
 * Check if the experiment (and thus the hardware( supports plate acquisition
 *
 * @param experiment_type String defining the experiment type.
 * @return boolean true if the hardware supports plate acquisition; false otherwise.
 */
DataModel.prototype.supportsPlateAcq = function(experiment_type) {

    switch (experiment_type) {

        case "LSR_FORTESSA_EXPERIMENT":
            return true;

        case "FACS_ARIA_EXPERIMENT":
        case "INFLUX_EXPERIMENT":
        case "S3E_EXPERIMENT":
        case "MOFLO_XDP_EXPERIMENT":
            return false;

        default:
            return false;
    }

};

/**
 * Get the dataset associated to a given sample
 * @param   sample  A sample
 * @param   node    The DynaTree (parent) node to which the generated 
 *                  nodes will be appended.
 * @return  dataset The dataset associated to the sample
 */
DataModel.prototype.getAndAddDatasetForTubeOrWell = function(sample, node) {

    // Alias
    var dataModelObj = this;

    // Get the dataset for the specified sample. We know that a sample
    // only has one dataset associated to it, which in turns only has one
    // URL, so we simplify a bit the structure.
    this.openbisServer.listDataSetsForSample(sample, true, function(response) {

        // Dataset
        var dataset = response.result[0];

        // Retrieve the file for the dataset and the associated URL
        dataModelObj.openbisServer.listFilesForDataSet(dataset.code, '/', true,
            function(response) {

                // Find the only fcs file and add its name and URL to the
                // DynaTree
                response.result.forEach(function(f) {

                    if (!f.isDirectory &&
                        f.pathInDataSet.toLowerCase().indexOf(".fcs") !== -1) {

                        // Append the file name to the dataset object for
                        // use in toDynaTree()
                        var filename = '';
                        var indx = f.pathInDataSet.lastIndexOf("/");
                        if (indx === -1 || indx === f.pathInDataSet.length - 1) {
                            // This should not happen, but we build in
                            // a fallback anyway
                            filename = f.pathInDataSet;
                        } else {
                            filename = f.pathInDataSet.substr(indx + 1);
                        }
                        dataset.filename = filename;

                        // Retrieve the file URL
                        dataModelObj.openbisServer.getDownloadUrlForFileForDataSetInSession(
                            dataset.code, f.pathInDataSet, function(url){

                                // Create a node to pass to toDynaTree()
                                var resp = {
                                    result: []
                                };

                                // Store it in the dataset object
                                var eUrl = encodeURI(url);
                                eUrl = eUrl.replace('+', '%2B');
                                dataset.url = eUrl;

                                // Add the results to the tree
                                resp.result.push(dataset);
                                dataModelObj.toDynaTree(resp, node);
                            });
                    }
                });

            });
    });
};

/**
 * Get the contained samples of specified container
 * @param   {String} containerSampleCode The code of the containing sample
 * @param   {Function} action   Function callback
 * @return  container           The container sample
 */
DataModel.prototype.getContainedSamples = function(containerSampleCode, action) {

    // Search criteria
    var sampleCriteria =
    {
        operator : "MATCH_ALL_CLAUSES",
        subCriterias: [
            {
                targetEntityKind: "SAMPLE_CONTAINER",
                criteria: {
                    matchClauses : [ 
                        {"@type":"AttributeMatchClause",
                            attribute : "CODE",
                            fieldType : "ATTRIBUTE",
                            desiredValue : containerSampleCode 
                        }]}
            }
        ]
    };
    
    // Search for container sample
    this.openbisServer.searchForSamples(sampleCriteria, action);
};

/**
 * Get the plates for current experiment
 * @param {String} type
 * @param {String} expCode
 * @param {Function} action Callback
 * @returns {Array} plates Array of plates.
 */
DataModel.prototype.getSamplesOfType = function(type, expCode, action) {

    // Experiment criteria
    var experimentCriteria =
    {
        targetEntityKind : "EXPERIMENT",
        criteria : {
            matchClauses :
                [ {"@type" : "AttributeMatchClause",
                    "attribute" : "CODE",
                    "fieldType" : "ATTRIBUTE",
                    "desiredValue" : expCode
                } ]
        }
    };

    // Sample (type) criteria
    var sampleCriteria =
    {
        subCriterias : [ experimentCriteria ],
        matchClauses :
            [ {"@type":"AttributeMatchClause",
                attribute : "TYPE",
                fieldType : "ATTRIBUTE",
                desiredValue : type
            } ],
        operator : "MATCH_ALL_CLAUSES"
    };

    // Search
    this.openbisServer.searchForSamples(sampleCriteria, action);

};

/**
 * Returns the type of the sample.
 * 
 * @param element An openBIS ISample or IDataset
 * @return string sample type ({exp_prefix}_PLATE, {exp_prefix}_TUBESET,
 *     {exp_prefix}_TUBE, {exp_prefix}_WELL, {exp_prefix}_FCSFILE)
 */
DataModel.prototype.getType = function(element) {

    if (element.hasOwnProperty("experimentTypeCode")) {
        return element.experimentTypeCode;
    } else if (element.hasOwnProperty("sampleTypeCode")) {
        return element.sampleTypeCode;
    } else if (element.hasOwnProperty("dataSetTypeCode")) {
        return element.dataSetTypeCode;
    } else if (element.hasOwnProperty("url")) {
        return (this.EXPERIMENT_PREFIX + "_FCSFILE");
    } else {
        return "Unknown";
    }
};

/**
 * Converts the results returned by openBIS to an array of nodes to be
 * used with DynaTree
 * 
 * @param response  The JSON-object returned by openBIS
 * @param node      A DynaTree node
 */
DataModel.prototype.toDynaTree = function(response, node) {

    // Alias
    var dataModelObj = this;

    // Server returned an error condition: set node status accordingly
    if (response.hasOwnProperty("error")) {
        node.setLazyNodeStatus(DTNodeStatus_Error, 
        {
            tooltip: response.error.data.exceptionTypeName,
            info: "Error retrieving information."
        });
        return;
    }

    // No objects found
    if (response.result.length === 0) {

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
    $.each(response.result, function(index, sample) {

        var elementType = dataModelObj.getType(sample);

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

                // The well is a child of a specimen
                var specimenNode = null;

                // Do we already have a Specimen node?
                var indx = dataModelObj.findIn(res, 
                    sample.properties[dataModelObj.EXPERIMENT_PREFIX + "_SPECIMEN"]);

                if (indx === -1) {

                    // Create a new Specimen node and add it to the array
                    specimenNode = {
                        title: sample.properties[dataModelObj.EXPERIMENT_PREFIX + "_SPECIMEN"],
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

                res.push({
                    title: sample.code,
                    icon: "tubeset.png",
                    isFolder: true,
                    isLazy: true,
                    expCode: node.data.expCode,
                    type: "tubes",
                    element: sample
                });
                break;

            case (dataModelObj.EXPERIMENT_PREFIX + "_TUBE"):

                // The tube is a child of a specimen
                specimenNode = null;

                // Do we already have a Specimen node?
                indx = dataModelObj.findIn(res, 
                    sample.properties[dataModelObj.EXPERIMENT_PREFIX + "_SPECIMEN"]);

                if (indx === -1) {

                    // Create a new Specimen node and add it to the array
                    specimenNode = {
                        title: sample.properties[dataModelObj.EXPERIMENT_PREFIX + "_SPECIMEN"],
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
                // the specimen into the res array
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

};

/**
 * Get the plates for current experiment
 * @param {Function} action Function callback
 * @returns {Array} plates  Array of plates.
 */
DataModel.prototype.getExperiment = function(action) { 
    // expId must be in an array: [expId]
    this.openbisServer.listExperimentsForIdentifiers([this.expId], action);
};

/**
 * Get the Tubes for current experiment
 * @param expCode Experiment code
 * @param node Node from the tree
 * @returns Array Array of Tubes.
 */
DataModel.prototype.getTubes = function(expCode, node) {

    // Alias
    var dataModelObj = this;

    // First get the Tubeset
    this.getSamplesOfType(dataModelObj.EXPERIMENT_PREFIX + "_TUBESET", expCode, function(response) {

        // Are there tubesets?
        if (response.result.length === 0) {
            dataModelObj.toDynaTree(response, node);
            return;
        }

        // Get the identifier of the Tubeset
        var identifier = response.result[0].identifier;

        // Now get all the Tubes that belong to the Tubeset
        dataModelObj.getContainedSamples(identifier, function(response) {

            dataModelObj.toDynaTree(response, node);

        });

    });
};

/**
  *Call an aggregation plug-in to copy the datasets associated to selected
 * node to the user folder.
 * @param experimentId string Experiment identifier
 * @param experimentType string Type of the experiment (LSR_FORTESSA_EXPERIMENT or FACS_ARIA_EXPERIMENT)
 * @param type string Type of the element to process
 * @param identifier string Identified of the element to process
 * @param specimen string Specimen name or ""
 * @param mode string One of "normal" or "zip"
 */
DataModel.prototype.exportDatasets = function(experimentId, experimentType, type, identifier, specimen, mode) {

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
};

/**
 * Process the results returned from the exportDatasets() server-side plug-in
 * @param response JSON object
 */
DataModel.prototype.processResultsFromExportDatasetsServerSidePlugin = function(response) {

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
            r_Success = false;

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
                r_Completed = row[1].value

                // If the processing is not completed, we wait a few seconds and trigger the
                // server-side plug-in again. The interval is defined by the admin.
                if (r_Completed == false) {

                    // We only need the UID of the job
                    var parameters = {};
                    parameters["uid"] = r_UID;

                    // Call the plug-in
                    setTimeout(function() {
                            DATAMODEL.openbisServer.createReportFromAggregationService(
                                CONFIG['dataStoreServer'], "export_flow_datasets",
                                parameters, DATAMODEL.processResultsFromExportDatasetsServerSidePlugin)
                        },
                        parseInt(CONFIG['queryPluginStatusInterval']));

                    // Return here
                    return;

                } else {

                    if (row.length != 8) {

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

                        if (r_Success == true) {
                            var snip = "<b>Congratulations!</b>&nbsp;";
                            if (r_NCopiedFiles == 1) {
                                snip = snip +
                                    "<span class=\"badge\">1</span> file was ";
                            } else {
                                snip = snip +
                                    "<span class=\"badge\">" +
                                    r_NCopiedFiles + "</span> files were ";
                            }
                            if (r_Mode == "normal") {
                                status = snip + "successfully exported to " +
                                    "{...}/" + r_RelativeExpFolder + ".";
                            } else {
                                // Add a placeholder to store the download URL.
                                status = snip + "successfully packaged. <span id=\"download_url_span\"></span>";
                            }
                            level = "success";
                        } else {
                            if (r_Mode == "normal") {
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
        if (r_Success == true && r_Mode == "zip") {
            DATAMODEL.openbisServer.createSessionWorkspaceDownloadUrl(r_ZipArchiveFileName,
                function(url) {
                    var downloadString =
                        '<img src="img/download.png" heigth="32" width="32"/>&nbsp;<a href="' + url + '">Download</a>!';
                    $("#download_url_span").html(downloadString);
                });
    }
};

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
DataModel.prototype.generateFCSPlot = function(node, code, paramX, paramY, displayX, displayY, maxNumEvents, samplingMethod) {

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

    // Must use global object
    DATAMODEL.openbisServer.createReportFromAggregationService(
        CONFIG['dataStoreServer'],
        "retrieve_fcs_events",
        parameters, DATAMODEL.processResultsFromRetrieveFCSEventsServerSidePlugin);
}

/**
 * Process the results returned from the retrieveFCSEvents() server-side plug-in
 * @param response JSON object
 */
DataModel.prototype.processResultsFromRetrieveFCSEventsServerSidePlugin = function(response) {

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
    var r_Data;
    var r_Code;
    var r_ParamX;
    var r_ParamY;
    var r_DisplayX;
    var r_DisplayY;
    var r_NumEvents;
    var r_MaxNumEvents;
    var r_SamplingMethod;
    var r_NodeKey;

    // First check if we have an error
    if (response.error) {

        status = "Sorry, could not process request.";
        level = "danger";
        r_Success = false;

    } else {

        // No obvious errors. Retrieve the results.
        status = "";
        if (response.result.rows.length != 1) {

            // Unexpected number of rows returned
            status = unexpected;
            level = "danger";

        } else {

            // We have a potentially valid result
            row = response.result.rows[0];

            // Retrieve the uid
            r_UID = row[0].value;

            // Retrieve the 'completed' status
            r_Completed = row[1].value

            // If the processing is not completed, we wait a few seconds and trigger the
            // server-side plug-in again. The interval is defined by the admin.
            if (r_Completed == false) {

                // We only need the UID of the job
                var parameters = {};
                parameters["uid"] = r_UID;

                // Call the plug-in
                setTimeout(function() {
                        DATAMODEL.openbisServer.createReportFromAggregationService(
                            CONFIG['dataStoreServer'], "retrieve_fcs_events",
                            parameters, DATAMODEL.processResultsFromRetrieveFCSEventsServerSidePlugin)
                    },
                    parseInt(CONFIG['queryPluginStatusInterval']));

                // Return here
                return;

            } else {

                // Extract returned values for clarity
                r_Success = row[2].value;
                r_ErrorMessage = row[3].value;
                r_Data = row[4].value;
                r_Code = row[5].value;
                r_ParamX = row[6].value;
                r_ParamY = row[7].value;
                r_DisplayX = row[8].value;
                r_DisplayY = row[9].value;
                r_NumEvents = row[10].value;   // Currently not used
                r_MaxNumEvents = row[11].value;
                r_SamplingMethod = row[12].value;
                r_NodeKey = row[13].value;

                if (r_Success == true) {
                    status = r_ErrorMessage;
                    level = "success";

                    // Plot the data
                    DATAVIEWER.plotFCSData(r_Data, r_ParamX, r_ParamY, r_DisplayX, r_DisplayY);

                    // Cache the plotted data
                    var dataKey = r_Code + "_" + r_ParamX + "_" + r_ParamY + "_" + r_MaxNumEvents.toString() +
                        "_" + r_DisplayX + "_" + r_DisplayY + "_" + r_SamplingMethod.toString();
                    DATAVIEWER.cacheFCSData(r_NodeKey, dataKey, r_Data);

                } else {
                    status = "Sorry, there was an error: \"" +
                        r_ErrorMessage + "\".";
                    level = "danger";
                }

            }

        }
    }

    // We only display errors
    if (r_Success == false) {
        DATAVIEWER.displayStatus(status, level);
    } else {
        DATAVIEWER.hideStatus();
    }

    return response;

};

/**
 * Update an outdated experiment.
 * @param expPermId string Experiment perm identifier.
 */
DataModel.prototype.upgradeExperiment = function(expPermId) {

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
                r_Success = false;
            } else {
                status = "";
                if (response.result.rows.length != 1) {
                    status = unexpected;
                    level = "danger";
                } else {
                    row = response.result.rows[0];
                    if (row.length != 2) {
                        status = unexpected;
                        level = "danger";
                    } else {

                        // Extract returned values for clarity
                        r_Success = row[0].value;
                        r_ErrorMessage = row[1].value;

                        if (r_Success == true) {
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
            if (r_Success == true) {
                setTimeout(function() {window.location.reload();}, 1000);
            }

        });
}

/**
 * Get, store and display the attachment info
 */
DataModel.prototype.retrieveAndDisplayAttachments = function() {

    // Get attachments
    var experimentId = {
        "@type" : "ExperimentIdentifierId",
        "identifier" : this.expId
    }

    // Alias
    var dataModelObj = this;

    // Retrieve the attachments
    this.openbisServer.listAttachmentsForExperiment(experimentId, false, function(response) {
        if (response.error) {
            dataModelObj.attachments = [];
            console.log("There was an error retrieving the attachments for current experiment!");
        } else {

            // Store the attachment array
            dataModelObj.attachments = response.result;

            // Display the info
            DATAVIEWER.displayAttachments(dataModelObj);
        }
    });

};

/**
 * Add parameter information to the FCS nodes
 * @param node An FCS file DynaTree node
 * @param action Function that renders the node.
 */
DataModel.prototype.getAndAddParameterInfoForDatasets = function(node, action) {

    // Consistency check on the node
    try {
        if (node.data.element.dataSetTypeCode != (this.EXPERIMENT_PREFIX + "_FCSFILE")) {
            throw("The node is not of the expected type!");
        }
    } catch(err) {
        console.log("The node is not of the expected type!")
        return;
    }

    // Old experiments might not have anything stored in {prefix}_FCSFILE_PARAMETERS.
    if (! node.data.element.properties[this.EXPERIMENT_PREFIX + "_FCSFILE_PARAMETERS"]) {
        return;
    }

    // If the node already contain the processed parameter information, we can go straight to
    // the rendering.
    if (! node.data.parameterInfo) {

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
            if (channelType != null && channelType != 6) {
                continue;
            }

            // Store the parameter name
            var name = parameters.getAttribute("P" + i + "N");
            names.push(name);

            // Store the composite name
            var pStr = parameters.getAttribute("P" + i + "S");
            var composite = name;
            if (pStr != "") {
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
            "numParameters" : parametersToDisplay,
            "numEvents" : numEvents,
            "names" : names,
            "compositeNames" : compositeNames,
            "display" : display
        }

    }

    // Now render
    action(node);

};
