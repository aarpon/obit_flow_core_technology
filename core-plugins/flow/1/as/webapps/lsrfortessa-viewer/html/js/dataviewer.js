/**
 * DataViewer class
 * 
 * @author Aaron Ponti
 *
 */

/**
 * A viewer to display DataModel entities to the html page.
 */
function DataViewer() {

    "use strict";

};

/**
 * Displays experiment info
 *
 * @param exp openBIS Experiment object
 */
DataViewer.prototype.displayExperimentInfo = function(exp) {

    // Display the experiment name
    $("#experimentNameView").html("<h2>" + exp.properties.LSR_FORTESSA_EXPERIMENT_NAME + "</h2>")

    // Display the experiment info
    var detailView = $("#detailView");
    detailView.empty();

    var experimentTagView = $("#experimentTagView");
    experimentTagView.empty();

    var experimentDescriptionView = $("#experimentDescriptionView");
    experimentDescriptionView.empty();

    var experimentAcquisitionDetailsView = $("#experimentAcquisitionDetailsView");
    experimentAcquisitionDetailsView.empty();

    // Get metaprojects (tags)
    var metaprojects = "";
    if (exp.metaprojects) {
        if (exp.metaprojects.length == 0) {
            metaprojects = "<i>None</i>";
        } else if (exp.metaprojects.length == 1) {
            metaprojects = exp.metaprojects[0].name;
        } else {
            for (var i = 0; i < exp.metaprojects.length; i++) {
                if (i < (exp.metaprojects.length - 1)) {
                    metaprojects = metaprojects.concat(exp.metaprojects[i].name + ", ");
                } else {
                    metaprojects = metaprojects.concat(exp.metaprojects[i].name);
                }
            }
        }
    }
    experimentTagView.append(this.prepareTitle("Tags", "info"));
    experimentTagView.append($("<p>").html(metaprojects));

    // Display the experiment description
    var description = exp.properties.LSR_FORTESSA_EXPERIMENT_DESCRIPTION;
    if (undefined === description || description == "") {
        description = "<i>No description provided.</i>";
    }
    experimentDescriptionView.append(this.prepareTitle("Description"));
    experimentDescriptionView.append($("<p>").html(description));

    // Display the acquisition details
    var acqDate = exp.properties.LSR_FORTESSA_EXPERIMENT_DATE;

    var acqDetails = "<p>Acquired on " + acqDate.substring(0, 10) + " on " +
        exp.properties.LSR_FORTESSA_EXPERIMENT_ACQ_HARDWARE + " by " +
        exp.properties.LSR_FORTESSA_EXPERIMENT_OWNER + " using " +
        exp.properties.LSR_FORTESSA_EXPERIMENT_ACQ_SOFTWARE + ".</p>";
    experimentAcquisitionDetailsView.append(this.prepareTitle("Acquisition details"));
    experimentAcquisitionDetailsView.append($("<p>").html(acqDetails));

};

/**
 * Draw the initial root structure. The tree will then be extended 
 * dynamically (via lazy loading) using DynaTree methods.
 * 
 * @param tree DynaTree object
 */
DataViewer.prototype.drawTree = function(tree) {

    // Display the tree
    $("#treeView").dynatree(tree);

};

/**
 * Display the node details and the actions associated with it
 * @param {Object} node from the DynaTree model
 */
DataViewer.prototype.displayDetailsAndActions = function(node) {
    
    // In theory, an unselectable node should not be selectable
    // - we build in an additional check here
    if (node.data.unselectable === true) {
        return;
    }

    // Store some references
    var statusID = $("#status");
    var detailViewActionID = $("#detailViewAction");
    var detailViewStatusID = $("#detailViewStatus");
    var detailViewSampleID = $("#detailViewSample");

    // Clear previous views
    statusID.empty();
    detailViewActionID.empty();
    detailViewStatusID.empty();
    detailViewSampleID.empty();

    // Adapt the display depending on the element type
    if (node.data.element) {

        // Samples
        switch (node.data.element["@type"]) {

            case "Sample":

                if (node.data.element.sampleTypeCode == "LSR_FORTESSA_PLATE") {

                    // Update details
                    detailViewSampleID.append(this.prepareTitle("Plate geometry"));
                    detailViewSampleID.append($("<p>").html(node.data.element.properties.LSR_FORTESSA_PLATE_GEOMETRY));

                }
                break;

            case "DataSet":

                if (node.data.element.dataSetTypeCode == "LSR_FORTESSA_FCSFILE") {

                    // Old experiments might not have anything stored in LSR_FORTESSA_FCSFILE_PARAMETERS.
                    if (! node.data.element.properties.LSR_FORTESSA_FCSFILE_PARAMETERS) {
                        break;
                    }

                    // Retrieve and store the parameter information
                    DATAMODEL.getAndAddParemeterInfoForDatasets(node, function() {

                        // Display the form to be used for parameter plotting
                        DATAVIEWER.renderParameterSelectionForm(node);

                    });

                }
                break;

        }
    }

    // Display the export action
    this.displayExportAction(node);
    
    // Display the download action
    this.displayDownloadAction(node);

};

/**
 * Build and display the code to trigger the server-side aggregation
 * plugin 'copy_datasets_to_userdir'
 * @param node: DataTree node
 */
DataViewer.prototype.displayExportAction = function(node) {
    
    // Get the type and identifier of the element associated to the node. 
    // If the node is associated to a specimen, the type and identifier
    // will instead be those of the parent node.
    var type = "";
    var identifier = "";
    var specimenName = "";
    var experimentId = null;

    // Get element type and code
    if (node.data.element) {
        
        // Get type
        switch (node.data.element["@type"]) {
            case "Experiment":
                experimentId = node.data.element.identifier;
                type = node.data.element.experimentTypeCode;
                identifier = node.data.element.identifier;
                break;
                
            case "Sample":
                experimentId = node.data.element.experimentIdentifierOrNull;
                type = node.data.element.sampleTypeCode;
                identifier = node.data.element.identifier;
                break;
                
            case "DataSet":
                experimentId = node.data.element.experimentIdentifier;
                type = node.data.element.dataSetTypeCode;
                identifier = node.data.element.code;
                break;
            
            default:
                experimentId = "";
                type = "";
                identifier = "";
        }
        
    } else {
        
        if (node.data.type && node.data.type == "specimen") {
            
            // Get the specimen name.
            // TODO: Use a dedicate property
            specimenName = node.data.title; 
            
            // In case of a specimen, we filter WELLS or TUBES for the 
            // associated property LSR_FORTESSA_SPECIMEN.
            // We must treat the two cases differently, though.
            //
            // In the case of wells, we can make use of the fact that 
            // wells are contained in a plate. So we can use the plate
            // identifier to get the wells, and then filter them by
            // specimen.
            //
            // In the case of tubes, they do not have a parent, so we 
            // simply need to get all tubes in the experiment and check
            // that their LSR_FORTESSA_SPECIMEN property matches the
            // given specimen.
             
            // Do we have a parent?
            if (node.parent && node.parent.data && node.parent.data.element) {
                
                // Reference
                var parent = node.parent;
                
                if (parent.data.element["@type"] == "Sample" &&
                    parent.data.element.sampleTypeCode == "LSR_FORTESSA_PLATE") {

                    // Type
                    type = "LSR_FORTESSA_PLATE";
                    
                    // Get plate's identifier
                    identifier = parent.data.element.identifier;
                    
                    // Experiment ID
                    experimentId = parent.data.element.experimentIdentifierOrNull;

                }

            } else {
                
                // We set the parent to point to the experiment
                type = "LSR_FORTESSA_TUBESET";
                
                // Walk up the tree until we reach the experiment
                while (node.parent) {
                    node = node.parent;
                    if (node.data.element && 
                        node.data.element["@type"] == "Experiment") {
                            identifier = node.data.element.identifier;
                            break; 
                    }
                }
                
                // Experiment ID (same as identifier)
                experimentId = identifier;
                
            }

        } else if (node.data.type && node.data.type == "tubesets") {
        
            // If there are no (loaded) children (yet), just return
            if (!node.childList || node.childList.length == 0) {
                if (node._isLoading) {
                    this.displayStatus("Please reselect this node to " + 
                    "display export option.</br />", "info");
                }
                return;
            }

            // Do we have real samples?
            if (node.childList.length == 1 && 
                node.childList[0].data && 
                node.childList[0].data.icon == "empty.png" &&
                node.childList[0].data.title === "<i>None</i>"!= -1) {
                    return;
            }

            // This is the same as the tubeset case before, but without
            // filtering by the specimen
            
            // Empty specimen
            specimenName = "";
            
            // Tubeset
            type = "LSR_FORTESSA_TUBESET";
                
            // Walk up the tree until we reach the experiment
            while (node.parent) {
                node = node.parent;
                if (node.data.element && 
                    node.data.element["@type"] == "Experiment") {
                        identifier = node.data.element.identifier;
                        break; 
                }
            }
                
            // Experiment ID (same as identifier)
            experimentId = identifier;
        
        } else if (node.data.type && node.data.type == "plate_container") {

            // If there are no (loaded) children (yet), just return
            if (!node.childList || node.childList.length == 0) {
                if (node._isLoading) {
                    this.displayStatus("Please reselect this node to " + 
                    "display export option.</br />", "info");
                }
                return;
            }

            // Do we have real samples?
            if (node.childList.length == 1 && 
                node.childList[0].data && 
                node.childList[0].data.icon == "empty.png" &&
                node.childList[0].data.title === "<i>None</i>"!= -1) {
                    return;
            }

            // Empty specimen
            specimenName = "";
            
            // All plates in the experiment
            type = "LSR_FORTESSA_ALL_PLATES";
                
            // Walk up the tree until we reach the experiment
            while (node.parent) {
                node = node.parent;
                if (node.data.element && 
                    node.data.element["@type"] == "Experiment") {
                        identifier = node.data.element.identifier;
                        break; 
                }
            }
                
            // Experiment ID (same as identifier)
            experimentId = identifier;

        }
        
    }

    // If no relevant type found, just return here
    if (type == "") {
        return;
    }

    // Build and display the call
    callAggregationPlugin = DATAMODEL.copyDatasetsToUserDir;

    // Display the "Export to your folder" button only if enabled in the configuration file
    if (CONFIG['enableExportToUserFolder'] == true) {

        $("#detailViewAction").append(
                "<span><a class=\"btn btn-xs btn-primary\" " +
                "href=\"#\" onclick='callAggregationPlugin(\"" +
                experimentId  + "\", \"" + type + "\", \"" + identifier +
                "\", \"" + specimenName + "\", \"normal\"); return false;'>" +
                "<img src=\"img/export.png\" />&nbsp;" +
                "Export to your folder</a></span>&nbsp;");

    }
        
    // Build and display the call for a zip archive
    $("#detailViewAction").append(
            "<span><a class=\"btn btn-xs btn-primary\" " +
            "href=\"#\" onclick='callAggregationPlugin(\"" +
            experimentId  + "\", \"" + type + "\", \"" + identifier +
            "\", \"" + specimenName + "\", \"zip\"); return false;'>" +
            "<img src=\"img/zip.png\" />&nbsp;" +
            "Download archive</a></span>&nbsp;");
};


/**
 * Build and display the link to download the FCS file via browser
 * @param node: DataTree node
 */
DataViewer.prototype.displayDownloadAction = function(node) {
    
    // Build and display the call
    if (node.data.element && node.data.element.hasOwnProperty("url")) {
        $("#detailViewAction").append(
            "<span><a class=\"btn btn-xs btn-primary\" " +
            "href=\"" + node.data.element.url + "\">" +
            "<img src=\"img/download.png\" />&nbsp;Download " +
            node.data.element.filename + "</a></span>");
    }
};

/**
 * Display status text color-coded by level.
 * @param status: text to be displayed
 * @param level: one of "default", "info", "success", "warning", "danger". Default is "default".
 */
DataViewer.prototype.displayStatus = function(status, level) {

    // Get the the status div
    var status_div = $("#status");

    // Make sure the status div is visible
    status_div.show();

    // Clear the status
    status_div.empty();

    // Make sure the level is valid
    if (["default", "success", "info", "warning", "danger"].indexOf(level) == -1) {
        console.log("Unknown level: reset to 'info'.")
        level = "default";
    }

    var d = $("<div>").addClass("alert alert-dismissable fade in").addClass("alert-" + level).html(status);
    status_div.append(d);

};

/**
 * Display attachment info and link to the Attachments tab.
 * @param attachments: list of attachments
 */
DataViewer.prototype.displayAttachments = function(dataMoverObj, attachments) {

    // Get the div
    var experimentAttachmentsViewId = $("#experimentAttachmentsView");

    // Clear the attachment div
    experimentAttachmentsViewId.empty();

    // Text
    var text = "";
    if (dataMoverObj.attachments.length == 0) {
        text = "There are no attachments.";
    } else if (dataMoverObj.attachments.length == 1) {
        text = "There is one attachment."
    } else {
        text = "There are " + dataMoverObj.attachments.length + " attachments";
    }
    // Link to the attachment tab
    var link = $("<a>").text(text).attr("href", "#").attr("title", text).click(
        function() {
            var url = "#entity=EXPERIMENT&permId=" + dataMoverObj.exp.permId +
                "&ui-subtab=attachment-section&ui-timestamp=" + (new Date().getTime());
            window.top.location.hash = url;
            return false;
        });

    experimentAttachmentsViewId.append(this.prepareTitle("Attachments"));

    // Display the link
    experimentAttachmentsViewId.append(link);

};

/**
 * Display the form with the parameter selections for the plotting.
 * @param target_div: div to which the form will be appended.
 * @param parameters: list of parameter names
 * @patam dataset_permid: permid of the dataset
 */
DataViewer.prototype.renderParameterSelectionForm = function(node) {

    // Check that the parameter info is present
    if (!node.data.parameterInfo) {
        return;
    }

    // Update details
    var detailViewSampleID = $("#detailViewSample");

    detailViewSampleID.append(this.prepareTitle("Number of events"));
    detailViewSampleID.append($("<p>").html(node.data.parameterInfo.numEvents));

    detailViewSampleID.append(this.prepareTitle("Number of parameters"));
    detailViewSampleID.append($("<p>").html(node.data.parameterInfo.numParameters));

    // Create the form
    var form = $("<form>").addClass("form-group").attr("id", "parameter_form");
    detailViewSampleID.append(form);
    var formId = $("#parameter_form");
    var select =  $("<select>").addClass("form_control").attr("id", "parameter_form_select");
    formId.append(select);
    var selectId = $("#parameter_form_select");

    // Add all options
    for (var i = 0; i < node.data.parameterInfo.numParameters; i++) {
        var name = node.data.parameterInfo["names"][i];
        var compositeName = node.data.parameterInfo["compositeNames"][i];
        selectId.append($("<option>")
            .attr("value", name)
            .text(compositeName));
    }
}

/**
 * Prepare a title div to be added to the page.
 * @param title Text for the title
 * @param level One of "default", "info", "success", "warning", "danger". Default is "default".
 */
DataViewer.prototype.prepareTitle = function(title, level) {


    // Make sure the level is valid
    if (["default", "success", "info", "warning", "danger"].indexOf(level) == -1) {
        console.log("Unknown level: reset to 'info'.")
        level = "default";
    }

    return ($("<p>").append($("<span>").addClass("label").addClass("label-" + level).text(title)));

}