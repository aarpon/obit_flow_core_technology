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

    var detailView, description;

    detailView = $("#detailView");
    detailView.empty();

    var spOp = "<span class=\"label label-default\">";
    var spCl = "</span>";
     
    // Underline experiment name in code
    code = exp.code;
    var indx = exp.code.lastIndexOf("_");
    if (indx != -1) {
        // Make sure we got the 18 random alphanumeric chars
        var suffix = exp.code.substr(indx);
        if (suffix.length == 19) {
            code = "<b>" + exp.code.substr(0, indx) + "</b>" + suffix;
        }
    }
    detailView.append(
        "<p>" + spOp + "Experiment code" + spCl + "</p>" + 
        "<p>" + code + "</p>"); 

    var acqDate = exp.properties.LSR_FORTESSA_EXPERIMENT_DATE;
    
    detailView.append(
        "<p>" + spOp + "Acquisition date" + spCl + "</p>" + 
        "<p>" + acqDate.substring(0, 10) + "</p>"); 
    
    detailView.append(
        "<p>" + spOp + "Acquisition hardware" + spCl + "</p>" + 
        "<p>" + exp.properties.LSR_FORTESSA_EXPERIMENT_ACQ_HARDWARE + "</p>"); 

    detailView.append(
        "<p>" + spOp + "Acquisition software" + spCl + "</p>" + 
        "<p>" + exp.properties.LSR_FORTESSA_EXPERIMENT_ACQ_SOFTWARE + "</p>"); 

    detailView.append(
        "<p>" + spOp + "Experiment owner" + spCl + "</p>" + 
        "<p>" + exp.properties.LSR_FORTESSA_EXPERIMENT_OWNER + "</p>"); 
        
    // Display the experiment description
    description = exp.properties.LSR_FORTESSA_EXPERIMENT_DESCRIPTION;
    if (description === "") {
        description = "<i>No description provided.</i>";
    }
    
    detailView.append(
        "<p>" + spOp + "Experiment description" + spCl + "</p>" + 
        "<p>" + description + "</p>");         
        
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

    // Clear previous views
    $("#detailViewSample").empty();
    $("#detailViewAction").empty();
    $("#detailViewStatus").empty();

    var spOp = "<span class=\"label label-info\">";
    var spCl = "</span>";
     
    // Adapt the display depending on the element type
    if (node.data.element) {
        if (node.data.element.hasOwnProperty("sampleTypeCode") &&
            node.data.element.sampleTypeCode == "LSR_FORTESSA_PLATE") {
                
            // Update details
            $("#detailViewSample").append(
                "<p>" + spOp + "Plate geometry" + spCl + "</p>" + 
                "<p>" + node.data.element.properties.LSR_FORTESSA_PLATE_GEOMETRY + "</p>"); 

        } else {
            // Pass
        }
    }

    spOp = "<span class=\"label label-warning\">";
    spCl = "</span>";

    $("#detailViewAction").append("<p>" + spOp + "Actions" + spCl + "</p>");
 
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
        };
        
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
                parent = node.parent;
                
                if (parent.data.element["@type"] == "Sample" &&
                    parent.data.element.sampleTypeCode == "LSR_FORTESSA_PLATE") {

                    // Type
                    type = "LSR_FORTESSA_PLATE";
                    
                    // Get plate's identifier
                    identifier = parent.data.element.identifier;
                    
                    // Experiment ID
                    experimentID = parent.data.element.experimentIdentifierOrNull;

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
    $("#detailViewAction").append(
        "<span><a class=\"btn btn-xs btn-primary\" " +
        "href=\"#\" onclick='callAggregationPlugin(\"" + 
        experimentId  + "\", \"" + type + "\", \"" + identifier +
        "\", \"" + specimenName + "\");'>" +
        "<img src=\"img/export.png\" />&nbsp;" + 
        "Export to your user folder</a></span>&nbsp;");
        
        
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
 * Draw the initial root structure. The tree will then be extended 
 * dynamically (via lazy loading) using DynaTree methods.
 * @param status: text to be displayed
 * @param level: one of "success", "info", "warning", "error". Default is
 * "info"
 * 
 * @param tree DynaTree object
 */
DataViewer.prototype.displayStatus = function(status, level) {

    // Display the status
    $("#detailViewStatus").empty();
    
    switch (level) {
        case "success":
            cls = "success";
            break;
        case "info":
            cls = "info";
            break;
        case "warning":
            cls = "warning";
            break;
        case "error":
            cls = "danger";
            break;
        default:
            cls = "info";
            break;
    }

    status = "<div class=\"alert alert-" + cls + " alert-dismissable\">" +
        status + "</div>";
    $("#detailViewStatus").html(status);

};