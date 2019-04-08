/**
 * DataViewer class
 *
 * A viewer to display DataModel entities to the html page.
 *
 * @author Aaron Ponti
 **/

define([], function () {

    "use strict";

    // Constructor
    function DataViewer() {

        // Make sure we are using it as a class
        if (!(this instanceof DataViewer)) {
            throw new TypeError("DataViewer constructor cannot be called as a function.");
        }
    }

    /**
     * Methods
     */

    DataViewer.prototype = {

        constructor: DataViewer,

        /**
         * Draw the initial root structure.
         *
         * The tree will then be extended dynamically (via lazy
         * loading) using DynaTree methods.
         *
         * @param tree DynaTree object
         */
        drawTree: function(tree) {

            // Display the tree
            $("#treeView").dynatree(tree);

        },

        /**
         * Prepare a title div to be added to the page.
         *
         * @param title Text for the title
         * @param level One of "default", "info", "success", "warning", "danger". Default is "default".
         */
        prepareTitle: function(title, level) {


            // Make sure the level is valid
            if (["default", "success", "info", "warning", "danger"].indexOf(level) === -1) {
                level = "default";
            }

            return ($("<p>").append($("<span>").addClass("label").addClass("label-" + level).text(title)));

        },

        /**
         * Displays experiment info
         *
         * @param experimentSample openBIS Experiment object
         */
        displayExperimentInfo: function(experimentSample) {

            // Get the experiment name view
            var experimentNameView_div = $("#experimentNameView");
            experimentNameView_div.empty();

            // Prepare title
            var titleId = $("<h2>").html(experimentSample.properties["$NAME"]);

            // check that the experiment is at the latest version
            if (!experimentSample.properties[DATAMODEL.EXPERIMENT_PREFIX + "_EXPERIMENT_VERSION"] ||
                experimentSample.properties[DATAMODEL.EXPERIMENT_PREFIX + "_EXPERIMENT_VERSION"] < DATAMODEL.EXPERIMENT_LATEST_VERSION) {

                // Prepend "Upgrade" button
                var updateButton = $("<input>")
                    .attr("type", "button")
                    .attr("value", "Upgrade")
                    .addClass("upgradeButton")
                    .click(function () {

                        // Disable the Upgrade button
                        $(this).prop("disabled", true);

                        // Call the server-side plug-in
                        DATAMODEL.upgradeExperiment(DATAMODEL.exp.permId);
                    });
                titleId.prepend(updateButton);

            }

            experimentNameView_div.append(titleId);

            // Display the experiment info
            var detailView = $("#detailView");
            detailView.empty();

            var experimentDescriptionView = $("#experimentDescriptionView");
            experimentDescriptionView.empty();

            var experimentAcquisitionDetailsView = $("#experimentAcquisitionDetailsView");
            experimentAcquisitionDetailsView.empty();

            // Display the experiment description
            var description = experimentSample.properties[DATAMODEL.EXPERIMENT_PREFIX + "_EXPERIMENT_DESCRIPTION"];
            if (undefined === description || description === "") {
                description = "<i>No description provided.</i>";
            }
            experimentDescriptionView.append(this.prepareTitle("Description"));
            experimentDescriptionView.append($("<p>").html(description));

            // Display the acquisition details
            var hardwareName = "";
            if (experimentSample.properties[DATAMODEL.EXPERIMENT_PREFIX + "_EXPERIMENT_ACQ_HARDWARE_FRIENDLY_NAME"]) {
                hardwareName = experimentSample.properties[DATAMODEL.EXPERIMENT_PREFIX + "_EXPERIMENT_ACQ_HARDWARE_FRIENDLY_NAME"];
            } else {
                hardwareName = " (generic) " + experimentSample.properties[DATAMODEL.EXPERIMENT_PREFIX + "_EXPERIMENT_ACQ_HARDWARE"];
            }
            var owner = "an unknown user";
            if (experimentSample.properties[DATAMODEL.EXPERIMENT_PREFIX + "_EXPERIMENT_OWNER"] !== undefined) {
                owner = experimentSample.properties[DATAMODEL.EXPERIMENT_PREFIX + "_EXPERIMENT_OWNER"];
            }
            var acqDetails =
                experimentSample.properties[DATAMODEL.EXPERIMENT_PREFIX + "_EXPERIMENT_ACQ_SOFTWARE"] + " on " +
                "<b>" + hardwareName + "</b>. Acquired by " + owner + " and registered on " +
                (new Date(experimentSample.registrationDate)).toDateString() + ".";
            experimentAcquisitionDetailsView.append(this.prepareTitle("Acquisition details"));
            experimentAcquisitionDetailsView.append($("<p>").html(acqDetails));

            // Display the tags
            this.displayTags(experimentSample);

            // Display the attachments
            this.displayAttachments(experimentSample);
        },

        /**
         * Display attachment info and link to the Attachments tab.
         *
         * @param experimentSample {...}_EXPERIMENT sample.
         */
        displayAttachments: function(experimentSample) {

            // Get the div
            var experimentAttachmentsViewId = $("#experimentAttachmentsView");

            // Clear the attachment div
            experimentAttachmentsViewId.empty();

            // Text
            var text = "";
            var n = 0;
            for (var i = 0; i < experimentSample.dataSets.length; i++) {
                if (experimentSample.dataSets[i].type.code === "ATTACHMENT") {
                    n += 1;
                }
            }
            if (n === 0) {
                text = "There are no attachments.";
            } else if (n === 1) {
                text = "There is one attachment";
            } else {
                text = "There are " + n + " attachments.";
            }

            // Link to the data-sets tab
            var link = $("<a>").text(text).attr("href", "#").attr("title", text).click(
                function () {
                    var url = "#entity=SAMPLE&permId=" + experimentSample.permId +
                        "&ui-subtab=data-sets-section&ui-timestamp=" + (new Date().getTime());
                    window.top.location.hash = url;
                    return false;
                });

            experimentAttachmentsViewId.append(this.prepareTitle("Attachments"));

            // Display the link
            experimentAttachmentsViewId.append(link);

        },

        /**
         * Display attachment info and link to the Attachments tab.
         *
         * @param experimentSample {...}_EXPERIMENT sample.
         */
        displayTags: function(experimentSample) {

            // Get the div
            var experimentTagView = $("#experimentTagView");
            experimentTagView.empty();

            // Get sample tags
            var sampleTags = "<i>None</i>";
            if (experimentSample.parents) {
                if (experimentSample.parents.length === 0) {
                    sampleTags = "<i>None</i>";
                } else {
                    var tags = [];
                    for (var i = 0; i < experimentSample.parents.length; i++) {
                        if (experimentSample.parents[i].type.code === "ORGANIZATION_UNIT") {
                            tags.push(experimentSample.parents[i].properties["$NAME"]);
                        }
                    }
                    sampleTags = tags.join(", ");
                }
            }
            experimentTagView.append(this.prepareTitle("Tags", "info"));
            experimentTagView.append($("<p>").html(sampleTags));

        },

        /**
         * Display the node details and the actions associated with it
         *
         * @param {Object} node from the DynaTree model
         */
        displayDetailsAndActions: function(node) {

            // Store some references
            var statusID = $("#status");
            var detailViewSampleName = $("#detailViewSampleName");
            var detailViewActionID = $("#detailViewAction");
            var detailViewActionExplID = $("#detailViewActionExpl");
            var detailViewSampleID = $("#detailViewSample");
            var detailViewPlotID = $('#detailViewPlot');

            // Clear previous views
            statusID.empty();
            detailViewSampleName.empty();
            detailViewActionID.empty();
            detailViewActionExplID.empty();
            detailViewSampleID.empty();
            detailViewPlotID.empty();

            // In theory, an unselectable node should not be selectable
            // - we build in an additional check here
            if (node.data.unselectable === true) {

                return;
            }

            // Display the node name
            detailViewSampleName.append($("<h4>").html(node.data.title));

            if (node.data.type) {
                if (node.data.type === "plate_container") {
                    detailViewSampleID.append($("<p>").html("This is the set of all plates contained in this experiment."));
                } else if (node.data.type === "tubesets") {
                    detailViewSampleID.append($("<p>").html("This is the virtual set of all tubes contained in this experiment."));
                } else if (node.data.type === "specimen") {
                    detailViewSampleID.append($("<p>").html("This is a specimen."));
                } else {
                    // Ignore
                }
            }

            // Do we have an openBIS object?
            if (null === node.data.element) {
                return;
            }

            // Adapt the display depending on the element type
            if (node.data.element.getType().code.endsWith("_EXPERIMENT")) {

                detailViewSampleID.append($("<p>").html("This experiment was registered on " +
                    (new Date(node.data.element.registrationDate)).toDateString() + "."));

            } else if (node.data.element.getType().code.endsWith("_PLATE")) {

                // Update details
                detailViewSampleID.append($("<p>").html("This plate has geometry " +
                    node.data.element.properties[DATAMODEL.EXPERIMENT_PREFIX + "_PLATE_GEOMETRY"] + "."));

            } else if (node.data.element.getType().code === "FACS_ARIA_WELL" ||
                node.data.element.getType().code === "FACS_ARIA_TUBE" ||
                node.data.element.getType().code === "INFLUX_TUBE" ||
                node.data.element.getType().code === "S3E_TUBE" ||
                node.data.element.getType().code === "MOFLO_XDP_TUBE") {

                // This code is specific for the BD FACS ARIA sorter and BD Influx Cell Sorter

                var sortType = "This is a standard sort.";
                if (node.data.element.properties[node.data.element.getType().code + "_ISINDEXSORT"] === "true") {
                    sortType = "This is an index sort.";
                }
                detailViewSampleID.append($("<p>").html(sortType));

            } else if (node.data.element.getType().code.endsWith("_WELL")) {

                // Another type of well
                detailViewSampleID.append($("<p>").html("This is a well."));

            } else if (node.data.element.getType().code.endsWith("_FCSFILE")) {

                // Append the acquisition date if present
                if (node.data.element.properties[DATAMODEL.EXPERIMENT_PREFIX + "_FCSFILE_ACQ_DATE"]) {

                    detailViewSampleID.empty();
                    var title = $("<h4>").html(node.data.title);
                    var regDate = new Date(node.data.element.properties[
                    DATAMODEL.EXPERIMENT_PREFIX + "_FCSFILE_ACQ_DATE"]).toDateString();
                    title.append($("<span>").addClass("fcsAcqDate").html(" (acquired " + regDate + ")"));
                    detailViewSampleID.append(title);
                }

                // Old experiments might not have anything stored in {exp_prefix}_FCSFILE_PARAMETERS.
                if (!node.data.element.properties[DATAMODEL.EXPERIMENT_PREFIX + "_FCSFILE_PARAMETERS"]) {
                    detailViewSampleID.append($("<p>").html(
                        "Sorry, there is no parameter information stored for this file. Please upgrade the experiment."));

                    return;
                }

                // Retrieve and store the parameter information
                DATAMODEL.getAndAddParameterInfoForDatasets(node, function () {

                    // Display the form to be used for parameter plotting
                    DATAVIEWER.renderParameterSelectionForm(node);

                });

            } else {
                // Nothing to do.
            }

            // Display the download action
            this.displayDownloadAction(node);

            // Display the export action
            this.displayExportAction(node);

        },

        /**
         * Build and display the code to trigger the server-side aggregation
         * plugin 'copy_datasets_to_userdir'
         *
         * @param node: DataTree node
         */
        displayExportAction: function(node) {

            // Get the type and identifier of the element associated to the node.
            // If the node is associated to a specimen, the type and identifier
            // will instead be those of the parent node.
            var type = "";
            var identifier = "";
            var specimenName = "";
            var experimentId = "";

            // Get element type and code
            if (node.data.element) {

                type = node.data.element.getType().code;
                if (node.data.element.identifier) {
                    identifier = node.data.element.identifier.identifier;
                } else {
                    identifier = "";
                }
                specimenName = "";
                experimentId = DATAMODEL.experimentSample.getExperiment().identifier;

                // // Get type
                // switch (node.data.element["@type"]) {
                //     case "Experiment":
                //         // experimentId = node.data.element.identifier;
                //         // type = node.data.element.experimentTypeCode;
                //         // identifier = node.data.element.identifier;
                //         break;
                //
                //     case "Sample":
                //         // experimentId = node.data.element.experimentIdentifierOrNull;
                //         // type = node.data.element.sampleTypeCode;
                //         // identifier = node.data.element.identifier;
                //         break;
                //
                //     case "DataSet":
                //         // experimentId = node.data.element.experimentIdentifier;
                //         // type = node.data.element.dataSetTypeCode;
                //         // identifier = node.data.element.code;
                //         break;
                //
                //     default:
                //         experimentId = "";
                //         type = "";
                //         identifier = "";
                // }

            } else {

                if (node.data.type && node.data.type === "specimen") {

                    // Get the specimen name.
                    specimenName = node.data.title;

                    // In case of a specimen, we filter WELLS or TUBES for the
                    // associated property {exp_prefix}_SPECIMEN.
                    // We must treat the two cases differently, though.
                    //
                    // In the case of wells, we can make use of the fact that
                    // wells are contained in a plate. So we can use the plate
                    // identifier to get the wells, and then filter them by
                    // specimen.
                    //
                    // In the case of tubes, they do not have a parent, so we
                    // simply need to get all tubes in the experiment and check
                    // that their {exp_prefix}_SPECIMEN property matches the
                    // given specimen.

                    // Do we have a parent?
                    if (node.parent && node.parent.data && node.parent.data.element) {

                        // Reference
                        var parent = node.parent;

                        if (parent.data.element["@type"] === "Sample" &&
                            parent.data.element.sampleTypeCode === (DATAMODEL.EXPERIMENT_PREFIX + "_PLATE")) {

                            // Type
                            type = DATAMODEL.EXPERIMENT_PREFIX + "_PLATE";

                            // Get plate's identifier
                            identifier = parent.data.element.identifier;

                            // Experiment ID
                            experimentId = parent.data.element.experimentIdentifierOrNull;

                        }

                    } else {

                        // We set the parent to point to the experiment
                        type = DATAMODEL.EXPERIMENT_PREFIX + "_TUBESET";

                        // Walk up the tree until we reach the experiment
                        while (node.parent) {
                            node = node.parent;
                            if (node.data.element &&
                                node.data.element["@type"] === "Experiment") {
                                identifier = node.data.element.identifier;
                                break;
                            }
                        }

                        // Experiment ID (same as identifier)
                        experimentId = identifier;

                    }

                } else if (node.data.type && node.data.type === "tubesets") {

                    // If there are no (loaded) children (yet), just return
                    if (!node.childList || node.childList.length == 0) {
                        if (node._isLoading) {
                            this.displayStatus("The actions for this node will be displayed next time you select it.</br />",
                                "info");
                        }
                        return;
                    }

                    // Do we have real samples?
                    if (node.childList.length === 1 &&
                        node.childList[0].data &&
                        node.childList[0].data.icon === "empty.png" &&
                        node.childList[0].data.title === "<i>None</i>" != -1) {
                        return;
                    }

                    // This is the same as the tubeset case before, but without
                    // filtering by the specimen

                    // Empty specimen
                    specimenName = "";

                    // Tubeset
                    type = DATAMODEL.EXPERIMENT_PREFIX + "_TUBESET";

                    // Walk up the tree until we reach the experiment
                    while (node.parent) {
                        node = node.parent;
                        if (node.data.element &&
                            node.data.element["@type"] === "Experiment") {
                            identifier = node.data.element.identifier;
                            break;
                        }
                    }

                    // Experiment ID (same as identifier)
                    experimentId = identifier;

                } else if (node.data.type && node.data.type === "plate_container") {

                    // If there are no (loaded) children (yet), just return
                    if (!node.childList || node.childList.length === 0) {
                        if (node._isLoading) {
                            this.displayStatus("The actions for this node will be displayed next time you select it.</br />",
                                "info");
                        }
                        return;
                    }

                    // Do we have real samples?
                    if (node.childList.length === 1 &&
                        node.childList[0].data &&
                        node.childList[0].data.icon === "empty.png" &&
                        node.childList[0].data.title === "<i>None</i>" != -1) {
                        return;
                    }

                    // Empty specimen
                    specimenName = "";

                    // All plates in the experiment
                    type = DATAMODEL.EXPERIMENT_PREFIX + "_ALL_PLATES";

                    // Walk up the tree until we reach the experiment
                    while (node.parent) {
                        node = node.parent;
                        if (node.data.element &&
                            node.data.element["@type"] === "Experiment") {
                            identifier = node.data.element.identifier;
                            break;
                        }
                    }

                    // Experiment ID (same as identifier)
                    experimentId = identifier;

                }

            }

            var img = null;
            var link = null;

            // Display the "Export to your folder" button only if enabled in the configuration file
            if (CONFIG['enableExportToUserFolder'] === true) {

                img = $("<img>")
                    .attr("src", "img/export.png")
                    .attr("width", 32)
                    .attr("height", 32);

                link = $("<a>")
                    .addClass("action")
                    .attr("href", "#")
                    .html("")
                    .hover(function () {
                            $("#detailViewActionExpl").html("Export to your folder.");
                        },
                        function () {
                            $("#detailViewActionExpl").html("");
                        })
                    .attr("title", "")
                    .click(function () {
                        DATAMODEL.exportDatasets(
                            experimentId, DATAMODEL.experimentType,
                            type, identifier, specimenName, "normal");
                        return false;
                    });

                link.prepend(img);

                $("#detailViewAction").append(link);

            }

            img = $("<img>")
                .attr("src", "img/zip.png")
                .attr("width", 32)
                .attr("height", 32);

            link = $("<a>")
                .addClass("action")
                .attr("href", "#")
                .html("")
                .attr("title", "")
                .hover(function () {
                        $("#detailViewActionExpl").html("Download archive.");
                    },
                    function () {
                        $("#detailViewActionExpl").html("");
                    })
                .click(function () {
                    DATAMODEL.exportDatasets(
                        experimentId, DATAMODEL.experimentType,
                        type, identifier, specimenName, "zip");
                    return false;
                });

            link.prepend(img);

            $("#detailViewAction").append(link);

        },

        /**
         * Build and display the link to download the FCS file via browser
         *
         * @param node: DataTree node
         */
        displayDownloadAction: function(node) {

            // Build and display the call
            if (node.data.element && node.data.element.hasOwnProperty("url")) {

                var img = $("<img>")
                    .attr("src", "img/download.png")
                    .attr("width", 32)
                    .attr("height", 32);

                var link = $("<a>")
                    .addClass("action")
                    .hover(function () {
                            $("#detailViewActionExpl").html("Download " + node.data.element.filename + ".");
                        },
                        function () {
                            $("#detailViewActionExpl").html("");
                        })
                    .attr("href", node.data.element.url)
                    .attr("title", "")
                    .html("")


                link.prepend(img);
                link.prepend(img);

                $("#detailViewAction").append(link);

            }
        },

        /**
         * Display status text color-coded by level.
         *
         * @param status: text to be displayed
         * @param level: one of "default", "info", "success", "warning", "danger". Default is "default".
         */
        displayStatus: function(status, level) {

            // Get the the status div
            var status_div = $("#status");

            // Clear the status
            status_div.empty();

            // Make sure the status div is visible
            status_div.show();

            // Make sure the level is valid
            if (["default", "success", "info", "warning", "danger"].indexOf(level) === -1) {
                level = "default";
            }

            var d = $("<div>")
                .addClass("alert fade in")
                .addClass("alert-" + level)
                .html(status);
            status_div.append(d);
        },

        /**
         * Hide the status div.
         */
        hideStatus: function() {
            $("#status").hide();
        },

        /**
         * Display the form with the parameter selections for the plotting.
         *
         * @param node: Tree node
         */
        renderParameterSelectionForm: function(node) {

            // Check that the parameter info is present
            if (!node.data.parameterInfo) {
                return;
            }

            // Update details
            var detailViewSampleID = $("#detailViewSample");

            detailViewSampleID.append($("<p>").html("This file contains " +
                node.data.parameterInfo.numParameters + " parameters and " +
                node.data.parameterInfo.numEvents + " events.")
            );

            // Create a form for the plot parameters
            var form = $("<form>")
                .addClass("form-group")
                .attr("id", "parameter_form");
            detailViewSampleID.append(form);
            var formId = $("#parameter_form");

            // Create divs to spatially organize the groups of parameters
            var xAxisDiv = $("<div>")
                .attr("id", "xAxisDiv")
                .addClass("plotBasicParamsDiv");
            var yAxisDiv = $("<div>")
                .attr("id", "yAxisDiv")
                .addClass("plotBasicParamsDiv");
            var eventsDiv = $("<div>")
                .attr("id", "eventsDiv")
                .addClass("plotBasicParamsDiv");
            var plotDiv = $("<div>")
                .attr("id", "plotDiv")
                .addClass("plotBasicParamsDiv");

            // Add them to the form
            formId.append(xAxisDiv);
            formId.append(yAxisDiv);
            formId.append(eventsDiv);
            formId.append(plotDiv);

            // X axis parameters
            xAxisDiv.append($("<label>")
                .attr("for", "parameter_form_select_X_axis")
                .html("X axis"));
            var selectXAxis = $("<select>")
                .addClass("form_control")
                .attr("id", "parameter_form_select_X_axis");
            xAxisDiv.append(selectXAxis);
            var selectXAxisId = $("#parameter_form_select_X_axis");

            // Y axis parameters
            yAxisDiv.append($("<label>")
                .attr("for", "parameter_form_select_Y_axis")
                .html("Y axis"));
            var selectYAxis = $("<select>")
                .addClass("form_control")
                .attr("id", "parameter_form_select_Y_axis");
            yAxisDiv.append(selectYAxis);
            var selectYAxisId = $("#parameter_form_select_Y_axis");

            // Add all options
            for (var i = 0; i < node.data.parameterInfo.numParameters; i++) {
                var name = node.data.parameterInfo["names"][i];
                var compositeName = node.data.parameterInfo["compositeNames"][i];
                selectXAxisId.append($("<option>")
                    .attr("value", name)
                    .text(compositeName));
                selectYAxisId.append($("<option>")
                    .attr("value", name)
                    .text(compositeName));
            }

            // Pre-select some parameters
            selectXAxisId.val(node.data.parameterInfo["names"][0]);
            selectYAxisId.val(node.data.parameterInfo["names"][1]);

            // Add a selector with the number of events to plot
            eventsDiv.append($("<label>")
                .attr("for", "parameter_form_select_num_events")
                .html("Events to plot"));
            var selectNumEvents = $("<select>")
                .addClass("form_control")
                .attr("id", "parameter_form_select_num_events");
            eventsDiv.append(selectNumEvents);
            var selectNumEventsId = $("#parameter_form_select_num_events");

            // Add the options
            var possibleOptions = [500, 1000, 2500, 5000, 10000, 20000, 50000, 100000];
            var numEventsInFile = parseInt(node.data.parameterInfo.numEvents);
            for (i = 0; i < possibleOptions.length; i++) {
                if (possibleOptions[i] < numEventsInFile) {
                    selectNumEventsId.append($("<option>")
                        .attr("value", possibleOptions[i])
                        .text(possibleOptions[i].toString()));
                }
            }
            selectNumEventsId.append($("<option>")
                .attr("value", node.data.parameterInfo.numEvents)
                .text(parseInt(node.data.parameterInfo.numEvents)));

            // Pre-select something reasonable
            if (node.data.parameterInfo.numEvents > possibleOptions[4]) {
                selectNumEventsId.val(parseInt(possibleOptions[4]));
            } else {
                selectNumEventsId.val(parseInt(node.data.parameterInfo.numEvents));
            }

            // Add "Plot" button
            var plotButton = $("<input>")
                .attr("type", "button")
                .attr("value", "Plot")
                .click(function () {

                    // Get the selected parameters and their display scaling
                    var paramX = selectXAxisId.find(":selected").val();
                    var paramY = selectYAxisId.find(":selected").val();
                    var displayX = selectScaleX.find(":selected").val();
                    var displayY = selectScaleY.find(":selected").val();

                    // How many events to plot?
                    var numEvents = selectNumEvents.val();

                    // Sampling method
                    var samplingMethod = selectSamplingMethod.find(":selected").val();

                    DATAMODEL.generateFCSPlot(
                        node,
                        node.data.element.code,
                        paramX,
                        paramY,
                        displayX,
                        displayY,
                        numEvents,
                        samplingMethod);
                });
            plotDiv.append(plotButton);

            // Add a selector with the scaling for axis X
            var xAxisScalingDiv = xAxisDiv.append($("<div>")
                .attr("id", "xAxisScalingDiv"));
            var xAxisScalingdId = $("#xAxisScalingDiv");
            xAxisScalingdId.append($("<label>")
                .attr("for", "parameter_form_select_scaleX")
                .html("Scale for X axis"));
            var selectScaleX = $("<select>")
                .addClass("form_control")
                .attr("id", "parameter_form_select_scaleX");
            xAxisScalingdId.append(selectScaleX);

            // Add the options
            possibleOptions = ["Linear", "Hyperlog"];
            for (i = 0; i < possibleOptions.length; i++) {
                selectScaleX.append($("<option>")
                    .attr("value", possibleOptions[i])
                    .text(possibleOptions[i]));
            }

            // Pre-select "Linear"
            selectScaleX.val(0);

            // Add a selector with the scaling for axis Y
            var yAxisScalingDiv = yAxisDiv.append($("<div>")
                .attr("id", "yAxisScalingDiv"));
            var yAxisScalingId = $("#yAxisScalingDiv");
            yAxisScalingId.append($("<label>")
                .attr("for", "parameter_form_select_scaleY")
                .html("Scale for Y axis"));
            var selectScaleY = $("<select>")
                .addClass("form_control")
                .attr("id", "parameter_form_select_scaleY");
            yAxisScalingId.append(selectScaleY);

            // Add the options
            possibleOptions = ["Linear", "Hyperlog"];
            for (var i = 0; i < possibleOptions.length; i++) {
                selectScaleY.append($("<option>")
                    .attr("value", possibleOptions[i])
                    .text(possibleOptions[i]));
            }

            // Pre-select "Linear"
            selectScaleY.val(0);

            // Add a selector with the sampling method
            var eventSamplingDiv = eventsDiv.append($("<div>")
                .attr("id", "eventSamplingDiv"));
            var eventSamplingId = $("#eventSamplingDiv");
            eventSamplingId.append($("<label>")
                .attr("for", "parameter_form_select_sampling_method")
                .html("Sampling"));
            var selectSamplingMethod = $("<select>")
                .addClass("form_control")
                .attr("id", "parameter_form_select_sampling_method");
            eventSamplingId.append(selectSamplingMethod);

            // Add the options
            possibleOptions = ["Regular", "First rows"];
            for (i = 0; i < possibleOptions.length; i++) {
                selectSamplingMethod.append($("<option>")
                    .attr("value", (i + 1))
                    .text(possibleOptions[i]));
            }

            // Pre-select "Linear"
            selectSamplingMethod.val(0);
        },

        /**
         * Display a scatter plot using HighCharts
         *
         * @param data list of (X, Y) points
         * @param xLabel X label
         * @param yLabel Y label
         * @param xDisplay string Display type of the parameter for the X axis ("Linear" or "Hyperlog")
         * @param yDisplay string Display type of the parameter for the Y axis ("Linear" or "Hyperlog")
         */
        plotFCSData: function(data, xLabel, yLabel, xDisplay, yDisplay) {

            // Make sure to have a proper array
            data = JSON.parse(data);

            // Axis type is always linear, since the transformations are all done server side.
            var xType = "linear";
            var yType = "linear";

            $('#detailViewPlot').highcharts({
                chart: {
                    type: 'scatter',
                    zoomType: 'xy'
                },
                title: {
                    text: yLabel + " vs. " + xLabel
                },
                subtitle: {
                    text: ''
                },
                xAxis: {
                    title: {
                        enabled: true,
                        text: xLabel
                    },
                    type: xType,
                    startOnTick: true,
                    endOnTick: true,
                    showLastLabel: true
                },
                yAxis: {
                    title: {
                        enabled: true,
                        text: yLabel
                    },
                    type: yType,
                    startOnTick: true,
                    endOnTick: true,
                    showLastLabel: true
                },
                plotOptions: {
                    area: {
                        turboThreshold: 10
                    },
                    scatter: {
                        marker: {
                            radius: 1,
                            states: {
                                hover: {
                                    enabled: true,
                                    lineColor: 'rgb(100,100,100)'
                                }
                            }
                        },
                        states: {
                            hover: {
                                marker: {
                                    enabled: false
                                }
                            }
                        },
                        tooltip: {
                            headerFormat: '',
                            pointFormat: '{point.x:.2f}, {point.y:.2f}'
                        }
                    }
                },
                series: [{
                    name: '',
                    color: 'rgba(223, 83, 83, .5)',
                    data: data
                }]
            });
        },

        /**
         * Caches the retrieved FCS data to the node with given key under its own data key.
         *
         * @param nodeKey key of the node to update.
         * @param dataKey key under which to store the data.
         * @param fcsData data to be plotted.
         */
        cacheFCSData: function(nodeKey, dataKey, fcsData) {

            // Retrieve the tree object
            var tree = $("#treeView").dynatree("getTree");
            if (tree) {
                // Load the node with specified key path
                var node = tree.getNodeByKey(nodeKey);
                if (node) {

                    // Cache the data
                    if (!node.data.cached) {
                        node.data.cached = {};
                    }
                    node.data.cached[dataKey] = fcsData;
                }
            }
        },
    };

    // Return a DataViewer object
    return DataViewer;
});
