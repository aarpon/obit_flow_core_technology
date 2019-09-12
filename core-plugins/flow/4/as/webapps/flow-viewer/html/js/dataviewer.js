/**
 * DataViewer class
 *
 * A viewer to display DataModel entities to the html page.
 *
 * @author Aaron Ponti
 **/

define(["d3", "c3"], function (d3, c3) {

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
         * Caches the retrieved FCS data to the node with given key under its own data key.
         *
         * @param nodeKey key of the node to update.
         * @param dataKey key under which to store the data.
         * @param fcsData data to be plotted.
         */
        cacheFCSData: function(nodeKey, dataKey, fcsData) {

            // Retrieve the tree object
            let tree = $("#treeView").dynatree("getTree");
            if (tree) {
                // Load the node with specified key path
                let node = tree.getNodeByKey(nodeKey);
                if (node) {

                    // Cache the data
                    if (!node.data.cached) {
                        node.data.cached = {};
                    }
                    node.data.cached[dataKey] = fcsData;
                }
            }
        },

        /**
         * Display attachment info and link to the Attachments tab.
         *
         * @param experimentSample {...}_EXPERIMENT sample.
         */
        displayAttachments: function(experimentSample) {

            // Get the div
            let experimentAttachmentsViewId = $("#experimentAttachmentsView");

            // Clear the attachment div
            experimentAttachmentsViewId.empty();

            // Text
            let text = "";
            let n = 0;
            for (let i = 0; i < experimentSample.dataSets.length; i++) {
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
            let link = $("<a>")
                .text(text)
                .attr("href", "#")
                .attr("title", text)
                .click(
                function () {
                    let url = "#entity=SAMPLE&permId=" + experimentSample.permId +
                        "&ui-subtab=data-sets-section&ui-timestamp=" + (new Date().getTime());
                    window.top.location.hash = url;
                    return false;
                });

            experimentAttachmentsViewId.append(this.prepareTitle("Attachments"));

            // Display the link
            experimentAttachmentsViewId.append(link);

        },

        /**
         * Display the node details and the actions associated with it
         *
         * @param {Object} node from the DynaTree model
         */
        displayDetailsAndActions: function(node) {

            // Store some references
            let statusID = $("#status");
            let detailViewSampleName = $("#detailViewSampleName");
            let detailViewActionID = $("#detailViewAction");
            let detailViewActionExplID = $("#detailViewActionExpl");
            let detailViewSampleID = $("#detailViewSample");
            let detailViewPlotID = $('#detailViewPlot');

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
                if (node.data.type === "ALL_PLATES") {
                    detailViewSampleID
                        .append($("<p>")
                            .html("This is the set of all plates contained in this experiment."));
                } else if (node.data.type === "TUBESET") {
                    detailViewSampleID
                        .append($("<p>")
                            .html("This is the virtual set of all tubes contained in this experiment."));
                } else if (node.data.type === "SPECIMEN") {
                    detailViewSampleID
                        .append($("<p>")
                            .html("This is a specimen."));
                } else {
                    // Ignore
                }
            }

            // Display the download action
            this.displayDownloadAction(node);

            // Display the export action
            this.displayExportAction(node);

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

            } else if (node.data.element.getType().code.endsWith("_TUBE")) {

                let sortType = "This is a standard sort.";
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
                    let title = $("<h4>").html(node.data.title);
                    let regDate = new Date(node.data.element.properties[
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
                DATAMODEL.getAndAddParameterInfoForDataSets(node, function () {

                    // Display the form to be used for parameter plotting
                    DATAVIEWER.renderParameterSelectionForm(node);

                });

            } else {
                // Nothing to do.
            }
        },

        /**
         * Build and display the link to download the FCS file via browser
         *
         * @param node: DataTree node
         */
        displayDownloadAction: function(node) {

            // Build and display the call
            if (node.data.element && node.data.element.hasOwnProperty("url")) {

                let img = $("<img>")
                    .attr("src", "img/download.png")
                    .attr("width", 32)
                    .attr("height", 32);

                let link = $("<a>")
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
         * Displays experiment info
         *
         * @param experimentSample openBIS Experiment object
         */
        displayExperimentInfo: function(experimentSample) {

            // Get the experiment name view
            let experimentNameView_div = $("#experimentNameView");
            experimentNameView_div.empty();

            // Prepare title
            let titleId = $("<h2>").html(experimentSample.properties["$NAME"]);

            experimentNameView_div.append(titleId);

            // Display the experiment info
            let detailView = $("#detailView");
            detailView.empty();

            let experimentDescriptionView = $("#experimentDescriptionView");
            experimentDescriptionView.empty();

            let experimentAcquisitionDetailsView = $("#experimentAcquisitionDetailsView");
            experimentAcquisitionDetailsView.empty();

            // Display the experiment description
            let description = experimentSample.properties[DATAMODEL.EXPERIMENT_PREFIX + "_EXPERIMENT_DESCRIPTION"];
            if (undefined === description || description === "") {
                description = "<i>No description provided.</i>";
            }
            experimentDescriptionView.append(this.prepareTitle("Description"));
            experimentDescriptionView.append($("<p>").html(description));

            // Display the acquisition details
            let hardwareName = "";
            if (experimentSample.properties[DATAMODEL.EXPERIMENT_PREFIX + "_EXPERIMENT_ACQ_HARDWARE_FRIENDLY_NAME"]) {
                hardwareName = experimentSample.properties[DATAMODEL.EXPERIMENT_PREFIX + "_EXPERIMENT_ACQ_HARDWARE_FRIENDLY_NAME"];
            } else {
                hardwareName = " (generic) " + experimentSample.properties[DATAMODEL.EXPERIMENT_PREFIX + "_EXPERIMENT_ACQ_HARDWARE"];
            }
            let owner = "an unknown user";
            if (experimentSample.properties[DATAMODEL.EXPERIMENT_PREFIX + "_EXPERIMENT_OWNER"] !== undefined) {
                owner = experimentSample.properties[DATAMODEL.EXPERIMENT_PREFIX + "_EXPERIMENT_OWNER"];
            }
            let acqDetails =
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
         * Build and display the code to trigger the server-side aggregation
         * plugin 'copy_datasets_to_userdir'
         *
         * @param node: DataTree node
         */
        displayExportAction: function(node) {

            // Get the type and identifier of the element associated to the node.
            // If the node is associated to a specimen, the type and identifier
            // will instead be those of the parent node.

            // Prepare the arguments for the service

            // Constants
            const collectionId = DATAMODEL.experimentSample.experiment.getIdentifier().identifier;
            const collectionType = DATAMODEL.experimentSample.experiment.getType().code;
            const experimentSampleId = DATAMODEL.experimentSampleId;
            const experimentSamplePermId = DATAMODEL.experimentSample.getPermId().toString();
            const experimentSampleType = DATAMODEL.experimentSampleType;

            // Variables
            let platePermId = "";
            let plateType = "";
            let task = "";

            // Now process the nodes. Only some will have an
            // export action associated to them!
            if (node.data.type === "EXPERIMENT_SAMPLE") {
                task = "EXPERIMENT_SAMPLE";
                platePermId = "";
                plateType = "";
            } else if (node.data.type === "ALL_PLATES") {
                task = "ALL_PLATES";
                platePermId = "";
                plateType = "";
            } else if (node.data.type === "PLATE") {
                task = "PLATE";
                platePermId = node.data.element.getPermId().permId;
                plateType = node.data.element.getType().code;
            } else if (node.data.type === "TUBESET") {
                task = "TUBESET";
                platePermId = "";
                plateType = "";
            } else {
                return;
            }

            let img = null;
            let link = null;

            // Display the "Export to your folder" button only if enabled in the configuration file
            if (CONFIG['enableExportToUserFolder'] === true) {

                img = $("<img>")
                    .attr("src", "img/export.png")
                    .attr("width", 32)
                    .attr("height", 32);

                let tooltip = "";
                switch(task) {
                    case "EXPERIMENT_SAMPLE":
                        tooltip = "Export whole experiment to your folder.";
                        break;
                    case "ALL_PLATES":
                        tooltip = "Export all plates to your folder.";
                        break;
                    case "TUBESET":
                        tooltip = "Export all tubes to your folder.";
                        break;
                    case "PLATE":
                        tooltip = "Export currently selected plate to your folder.";
                        break;
                    default:
                        tooltip = "Export to your folder.";
                        break;
                }

                link = $("<a>")
                    .addClass("action")
                    .attr("href", "#")
                    .html("")
                    .hover(function () {
                            $("#detailViewActionExpl").html(tooltip);
                        },
                        function () {
                            $("#detailViewActionExpl").html("");
                        })
                    .attr("title", "")
                    .click(function () {
                        DATAMODEL.callServerSidePluginExportDataSets(
                            task, collectionId, collectionType,
                            experimentSampleId, experimentSamplePermId,
                            experimentSampleType, platePermId, plateType, "normal");
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
                    DATAMODEL.callServerSidePluginExportDataSets(
                        task, collectionId, collectionType,
                        experimentSampleId, experimentSamplePermId,
                        experimentSampleType, platePermId, plateType, "zip");
                    return false;
                });

            link.prepend(img);

            $("#detailViewAction").append(link);

        },

        /**
         * Display status text color-coded by level.
         *
         * @param status: text to be displayed
         * @param level: one of "default", "info", "success", "warning", "danger". Default is "default".
         */
        displayStatus: function(status, level) {

            // Get the the status div
            let status_div = $("#status");

            // Clear the status
            status_div.empty();

            // Make sure the status div is visible
            status_div.show();

            // Make sure the level is valid
            if (["default", "success", "info", "warning", "danger"].indexOf(level) === -1) {
                level = "default";
            }

            let d = $("<div>")
                .addClass("alert fade in")
                .addClass("alert-" + level)
                .html(status);
            status_div.append(d);
        },

        /**
         * Display attachment info and link to the Attachments tab.
         *
         * @param experimentSample {...}_EXPERIMENT sample.
         */
        displayTags: function(experimentSample) {

            // Get the div
            let experimentTagView = $("#experimentTagView");
            experimentTagView.empty();

            // Get sample tags
            let sampleTags = "<i>None</i>";
            if (experimentSample.parents) {
                if (experimentSample.parents.length === 0) {
                    sampleTags = "<i>None</i>";
                } else {
                    let tags = [];
                    for (let i = 0; i < experimentSample.parents.length; i++) {
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
         * Hide the status div.
         */
        hideStatus: function() {
            $("#status").hide();
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
            let parsed_data = JSON.parse(data);

            // Prepend data names to be compatible with C3.js
            parsed_data[0].unshift("x_values");
            parsed_data[1].unshift("y_values");

            // Plot the data
            c3.generate({
                bindto: '#detailViewPlot',
                title: {
                    text: yLabel + " vs. " + xLabel
                },
                data: {
                    xs: {
                        y_values: "x_values"
                    },
                    columns: [
                        parsed_data[0],
                        parsed_data[1],
                    ],
                    names: {
                        y_values: yLabel
                    },
                    type: 'scatter'
                },
                axis: {
                    x: {
                        label: xLabel,
                        tick: {
                            fit: false
                        }
                    },
                    y: {
                        label: yLabel,
                        tick: {
                            fit: false
                        }
                    }
                },
                legend: {
                    show: false
                },
                tooltip: {
                    format: {
                        title: function (d) {
                            const format = d3.format(',');
                            return xLabel + " | " + format(d);
                            },
                        value: function (value, ratio, id) {
                            const format = d3.format(',');
                            return format(value);
                        }
                    }
                },
                zoom: {
                    enabled: true,
                    rescale: true
                },
            });
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
         * Display the form with the parameter selections for the plotting.
         *
         * @param node: Tree node
         */
        renderParameterSelectionForm: function(node) {

            // Get details div
            let detailViewSampleID = $("#detailViewSample");

            // Check that the parameter info is present
            if (! node.data.parameterInfo) {

                // Display a button to trigger experiment update
                let updateButton = $("<input>")
                    .attr("type", "button")
                    .attr("value", "Upgrade experiment")
                    .addClass("upgradeButton")
                    .click(function () {

                        // Disable the Upgrade button
                        $(this).prop("disabled", true);
                        $(this).attr("value", "Please wait: the page will reload automatically")

                        // Call the server-side plug-in
                        DATAMODEL.callServerSidePluginUpgradeExperiment(
                            DATAMODEL.experimentSample.experiment.permId.permId,
                            DATAMODEL.experimentSample.permId.permId);
                    });
                    detailViewSampleID.empty();
                    detailViewSampleID.append(updateButton);

                return;
            }

            detailViewSampleID.append($("<p>").html("This file contains " +
                node.data.parameterInfo.numParameters + " parameters and " +
                node.data.parameterInfo.numEvents + " events.")
            );

            // Create a form for the plot parameters
            let form = $("<form>")
                .addClass("form-group")
                .attr("id", "parameter_form");
            detailViewSampleID.append(form);
            let formId = $("#parameter_form");

            // Create divs to spatially organize the groups of parameters
            let xAxisDiv = $("<div>")
                .attr("id", "xAxisDiv")
                .addClass("plotBasicParamsDiv");
            let yAxisDiv = $("<div>")
                .attr("id", "yAxisDiv")
                .addClass("plotBasicParamsDiv");
            let eventsDiv = $("<div>")
                .attr("id", "eventsDiv")
                .addClass("plotBasicParamsDiv");
            let plotDiv = $("<div>")
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
            let selectXAxis = $("<select>")
                .addClass("form_control")
                .attr("id", "parameter_form_select_X_axis");
            xAxisDiv.append(selectXAxis);
            let selectXAxisId = $("#parameter_form_select_X_axis");

            // Y axis parameters
            yAxisDiv.append($("<label>")
                .attr("for", "parameter_form_select_Y_axis")
                .html("Y axis"));
            let selectYAxis = $("<select>")
                .addClass("form_control")
                .attr("id", "parameter_form_select_Y_axis");
            yAxisDiv.append(selectYAxis);
            let selectYAxisId = $("#parameter_form_select_Y_axis");

            // Add all options
            for (let i = 0; i < node.data.parameterInfo.numParameters; i++) {
                let name = node.data.parameterInfo["names"][i];
                let compositeName = node.data.parameterInfo["compositeNames"][i];
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
            let selectNumEvents = $("<select>")
                .addClass("form_control")
                .attr("id", "parameter_form_select_num_events");
            eventsDiv.append(selectNumEvents);
            let selectNumEventsId = $("#parameter_form_select_num_events");

            // Add the options
            let possibleOptions = [500, 1000, 2500, 5000, 10000, 20000, 50000, 100000];
            let numEventsInFile = parseInt(node.data.parameterInfo.numEvents);
            for (let i = 0; i < possibleOptions.length; i++) {
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
            let plotButton = $("<input>")
                .attr("type", "button")
                .attr("value", "Plot")
                .click(function () {

                    // Get the selected parameters and their display scaling
                    let paramX = selectXAxisId.find(":selected").val();
                    let paramY = selectYAxisId.find(":selected").val();
                    let displayX = selectScaleX.find(":selected").val();
                    let displayY = selectScaleY.find(":selected").val();

                    // How many events to plot?
                    let numEvents = selectNumEvents.val();

                    // Sampling method
                    let samplingMethod = selectSamplingMethod.find(":selected").val();

                    DATAMODEL.callServerSidePluginGenerateFCSPlot(
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
            let xAxisScalingDiv = xAxisDiv.append($("<div>")
                .attr("id", "xAxisScalingDiv"));
            let xAxisScalingdId = $("#xAxisScalingDiv");
            xAxisScalingdId.append($("<label>")
                .attr("for", "parameter_form_select_scaleX")
                .html("Scale for X axis"));
            let selectScaleX = $("<select>")
                .addClass("form_control")
                .attr("id", "parameter_form_select_scaleX");
            xAxisScalingdId.append(selectScaleX);

            // Add the options
            possibleOptions = ["Linear", "Hyperlog"];
            for (let i = 0; i < possibleOptions.length; i++) {
                selectScaleX.append($("<option>")
                    .attr("value", possibleOptions[i])
                    .text(possibleOptions[i]));
            }

            // Pre-select "Linear"
            selectScaleX.val(0);

            // Add a selector with the scaling for axis Y
            let yAxisScalingDiv = yAxisDiv.append($("<div>")
                .attr("id", "yAxisScalingDiv"));
            let yAxisScalingId = $("#yAxisScalingDiv");
            yAxisScalingId.append($("<label>")
                .attr("for", "parameter_form_select_scaleY")
                .html("Scale for Y axis"));
            let selectScaleY = $("<select>")
                .addClass("form_control")
                .attr("id", "parameter_form_select_scaleY");
            yAxisScalingId.append(selectScaleY);

            // Add the options
            possibleOptions = ["Linear", "Hyperlog"];
            for (let i = 0; i < possibleOptions.length; i++) {
                selectScaleY.append($("<option>")
                    .attr("value", possibleOptions[i])
                    .text(possibleOptions[i]));
            }

            // Pre-select "Linear"
            selectScaleY.val(0);

            // Add a selector with the sampling method
            let eventSamplingDiv = eventsDiv.append($("<div>")
                .attr("id", "eventSamplingDiv"));
            let eventSamplingId = $("#eventSamplingDiv");
            eventSamplingId.append($("<label>")
                .attr("for", "parameter_form_select_sampling_method")
                .html("Sampling"));
            let selectSamplingMethod = $("<select>")
                .addClass("form_control")
                .attr("id", "parameter_form_select_sampling_method");
            eventSamplingId.append(selectSamplingMethod);

            // Add the options
            possibleOptions = ["Regular", "First rows"];
            for (let i = 0; i < possibleOptions.length; i++) {
                selectSamplingMethod.append($("<option>")
                    .attr("value", (i + 1))
                    .text(possibleOptions[i]));
            }

            // Pre-select "Linear"
            selectSamplingMethod.val(0);
        }
    };

    // Return a DataViewer object
    return DataViewer;
});
