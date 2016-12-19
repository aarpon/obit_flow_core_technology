/**
 * This file configures the web application.
 */

// WEB APPLICATION CONFIGURATION SETTINGS
var CONFIG = {};

// Set to true to enable an action button "Export to your user folder"
// to export the data to the user folder as configured in
// `dss/reporting-plugins/export_microscopy_datasets/plugin.properties`.
//
// Possible values: true | false.
CONFIG.enableExportToUserFolder = true;

// Set the target datastore server if the default value ("DSS1") is not correct.
//
// See variable `data-store-server-code` in DSS configuration file
// `openbis/servers/datastore_server/etc/service.properties'.
CONFIG.dataStoreServer = "DSS1";
