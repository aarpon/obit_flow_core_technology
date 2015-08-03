# -*- coding: utf-8 -*-

'''
Aggregation plug-in to generate FCS plots.

The first time a scatterplot is generated for an FCS file, the data is read
and stored in a CSV file that is registered in openBIS.
Later plots will use the CSV file directly.

@author: Aaron Ponti
'''

import csv
import os.path
import logging
import java.io.File
import ch.ethz.scu.obit.bdfacsdivafcs.readers.FCSReader as FCSReader
import com.xhaus.jyson.JysonCodec as json


def setUpLogging():
    """Sets up logging and returns the logger object."""
    
    # Get path to containing folder
    # __file__ does not work (reliably) in Jython
    rpPath = "../core-plugins/flow/1/dss/reporting-plugins/generate_fcs_plots"

    # Path to the logs subfolder
    logPath = os.path.join(rpPath, "logs")

    # Make sure the logs subfolder exist
    if not os.path.exists(logPath):
        os.makedirs(logPath)

    # Path for the log file
    logFile = os.path.join(logPath, "generate_fcs_plot_log.txt")

    # Create the logger
    logging.basicConfig(filename=logFile, level=logging.DEBUG, 
                        format='%(asctime)-15s %(levelname)s: %(message)s')
    _logger = logging.getLogger("FlowFCSPlotter")

    return _logger

    
def getFileForCode(code):
    """
    Get the path to the FCS file that is associated to the given dataSet.
    If not files are found, returns [].
    """

    dataSetFiles = []

    content = contentProvider.getContent(code)
    nodes = content.listMatchingNodes("original", ".*\.fcs")
    if nodes is not None:
        for node in nodes:
            fileName = node.tryGetFile()
            if fileName is not None:
                fileName = str(fileName)
                if fileName.lower().endswith(".fcs"):
                    dataSetFiles.append(fileName)

    # Return the files
    return dataSetFiles


def getSessionCSVFileForFCSFile(fcsFile):
    """Return the path of the CSV file in the session workspace for the given FCS file."""

    # Get the session workspace
    sessionWorkspace = sessionWorkspaceProvider.getSessionWorkspace()

    # The user folder now will point to the Session Workspace
    sessionWorskpacePath = sessionWorkspace.absolutePath

    # Prepare the output csv file
    baseName = os.path.basename(fcsFile)
    fName = os.path.splitext(baseName)[0] + ".csv"
    csvFile = os.path.join(sessionWorskpacePath, fName)

    return csvFile


# Plug-in entry point
def aggregate(parameters, tableBuilder):

    # Set up logging
    _logger = setUpLogging()

    # Get the parameters
    code = parameters.get("code")

    # Get the entity type
    paramX = parameters.get("paramX")

    # Get the entity code
    paramY = parameters.get("paramY")

    # Log parameter info
    _logger.info("Requested plot for dataset " + code + 
                " and parameters (" + paramX + ", " + paramY + ")")

    # Get the FCS file to process
    dataSetFiles = getFileForCode(code)

    # Prepare the data
    dataJSON = ""
    
    message = ""

    if len(dataSetFiles) != 1:

        message = "Could not retrieve the FCS file to process!"
        _logger.error(message)
        success = False

    else:

        # Get the FCS file path
        fcsFile = dataSetFiles[0] 

        # Log
        _logger.info("Dataset code " + code + " corresponds to FCS file " + fcsFile)

        # Get the associated CSV file path
        csvFile = getSessionCSVFileForFCSFile(fcsFile)

        # Does the csv file already exist in the session?
        success = True
        if not os.path.exists(csvFile):

            # Log
            _logger.info("CVS file does not exist yet: processing FCS file " + fcsFile)

            # Open the FCS file
            reader = FCSReader(java.io.File(fcsFile), True);

            # Parse the file with data
            if not reader.parse():

                message = "Could not process file " + os.path.basename(fcsFile)
                success = False
                
                # Log error
                _logger.error(message)

            else:

                # Writing the FCS file to the session workspace
                if not reader.exportDataToCSV(java.io.File(csvFile)):

                    message = "Could not write data to CSV file " + os.path.basename(csvFile)
                    success = False
                    
                    # Log error
                    _logger.error(message)
                    
                else:
                    
                    # The CSV file was successfully generated
                    message = "The CVS file " + os.path.basename(csvFile) + " was successfully created!"
                    success = True

                    # Log
                    _logger.info(message)
                    
        else:

            message = "The CVS file " + os.path.basename(csvFile) + " already exists in the session. Re-using it."
            success = True
            
            # Log
            _logger.info(message)

        if success:
            
            # Read the file
            content = csv.reader(open(csvFile))

            # Read the column names from the first line
            fields = content.next()

            # Find the indices of the requested parameters
            indxX = int(fields.index(paramX))
            indxY = int(fields.index(paramY))

            # Prepare the data array
            data = []

            # Now collect all data
            for row in content:

                data.append([float(row[indxX]), float(row[indxY])])        

            # JSON encode the data array
            dataJSON = json.dumps(data) 
            
            # Log
            _logger.info("Successfully processed file " + csvFile)

            # Success
            success = True


    # Add the table headers
    tableBuilder.addHeader("Success")
    tableBuilder.addHeader("Message")
    tableBuilder.addHeader("Data")

    # Store the results in the table
    row = tableBuilder.addRow()
    row.setCell("Success", success)
    row.setCell("Message", message)
    row.setCell("Data", dataJSON)
