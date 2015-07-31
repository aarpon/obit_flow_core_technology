# -*- coding: utf-8 -*-

'''
Aggregation plug-in to generate FCS plots.

The first time a scatterplot is generated for an FCS file, the data is read
and stored in a CSV file that is registered in openBIS.
Later plots will use the CSV file directly.

@author: Aaron Ponti
'''

import os.path
import java.io.File
import ch.ethz.scu.obit.bdfacsdivafcs.readers.FCSReader as FCSReader


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

    # Get the parameters
    code = parameters.get("code")

    # Get the entity type
    paramX = parameters.get("paramX")

    # Get the entity code
    paramY = parameters.get("paramY")

    # Get the FCS file to process
    dataSetFiles = getFileForCode(code)

    if len(dataSetFiles) != 1:

        message = "Could not retrieve the FCS file to process!"
        success = False

    else:

        # Get the FCS file path
        fcsFile = dataSetFiles[0] 

        # Get the associated CSV file path
        csvFile = getSessionCSVFileForFCSFile(fcsFile)

        # Does the csv file already exist in the session?
        if not os.path.exists(csvFile):

            # Open the FCS file
            reader = FCSReader(java.io.File(fcsFile), True);

            # Parse the file with data
            if not reader.parse():

                message = "Could not read file " + os.path.basename(fcsFile)
                success = False

            else:

                # Writing the FCS file to the session workspace
                if not reader.exportDataToCSV(java.io.File(csvFile)):

                    message = "Could not write data to CSV file " + os.path.basename(fcsFile)
                    success = False

                else:

                    message = "The CSV file " + csvFile + " was successfully written to the session."
                    success = True

        else:

            message = "The CSV file " + csvFile + " already exists in the session."
            success = True


    # Add the table headers
    tableBuilder.addHeader("Success")
    tableBuilder.addHeader("Message")
    tableBuilder.addHeader("XData")
    tableBuilder.addHeader("YData")

    # Store the results in the table (test!)
    # TODO: Return the real data!
    row = tableBuilder.addRow()
    row.setCell("Success", success)
    row.setCell("Message", message)
    row.setCell("XData", "[1, 2, 3, 4, 5]")
    row.setCell("YData", "[1.2, 4.5, 8.9, 15.7, 27.3]")
