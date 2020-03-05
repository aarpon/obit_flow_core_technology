# -*- coding: utf-8 -*-

# Notes:
#
# - this plug-in uses LRCache.jar from export_flow_datasets/lib.
# - this plug-in requires Jython version 2.7 (for json module)

'''
Aggregation plug-in to generate FCS plots.

The first time a scatterplot is generated for an FCS file, the data is read
and stored in a CSV file that is registered in openBIS.
Later plots will use the CSV file directly.

@author: Aaron Ponti
'''

import os.path
import logging
import java.io.File
import java.util.ArrayList
import json
import uuid
from threading import Thread
from ch.ethz.scu.obit.flow.readers import FCSReader
from ch.ethz.scu.obit.flow.readers import Hyperlog
from ch.ethz.scu.obit.common.server.longrunning import LRCache


def setUpLogging():
    """Sets up logging and returns the logger object."""

    # Get path to containing folder
    # __file__ does not work (reliably) in Jython
    rpPath = "../core-plugins/flow/4/dss/reporting-plugins/retrieve_fcs_events"

    # Path to the logs subfolder
    logPath = os.path.join(rpPath, "logs")

    # Make sure the logs subfolder exist
    if not os.path.exists(logPath):
        os.makedirs(logPath)

    # Path for the log file
    logFile = os.path.join(logPath, "log.txt")

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


# Plug-in entry point
#
# This plug-in always returns immediately. The first time it is called, it
# starts the retrieve process in a separate thread and returns a unique ID to
# the client that will later use to retrieve the state of the progress.
#
# This method takes a list of parameters that also returns in a table (tableBuilder)
# to the client. The names of the input parameters match the corresponding
# column names. The following list describes the input parameters:
#
# uid      : unique identifier of the running plug-in. The first time it is
#            either omitted or passed as "" from the client, since it is the
#            server that creates the unique ID for the job. After this is
#            returned to the client in the first call, it must be passed on
#            again as a parameter to the server.
# code     : code of the FCS file to be loaded to retrieve the data to be plotted.
# paramX   : name of the parameter for the X axis
# paramY   : name of the parameter for the Y axis
# displayX : scaling (linear or logarithmic) of the X axis -- CURRENTLY UNUSED
# displayY : scaling (linear or logarithmic) of the Y axis -- CURRENTLY UNUSED
# numEvents: total number of events known to be in the file
# maxNumEvents: max number of events to be returned for plotting.
# nodeKey  : key of the FCS node in the tree. This is not used here, but needs
#            to be passed back at the end of the process since it will be used
#            for caching the data in the node itself to speed up subsequent
#            plots.
#
# The following are NOT input parameters and are only returned in the
# tableBuilder (i.e. all the input parameters above are ALSO returned):
#
# completed: True if the process has completed in the meanwhile, False if it
#            is still running.
# success  : True if the process completed successfully, False otherwise.
# message  : message to be displayed in the client. Please notice that this is
#            not necessarily an error message (i.e. is success is True it will
#            be a success message).
# data     : the data read from the FCS/CSV file to be plotted in the client
def aggregate(parameters, tableBuilder):

    # Add the table headers
    tableBuilder.addHeader("uid")
    tableBuilder.addHeader("completed")
    tableBuilder.addHeader("success")
    tableBuilder.addHeader("message")
    tableBuilder.addHeader("data")
    tableBuilder.addHeader("code")
    tableBuilder.addHeader("paramX")
    tableBuilder.addHeader("paramY")
    tableBuilder.addHeader("displayX")
    tableBuilder.addHeader("displayY")
    tableBuilder.addHeader("numEvents")
    tableBuilder.addHeader("maxNumEvents")
    tableBuilder.addHeader("samplingMethod")
    tableBuilder.addHeader("nodeKey")

    # Get the ID of the call if it already exists
    uid = parameters.get("uid");

    if uid is None or uid == "":

        # Create a unique id
        uid = str(uuid.uuid4())

        # Fill in relevant information
        row = tableBuilder.addRow()
        row.setCell("uid", uid)
        row.setCell("completed", False)
        row.setCell("success", True)
        row.setCell("message", "")
        row.setCell("data", "")
        row.setCell("code", "")
        row.setCell("paramX", "")
        row.setCell("paramY", "")
        row.setCell("displayX", "")
        row.setCell("displayY", "")
        row.setCell("numEvents", "")
        row.setCell("maxNumEvents", "")
        row.setCell("samplingMethod", "")
        row.setCell("nodeKey", "")

        # Launch the actual process in a separate thread
        thread = Thread(target=retrieveProcess,
                        args=(parameters, tableBuilder, uid))
        thread.start()

        # Return immediately
        return

    # The process is already running in a separate thread. We get current
    # results and return them
    resultToSend = LRCache.get(uid);
    if resultToSend is None:
        # This should not happen
        raise Exception("Could not retrieve results from result cache!")

    # Fill in relevant information
    row = tableBuilder.addRow()
    row.setCell("uid", resultToSend["uid"])
    row.setCell("completed", resultToSend["completed"])
    row.setCell("success", resultToSend["success"])
    row.setCell("message", resultToSend["message"])
    row.setCell("data", resultToSend["data"])
    row.setCell("code", resultToSend["code"])
    row.setCell("paramX", resultToSend["paramX"])
    row.setCell("paramY", resultToSend["paramY"])
    row.setCell("displayX", resultToSend["displayX"])
    row.setCell("displayY", resultToSend["displayY"])
    row.setCell("numEvents", resultToSend["numEvents"])
    row.setCell("maxNumEvents", resultToSend["maxNumEvents"])
    row.setCell("samplingMethod", resultToSend["samplingMethod"])
    row.setCell("nodeKey", resultToSend["nodeKey"])


# Perform the retrieve process in a separate thread
def retrieveProcess(parameters, tableBuilder, uid):

    # Make sure to initialize and store the results. We need to have them since
    # most likely the client will try to retrieve them again before the process
    # is finished.
    resultToStore = {}
    resultToStore["uid"] = uid
    resultToStore["completed"] = False
    resultToStore["success"] = True
    resultToStore["message"] = ""
    resultToStore["data"] = ""

    # Get the parameters

    # Get the entity code
    code = parameters.get("code")
    resultToStore["code"] = code

    # Get the X-axis parameter
    paramX = parameters.get("paramX")
    resultToStore["paramX"] = paramX

    # Get the Y-axis parameter
    paramY = parameters.get("paramY")
    resultToStore["paramY"] = paramY

    # Get the X-axis scaling
    displayX = parameters.get("displayX")
    resultToStore["displayX"] = displayX

    # Get the Y-axis scaling
    displayY = parameters.get("displayY")
    resultToStore["displayY"] = displayY

    # Number of events known to be in the file
    numEvents = int(parameters.get("numEvents"))
    resultToStore["numEvents"] = numEvents

    # Maximum number of events to return
    maxNumEvents = int(parameters.get("maxNumEvents"))
    resultToStore["maxNumEvents"] = maxNumEvents

    # Sampling
    samplingMethod = parameters.get("samplingMethod")
    resultToStore["samplingMethod"] = samplingMethod

    # Node key
    nodeKey = parameters.get("nodeKey")
    resultToStore["nodeKey"] = nodeKey

    # Store them into the cache
    LRCache.set(uid, resultToStore)

    # Set up logging
    _logger = setUpLogging()

    # Log parameter info
    _logger.info("Requested events for dataset " + code +
                " and parameters (" + paramX + ", " + paramY + ")")
    _logger.info("Requested scaling for parameter " + paramX + ": " + displayX)
    _logger.info("Requested scaling for parameter " + paramY + ": " + displayY)
    _logger.info("Requested sampling method: " + samplingMethod)
    _logger.info("Number of events in file: " + str(numEvents) +
                "; maximum number of events to return: " + str(maxNumEvents))

    # Get the FCS file to process
    dataSetFiles = getFileForCode(code)

    # Prepare the data
    dataJSON = ""

    if len(dataSetFiles) != 1:

        # Build the error message
        message = "Could not retrieve the FCS file to process!"

        # Log the error
        _logger.error(message)

        # Store the results and set the completed flag
        resultToStore["completed"] = True
        resultToStore["success"] = False
        resultToStore["message"] = message

        # Return here
        return

    else:

        # Get the FCS file path
        fcsFile = dataSetFiles[0]

        # Log
        _logger.info("Dataset code " + code + " corresponds to FCS file " + \
                     fcsFile)

        # Open the FCS file
        reader = FCSReader(java.io.File(fcsFile), True);

        # Parse the file with data
        if not reader.parse():

            # Build the error message
            message = "Could not process file " + os.path.basename(fcsFile)

            # Log the error
            _logger.error(message)

            # Store the results and set the completed flag
            resultToStore["completed"] = True
            resultToStore["success"] = False
            resultToStore["message"] = message

            # Return here
            return

        # Preparation steps were successful
        parameterNames = reader.getParameterNames()

        # Find the indices of the requested parameters
        indxX = int(parameterNames.indexOf(paramX))
        indxY = int(parameterNames.indexOf(paramY))

        # Prepare the data arrays
        data = []

        # Actual number of events to be extracted
        actualNumEvents = min(maxNumEvents, numEvents)

        # Data sampling method.
        #
        # Method 1: the get the requested number of events, we will sub-
        #           sample the file by skipping a certain number of rows
        #           ("step") in between the returned once.
        # Method 2: to get the requested number of events, we just return
        #           the first N rows at the beginning of the file. This is
        #           faster, and as far as the experts say, should still be
        #           reasonably representative of the underlying population.
        if samplingMethod == "1":
            sample = True
        else:
            sample = False

        # Now collect the first maxNumEvents rows
        dataX = reader.getDataPerColumnIndex(indxX, actualNumEvents, sample)
        dataY = reader.getDataPerColumnIndex(indxY, actualNumEvents, sample)

        # Is the Hyperlog scaling requested?
        if displayX == "Hyperlog":
            params = Hyperlog.estimateParamHeuristic(dataX)
            Hx = Hyperlog(params[0], params[1], params[2], params[3])
            dataX = Hx.transform(dataX)
            dataX = Hyperlog.arrayMult(dataX, params[0])

        if displayY == "Hyperlog":
            params = Hyperlog.estimateParamHeuristic(dataY)
            Hy = Hyperlog(params[0], params[1], params[2], params[3])
            dataY = Hy.transform(dataY)
            dataY = Hyperlog.arrayMult(dataY, params[0])

        # Build array to JSONify and return to the client
        # Data is returned as a 2 x n array:
        # data[0] is dataX; data[1] is dataY
        data.append([float(dx) for dx in dataX])
        data.append([float(dy) for dy in dataY])
        
        # This is maintained for historical reasons. Each
        # [x, y] point is stored in a position in the array.
        # for i in range (actualNumEvents):
        #     data.append([float(dataX[i]), float(dataY[i])])

        # JSON encode the data array
        dataJSON = json.dumps(data)

        # Success message
        message = "Successfully processed file " + fcsFile

        # Log
        _logger.info(message)

        # Success
        success = True

        # Store the results and set the completed flag
        resultToStore["completed"] = True
        resultToStore["success"] = True
        resultToStore["message"] = message
        resultToStore["data"] = dataJSON
