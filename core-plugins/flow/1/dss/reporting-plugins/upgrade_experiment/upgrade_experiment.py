# -*- coding: utf-8 -*-

# Ingestion service: upgrade an experiment structure to current version

# Note: this plug-in uses LRCache.jar from export_bdfacsdiva_datasets/lib.

import os.path
import logging
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchCriteria
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchSubCriteria
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto.SearchCriteria import MatchClause
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto.SearchCriteria import MatchClauseAttribute
import ch.ethz.scu.obit.bdfacsdivafcs.readers.FCSReader as FCSReader
import java.io.File
import xml.etree.ElementTree as xml
from ch.ethz.scu.obit.common.server.longrunning import LRCache
import uuid
from threading import Thread


def dictToXML(d):
    """Converts a dictionary into an XML string."""

    # Create an XML node
    node = xml.Element("Parameters")

    # Add all attributes to the XML node
    it = d.entrySet().iterator()
    while it.hasNext():
        pair = it.next();
        node.set(pair.getKey(), pair.getValue())
        it.remove() # Avoids a ConcurrentModificationException

    # Convert to XML string
    xmlString = xml.tostring(node, encoding="UTF-8")

    # Return the XML string
    return xmlString


def formatExpDateForPostgreSQL(dateStr):
    """Format the experiment date to be compatible with postgreSQL's
    'timestamp' data type.

    @param Date stored in the FCS file, in the form 01-JAN-2013
    @return Date in the form 2013-01-01
    """

    monthMapper = {'JAN': '01', 'FEB': '02', 'MAR': '03', 'APR': '04',
                   'MAY': '05', 'JUN': '06', 'JUL': '07', 'AUG': '08',
                   'SEP': '09', 'OCT': '10', 'NOV': '11', 'DEC': '12'}

    # Separate the date into day, month, and year
    (day, month, year) = dateStr.split("-")

    # Try mapping the month to digits (e.g. "06"). If the mapping does
    # not work, return "NOT_FOUND"
    month = monthMapper.get(month, "NOT_FOUND")

    # Build the date in the correct format. If the month was not found,
    # return 01-01-1970
    if month == "NOT_FOUND":
        return "1970-01-01"
    else:
        return year + "-" + month + "-" + day


def setUpLogging():
    """Sets up logging and returns the logger object."""

    # Get path to containing folder
    # __file__ does not work (reliably) in Jython
    rpPath = "../core-plugins/flow/1/dss/reporting-plugins/upgrade_experiment"

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
# starts the upgrade process in a separate thread and returns a unique ID to 
# the client that will later use to retrieve the state of the progress.
#
# This method takes a list of parameters that also returns in a table (tableBuilder)
# to the client. The names of the input parameters match the corresponding 
# column names.
#
# uid      : unique identifier of the running plug-in. The first time it is
#            either omitted or passed as "" from the client, since it is the
#            server that creates the unique ID for the job. After this is 
#            returned to the client in the first call, it must be passed on
#            again as a parameter to the server.
#
# expPermId: experiment identifier.
#
# This plug-in returns a table to the client with the following columns:.
# 
# uid      : unique identifier of the running plug-in. The first time it is
#            either omitted or passed as "" from the client, since it is the
#            server that creates the unique ID for the job. After this is 
#            returned to the client in the first call, it must be passed on
#            again as a parameter to the server.
# completed: True if the process has completed in the meanwhile, False if it 
#            is still running.
# success  : True if the process completed successfully, False otherwise.
# message  : message to be displayed in the client. Please notice that this is 
#            not necessarily an error message (i.e. is success is True it will
#            be a success message). 
def process(transaction, parameters, tableBuilder):
    """Update old flow experiments that have some missing or incorrect
    information.
    """

    # Add the table headers
    tableBuilder.addHeader("uid")
    tableBuilder.addHeader("completed")
    tableBuilder.addHeader("success")
    tableBuilder.addHeader("message")

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

        # Launch the actual process in a separate thread
        thread = Thread(target = upgradeProcess,
                        args = (transaction, parameters, tableBuilder, uid))
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


# Perform the experiment upgrade in a separate thread
def upgradeProcess(transaction, parameters, tableBuilder, uid):

    # Make sure to initialize and store the results. We need to have them since
    # most likely the client will try to retrieve them again before the process
    # is finished.
    resultToStore = {}
    resultToStore["uid"] = uid
    resultToStore["completed"] = False
    resultToStore["success"] = True
    resultToStore["message"] = ""
    LRCache.set(uid, resultToStore)

    # Latest experiment version
    EXPERIMENT_VERSION = 1

    # Set up logging
    _logger = setUpLogging()

    # Retrieve parameters from client
    expPermId = parameters.get("expPermId")

    # Log parameter info
    _logger.info("Requested update of experiment " + expPermId + ".")

    # Get the experiment
    expCriteria = SearchCriteria()
    expCriteria.addMatchClause(MatchClause.createAttributeMatch(MatchClauseAttribute.PERM_ID, expPermId))
    experiments = searchService.searchForExperiments(expCriteria)

    # If we did not get the experiment, return here with an error
    if len(experiments) != 1:

        # Build the error message
        message = "The experiment with permID " + expPermId + " could not be found."

        # Log the error
        _logger.error(message)

        # Store the results and set the completed flag
        resultToStore["completed"] = True
        resultToStore["success"] = False
        resultToStore["message"] = message
        LRCache.set(uid, resultToStore)

        # Return here
        return

    # Get the experiment
    experiment = experiments[0]

    # Get the experiment type
    experimentType = experiment.getExperimentType()

    # Log
    _logger.info("Successfully retrieved Experiment with permId " + 
                 expPermId + " and type " + experimentType + ".")

    # Build the corresponding dataset type
    experimentPrefix = experimentType[0:experimentType.find("_EXPERIMENT")]
    dataSetType = experimentPrefix + "_FCSFILE"

    # Retrieve all FCS files contained in the experiment
    searchCriteria = SearchCriteria()
    searchCriteria.addMatchClause(MatchClause.createAttributeMatch(MatchClauseAttribute.TYPE, dataSetType))
    expCriteria = SearchCriteria()
    expCriteria.addMatchClause(MatchClause.createAttributeMatch(MatchClauseAttribute.PERM_ID, expPermId))
    searchCriteria.addSubCriteria(SearchSubCriteria.createExperimentCriteria(expCriteria))
    dataSets = searchService.searchForDataSets(searchCriteria)

    # Log
    _logger.info("Retrieved " + str(len(dataSets)) +
                 " dataset(s) for experiment with permId " + expPermId + ".")

    # If we did not get the datasets, return here with an error
    if dataSets is None:

        # Build the error message
        message = "No FCS files could be found for experiment with permID " + \
            expPermId + "."

        # Log the error
        _logger.error(message)

        # Store the results and set the completed flag
        resultToStore["completed"] = True
        resultToStore["success"] = False
        resultToStore["message"] = message
        LRCache.set(uid, resultToStore)

        # Return here
        return

    # Get the file from the first dataset
    files = getFileForCode(dataSets[0].getDataSetCode())
    if len(files) != 1:

        # Build the error message
        message = "Could not retrieve the FCS file to process!"

        # Log the error
        _logger.error(message)

        # Store the results and set the completed flag
        resultToStore["completed"] = True
        resultToStore["success"] = False
        resultToStore["message"] = message
        LRCache.set(uid, resultToStore)

        # Return here
        return

    # Get the file
    fcsFile = files[0]

    # Log
    _logger.info("Reading file " + fcsFile + ".")

    # Open the FCS file
    reader = FCSReader(java.io.File(fcsFile), False)

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
        LRCache.set(uid, resultToStore)

        # Return here
        return

    # Log
    _logger.info("Successfully parsed file " + fcsFile + ".")

    #
    #
    # EXPERIMENT NAME
    #
    #

    # Get the experiment name from the file
    expNameFromFile = reader.getCustomKeyword("EXPERIMENT NAME")

    # Log
    _logger.info("Experiment name from file: " + expNameFromFile)

    # Get the experiment name from the registered Experiment
    currentExpName = experiment.getPropertyValue(experimentType + "_NAME")

    # Log
    _logger.info("Experiment name from openBIS: " + currentExpName)

    # We need the Experiment to be mutable
    try:
        mutableExperiment = transaction.makeExperimentMutable(experiment)
    except Exception:
        # Log
        _logger.error("Make experiment mutable failed: " + str(Exception))

    # Log
    _logger.info("Mutable experiment: " + str(mutableExperiment))

    # Are the experiment names matching?
    if expNameFromFile == currentExpName:

        # Log
        _logger.info("Registered experiment name matches the experiment " +
                     "name from the FCS file.")

    else:

        # Update the registered Experiment name
        mutableExperiment.setPropertyValue(experimentType + "_NAME", expNameFromFile)

        # Log
        _logger.info("Updated registered experiment name from '" + 
                     currentExpName + "' to '" + expNameFromFile + "'.")

    #
    #
    # FCS FILE PARAMETERS AND ACQUISITION DATE
    #
    #

    hardwareString = experimentType[0:experimentType.find("_EXPERIMENT")]
    parameterProperty = hardwareString + "_FCSFILE_PARAMETERS"
    acqDateProperty = hardwareString + "_FCSFILE_ACQ_DATE"

    # Log
    _logger.info("Checking properties of " + str(len(dataSets)) + " file(s).")

    for dataSet in dataSets:

        # Check whether the parameters are stored for the file
        parameters = dataSet.getPropertyValue(parameterProperty)

        # Check whether the acquisition date is stored for the file
        acqDate = dataSet.getPropertyValue(acqDateProperty)

        if parameters is None or acqDate is None:

            # Make the DataSet mutable for update
            mutableDataSet = transaction.makeDataSetMutable(dataSet)

            # Get the file from the dataset
            files = getFileForCode(dataSet.getDataSetCode())
            if len(files) != 1:

                # Build the error message
                message = "Could not retrieve the FCS file to process!"

                # Log the error
                _logger.error(message)

                # Store the results and set the completed flag
                resultToStore["completed"] = True
                resultToStore["success"] = False
                resultToStore["message"] = message
                LRCache.set(uid, resultToStore)

                # Return here
                return     
    
            # Get the FCS file
            fcsFile = files[0]

            # Open and parse the FCS file
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
                LRCache.set(uid, resultToStore)

                # Return here
                return

            if acqDate is None:

                # Get and format the acquisition date
                dateStr = formatExpDateForPostgreSQL(
                    reader.getStandardKeyword("$DATE"))

                # Update the dataSet
                mutableDataSet.setPropertyValue(acqDateProperty, dateStr)

                # Log
                _logger.info("The acquisition date of file " + str(fcsFile) +
                             " was set to: " + dateStr + ".") 

            if parameters is None:

                # Get the parameters 
                parametersAttr = reader.parametersAttr

                if parametersAttr is None:

                    # Build the error message
                    message = "Could not read parameters from file " + \
                        os.path.basename(fcsFile)


                    # Log the error
                    _logger.error(message)

                    # Store the results and set the completed flag
                    resultToStore["completed"] = True
                    resultToStore["success"] = False
                    resultToStore["message"] = message
                    LRCache.set(uid, resultToStore)

                    # Return here
                    return

                # Convert the parameters to XML
                parametersXML = dictToXML(parametersAttr)

                # Now store them in the dataSet
                mutableDataSet.setPropertyValue(parameterProperty, parametersXML)

                # Log
                _logger.info("The parameters for file " + str(fcsFile) +
                             " were successfully stored (in XML).") 


    # Update the version of the experiment
    mutableExperiment.setPropertyValue(experimentType + "_VERSION",
                                       str(EXPERIMENT_VERSION))

    # Store success
    message = "Congratulations! The experiment was successfully upgraded " + \
        "to the latest version."

    # Log
    _logger.info(message)

    # Store the results and set the completed flag
    resultToStore["completed"] = True
    resultToStore["success"] = True
    resultToStore["message"] = message
    LRCache.set(uid, resultToStore)
