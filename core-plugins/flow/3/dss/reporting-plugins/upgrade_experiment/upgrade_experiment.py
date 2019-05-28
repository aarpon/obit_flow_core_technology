# -*- coding: utf-8 -*-

# Ingestion service: upgrade an experiment structure to current version

import os.path
import logging
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchCriteria
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchSubCriteria
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto.SearchCriteria import MatchClause
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto.SearchCriteria import MatchClauseAttribute
import ch.ethz.scu.obit.flow.readers.FCSReader as FCSReader
import java.io.File
import xml.etree.ElementTree as xml
from __builtin__ import None


def _dictToXML(d):
    """Converts a dictionary into an XML string."""

    # Create an XML node
    node = xml.Element("Parameters")

    # Add all attributes to the XML node
    it = d.entrySet().iterator()
    while it.hasNext():
        pair = it.next();
        node.set(pair.getKey(), pair.getValue())
        it.remove()  # Avoids a ConcurrentModificationException

    # Convert to XML string
    xmlString = xml.tostring(node, encoding="UTF-8")

    # Return the XML string
    return xmlString


def _formatExpDateForPostgreSQL(dateStr):
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


def _setUpLogging():
    """Sets up logging and returns the logger object."""

    # Get path to containing folder
    # __file__ does not work (reliably) in Jython
    rpPath = "../core-plugins/flow/3/dss/reporting-plugins/upgrade_experiment"

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
    _logger = logging.getLogger("ExperimentUpgrader")

    return _logger


def _getFileForCode(code):
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


def _getDataSetsForSample(sampleIdentifier, dataSetType):
    """Return the dataSet of given type for specified sample."""

    # Set search criteria to retrieve the dataSet associated with the sample
    searchCriteria = SearchCriteria()
    searchCriteria.addMatchClause(
        MatchClause.createAttributeMatch(
            MatchClauseAttribute.TYPE,
            dataSetType)
        )

    sampleCriteria = SearchCriteria()
    sampleCriteria.addMatchClause(
        MatchClause.createAttributeMatch(
            MatchClauseAttribute.CODE,
            sampleIdentifier)
        )

    searchCriteria.addSubCriteria(
        SearchSubCriteria.createSampleCriteria(
            sampleCriteria)
        )
    dataSetList = searchService.searchForDataSets(searchCriteria)

    if len(dataSetList) != 1:
        []

    # Return the dataSet
    return dataSetList


def _getChildSamples(parentSampleType, parentSamplePermId, sampleType):
    """Return the samples of given type for specified parent sample."""

    # The samples are of type 'sampleType'
    searchCriteria = SearchCriteria()
    searchCriteria.addMatchClause(
        MatchClause.createAttributeMatch(
            MatchClauseAttribute.TYPE,
            sampleType)
        )

    # The samples have given parent
    expSampleCriteria = SearchCriteria()
    expSampleCriteria.addMatchClause(
        MatchClause.createAttributeMatch(
            MatchClauseAttribute.TYPE,
            parentSampleType)
        )
    expSampleCriteria.addMatchClause(
        MatchClause.createAttributeMatch(
            MatchClauseAttribute.PERM_ID,
            parentSamplePermId)
        )
    searchCriteria.addSubCriteria(
        SearchSubCriteria.createSampleParentCriteria(expSampleCriteria)
    )

    # Now search
    samples = searchService.searchForSamples(searchCriteria)

    # Return the samples
    return samples


def _getExperimentSample(collectionPermId, expSamplePermId):
    """Retrieve the experiment sample."""

    # Get the experiment sample
    sampleCriteria = SearchCriteria()
    sampleCriteria.addMatchClause(
        MatchClause.createAttributeMatch(
            MatchClauseAttribute.PERM_ID,
            expSamplePermId)
        )
    expCriteria = SearchCriteria()
    expCriteria.addMatchClause(
        MatchClause.createAttributeMatch(
            MatchClauseAttribute.PERM_ID,
            collectionPermId)
        )
    # Add the experiment subcriteria
    sampleCriteria.addSubCriteria(
        SearchSubCriteria.createExperimentCriteria(
            expCriteria)
        )

    # Search
    expSampleList = searchService.searchForSamples(sampleCriteria)

    if len(expSampleList) != 1:
        return None

    # Return the experiment sample
    return expSampleList[0]


def process(transaction, parameters, tableBuilder):
    """Update old flow experiments that have some missing or incorrect
    information.
    
    """

    # Latest experiment version
    EXPERIMENT_VERSION = 2

    # Set up logging
    _logger = _setUpLogging()

    # Prepare the return table
    tableBuilder.addHeader("success")
    tableBuilder.addHeader("message")

    # Add a row for the results
    row = tableBuilder.addRow()

    # Retrieve parameters from client
    collectionPermId = parameters.get("collectionPermId")
    expSamplePermId = parameters.get("expSamplePermId")

    # Log parameter info
    _logger.info("Requested update of experiment " + collectionPermId + "/" + expSamplePermId + ".")

    # Get the experiment sample
    expSample = _getExperimentSample(collectionPermId, expSamplePermId)

    # If we did not get the experiment, return here with an error
    if expSample is None:

        # Prepare the return arguments
        success = False
        message = "The experiment with permID " + collectionPermId + "/" + expSamplePermId + " could not be found."

        # Log the error
        _logger.error(message)

        # Add the results to current row
        row.setCell("success", success)
        row.setCell("message", message)

        # Return here
        return

    # Get the experiment type
    expSampleType = expSample.getSampleType()

    # Log
    _logger.info("Successfully retrieved Experiment with permId " +
                 expSamplePermId + " and type " + expSampleType + ".")

    # Experiment prefix
    experimentPrefix = expSampleType[0:expSampleType.find("_EXPERIMENT")]

    # Get either _WELL or _TUBE samples
    tubes = _getChildSamples(expSampleType, expSamplePermId, experimentPrefix + "_TUBE")
    if len(tubes) == 0:
        wells = _getChildSamples(expSampleType, expSamplePermId, experimentPrefix + "_WELL")
        if len(wells) == 0:

            # Prepare the return arguments
            success = False
            message = "The experiment sample does not appear to contain neither tubes nor wells!"

            # Log the error
            _logger.error(message)

            # Add the results to current row
            row.setCell("success", success)
            row.setCell("message", message)

            # Return here
            return
        else:
            tubes = wells

    # For some additional robustness, we try all tubes until
    # we found a good one (although they should ALL be good)
    dataSets = []
    for tube in tubes:

        # Sample identifier to get the dataset
        sampleIdentifier = tube.getSampleIdentifier()

        # Get the dataSet
        dataSets = _getDataSetsForSample(sampleIdentifier, experimentPrefix + "_FCSFILE")

        if len(dataSets) > 0:
            break

    # If we did not get a usable dataSet, return here with an error
    if len(dataSets) == 0:

        # Prepare the return arguments
        success = False
        message = "No datasets could be found for experiment " + collectionPermId + "/" + expSamplePermId + "."

        # Log the error
        _logger.error(message)

        # Add the results to current row
        row.setCell("success", success)
        row.setCell("message", message)

        # Return here
        return

    # Get the file from the first dataset
    files = _getFileForCode(dataSets[0].getDataSetCode())
    if len(files) != 1:

        # Prepare the return arguments
        success = False
        message = "Could not retrieve the FCS file to process!"

        # Log the error
        _logger.error(message)

        # Add the results to current row
        row.setCell("success", success)
        row.setCell("message", message)

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

        # Prepare the return arguments
        success = False
        message = "Could not process file " + os.path.basename(fcsFile)

        # Log the error
        _logger.error(message)

        # Add the results to current row
        row.setCell("success", success)
        row.setCell("message", message)

        # Return here
        return

    #
    #
    # EXPERIMENT NAME
    #
    #

    # Get the experiment name from the file
    expNameFromFile = reader.getCustomKeyword("EXPERIMENT NAME")

    # Get the experiment name from the registered Experiment sample
    currentExpName = expSample.getPropertyValue(expSampleType + "_NAME")
    if currentExpName == "":
        try:
            currentExpName = expSample.getPropertyValue("$NAME")
        except:
            pass

    # Inform
    _logger.info("Experiment name is registered as " + currentExpName)

    # We need the ExperimentSample to be mutable
    mutableExperimentSample = transaction.makeSampleMutable(expSample)

    # Are the experiment names matching?
    if expNameFromFile == currentExpName:

        # Log
        _logger.info("Registered experiment name matches the experiment " +
                     "name from the FCS file.")

    else:

        # Update the registered Experiment name
        mutableExperimentSample.setPropertyValue(expSampleType + "_NAME", expNameFromFile)
        mutableExperimentSample.setPropertyValue("$NAME", expNameFromFile)

        # Log
        _logger.info("Updated registered experiment sample name from '" +
                     currentExpName + "' to '" + expNameFromFile + "'.")

    #
    #
    # FCS FILE PARAMETERS AND ACQUISITION DATE
    #
    #

    hardwareString = expSampleType[0:expSampleType.find("_EXPERIMENT")]
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
            files = _getFileForCode(dataSet.getDataSetCode())
            if len(files) != 1:

                # Prepare the return arguments
                success = False
                message = "Could not retrieve the FCS file to process!"

                # Log the error
                _logger.error(message)

                # Add the results to current row
                row.setCell("success", success)
                row.setCell("message", message)

                # Return here
                return

            # Get the FCS file
            fcsFile = files[0]

            # Open and parse the FCS file
            reader = FCSReader(java.io.File(fcsFile), True);

            # Parse the file with data
            if not reader.parse():

                # Prepare the return arguments
                success = False
                message = "Could not process file " + os.path.basename(fcsFile)

                # Log the error
                _logger.error(message)

                # Add the results to current row
                row.setCell("success", success)
                row.setCell("message", message)

                # Return here
                return

            if acqDate is None:

                # Get and format the acquisition date
                dateStr = _formatExpDateForPostgreSQL(
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

                    # Prepare the return arguments
                    success = False
                    message = "Could not read parameters from file " + \
                    os.path.basename(fcsFile)

                    # Log the error
                    _logger.error(message)

                    # Add the results to current row
                    row.setCell("success", success)
                    row.setCell("message", message)

                    # Return here
                    return

                # Convert the parameters to XML
                parametersXML = _dictToXML(parametersAttr)

                # Now store them in the dataSet
                mutableDataSet.setPropertyValue(parameterProperty, parametersXML)

                # Log
                _logger.info("The parameters for file " + str(fcsFile) +
                             " were successfully stored (in XML).")

    # Update the version of the experiment
    mutableExperimentSample.setPropertyValue(expSampleType + "_VERSION",
                                             str(EXPERIMENT_VERSION))

    success = True
    message = "Congratulations! The experiment was successfully upgraded " + \
        "to the latest version."

    # Log
    _logger.info(message)

    # Add the results to current row
    row.setCell("success", success)
    row.setCell("message", message)
