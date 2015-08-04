# -*- coding: utf-8 -*-

# Ingestion service: create a project with user-defined name in given space


import os.path
import logging
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchCriteria
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchSubCriteria
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto.SearchCriteria import MatchClause
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto.SearchCriteria import MatchClauseAttribute
import ch.ethz.scu.obit.bdfacsdivafcs.readers.FCSReader as FCSReader
import java.io.File

def setUpLogging():
    """Sets up logging and returns the logger object."""

    # Get path to containing folder
    # __file__ does not work (reliably) in Jython
    rpPath = "../core-plugins/flow/1/dss/reporting-plugins/update_outdated_experiment"

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


def process(transaction, parameters, tableBuilder):
    """Update old flow experiments that have some missing or incorrect information.
    
    """
    
    # Set up logging
    _logger = setUpLogging()
    
    # Prepare the return table
    tableBuilder.addHeader("success")
    tableBuilder.addHeader("message")

    # Add a row for the results
    row = tableBuilder.addRow()

    # Retrieve parameters from client
    expPermId = parameters.get("expPermId")
    dataSetType = parameters.get("dataSetType")

    # Log parameter info
    _logger.info("Requested update of experiment " + expPermId + 
                " and FCS files of type " + dataSetType + ".")

    # Get the experiment
    # TODO: Retrieve an Experiment, not an ImmutableExperiment!
    expCriteria = SearchCriteria()
    expCriteria.addMatchClause(MatchClause.createAttributeMatch(MatchClauseAttribute.PERM_ID, expPermId))
    experiments = searchService.searchForExperiments(expCriteria)

    # If we did not get the experiment, return here with an error
    if len(experiments) != 1:
        
        # Prepare the return arguments
        success = False
        message = "The experiment with permID " + expPermId + " could not be found."

        # Log the error
        _logger.error(message)
        
        # Add the results to current row
        row.setCell("success", success)
        row.setCell("message", message)

        # Return here
        return
    
    # Get the experiment
    experiment = experiments[0]

    # Get the experiment type
    experimentType = experiment.getExperimentType()

    # Log
    _logger.info("Successfully retrieved Experiment with permId " + expPermId + " and type " + experimentType + ".")

    # Retrieve all FCS files contained in the experiment
    searchCriteria = SearchCriteria()
    searchCriteria.addMatchClause(MatchClause.createAttributeMatch(MatchClauseAttribute.TYPE, dataSetType))
    expCriteria = SearchCriteria()
    expCriteria.addMatchClause(MatchClause.createAttributeMatch(MatchClauseAttribute.PERM_ID, expPermId))
    searchCriteria.addSubCriteria(SearchSubCriteria.createExperimentCriteria(expCriteria))
    dataSets = searchService.searchForDataSets(searchCriteria)
    
    # If we did not get the datasets, return here with an error
    if dataSets is None:
        
        # Prepare the return arguments
        success = False
        message = "No FCS files could be found for experiment with permID " + expPermId + "."

        # Log the error
        _logger.error(message)
        
        # Add the results to current row
        row.setCell("success", success)
        row.setCell("message", message)

        # Return here
        return        
    
    # Get the file from the first dataset
    files = getFileForCode(dataSets[0].getDataSetCode())
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

    # Get the experiment name from the file
    expNameFromFile = reader.getCustomKeyword("EXPERIMENT NAME")

    # Get the experiment name from the registered Experiment
    currentExpName = experiment.getPropertyValue(experimentType + "_NAME")

    # Are the experiment names matching?
    if expNameFromFile == currentExpName:
        
        # Log
        _logger.info("Registered experiment name matches the experiment name from the FCS file.")   
    
    else:
        
        # TODO: Retrieve an Experiment, not an ImmutableExperiment!
        
        # Update the registered Experiment name
        experiment.setPropertyValue(experimentType + "_NAME", expNameFromFile)

        # Log
        _logger.info("Updated registered experiment name from '" + currentExpName + "' to '" + expNameFromFile + "'.")   
         
        success = True
        message = "Currently registered experiment name is " + currentExpName + " while the experiment name from the file is " + expNameFromFile + " ."

    success = True
    message = "All done."

    # Add the results to current row
    row.setCell("success", success)
    row.setCell("message", message)
