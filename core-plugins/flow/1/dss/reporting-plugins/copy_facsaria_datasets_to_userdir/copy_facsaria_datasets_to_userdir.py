# -*- coding: utf-8 -*-

'''
Aggregation plug-in to copy all FCS files under a specified FACSAria III element to the user folder.
@author: Aaron Ponti
'''

from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchCriteria
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchSubCriteria
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto.SearchCriteria import MatchClause
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto.SearchCriteria import MatchClauseAttribute
import os
import subprocess
import sys
import re
import zipfile
import java.io.File


def touch(full_file):
    """Touches a file.
    """
    f = open(full_file, 'w')
    f.close()
    

def zip_folder(folder_path, output_path):
    """Zip the contents of an entire folder recursively. Please notice that
    empty sub-folders will NOT be included in the archive.
    """

    # Note: os.path.relpath() does not exist in Jython.
    # target = os.path.relpath(folder_path, start=os.path.dirname(folder_path))
    target = folder_path[folder_path.rfind(os.sep) + 1:]

    # Simple trick to build relative paths
    root_len = folder_path.find(target)

    try:
        
        # Open zip file (no compression)
        zip_file = zipfile.ZipFile(output_path, 'w', zipfile.ZIP_STORED)
        
        # Now recurse into the folder
        for root, folders, files in os.walk(folder_path):

            # We do not process folders. This is only useful to store empty
            # folders to the archive, but 1) jython's zipfile implementation
            # throws:
            #
            #     Exception: [Errno 21] Is a directory <directory_name>
            #
            # when trying to write a directory to a zip file (in contrast to 
            # Python's implementation) and 2) oBIT does not export empty
            # folders in the first place.

            # Build the relative directory path (current root)
            relative_dir_path = os.path.abspath(root)[root_len:]

            # If a folder only contains a subfolder, we disrupt the hierarchy,
            # unless we add a file.
            if len(files) == 0:
                touch(os.path.join(root, '~'))
                files.append('~')

            # Include all files
            for file_name in files:
                
                # Full file path to add
                full_file_path = os.path.join(root, file_name)
                relative_file_path = os.path.join(relative_dir_path, file_name)

                # Workaround problem with file name encoding
                full_file_path = full_file_path.encode('latin-1')
                relative_file_path = relative_file_path.encode('latin-1')

                # Write to zip
                zip_file.write(full_file_path, relative_file_path, \
                               zipfile.ZIP_STORED)

    except IOError, message:
        raise Exception(message)

    except OSError, message:
        raise Exception(message)

    except zipfile.BadZipfile, message:
        raise Exception(message)

    finally:
        zip_file.close()
        
        
class Mover():
    """
    Takes care of organizing the files to be copied to the user folder and
    performs the actual copying.
    """

    def __init__(self, experimentId, entityType, entityId, specimen, mode, \
                 userId, properties):
        '''Constructor'''

        # Store properties
        self._properties = properties

        # Experiment identifier
        self._experimentId = experimentId

        # Experiment code
        # If no / is found, _experimentCode will be the same as _experimentId
        self._experimentCode = self._experimentId[self._experimentId.rfind("/") + 1:]

        # Entity type
        self._entityType = entityType

        # Entity id
        self._entityId = entityId

        # Entity code
        self._entityCode = self._entityId[self._entityId.rfind("/") + 1:]

        # Specimen name (or "")
        self._specimen = specimen

        # User folder: depending on the 'mode' settings, the user folder changes
        if mode =="normal":
            
            # Standard user folder
            self._userFolder = os.path.join(self._properties['base_dir'], \
                                            userId, self._properties['export_dir'])
            
        elif mode == "zip":

            # Get the path to the user's Session Workspace
            sessionWorkspace = sessionWorkspaceProvider.getSessionWorkspace()

            # The user folder now will point to the Session Workspace
            self._userFolder = sessionWorkspace.absolutePath

        else:
            raise Exception("Bad value for argument 'mode' (" + mode  +")")

        # Store the mode
        self._mode = mode

        # Make sure the use folder (with export subfolder) exists and has
        # the correct permissions
        if not os.path.isdir(self._userFolder):
            self._createDir(self._userFolder)

        # Get the experiment
        self._experiment = searchService.getExperiment(self._experimentId)

        # Get the experiment name
        self._experimentName = \
            self._experiment.getPropertyValue("FACS_ARIA_EXPERIMENT_NAME")

        # Export full path in user/tmp folder
        self._rootExportPath = os.path.join(self._userFolder, self._experimentCode)

        # Experiment full path within the export path
        self._experimentPath = os.path.join(self._rootExportPath,
                                            self._experimentName)

        # Current path: this is used to keep track of the path where to copy
        # files when navigating the experiment hierarchy
        self._currentPath = ""

        # Message (in case of error)
        self._message = "";

        # Keep track of the number of copied files
        self._numCopiedFiles = 0


    # Public methods
    # =========================================================================

    def process(self):
        """
        Uses the information stored in the Mover object to reconstruct the
        structure of the experiment and copies it to the user folder. If the
        processing was successful, the method returns True. Otherwise,
        it returns False.
        """

        # Check that the entity code is set properly (in the constructor)
        if self._entityCode == '':
            self._message = "Could not get entity code from identifier!"
            return False

        # Check that the experiment could be retrieved
        if self._experiment == None:
            self._message = "Could not retrieve experiment with " \
            "identifier " + self._entityId + "!"
            return False

        # At this stage we can create the experiment folder in the user dir
        # (and export root)
        if not self._createRootAndExperimentFolder():
            self._message = "Could not create experiment folder " + \
            self._rootExportPath
            return False

        # Now process depending on the entity type
        if self._entityType == "FACS_ARIA_EXPERIMENT":

            # Copy all datasets contained in this experiment
            return self._copyDataSetsForExperiment()

        elif self._entityType == "FACS_ARIA_ALL_PLATES":

            # Copy all datasets for all plates Experiment
            return self._copyDataSetsForPlates()

        elif self._entityType == "FACS_ARIA_TUBESET":

            # Copy all datasets for the tubes in the Experiment optionally
            # filtered by given specimen (if stored in self._specimen)
            return self._copyDataSetsForTubes()

        elif self._entityType == "FACS_ARIA_PLATE":

            # Copy all the datasets contained in selected plate
            return self._copyDataSetsForPlate()

        elif self._entityType == "FACS_ARIA_WELL":

            # Copy the datasets contained in this well
            return self._copyDataSetsForWell()

        elif self._entityType == "FACS_ARIA_TUBE":

            # Copy the datasets contained in this tube
            return self._copyDataSetsForTube()

        elif self._entityType == "FACS_ARIA_FCSFILE":

            # Copy current FCS file sample
            return self._copyDataSetForFCSFileSample()

        else:

            self._message = "Unknown entity!"
            return False

        # Return
        return True


    def compressIfNeeded(self):
        """Compresses the exported experiment folder to a zip archive
        but only if the mode was "zip".
        """

        if self._mode == "zip":
            zip_folder(self._rootExportPath, self.getZipArchiveFullPath())


    def getZipArchiveFullPath(self):
        """Return the full path of the zip archive (or "" if mode was "normal").
        """
        
        if self._mode == "zip":
            return self._rootExportPath + ".zip"
        
        return ""


    def getZipArchiveFileName(self):
        """Return the file name of the zip archive without path."""

        if self._mode == "zip":
            fullFile = java.io.File(self.getZipArchiveFullPath())
            return fullFile.getName()

        return ""

        
    def getErrorMessage(self):
        """
        Return the error message (in case process() returned failure)
        """
        return self._message


    def getNumberOfCopiedFiles(self):
        """
        Return the number of copied files.
        """
        return self._numCopiedFiles


    def getRelativeRootExperimentPath(self):
        """
        Return the experiment path relative to the user folder.
        """
        return userId + "/" + \
            self._rootExportPath[self._rootExportPath.rfind(self._properties['export_dir']):]


    # Private methods
    # =========================================================================

    def _copyDataSetsForExperiment(self):
        """
        Copies all FCS files in the experiment to the user directory
        reconstructing the sample hierarchy. Plates will map to subfolders.
        Tubes will be at the experiment root.

        Returns True for success. In case of error, returns False and sets
        the error message in self._message -- to be retrieved with the
        getErrorMessage() method.
        """

        # Copy all tubes - if they could not be copied, we abort
        if not self._copyDataSetsForTubes():
            return False

        # Copy the plates
        if not self._copyDataSetsForPlates():
            return False

        # Return success
        return True


    def _copyDataSetsForPlate(self, plate=None):
        """
        Copy all FCS files for given plate in the experiment to the user
        directory. If the plate is not passed, it will be retrieved
        using self._entityId. The plate will map to a subfolder. Optionally,
        the fcs files may be filtered by self._specimen, if set.

        Returns True for success. In case of error, returns False and sets
        the error message in self._message -- to be retrieved with the
        getErrorMessage() method.
        """

        # Get the plate
        if plate is None:
            plate = searchService.getSample(self._entityId)

        # Get plate code and name
        plateCode = plate.getCode()
        plateName = plate.getPropertyValue("FACS_ARIA_PLATE_NAME")

        # Create a folder for the plate
        self._currentPath = os.path.join(self._experimentPath, plateName)
        self._createDir(self._currentPath)

        # If required, create folder for the specimen
        if self._specimen != "":
            self._currentPath = os.path.join(self._currentPath, self._specimen)
            self._createDir(self._currentPath)

        # Get all datasets for the plate
        dataSets = self._getDataSetsForPlate(plateCode)
        if len(dataSets) == 0:
            self._message = "Could not retrieve datasets for plate with code " + plateCode + "."
            return False

        # Get all fcs files for the datasets
        dataSetFiles = self._getFilesForDataSets(dataSets)
        if len(dataSetFiles) == 0:
            self._message = "Could not retrieve files for datasets from plate " + plateCode + "."
            return False

        # Copy the files to the user folder (in the plate folder)
        for fcsFile in dataSetFiles:
            self._copyFile(fcsFile, self._currentPath)

        # Return success
        return True


    def _copyDataSetsForPlates(self):
        """
        Copy all FCS files for the plates in the experiment to the user
        directory. Each plate will map to a subfolder.

        Returns True for success. In case of error, returns False and sets
        the error message in self._message -- to be retrieved with the
        getErrorMessage() method.
        """

        # Get the plates (if some exist)
        plates = self._getAllPlates()
        if len(plates) == 0:
            return True

        # Now iterate over the plates, retrieve their datasets and fcs files
        # and copy them to the plate subfolders
        for plate in plates:
            if not self._copyDataSetsForPlate(plate):
                self._message = "Could not retrieve datasets for plate."
                return False

        # Return
        return True


    def _copyDataSetsForTubes(self):
        """
        Copy all FCS files for the tubes in the experiment to the user
        directory. Tubes will be at the experiment root.

        Returns True for success. In case of error, returns False and sets
        the error message in self._message -- to be retrieved with the
        getErrorMessage() method.
        """

        # Get the tubes (if some exist) optionally filtered by the specimen
        tubes = self._getAllTubes()
        if len(tubes) == 0:
            return True

        # If the specimen is set, we create a folder for it
        if self._specimen != "":
            self._currentPath = os.path.join(self._experimentPath, self._specimen)
            self._createDir(self._currentPath)
        else:
            self._currentPath = self._experimentPath

        # Now iterate over the tubes and retrieve their datasets
        dataSets = []
        for tube in tubes:
            tubeCode = tube.getCode()
            dataSetsForSample = self._getDataSetForTube(tubeCode)
            dataSets.extend(dataSetsForSample)

        if len(dataSets) == 0:
            self._message = "Could not retrieve datasets for tubes in " \
            "experiment with code " + self._experimentCode + "."
            return False

        # Get all fcs files for the datasets
        dataSetFiles = self._getFilesForDataSets(dataSets)
        if len(dataSetFiles) == 0:
            self._message = "Could not retrieve files for datasets from tubes."
            return False

        # Copy the files
        for fcsFile in dataSetFiles:
            self._copyFile(fcsFile, self._currentPath)

        # Return success
        return True


    def _copyDataSetsForWell(self):
        """
        Copy the datasets belonging to selected well to the expriment root.

        Returns True for success. In case of error, returns False and sets
        the error message in self._message -- to be retrieved with the
        getErrorMessage() method.
        """

        # Get the plate containing it
        # TODO

        # Create a subfolder for the plate
        # TODO

        # Get the datasets for the well
        dataSets = self._getDataSetForWell()

        # Get all fcs files for the datasets
        dataSetFiles = self._getFilesForDataSets(dataSets)
        if len(dataSetFiles) == 0:
            self._message = "Could not retrieve files for datasets from well."
            return False

        # Store at the experiment level
        self._currentPath = self._experimentPath

        # Copy the files
        for fcsFile in dataSetFiles:
            self._copyFile(fcsFile, self._currentPath)

        # Return success
        return True


    def _copyDataSetsForTube(self):
        """
        Copy the datasets belonging to selected tube at the experiment root.

        Returns True for success. In case of error, returns False and sets
        the error message in self._message -- to be retrieved with the
        getErrorMessage() method.
        """

        # Get the datasets for the well
        dataSets = self._getDataSetForTube()

        # Get all fcs files for the datasets
        dataSetFiles = self._getFilesForDataSets(dataSets)
        if len(dataSetFiles) == 0:
            self._message = "Could not retrieve files for datasets from tube."
            return False

        # Store at the experiment level
        self._currentPath = self._experimentPath

        # Copy the files
        for fcsFile in dataSetFiles:
            self._copyFile(fcsFile, self._currentPath)

        # Return success
        return True


    def _copyDataSetForFCSFileSample(self):
        """
        Copy the datasets belonging to the selected FCS file sample at
        the experiment root.

        Returns True for success. In case of error, returns False and sets
        the error message in self._message -- to be retrieved with the
        getErrorMessage() method.
        """

        # Get the datasets for the FCSFile sample
        dataSets = self._getDataSetForFCSFileSample()

        # Get all fcs files for the datasets
        dataSetFiles = self._getFilesForDataSets(dataSets)
        if len(dataSetFiles) == 0:
            self._message = "Could not retrieve files for datasets from FCSFile sample."
            return False

        # Store at the experiment level
        self._currentPath = self._experimentPath

        # Copy the files
        for fcsFile in dataSetFiles:
            self._copyFile(fcsFile, self._currentPath)

        # Return success
        return True


    def _copyFile(self, source, dstDir):
        """Copies the source file (with full path) to directory dstDir.
        We use a trick to preserve the NFSv4 ACLs: since copying the file
        loses them, we first touch the destination file to create it, and
        then we overwrite it.
        """
        dstFile = os.path.join(dstDir, os.path.basename(source))
        subprocess.call(["/bin/touch", dstFile])
        subprocess.call(["/bin/cp", source, dstDir])
        self._numCopiedFiles += 1


    def _createDir(self, dirFullPath):
        """Creates the passed directory (with full path).
        """
        os.makedirs(dirFullPath)


    def _createRootAndExperimentFolder(self):
        """
        Create the experiment folder. Notice that it uses information already
        stored in the object, but this info is filled in in the constructor, so
        it is safe to assume it is there if nothing major went wrong. In this
        case, the method will return False and no folder will be created.
        Otherwise, the method returns True.

        Please notice that if the experiment folder already exists, _{digit}
        will be appended to the folder name, to ensure that the folder is
        unique. The updated folder name will be stored in the _rootExportPath
        property.
        """

        # This should not happen
        if self._rootExportPath == "":
            return False

        # Make sure that the experiment folder does not already exist
        expPath = self._rootExportPath

        # Does the folder already exist?
        if os.path.exists(expPath):
            counter = 1
            ok = False
            while not ok:
                tmpPath = expPath + "_" + str(counter)
                if not os.path.exists(tmpPath):
                    expPath = tmpPath
                    ok = True
                else:
                    counter += 1

        # Update the root and experiment paths
        self._rootExportPath = expPath
        self._experimentPath = os.path.join(self._rootExportPath,
                                            self._experimentName)

        # Create the root folder
        self._createDir(self._rootExportPath)

        # And now create the experiment folder (in the root folder)
        self._createDir(self._experimentPath)

        # Return success
        return True


    def _getDataSetsForPlate(self, plateCode=None):
        """
        Return a list of datasets belonging to the plate with specified ID
        optionally filtered by self._specimen. If none are found, return [].

        If no plateCode is given, it is assumed that the plate is the passed
        entity with code self._entityCode.
        """
        if plateCode is None:
            plateCode = self._entityCode

        # Set search criteria to retrieve all wells contained in the plate
        searchCriteria = SearchCriteria()
        plateCriteria = SearchCriteria()
        plateCriteria.addMatchClause(MatchClause.createAttributeMatch(MatchClauseAttribute.CODE, plateCode))
        searchCriteria.addSubCriteria(SearchSubCriteria.createSampleContainerCriteria(plateCriteria))
        wells = searchService.searchForSamples(searchCriteria)

        if len(wells) == 0:
            self._message = "Could not retrieve wells for plate with " \
            "code " + plateCode + "."
            return wells

        # Check that the specimen matches (if needed)
        if self._specimen != "":
            wells = [well for well in wells if \
                       well.getPropertyValue("FACS_ARIA_SPECIMEN") == self._specimen]

        # Now iterate over the samples and retrieve their datasets
        dataSets = []
        for well in wells:
            wellCode = well.getCode()
            dataSetsForWell = self._getDataSetForWell(wellCode)
            dataSets.extend(dataSetsForWell)

        if len(dataSets) == 0:
            self._message = "Could not retrieve datasets for wells in " \
            "plate with code " + plateCode + " from experiment " \
            "with code " + self._experimentCode + "."

        # Return
        return dataSets


    def _getDataSetForWell(self, wellCode=None):
        """
        Get the datasets belonging to the well with specified code. If none
        are found, return [].

        If no wellCode is given, it is assumed that the well is the passed
        entity with code self._entityCode.
        """

        if wellCode is None:
            wellCode = self._entityCode

        # Set search criteria to retrieve the dataset contained in the well
        searchCriteria = SearchCriteria()
        wellCriteria = SearchCriteria()
        wellCriteria.addMatchClause(MatchClause.createAttributeMatch(MatchClauseAttribute.CODE, wellCode))
        searchCriteria.addSubCriteria(SearchSubCriteria.createSampleCriteria(wellCriteria))
        dataSets = searchService.searchForDataSets(searchCriteria)

        if len(dataSets) == 0:
            self._message = "Could not retrieve datasets for well " \
            "with code " + wellCode + "."

        # Return
        return dataSets


    def _getDataSetForTube(self, tubeCode=None):
        """
        Get the datasets belonging to the tube with specified tube code.
        If none is found, return [].

        If no tubeCode is given, it is assumed that the tube is the passed
        entity with code self._entityCode.
        """

        if tubeCode is None:
            tubeCode = self._entityCode

        # Set search criteria to retrieve the dataset contained in the tube
        searchCriteria = SearchCriteria()
        tubeCriteria = SearchCriteria()
        tubeCriteria.addMatchClause(MatchClause.createAttributeMatch(MatchClauseAttribute.CODE, tubeCode))
        searchCriteria.addSubCriteria(SearchSubCriteria.createSampleCriteria(tubeCriteria))
        dataSets = searchService.searchForDataSets(searchCriteria)

        if len(dataSets) == 0:
            self._message = "Could not retrieve datasets for tube " \
            "with code " + tubeCode + "."

        # Return
        return dataSets


    def _getDataSetForFCSFileSample(self):
        """
        Get the FCS file for the sample with type FACS_ARIA_FCSFILE.
        """

        # Get the dataset for current FCS file sample
        dataSets = searchService.getDataSet(self._entityId)
        if dataSets is None:
            self._message = "Could not retrieve datasets for " \
            "FCS file with identifier " + self._entityId + "!"
        else:
            dataSets = [dataSets]

        # Return
        return dataSets


    def _getFilesForDataSets(self, dataSets):
        """
        Get the list of FCS file paths that correspond to the input list
        of datasets. If not files are found, returns [].
        """

        if len(dataSets) == 0:
            return []

        dataSetFiles = []
        for dataSet in dataSets:
            content = contentProvider.getContent(dataSet.getDataSetCode())
            nodes = content.listMatchingNodes("original", ".*\.fcs")
            if nodes is not None:
                for node in nodes:
                    fileName = node.tryGetFile()
                    if fileName is not None:
                        fileName = str(fileName)
                        if fileName.lower().endswith(".fcs"):
                            dataSetFiles.append(fileName)

        if len(dataSetFiles) == 0:
            self._message = "Could not retrieve dataset files!"

        # Return the files
        return dataSetFiles


    def _getAllPlates(self):
        """
        Get all plates in the experiment. Returns [] if none are found.
        """

        # Set search criteria to retrieve all plates in the experiment
        searchCriteria = SearchCriteria()
        searchCriteria.addMatchClause(MatchClause.createAttributeMatch(MatchClauseAttribute.TYPE, "FACS_ARIA_PLATE"))
        expCriteria = SearchCriteria()
        expCriteria.addMatchClause(MatchClause.createAttributeMatch(MatchClauseAttribute.CODE, self._experimentCode))
        searchCriteria.addSubCriteria(SearchSubCriteria.createExperimentCriteria(expCriteria))
        plates = searchService.searchForSamples(searchCriteria)

        if len(plates) == 0:
            self._message = "Could not retrieve plates for experiment with code " + self._experimentCode + "."
            return plates

        # Return the plates
        return plates


    def _getAllTubes(self):
        """
        Get all tubes in the experiment. If the specimen is set (self._specimen),
        then return only those tubes that belong to it.
        Returns [] if none are found.
        """

        # Set search criteria to retrieve all tubes in the experiment
        # All tubes belong to a virtual tubeset - so the set of tubes in the
        # experiment is exactly the same as the set of tubes in the virtual
        # tubeset
        searchCriteria = SearchCriteria()
        searchCriteria.addMatchClause(MatchClause.createAttributeMatch(MatchClauseAttribute.TYPE, "FACS_ARIA_TUBE"))
        expCriteria = SearchCriteria()
        expCriteria.addMatchClause(MatchClause.createAttributeMatch(MatchClauseAttribute.CODE, self._experimentCode))
        searchCriteria.addSubCriteria(SearchSubCriteria.createExperimentCriteria(expCriteria))
        tubes = searchService.searchForSamples(searchCriteria)

        if len(tubes) == 0:
            self._message = "Could not retrieve tubes for experiment with code " + self._experimentCode + "."
            return tubes

        # Check that the specimen matches (if needed)
        if self._specimen != "":
            tubes = [tube for tube in tubes if \
                     tube.getPropertyValue("FACS_ARIA_SPECIMEN") == self._specimen]

        # Return the (filtered) tubes
        return tubes

# Parse properties file for custom settings
def parsePropertiesFile():
    """Parse properties file for custom plug-in settings."""

    filename = "../core-plugins/flow/1/dss/reporting-plugins/copy_facsaria_datasets_to_userdir/plugin.properties"
    var_names = ['base_dir', 'export_dir']

    properties = {}
    try:
        fp = open(filename, "r")
    except:
        return properties

    try:
        for line in fp:
            line = re.sub('[ \'\"\n]', '', line)
            parts = line.split("=")
            if len(parts) == 2:
                if parts[0] in var_names:
                    properties[parts[0]] = parts[1]
    finally:
        fp.close()

    # Check that all variables were found
    if len(properties.keys()) == 0:
        return None

    found_vars = properties.keys()

    for var_name in var_names:
        if var_name not in found_vars:
            return None

    # Everything found
    return properties

# Plug-in entry point
def aggregate(parameters, tableBuilder):

    # Get parameters from plugin.properties
    properties = parsePropertiesFile()
    if properties is None:
        raise Exception("Could not process plugin.properties")

    # Get the experiment identifier
    experimentId = parameters.get("experimentId")

    # Get the entity type
    entityType = parameters.get("entityType")

    # Get the entity code
    entityId = parameters.get("entityId")

    # Get the specimen name
    specimen = parameters.get("specimen")
    
    # Get the mode
    mode = parameters.get("mode")

    # Instantiate the Mover object - userId is a global variable
    # made available to the aggregation plug-in
    mover = Mover(experimentId, entityType, entityId, specimen, mode, userId,
                  properties)

    # Process
    success = mover.process()
    
    # Compress
    if mode == "zip":
        mover.compressIfNeeded()
        
    # Get some results info
    nCopiedFiles = mover.getNumberOfCopiedFiles()
    errorMessage = mover.getErrorMessage();
    relativeRootExpFolder = mover.getRelativeRootExperimentPath()
    zipFileName = mover.getZipArchiveFileName()

    # Add the table headers
    tableBuilder.addHeader("Success")
    tableBuilder.addHeader("Message")
    tableBuilder.addHeader("nCopiedFiles")
    tableBuilder.addHeader("relativeExpFolder")
    tableBuilder.addHeader("zipArchiveFileName")
    tableBuilder.addHeader("Mode")

    # Store the results in the table
    row = tableBuilder.addRow()
    row.setCell("Success", success)
    row.setCell("Message", errorMessage)
    row.setCell("nCopiedFiles", nCopiedFiles)
    row.setCell("relativeExpFolder", relativeRootExpFolder)
    row.setCell("zipArchiveFileName", zipFileName)
    row.setCell("Mode", mode)

    # Email result to the user
    if success == True:
        
        subject = "FACSAria III: successfully processed requested data"
        
        if nCopiedFiles == 1:
            snip = "One file was "
        else:
            snip = str(nCopiedFiles) + " files were "

        if mode == "normal":
            body = snip + "successfully exported to {...}/" + relativeRootExpFolder + "."
        else:
            body = snip + "successfully packaged for download."
            
    else:
        subject = "FACSAria III: error processing request!"
        body = "Sorry, there was an error processing your request. " + \
        "Please send your administrator the following report:\n\n" + \
        "\"" + errorMessage + "\"\n"

    # Send
    try:
        mailService.createEmailSender().withSubject(subject).withBody(body).send()
    except:
        sys.stderr.write("copy_facsaria_datasets_to_userdir: Failure sending email to user!")
