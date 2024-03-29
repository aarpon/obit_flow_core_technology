# -*- coding: utf-8 -*-

'''
Aggregation plug-in to copy all FCS files under a specified FLOW element
to the user folder.or to the session workspace for download.
@author: Aaron Ponti
'''

from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchCriteria
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchSubCriteria
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto.SearchCriteria import MatchClause
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto.SearchCriteria import MatchClauseAttribute
from ch.systemsx.cisd.base.utilities import OSUtilities
import os
import subprocess
import sys
import re
import zipfile
import java.io.File
from ch.ethz.scu.obit.common.server.longrunning import LRCache
import uuid
from threading import Thread
import logging
from __builtin__ import True, None

_DEBUG = False


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
        zip_file = zipfile.ZipFile(output_path, 'w', zipfile.ZIP_STORED, allowZip64=True)

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

    def __init__(self, task, collectionId, collectionType, expSampleId, expSamplePermId,
                 expSampleType, platePermId, plateType, mode, userId, properties, logger):
        """Constructor

        task           : helper argument to define what to export. 
        collectionId   : id of the collection
        collectionType : type of the collection
        expSampleId    : id of the experiment sample.
        expSamplePermId: permId of the experiment sample.
        expSampleType  : type of the experiment sample.
        entityId       : id of the entity to export (with children)
        entityType     : type of the entity to export
        mode           : "normal", or "zip". If mode is "normal", the files
                         will be copied to the user folder; if mode is "zip", the
                         files will be packaged into a zip files and served for 
                         download via the browser.
        userId         : user id.
        properties     : plug-in properties.
        logger         : logger.
        """

        # Logger
        self._logger = logger

        # Inform
        if _DEBUG:
            self._logger.info("Mover called with parameters:\n" + \
                              "    task            = " + task + "\n" +
                              "    collectionId    = " + collectionId + "\n" +
                              "    collectionType  = " + collectionType + "\n" +
                              "    expSampleId     = " + expSampleId + "\n" +
                              "    expSamplePermId = " + expSamplePermId + "\n" +
                              "    expSampleType   = " + expSampleType + "\n" +
                              "    platePermId     = " + platePermId + "\n" +
                              "    plateType       = " + plateType + "\n" +
                              "    mode            = " + mode + "\n" +
                              "    userId          = " + userId + "\n" +
                              "    properties      = " + str(properties) + "\n")

        # Store properties
        self._properties = properties

        # Store task
        self._task = task

        # Experiment identifier
        self._collectionId = collectionId

        # Experiment type
        self._collectionType = collectionType

        # Experiment sample id
        self._expSampleId = expSampleId

        # Experiment sample perm id
        self._expSamplePermId = expSamplePermId

        # Experiment type
        self._expSampleType = expSampleType

        # Entity id
        self._platePermId = platePermId

        # Entity type
        self._plateType = plateType

        # Get the EXPERIMENT SAMPLE object
        self._experimentSample = self._getFlowExperimentSample()
        if self._experimentSample is None:
            raise Exception("Could not retrieve experiment sample with permId " + \
                             self._expSamplePermId + ".")

        # Get the COLLECTION object
        self._experiment = searchService.getExperiment(self._collectionId)

        # Get the PLATE object
        self._plate = None
        if self._platePermId != "" and self._plateType != "":
            self._plate = self._retrieveSampleWithTypeAndPermId(self._platePermId, self._plateType)

        # Set all relevant entity types for current experiment type
        self._expSamplePrefix = self._expSampleType[0:self._expSampleType.find("_EXPERIMENT")]

        # Original experiment (sample) name
        originalExperimentSampleName = self._experimentSample.getPropertyValue("$NAME")

        # Experiment sample name (to be used in the output folder)
        self._experimentSampleName = self._expSampleId[self._expSampleId.rfind("/") + 1:] + \
            "/" + originalExperimentSampleName

        # Inform
        self._logger.info("Experiment name: " + self._experimentSampleName)

        # Collection name (to be used in the output folder)
        self._collectionName = self._collectionId[self._collectionId.rfind("/") + 1:]

        # User folder: depending on the 'mode' settings, the user folder changes
        if mode == "normal":

            # Standard user folder
            self._userFolder = os.path.join(self._properties['base_dir'], \
                                            userId, self._properties['export_dir'])

        elif mode == "zip":

            # Get the path to the user's Session Workspace
            sessionWorkspace = sessionWorkspaceProvider.getSessionWorkspace()

            # The user folder now will point to the Session Workspace
            self._userFolder = sessionWorkspace.absolutePath

        else:
            raise Exception("Bad value for argument 'mode' (" + mode + ")")

        if _DEBUG:
            self._logger.info("User folder for mode " + mode + " is " + self._userFolder)

        # Store the mode
        self._mode = mode

        # Make sure the use folder (with export subfolder) exists and has
        # the correct permissions
        if not os.path.isdir(self._userFolder):
            self._createDir(self._userFolder)

        # Root (collection) of the export folder
        self._rootExportPath = os.path.join(self._userFolder, self._collectionName)

        # Experiment full path within the root export path
        self._experimentPath = os.path.join(self._rootExportPath, self._experimentSampleName)

        # Current path: this is used to keep track of the path where to copy
        # files when navigating the experiment hierarchy
        self._currentPath = ""

        # Message (in case of error)
        self._message = ""

        # Info
        if _DEBUG:
            self._logger.info("Target experiment folder: " + self._experimentPath)

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

        # Create the experiment (sample) folder in the user/export
        if not self._createRootAndExperimentFolder():
            self._message = "Could not create experiment folder " + \
            self._rootExportPath
            self._logger.error(self._message)
            return False

        self._logger.info("Starting copy...")

        # Now process depending on the task
        if self._task == "EXPERIMENT_SAMPLE":

            # Copy all datasets contained in this experiment
            return self._copyDataSetsForExperiment()

        if self._task == "ALL_PLATES":

            # Copy all datasets for all plates Experiment
            return self._copyDataSetsForPlates()

        if self._task == "TUBESET":

            # Copy all datasets for all plates Experiment
            return self._copyDataSetsForTubes()

        if self._task == "PLATE":

            # Copy all the datasets contained in selected plate
            return self._copyDataSetsForPlate(self._plate)

        else:

            self._message = "Unknown task!"
            self._logger.error(self._message)
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

    def _getFlowExperimentSample(self):
        """Find the {FLOW}_EXPERIMENT sample with given Id."""

        # Inform
        if _DEBUG:
            self._logger.info("Retrieving experiment sample of code " + \
                              self._expSampleId + ", permId " + self._expSamplePermId + \
                              " and type " + self._expSampleType)

        # Search sample of type MICROSCOPY_EXPERIMENT with specified CODE
        sampleCriteria = SearchCriteria()
        sampleCriteria.addMatchClause(
            MatchClause.createAttributeMatch(
                MatchClauseAttribute.TYPE,
                self._expSampleType))
        sampleCriteria.addMatchClause(
            MatchClause.createAttributeMatch(
                MatchClauseAttribute.PERM_ID,
                self._expSamplePermId))

        # Search
        samples = searchService.searchForSamples(sampleCriteria)

        if len(samples) == 0:
            samples = []
            self._message = "Could not retrieve " + self._expSampleType + " sample with code " + \
                             self._expSampleId + ", permId " + self._expSamplePermId + \
                             " and type " + self._expSampleType + "."
            self._logger.error(self._message)
            return samples

        if _DEBUG:
            self._logger.info("Successfully returned sample " + self._expSampleId)

        # Return
        return samples[0]

    def _retrieveSampleWithTypeAndPermId(self, samplePermId, sampleType):
        """
        Retrieve a sample belonging to current experiment 
        sample and collection having specified type and perm id.
        """
        # The sample is of type 'sampleType' and has id 'sampleId'
        searchCriteria = SearchCriteria()
        searchCriteria.addMatchClause(
            MatchClause.createAttributeMatch(
                MatchClauseAttribute.TYPE,
                sampleType)
            )
        searchCriteria.addMatchClause(
            MatchClause.createAttributeMatch(
                MatchClauseAttribute.PERM_ID,
                samplePermId)
            )

        # Now search
        samples = searchService.searchForSamples(searchCriteria)

        if len(samples) != 1:
            self._message = "Sample with id " + sampleId + \
            " and type " + sampleType + "not found!"
            self._logger.error(self._message)
            return None

        # Return the sample
        return samples[0]

    def _retrieveAllSamplesWithType(self, sampleType):
        """
        Retrieve all samples belonging to current experiment 
        sample and collection having specified type.
        """

        # The samples are of type 'sampleType'
        searchCriteria = SearchCriteria()
        searchCriteria.addMatchClause(
            MatchClause.createAttributeMatch(
                MatchClauseAttribute.TYPE,
                sampleType)
            )

        # The samples have parent _EXPERIMENT_SAMPLE
        expSampleCriteria = SearchCriteria()
        expSampleCriteria.addMatchClause(
            MatchClause.createAttributeMatch(
                MatchClauseAttribute.TYPE,
                self._expSampleType)
            )
        expSampleCriteria.addMatchClause(
            MatchClause.createAttributeMatch(
                MatchClauseAttribute.PERM_ID,
                self._expSamplePermId)
            )
        searchCriteria.addSubCriteria(
            SearchSubCriteria.createSampleParentCriteria(expSampleCriteria)
        )

        # Now search
        samples = searchService.searchForSamples(searchCriteria)

        # Return the samples
        return samples

    def _retrieveAllSamplesWithTypeAndParentWithPermId(self, sampleType, parentSamplePermId, parentSampleType):
        """
        Retrieve all samples belonging to current experiment 
        sample and collection having specified type.
        """

        if _DEBUG:
            self._logger.info("Retrieving samples of type " + sampleType +
                              " with parent sample with perm id " + parentSamplePermId +
                              " and type " + parentSampleType)

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

    def _retrieveAllSamplesWithTypeAndParent(self, sampleType, parentSampleId, parentSampleType):
        """
        Retrieve all samples belonging to current experiment 
        sample and collection having specified type.
        """

        if _DEBUG:
            self._logger.info("Retrieving samples of type " + sampleType +
                              " with parent sample with id " + parentSampleId +
                              " and type " + parentSampleType)

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
                MatchClauseAttribute.CODE,
                parentSampleId)
            )
        searchCriteria.addSubCriteria(
            SearchSubCriteria.createSampleParentCriteria(expSampleCriteria)
        )

        # Now search
        samples = searchService.searchForSamples(searchCriteria)

        # Return the samples
        return samples

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

    def _copyDataSetsForPlate(self, plate):
        """
        Copy all FCS files for given plate in the experiment to the user
        directory. If the plate is not passed, it will be retrieved
        using self._entityId. The plate will map to a subfolder.

        Returns True for success. In case of error, returns False and sets
        the error message in self._message -- to be retrieved with the
        getErrorMessage() method.
        """

        # Get plate code and name
        plateCode = plate.getCode()
        plateName = plate.getPropertyValue("$NAME")

        if _DEBUG:
            self._logger.info("Processing plate with name " + plateName)

        # Create a folder for the plate
        self._currentPath = os.path.join(self._experimentPath, plateName)
        self._createDir(self._currentPath)

        if _DEBUG:
            self._logger.info("Plate with name " + plateName + " will be exported to " + self._currentPath)

        # Get all datasets for the plate
        dataSets = self._getDataSetsForPlate(plateCode)
        if len(dataSets) == 0:
            self._message = "Could not retrieve datasets for plate with code " + plateCode + "."
            self._logger.error(self._message)
            return False

        # Get all fcs files for the datasets
        dataSetFiles = self._getFilesForDataSets(dataSets)
        if len(dataSetFiles) == 0:
            self._message = "Could not retrieve files for datasets from plate " + plateCode + "."
            self._logger.error(self._message)
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

        # Reset the current target folder to the root of the experiment sample
        self._currentPath = self._experimentPath

        # Now iterate over the plates, retrieve their datasets and fcs files
        # and copy them to the plate subfolders
        for plate in plates:
            if not self._copyDataSetsForPlate(plate):
                self._message = "Could not retrieve datasets for plate."
                self._logger.error(self._message)
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

        # Get the tubes (if some exist)
        tubes = self._getAllTubes()
        if len(tubes) == 0:
            return True

        # Reset the current target folder to the root of the experiment sample
        self._currentPath = self._experimentPath

        # Now iterate over the tubes and retrieve their datasets
        dataSets = []
        for tube in tubes:
            tubeId = tube.getCode()
            dataSetsForSample = self._getDataSetForTube(tubeId)
            dataSets.extend(dataSetsForSample)

        if _DEBUG:
            self._logger.info("Found " + str(len(dataSets)) + " datasets")

        if len(dataSets) == 0:
            self._message = "Could not retrieve datasets for tubes in " \
            "experiment with code " + self._experimentCode + "."
            self._logger.error(self._message)
            return False

        # Get all fcs files for the datasets
        dataSetFiles = self._getFilesForDataSets(dataSets)

        if _DEBUG:
            self._logger.info("Found " + str(len(dataSetFiles)) + " dataset files")

        if len(dataSetFiles) == 0:
            self._message = "Could not retrieve files for datasets from tubes."
            self._logger.error(self._message)
            return False

        if _DEBUG:
            self._logger.info("Exporting tubes to folder " + self._currentPath)

        # Copy the files
        for fcsFile in dataSetFiles:
            if _DEBUG:
                self._logger.info("Copying " + str(fcsFile) + " to " + str(self._currentPath))
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
        touch = "/usr/bin/touch" if OSUtilities.isMacOS() else "/bin/touch"
        subprocess.call([touch, dstFile])
        subprocess.call(["/bin/cp", source, dstDir])
        self._logger.info("Copying file " + source + " to " + dstDir)
        self._numCopiedFiles += 1

    def _createDir(self, dirFullPath):
        """Creates the passed directory (with full path).
        """
        if os.path.isdir(dirFullPath):
            if _DEBUG:
                self._logger.info("Folder " + dirFullPath + " already exists.")
            return

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
        if self._rootExportPath == "" or self._experimentPath == "":
            return False

        # Make sure that the experiment folder does not already exist
        expPath = self._experimentPath

        # Does the folder already exist? It if does, append an increasing
        # numeric index.
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
        self._experimentPath = os.path.join(self._rootExportPath, expPath)

        # Create the root folder
        self._createDir(self._rootExportPath)

        # And now create the experiment folder (in the root folder)
        self._createDir(self._experimentPath)

        if _DEBUG:
            self._logger.info("Successfully created folder " + self._experimentPath)

        # Return success
        return True

    def _getDataSetsForPlate(self, platePermId):
        """
        Return a list of datasets belonging to the plate with specified ID.
        If none are found, return [].
        """

        # Get the wells
        wells = self._retrieveAllSamplesWithTypeAndParent(
            self._expSamplePrefix + "_WELL",
            platePermId,
            self._expSamplePrefix + "_PLATE")

        if len(wells) == 0:
            self._message = "Could not retrieve wells for plate with " \
            "code " + plateCode + "."
            self._logger.error(self._message)
            return wells

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
            self._logger.error(self._message)

        # Return
        return dataSets

    def _getDataSetForWell(self, wellId=None):
        """
        Get the datasets belonging to the well with specified code. If none
        are found, return [].

        If no wellId is given, it is assumed that the well is the passed
        entity with code self._entityId.
        """

        if wellId is None:
            wellId = self._entityId

        # Set search criteria to retrieve the dataset contained in the well
        searchCriteria = SearchCriteria()
        wellCriteria = SearchCriteria()
        wellCriteria.addMatchClause(
            MatchClause.createAttributeMatch(
                MatchClauseAttribute.CODE,
                wellId)
            )
        searchCriteria.addSubCriteria(
            SearchSubCriteria.createSampleCriteria(
                wellCriteria)
        )
        dataSets = searchService.searchForDataSets(searchCriteria)

        if len(dataSets) == 0:
            self._message = "Could not retrieve datasets for well " \
            "with code " + wellId + "."
            self._logger.error(self._message)

        # Return
        return dataSets

    def _getDataSetForTube(self, tubeCode):
        """
        Get the datasets belonging to the tube with specified tube code.
        If none is found, return [].
        """

        if _DEBUG:
            self._logger.info("Searching for tube with code " + tubeCode)

        # Set search criteria to retrieve the dataset contained in the tube
        searchCriteria = SearchCriteria()
        tubeCriteria = SearchCriteria()
        tubeCriteria.addMatchClause(
            MatchClause.createAttributeMatch(
                MatchClauseAttribute.CODE,
                tubeCode))
        searchCriteria.addSubCriteria(
            SearchSubCriteria.createSampleCriteria(
                tubeCriteria)
            )
        dataSets = searchService.searchForDataSets(searchCriteria)

        if _DEBUG:
            self._logger.info("Retrieved " + str(len(dataSets)) + \
                              " datasets for tube with code " + tubeCode)

        if len(dataSets) == 0:
            self._message = "Could not retrieve datasets for tube " \
            "with code " + tubeCode + "."
            self._logger.error(self._message)

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
            self._logger.error(self._message)

        # Return the files
        return dataSetFiles

    def _getAllPlates(self):
        """
        Get all plates in the experiment. Returns [] if none are found.
        """

        # Set the sample type
        sampleType = self._expSamplePrefix + "_PLATE"

        if _DEBUG:
            self._logger.info("Finding all samples of type " + sampleType)

        # Retrieve all plates
        plates = self._retrieveAllSamplesWithTypeAndParentWithPermId(sampleType,
                                                           self._expSamplePermId,
                                                           self._expSampleType)
        if _DEBUG:
            self._logger.info("Found " + str(len(plates)) + " plates.")

        if len(plates) == 0:
            self._message = "No plates found!"
            self._logger.error(self._message)
            return []

        # Return the tubes
        return plates

    def _getAllTubes(self):
        """
        Get all tubes in the experiment. Returns [] if none are found.
        """

        # Set the sample type
        sampleType = self._expSamplePrefix + "_TUBE"

        if _DEBUG:
            self._logger.info("Finding all samples of type " + sampleType)

        # Retrieve all tubes
        tubes = self._retrieveAllSamplesWithTypeAndParentWithPermId(sampleType,
                                                          self._expSamplePermId,
                                                          self._expSampleType)
        if _DEBUG:
            self._logger.info("Found " + str(len(tubes)) + " tubes.")

        if len(tubes) == 0:
            self._message = "No tubes found!"
            self._logger.error(self._message)
            return tubes

        # Return the tubes
        return tubes


# Parse properties file for custom settings
def parsePropertiesFile():
    """Parse properties file for custom plug-in settings."""

    filename = "../core-plugins/flow/4/dss/reporting-plugins/export_flow_datasets/plugin.properties"
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

    # Make sure that there are no Windows line endings
    for var_name in var_names:
        properties[var_name] = properties[var_name].replace('\r', '')

    # Everything found
    return properties


# Plug-in entry point
#
# Input parameters:
#
# uid           : job unique identifier (see below)
# experimentId  : experiment identifier
# experimentSampleType: experiment type
# entityType    : entity type
# entityId      : entity ID
# mode          : requested mode of operation: one of 'normal', 'zip'.
#
# This method returns a table to the client with a different set of columns
# depending on whether the plug-in is called for the first time and the process
# is just started, or if it is queried for completeness at a later time.
#
# At the end of the first call, a table with following columns is returned:
#
# uid      : unique identifier of the running plug-in
# completed: indicated if the plug-in has finished. This is set to False in the
#            first call.
#
# Later calls return a table with the following columns:
#
# uid      : unique identifier of the running plug-in. This was returned to
#            the client in the first call and was passed on again as a parameter.
#            Here it is returned again to make sure that client and server
#            always know which task they are talking about.
# completed: True if the process has completed in the meanwhile, False if it
#            is still running.
# success  : True if the process completed successfully, False otherwise.
# message  : error message in case success was False.
# nCopiedFiles: total number of copied files.
# relativeExpFolder: folder to the copied folder relative to the root of the
#            export folder.
# zipArchiveFileName: file name of the zip in case compression was requested.
# mode     : requested mode of operation.
def aggregate(parameters, tableBuilder):

    # Get the ID of the call if it already exists
    uid = parameters.get("uid");

    if uid is None or uid == "":

        # Create a unique id
        uid = str(uuid.uuid4())

        # Add the table headers
        tableBuilder.addHeader("uid")
        tableBuilder.addHeader("completed")

        # Fill in relevant information
        row = tableBuilder.addRow()
        row.setCell("uid", uid)
        row.setCell("completed", False)

        # Launch the actual process in a separate thread
        thread = Thread(target=aggregateProcess,
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

    # Add the table headers
    tableBuilder.addHeader("uid")
    tableBuilder.addHeader("completed")
    tableBuilder.addHeader("success")
    tableBuilder.addHeader("message")
    tableBuilder.addHeader("nCopiedFiles")
    tableBuilder.addHeader("relativeExpFolder")
    tableBuilder.addHeader("zipArchiveFileName")
    tableBuilder.addHeader("mode")

    # Store current results in the table
    row = tableBuilder.addRow()
    row.setCell("uid", resultToSend["uid"])
    row.setCell("completed", resultToSend["completed"])
    row.setCell("success", resultToSend["success"])
    row.setCell("message", resultToSend["message"])
    row.setCell("nCopiedFiles", resultToSend["nCopiedFiles"])
    row.setCell("relativeExpFolder", resultToSend["relativeExpFolder"])
    row.setCell("zipArchiveFileName", resultToSend["zipArchiveFileName"])
    row.setCell("mode", resultToSend["mode"])


# Actual work process
def aggregateProcess(parameters, tableBuilder, uid):

    # Make sure to initialize and store the results. We need to have them since
    # most likely the client will try to retrieve them again before the process
    # is finished.
    resultToStore = {}
    resultToStore["uid"] = uid
    resultToStore["success"] = True
    resultToStore["completed"] = False
    resultToStore["message"] = ""
    resultToStore["nCopiedFiles"] = ""
    resultToStore["relativeExpFolder"] = ""
    resultToStore["zipArchiveFileName"] = ""
    resultToStore["mode"] = ""
    LRCache.set(uid, resultToStore)

    # Get path to containing folder
    # __file__ does not work (reliably) in Jython
    dbPath = "../core-plugins/flow/4/dss/reporting-plugins/export_flow_datasets"

    # Path to the logs subfolder
    logPath = os.path.join(dbPath, "logs")

    # Make sure the logs subforder exist
    if not os.path.exists(logPath):
        os.makedirs(logPath)

    # Path for the log file
    logFile = os.path.join(logPath, "log.txt")

    # Set up logging
    logging.basicConfig(filename=logFile, level=logging.DEBUG,
                        format='%(asctime)-15s %(levelname)s: %(message)s')
    logger = logging.getLogger()

    # Get parameters from plugin.properties
    properties = parsePropertiesFile()
    if properties is None:
        msg = "Could not process plugin.properties"
        logger.error(msg)
        raise Exception(msg)

    if properties['base_dir'] == "" or properties['export_dir'] == '':
        msg = "Please set valid value for 'base_dir' and 'export_dir' in plugin.properties"
        logger.error(msg)
        raise Exception(msg)

    # Dump the properties dictionary to log
    logger.info(str(parameters))

    # Get the task
    task = parameters["task"]

    # Get the experiment identifier
    collectionId = parameters["collectionId"]

    # Get the experiment type
    collectionType = parameters["collectionType"]

    # Get the experiment sample identifier
    expSampleId = parameters.get("expSampleId")

    # Get the experiment sample perm identifier
    expSamplePermId = parameters.get("expSamplePermId")

    # Get the experiment sample type
    expSampleType = parameters.get("expSampleType")

    # Get the plate code
    platePermId = parameters.get("platePermId")

    # Get the plate type
    plateType = parameters.get("plateType")

    # Get the mode
    mode = parameters.get("mode")

    # Info
    logger.info("Aggregation plug-in called with following parameters:")
    logger.info("task            = " + task)
    logger.info("collectionId    = " + collectionId)
    logger.info("collectionType  = " + collectionType)
    logger.info("expSampleId     = " + expSampleId)
    logger.info("expSamplePermId = " + expSamplePermId)
    logger.info("expSampleType   = " + expSampleType)
    logger.info("platePermId     = " + platePermId)
    logger.info("plateType       = " + plateType)
    logger.info("mode            = " + mode)
    logger.info("userId          = " + userId)
    logger.info("Aggregation plugin properties:")
    logger.info("properties      = " + str(properties))

    # Consistency check: task must be one of a known set
    if task != "EXPERIMENT_SAMPLE" and \
        task != "ALL_PLATES" and \
        task != "PLATE" and \
        task != "TUBESET":
        msg = "The requested task " + task + " is not known!"
        logger.error(msg)
        raise Exception(msg)

    logger.info("Requested task: " + task)

    # Instantiate the Mover object - userId is a global variable
    # made available to the aggregation plug-in
    mover = Mover(task, collectionId, collectionType, expSampleId, expSamplePermId,
                  expSampleType, platePermId, plateType, mode, userId, properties, logger)

    # Process
    success = mover.process()
    logger.info("Process ended successfully.")

    # Compress
    if mode == "zip":
        mover.compressIfNeeded()

    # Get some results info
    nCopiedFiles = mover.getNumberOfCopiedFiles()
    errorMessage = mover.getErrorMessage()
    relativeExpFolder = mover.getRelativeRootExperimentPath()
    zipFileName = mover.getZipArchiveFileName()

    # Update results and store them
    resultToStore["uid"] = uid
    resultToStore["completed"] = True
    resultToStore["success"] = success
    resultToStore["message"] = errorMessage
    resultToStore["nCopiedFiles"] = nCopiedFiles
    resultToStore["relativeExpFolder"] = relativeExpFolder
    resultToStore["zipArchiveFileName"] = zipFileName
    resultToStore["mode"] = mode
    LRCache.set(uid, resultToStore)

    # Email result to the user
    if success == True:

        subject = "Flow export: successfully processed requested data"

        if nCopiedFiles == 1:
            snip = "One file was "
        else:
            snip = str(nCopiedFiles) + " files were "

        if mode == "normal":
            body = snip + "successfully exported to {...}/" + relativeExpFolder + "."
        else:
            body = snip + "successfully packaged for download: " + zipFileName

    else:
        subject = "Flow export: error processing request!"
        body = "Sorry, there was an error processing your request. " + \
        "Please send your administrator the following report:\n\n" + \
        "\"" + errorMessage + "\"\n"

    # Send
    try:
        mailService.createEmailSender().withSubject(subject).withBody(body).send()
    except:
        sys.stderr.write("export_flow_datasets: Failure sending email to user!")
