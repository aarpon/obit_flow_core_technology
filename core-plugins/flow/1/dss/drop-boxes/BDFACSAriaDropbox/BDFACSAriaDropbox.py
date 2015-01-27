# -*- coding: utf-8 -*-

"""
@author: Aaron Ponti
"""

import re
import os
import logging
import xml.etree.ElementTree as xml
from datetime import datetime
import java.io.File
from org.apache.commons.io import FileUtils

#
# The Processor class performs all steps required for registering datasets
# from the assigned dropbox folder
#
class Processor:
    """Registers datasets from the dropbox folder"""

    # A transaction object passed by openBIS
    transaction = None

    # The incoming folder to process (a java.io.File object)
    incoming = ""

    # Constructor
    def __init__(self, transaction, logFile):

        self.transaction = transaction
        self.incoming = transaction.getIncoming()

        # Set up logging
        self._logger = logging.getLogger('BDFACSAriaDropbox')
        self._logger.setLevel(logging.DEBUG)
        fh = logging.FileHandler(logFile)
        fh.setLevel(logging.DEBUG)
        format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        formatter = logging.Formatter(format)
        fh.setFormatter(formatter)
        self._logger.addHandler(fh)


    def createExperiment(self, expId, expName,
                         expType="FACS_ARIA_EXPERIMENT"):
        """Create an experiment with given Experiment ID extended with the addition
        of a string composed from current date and time.

        @param expID, the experiment ID
        @param expName, the experiment name
        @param expType, the experiment type that must already exist; optional,
        default is "FACS_ARIA_EXPERIMENT"
        """

        # Make sure to keep the code length within the limits imposed by
        # openBIS for codes
        if len(expId) > 41:
            expId = expId[0:41]

        # Create univocal ID
        expId = expId + "_" + self.getCustomTimeStamp()

        # Create the experiment
        self._logger.info("Register experiment %s" % expId)
        exp = self.transaction.createNewExperiment(expId, expType)
        if not exp:
            msg = "Could not create experiment " + expId + "!"
            self._logger.error(msg)
            raise Exception(msg)
        else:
            self._logger.info("Created experiment with ID " + expId + ".")

        # Store the name
        exp.setPropertyValue("FACS_ARIA_EXPERIMENT_NAME", expName)

        return exp


    def createSampleWithGenCode(self, spaceCode,
                                sampleType="FACS_ARIA_PLATE"):
        """Create a sample with automatically generated code.

        @param spaceCode, the code of the space
        @param sampleType, the sample type that must already exist
        @return sample An ISample
        """

        # Make sure there are not slashes in the spaceCode
        spaceCode = spaceCode.replace("/", "")

        # Create the sample
        sample = self.transaction.createNewSampleWithGeneratedCode(spaceCode, sampleType)
        if not sample:
            msg = "Could not create sample with generated code"
            self._logger.error(msg)
            raise Exception(msg)

        return sample


    def formatExpDateForPostgreSQL(self, expDate):
        """Format the experiment date to be compatible with postgreSQL's
        'timestamp' data type.

        @param Date stored in the FCS file, in the form 01-JAN-2013
        @return Date in the form 2013-01-01
        """

        monthMapper = {'JAN': '01', 'FEB': '02', 'MAR': '03', 'APR': '04',
                       'MAY': '05', 'JUN': '06', 'JUL': '07', 'AUG': '08',
                       'SEP': '09', 'OCT': '10', 'NOV': '11', 'DEC': '12'}

        # Separate the date into day, month, and year
        (day, month, year) = expDate.split("-")

        # Try mapping the month to digits (e.g. "06"). If the mapping does
        # not work, return "NOT_FOUND"
        month = monthMapper.get(month, "NOT_FOUND")

        # Build the date in the correct format. If the month was not found,
        # return 01-01-1970
        if (month == "NOT_FOUND"):
            self._logger.info("Invalid experiment date %s found. " \
                         "Reverting to 1970/01/01." % expDate)
            return "1970-01-01"
        else:
            return (year + "-" + month + "-" + day)


    def getCustomTimeStamp(self):
        """Create an univocal time stamp based on the current date and time
        (works around incomplete API of Jython 2.5).
        """

        t = datetime.now()
        return (t.strftime("%y%d%m%H%M%S") + unicode(t)[20:])


    def getSubFolders(self):
        """Return a list of subfolders of the passed incoming directory.

        @return list of subfolders (String)
        """

        incomingStr = self.incoming.getAbsolutePath()
        return [name for name in os.listdir(incomingStr)
                if os.path.isdir(os.path.join(incomingStr, name))]


    def processExperiment(self, experimentNode,
                          openBISExpType="FACS_ARIA_EXPERIMENT"):
        """Register an IExperimentUpdatable based on the Experiment XML node.

        @param experimentNode An XML node corresponding to an Experiment
        @param openBISExpType The experiment type
        @return IExperimentUpdatable experiment
        """

        # Get the openBIS identifier
        openBISIdentifier = experimentNode.attrib.get("openBISIdentifier")

        # Get the experiment name
        expName = experimentNode.attrib.get("name")

        # Get the experiment date and reformat it to be compatible
        # with postgreSQL
        expDate = self.formatExpDateForPostgreSQL(experimentNode.attrib.get("date"))

        # Get the description
        description = experimentNode.attrib.get("description")

        # Get the acquisition hardware
        acqHardware = experimentNode.attrib.get("acq_hardware")

        # Get the acquisition software
        acqSoftware = experimentNode.attrib.get("acq_software")

        # Get the owner name
        owner = experimentNode.attrib.get("owner_name")

        # Get attachments
        attachments = experimentNode.attrib.get("attachments")

        # Create the experiment (with corrected ID if needed: see above)
        openBISExperiment = self.createExperiment(openBISIdentifier,
                                                  expName, openBISExpType)
        if not openBISExperiment:
            msg = "Could not create experiment " + openBISIdentifier
            self._logger.error(msg)
            raise Exception(msg)

        # Set the date
        openBISExperiment.setPropertyValue("FACS_ARIA_EXPERIMENT_DATE",
                                           expDate)
        # Set the description
        openBISExperiment.setPropertyValue("FACS_ARIA_EXPERIMENT_DESCRIPTION",
                                           description)

        # Set the acquisition hardware
        openBISExperiment.setPropertyValue("FACS_ARIA_EXPERIMENT_ACQ_HARDWARE",
                                           acqHardware)

        # Set the acquisition software
        openBISExperiment.setPropertyValue("FACS_ARIA_EXPERIMENT_ACQ_SOFTWARE",
                                           acqSoftware)

        # Set the experiment owner
        openBISExperiment.setPropertyValue("FACS_ARIA_EXPERIMENT_OWNER",
                                           owner)

        # Add the attachments
        if attachments is not None:

            # Extract all relative file names 
            attachmentFiles = attachments.split(";")

            for f in attachmentFiles:

                # This is an additional security step
                if f == '':
                    continue

                # Inform
                msg = "Adding file attachment " + f 
                self._logger.info(msg)

                # Build the full path
                attachmentFilePath = os.path.join(self.incoming.getAbsolutePath(),
                                                  f)

                # Extract the file name
                attachmentFileName = os.path.basename(attachmentFilePath)

                # Read the attachment into a byte array
                javaFile = java.io.File(attachmentFilePath)
                byteArray = FileUtils.readFileToByteArray(javaFile)

                # Add attachment
                openBISExperiment.addAttachment(attachmentFilePath,
                                                attachmentFileName,
                                                "", byteArray)

        # Return the openBIS Experiment object
        return openBISExperiment


    def processFCSFile(self, fcsFileNode, openBISTube, openBISExperiment):
        """Register the FCS File using the parsed properties file.

        @param fcsFileNode An XML node corresponding to an FCS file (dataset)
        @param openBISTube  An ISample object representing a Tube or Well
        @param openBISExperiment An ISample object representing an Experiment
        """

        # Dataset type
        datasetType = "FACS_ARIA_FCSFILE"

        # Create a new dataset
        dataset = self.transaction.createNewDataSet()
        if not dataset:
            msg = "Could not get or create dataset"
            self._logger.error(msg)
            raise Exception(msg)

        # Set the dataset type
        dataset.setDataSetType(datasetType)

        # Assign the dataset to the sample
        dataset.setSample(openBISTube)

        # Assign the dataset to the experiment
        dataset.setExperiment(openBISExperiment)

        # Set the file type
        dataset.setFileFormatType("FCS")

        # Assign the file to the dataset (we will use the absolute path)
        fileName = fcsFileNode.attrib.get("relativeFileName")
        fileName = os.path.join(self.incoming.getAbsolutePath(), fileName)

        # Log
        self._logger.info("Registering file: " + fileName)

        # Move the file
        self.transaction.moveFile(fileName, dataset)


    def processTray(self, trayNode, openBISExperiment):
        """Register a Tray (Plate) based on the Tray XML node
        and an IExperimentUpdatable object.

        @param trayNode An XML node corresponding to a Tray (Plate)
        @param openBISExperiment An IExperimentUpdatable object
        @param openBISSampleType sample type (default "FACS_ARIA_PLATE")
        @return ISample sample, or null
        """

        # openBIS sample type
        openBISSampleType = "FACS_ARIA_PLATE"

        # Get the identifier of the space all relevant attributes
        openBISSpaceIdentifier = \
            trayNode.attrib.get("openBISSpaceIdentifier")

        # Get the tray name
        name = trayNode.attrib.get("name")

        # Get the tray geometry
        trayGeometry = trayNode.attrib.get("trayGeometry")

        # Create the sample. The Plate is configured in openBIS to
        # auto-generate its own identifier.
        openBISTray = self.createSampleWithGenCode(openBISSpaceIdentifier,
                                                   openBISSampleType)
        if not openBISTray:
            msg = "Could not create plate sample."
            self._logger.error(msg)
            raise Exception(msg)

        # Set the experiment for the sample
        openBISTray.setExperiment(openBISExperiment)

        # Set the plate name
        openBISTray.setPropertyValue("FACS_ARIA_PLATE_NAME", name)

        # Set the tray geometry
        openBISTray.setPropertyValue("FACS_ARIA_PLATE_GEOMETRY", trayGeometry)

        # Return the openBIS ISample object
        return openBISTray


    def processTubeOrWell(self, tubeNode, openBISContainerSample,
                          specimenName, openBISExperiment):
        """Register a Tube or Well (as a child of a Specimen) based on the Tube or
        Well XML node and an ISample object.

        The associated fcs file is attached as a IDataset

        @param tubeNode An XML node corresponding to a Tube or Well
        @param openBISContainerSample  An ISample object that will contain
        the Tube or Well
        @param specimenName Name of the specimen associated to the Tube or Well
        @param openBISExperiment The IExperiment to which the Tube belongs
        @param openBISSpecimenType (default "FACS_ARIA_TUBE"), the
        sample type. One of FACS_ARIA_TUBE and FACS_ARIA_WELL.
        @return ISample sample, or null
        """

        # Get the name
        name = tubeNode.attrib.get("name")

        # openBIS type
        if tubeNode.tag == "Tube":
            openBISSpecimenType = "FACS_ARIA_TUBE"
        elif tubeNode.tag == "Well":
            openBISSpecimenType = "FACS_ARIA_WELL"
        else:
            msg = "Unknown tube type" + tubeNode.tag
            self._logger.error(msg)
            raise Exception(msg)

        # Build the openBIS Identifier
        openBISSpaceIdentifier = \
            tubeNode.attrib.get("openBISSpaceIdentifier")

        # Create the sample. The Tube/Well is configured in openBIS to
        # auto-generate its own identifier.
        openBISTube = self.createSampleWithGenCode(openBISSpaceIdentifier,
                                                   openBISSpecimenType)
        if not openBISTube:
            msg = "Could not create sample with auto-generated identifier"
            self._logger.error(msg)
            raise Exception(msg)

        # Set the experiment to which it belongs
        openBISTube.setExperiment(openBISExperiment)

        # Set the Specimen name as a property
        openBISTube.setPropertyValue("FACS_ARIA_SPECIMEN", specimenName)

        # Set the name
        if openBISSpecimenType == "FACS_ARIA_TUBE":
            openBISTube.setPropertyValue("FACS_ARIA_TUBE_NAME", name)
        elif openBISSpecimenType == "FACS_ARIA_WELL":
            openBISTube.setPropertyValue("FACS_ARIA_WELL_NAME", name)
        else:
            msg = "Unknown value for openBISSpecimenType."
            self._logger.error(msg)
            raise Exception(msg)

        # Get the index sort property
        indexSort = tubeNode.attrib.get("indexSort")
        if indexSort is not None:
            if tubeNode.tag == "Tube":
                openBISTube.setPropertyValue("FACS_ARIA_TUBE_ISINDEXSORT", indexSort)
            elif tubeNode.tag == "Well":
                openBISTube.setPropertyValue("FACS_ARIA_WELL_ISINDEXSORT", indexSort)
            else:
                msg = "Unknown tube type" + tubeNode.tag
                self._logger.error(msg)
                raise Exception(msg)

        # Set the TubeSet container
        openBISTube.setContainer(openBISContainerSample)

        # Return the openBIS ISample
        return openBISTube


    def processTubeSet(self, experimentNode, openBISExperiment):
        """Register a TubeSet (virtual tube container).

        @param experimentNode An XML node corresponding to an Experiment
        @param openBISExperiment An IExperimentUpdatable object
        @param openBISSampleType  The TubeSet sample type
        @return ISample sample, or null
        """

        # Sample type
        openBISSampleType = "FACS_ARIA_TUBESET"

        # Get the identifier of the space all relevant attributes
        openBISSpaceIdentifier = \
            experimentNode.attrib.get("openBISSpaceIdentifier")

        # Create the sample. The Tubeset is configured in openBIS to
        # auto-generate its own identifier.
        openBISTubeSet = self.createSampleWithGenCode(openBISSpaceIdentifier,
                                                      openBISSampleType)
        if not openBISTubeSet:
            msg = "Could not get or create TubeSet"
            self._logger.error(msg)
            raise Exception(msg)

        # Set the experiment for the sample
        openBISTubeSet.setExperiment(openBISExperiment)

        self._logger.info("Created new TubeSet " \
                     "with identifier %s, sample type %s" \
                     % (openBISTubeSet.getSampleIdentifier(),
                        openBISSampleType))

        # Return the openBIS ISample object
        return openBISTubeSet


    def register(self, tree):
        """Register the Experiment using the parsed properties file.

        @param tree ElementTree parsed from the properties XML file
        """

        # Get the root node (obitXML)
        root = tree.getroot()

        # Create a virtual TubeSet
        openBISTubeSet = None

        # Iterate over the children (Experiments)
        for experimentNode in root:

            # The tag of the immediate children of the root experimentNode
            # must be Experiment
            if experimentNode.tag != "Experiment":
                msg = "Expected Experiment node, found " + experimentNode.tag
                self._logger.error(msg)
                raise Exception(msg)

            # Process an Experiment XML node and get/create an IExperimentUpdatable
            openBISExperiment = self.processExperiment(experimentNode,
                                                       "FACS_ARIA_EXPERIMENT")

            # Process children of the Experiment
            for childNode in experimentNode:

                # The child of an Experiment can be a Tray or a Specimen
                nodeType = childNode.tag

                if nodeType == "Specimen":

                    # A specimen is a direct child of an experiment if there
                    # is no plate, and the FCS files are therefore associated
                    # to tubes. In this case, we create a virtual TubeSet
                    # sample container (one for all Tubes in the experiment).
                    if openBISTubeSet is None:
                        openBISTubeSet = self.processTubeSet(experimentNode,
                                                             openBISExperiment)

                    # The only information we need from the Specimen is its
                    # name to associate to the Tubes as property
                    specimenNameProperty = childNode.attrib.get("name")

                    # Now iterate over the children of the Specimen
                    for tubeNode in childNode:

                        # The child of a Specimen is a Tube
                        if tubeNode.tag != "Tube":
                            msg = "Expected Tube node!"
                            self._logger.error(msg)
                            raise Exception(msg)

                        # Process the tube node and get the openBIS object
                        openBISTube = self.processTubeOrWell(tubeNode,
                                                       openBISTubeSet,
                                                       specimenNameProperty,
                                                       openBISExperiment)

                        # Now process the FCS file
                        for fcsNode in tubeNode:

                            # The child of a Tube is an FCSFile
                            if fcsNode.tag != "FCSFile":
                                msg = "Expected FSC File node!"
                                self._logger.error(msg)
                                raise Exception(msg)

                            # Process the FCS file node
                            self.processFCSFile(fcsNode, openBISTube,
                                                openBISExperiment)

                elif nodeType == "Tray":

                    # Process the tray node and get the openBIS object
                    openBISTray = self.processTray(childNode,
                                                   openBISExperiment)

                    # Now iterate over the children of the Tray
                    for specimenNode in childNode:

                        # The child of a Tray is a Specimen
                        if specimenNode.tag != "Specimen":
                            msg = "Expected Specimen node!"
                            self._logger.error(msg)
                            raise Exception(msg)

                        # The only information we need from the Specimen is its
                        # name to associate to the Wells as property
                        specimenNameProperty = specimenNode.attrib.get("name")

                        for wellNode in specimenNode:

                            # The child of a Specimen is a Tube
                            if wellNode.tag != "Well":
                                msg = "Expected Well node!"
                                self._logger.error(msg)
                                raise Exception(msg)

                            # Process the tube node and get the openBIS object
                            openBISWell = self.processTubeOrWell(wellNode,
                                                           openBISTray,
                                                           specimenNameProperty,
                                                           openBISExperiment)

                            # Now process the FCS file
                            for fcsNode in wellNode:

                                # The child of a Tube is an FCSFile
                                if fcsNode.tag != "FCSFile":
                                    msg = "Expected FSC File node!"
                                    self._logger.error(msg)
                                    raise Exception(msg)

                                # Process the FCS file node
                                self.processFCSFile(fcsNode, openBISWell,
                                                    openBISExperiment)

                else:

                    msg = "The Node must be either a Specimen or a Tray"
                    self._logger.error(msg)
                    raise Exception(msg)

        # Log that we are finished with the registration
        self._logger.info("Registration completed")


    def run(self):
        """Run the registration."""

        # Make sure that incoming is a folder
        if not self.incoming.isDirectory():
            msg = "Incoming MUST be a folder!"
            self._logger.error(msg)
            raise Exception(msg)

        # Log
        self._logger.info("Incoming folder: " + self.incoming.getAbsolutePath())

        # There must be just one subfolder: the user subfolder
        subFolders = self.getSubFolders()
        if len(subFolders) != 1:
            msg = "Expected user subfolder!"
            self._logger.error(msg)
            raise Exception(msg)

        # Set the user folder
        userFolder = os.path.join(self.incoming.getAbsolutePath(),
                                  subFolders[0])

        # In the user subfolder we must find the data_structure.ois file
        dataFileName = os.path.join(userFolder, "data_structure.ois")
        if not os.path.exists(dataFileName):
            msg = "File data_structure.ois not found!"
            self._logger.error(msg)
            raise Exception(msg)

        # Now read the data structure file and store all the pointers to
        # the properties files. The paths are stored relative to self.incoming,
        # so we can easily build the full file paths.
        propertiesFileList = []
        f = open(dataFileName)
        try:
            for line in f:
                line = re.sub('[\r\n]', '', line)
                propertiesFile = os.path.join(self.incoming.getAbsolutePath(),
                                            line)
                propertiesFileList.append(propertiesFile)
        finally:
            f.close()

        # Process (and ultimately register) all experiments
        for propertiesFile in propertiesFileList:

            # Log
            self._logger.info("* * * Processing: " + propertiesFile + " * * *")

            # Read the properties file into an ElementTree
            tree = xml.parse(propertiesFile)

            # Now register the experiment
            self.register(tree)


def process(transaction):
    """Dropbox entry point.

    @param transaction, the transaction object
    """

    # Get path to containing folder
    # __file__ does not work (reliably) in Jython
    dbPath = "../core-plugins/flow/1/dss/drop-boxes/BDFACSAriaDropbox"

    # Path to the logs subfolder
    logPath = os.path.join(dbPath, "logs")

    # Make sure the logs subforder exist
    if not os.path.exists(logPath):
        os.makedirs(logPath)

    # Path for the log file
    logFile = os.path.join(logPath, "registration_log.txt")

    # Create a Processor
    processor = Processor(transaction, logFile)

    # Run
    processor.run()
