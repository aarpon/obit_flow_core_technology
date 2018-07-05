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
    _transaction = None

    # The incoming folder to process (a java.io.File object)
    _incoming = ""

    # The user name
    _username = ""

    # The logger
    _logger = None

    # Constructor
    def __init__(self, transaction, logFile):

        self._transaction = transaction
        self._incoming = transaction.getIncoming()
        self._username = ""

        # Set up logging
        logging.basicConfig(filename=logFile, level=logging.DEBUG,
                            format='%(asctime)-15s %(levelname)s: %(message)s')
        self._logger = logging.getLogger("BDInflux")

    def dictToXML(self, d):
        """Converts a dictionary into an XML string."""

        # Create an XML node
        node = xml.Element("Parameters")

        # Add all attributes to the XML node
        for k, v in d.iteritems():
            node.set(k, v)

        # Convert to XML string
        xmlString = xml.tostring(node, encoding="UTF-8")

        # Return the XML string
        return xmlString

    def createExperiment(self, expId, expName):
        """Create an experiment with given Experiment ID extended with the addition
        of a string composed from current date and time.

        @param expID, the experiment ID
        @param expName, the experiment name
        """

        # Make sure to keep the code length within the limits imposed by
        # openBIS for codes
        if len(expId) > 41:
            expId = expId[0:41]

        # Create univocal ID
        expId = expId + "_" + self.getCustomTimeStamp()

        # Create the experiment
        self._logger.info("Register experiment %s" % expId)
        exp = self._transaction.createNewExperiment(expId, "INFLUX_EXPERIMENT")
        if not exp:
            msg = "Could not create experiment " + expId + "!"
            self._logger.error(msg)
            raise Exception(msg)
        else:
            self._logger.info("Created experiment with ID " + expId + ".")

        # Store the name
        exp.setPropertyValue("INFLUX_EXPERIMENT_NAME", expName)

        return exp

    def createSampleWithGenCode(self, spaceCode, openBISExperiment, sampleType):
        """Create a sample with automatically generated code.

        @param spaceCode, the code of the space
        @param openBISExperiment, the openBIS Experiment object
        @param sampleType, the sample type that must already exist
        @return sample An ISample
        """

        if self._transaction.serverInformation.get('project-samples-enabled') == 'true':

            identifier = openBISExperiment.getExperimentIdentifier()
            project_identifier = identifier[:identifier.rfind('/')]
            sample = self._transaction.createNewProjectSampleWithGeneratedCode(project_identifier,
                                                                               sampleType)
        else:

            # Make sure there are not slashes in the spaceCode
            spaceCode = spaceCode.replace("/", "")

            # Create the sample
            sample = self._transaction.createNewSampleWithGeneratedCode(spaceCode,
                                                                    sampleType)

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

        incomingStr = self._incoming.getAbsolutePath()
        return [name for name in os.listdir(incomingStr)
                if os.path.isdir(os.path.join(incomingStr, name))]

    def processExperiment(self, experimentNode):
        """Register an IExperimentUpdatable based on the Experiment XML node.

        @param experimentNode An XML node corresponding to an Experiment
        @return IExperimentUpdatable experiment
        """

        # Get the experiment version
        expVersion = experimentNode.attrib.get("version")
        if expVersion is None:
            expVersion = "0"

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
        openBISExperiment = self.createExperiment(openBISIdentifier, expName)
        if not openBISExperiment:
            msg = "Could not create experiment " + openBISIdentifier
            self._logger.error(msg)
            raise Exception(msg)

        # Get comma-separated tag list
        tagList = experimentNode.attrib.get("tags")
        if tagList != None and tagList != "":

            # Retrieve or create the tags
            openBISTags = self.retrieveOrCreateTags(tagList)

            # Set the metaprojects (tags)
            for openBISTag in openBISTags:
                openBISTag.addEntity(openBISExperiment)

        # Set the experiment version
        openBISExperiment.setPropertyValue("INFLUX_EXPERIMENT_VERSION",
                                           expVersion)

        # Set the date
        openBISExperiment.setPropertyValue("INFLUX_EXPERIMENT_DATE",
                                           expDate)

        # Set the description
        openBISExperiment.setPropertyValue("INFLUX_EXPERIMENT_DESCRIPTION",
                                           description)

        # Set the acquisition hardware
        openBISExperiment.setPropertyValue("INFLUX_EXPERIMENT_ACQ_HARDWARE",
                                           acqHardware)

        # Set the acquisition hardware friendly name
        openBISExperiment.setPropertyValue("INFLUX_EXPERIMENT_ACQ_HARDWARE_FRIENDLY_NAME",
                                           self._machinename)

        # Set the acquisition software
        openBISExperiment.setPropertyValue("INFLUX_EXPERIMENT_ACQ_SOFTWARE",
                                           acqSoftware)

        # Set the experiment owner
        openBISExperiment.setPropertyValue("INFLUX_EXPERIMENT_OWNER",
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
                attachmentFilePath = os.path.join(self._incoming.getAbsolutePath(),
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
        @param openBISTube  An ISample object representing a Tube
        @param openBISExperiment An ISample object representing an Experiment
        """

        # Dataset type
        datasetType = "INFLUX_FCSFILE"

        # Create a new dataset
        dataset = self._transaction.createNewDataSet()
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

        # Get the parameter node
        for parameterNode in fcsFileNode:

            if parameterNode.tag != "FCSFileParamList":
                msg = "Expected FSC File Parameter List node!"
                self._logger.error(msg)
                raise Exception(msg)

            parametersXML = self.dictToXML(parameterNode.attrib)

            # Store the parameters in the INFLUX_FCSFILE_PARAMETERS property
            dataset.setPropertyValue("INFLUX_FCSFILE_PARAMETERS", parametersXML)

            # Log the parameters
            self._logger.info("FCS file parameters (XML): " + str(parametersXML))

        # Assign the file to the dataset (we will use the absolute path)
        fileName = fcsFileNode.attrib.get("relativeFileName")
        fileName = os.path.join(self._incoming.getAbsolutePath(), fileName)

        # Log
        self._logger.info("Registering file: " + fileName)

        # Move the file
        self._transaction.moveFile(fileName, dataset)

    def processTube(self, tubeNode, openBISContainerSample,
                          specimenName, openBISExperiment):
        """Register a Tube (as a child of a Specimen) based on the Tube XML
        node and an ISample object.

        The associated fcs file is attached as a IDataset

        @param tubeNode An XML node corresponding to a Tube
        @param openBISContainerSample  An ISample object that will contain
        the Tube
        @param specimenName Name of the specimen associated to the Tube
        @param openBISExperiment The IExperiment to which the Tube belongs
        @return ISample sample, or null
        """

        # Get the name
        name = tubeNode.attrib.get("name")

        # openBIS type
        if tubeNode.tag == "Tube":
            openBISSpecimenType = "INFLUX_TUBE"
        else:
            msg = "Unknown tube type" + tubeNode.tag
            self._logger.error(msg)
            raise Exception(msg)

        # Build the openBIS Identifier
        openBISSpaceIdentifier = \
            tubeNode.attrib.get("openBISSpaceIdentifier")

        # Create the sample. The Tube is configured in openBIS to
        # auto-generate its own identifier.
        openBISTube = self.createSampleWithGenCode(openBISSpaceIdentifier,
                                                   openBISExperiment,
                                                   openBISSpecimenType)
        if not openBISTube:
            msg = "Could not create sample with auto-generated identifier"
            self._logger.error(msg)
            raise Exception(msg)

        # Set the experiment to which it belongs
        openBISTube.setExperiment(openBISExperiment)

        # Set the Specimen name as a property
        openBISTube.setPropertyValue("INFLUX_SPECIMEN", specimenName)

        # Set the name
        openBISTube.setPropertyValue("INFLUX_TUBE_NAME", name)

        # Get the index sort property
        indexSort = tubeNode.attrib.get("indexSort")
        if indexSort is not None:
            openBISTube.setPropertyValue("INFLUX_TUBE_ISINDEXSORT", indexSort)

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
        openBISSampleType = "INFLUX_TUBESET"

        # Get the identifier of the space all relevant attributes
        openBISSpaceIdentifier = \
            experimentNode.attrib.get("openBISSpaceIdentifier")

        # Create the sample. The Tubeset is configured in openBIS to
        # auto-generate its own identifier.
        openBISTubeSet = self.createSampleWithGenCode(openBISSpaceIdentifier,
                                                      openBISExperiment,
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

        # Store the username
        self._username = root.attrib.get("userName")

        # Store the machine name
        machinename = root.attrib.get("machineName")
        if machinename is None:
            machinename = ""
        self._machinename = machinename

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
            openBISExperiment = self.processExperiment(experimentNode)

            # Process children of the Experiment
            for childNode in experimentNode:

                # The child of an Experiment can be a Tray or a Specimen
                nodeType = childNode.tag

                if nodeType == "Specimen":

                    # A specimen is a direct child of an experiment, and
                    # the FCS files are therefore associated to tubes.
                    # In this case, we create a virtual TubeSet sample
                    # container (one for all Tubes in the experiment).
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
                        openBISTube = self.processTube(tubeNode,
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

                    # The BD Influx cell sorter does not support plates
                    msg = "The BD Influx cell sorter does not support plates!"
                    self._logger.error(msg)
                    raise Exception(msg)

                else:

                    msg = "The Node must be a Specimen"
                    self._logger.error(msg)
                    raise Exception(msg)

        # Log that we are finished with the registration
        self._logger.info("Registration completed")

    def retrieveOrCreateTags(self, tagList):
        """Retrieve or create the tags (metaprojects) with specified names."""

        # Initialize openBISTags list
        openBISTags = []

        # Make sure tagList is not None
        if tagList is None:
            return []

        # Get the individual tag names (with no blank spaces)
        tags = ["".join(t.strip()) for t in tagList.split(",")]

        # Process all tags (metaprojects)
        for tag in tags:
            if len(tag) == 0:
                continue

            # Retrieve the tag (metaproject)
            metaproject = self._transaction.getMetaproject(tag, self._username)
            if metaproject is None:

                # Create the tag (metaproject)
                logging("Creating metaproject " + tag)

                metaproject = self._transaction.createNewMetaproject(tag,
                                                                     "",
                                                                     self._username)

                # Check that creation was succcessful
                if metaproject is None:
                    msg = "Could not create metaproject " + tag + \
                    "for user " + self._username
                    self._logger.error(msg)
                    raise Exception(msg)

            # Add the created metaproject to the list
            openBISTags.append(metaproject)

        return openBISTags

    def run(self):
        """Run the registration."""

        # Make sure that incoming is a folder
        if not self._incoming.isDirectory():
            msg = "Incoming MUST be a folder!"
            self._logger.error(msg)
            raise Exception(msg)

        # Log
        self._logger.info("Incoming folder: " + self._incoming.getAbsolutePath())

        # There must be just one subfolder: the user subfolder
        subFolders = self.getSubFolders()
        if len(subFolders) != 1:
            msg = "Expected user subfolder!"
            self._logger.error(msg)
            raise Exception(msg)

        # Set the user folder
        userFolder = os.path.join(self._incoming.getAbsolutePath(),
                                  subFolders[0])

        # In the user subfolder we must find the data_structure.ois file
        dataFileName = os.path.join(userFolder, "data_structure.ois")
        if not os.path.exists(dataFileName):
            msg = "File data_structure.ois not found!"
            self._logger.error(msg)
            raise Exception(msg)

        # Now read the data structure file and store all the pointers to
        # the properties files. The paths are stored relative to self._incoming,
        # so we can easily build the full file paths.
        propertiesFileList = []
        f = open(dataFileName)
        try:
            for line in f:
                line = re.sub('[\r\n]', '', line)
                propertiesFile = os.path.join(self._incoming.getAbsolutePath(),
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
    dbPath = "../core-plugins/flow/2/dss/drop-boxes/BDInfluxDropbox"

    # Path to the logs subfolder
    logPath = os.path.join(dbPath, "logs")

    # Make sure the logs subforder exist
    if not os.path.exists(logPath):
        os.makedirs(logPath)

    # Path for the log file
    logFile = os.path.join(logPath, "log.txt")

    # Create a Processor
    processor = Processor(transaction, logFile)

    # Run
    processor.run()
