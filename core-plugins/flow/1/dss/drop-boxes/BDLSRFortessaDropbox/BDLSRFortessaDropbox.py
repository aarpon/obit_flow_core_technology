"""
@author: Aaron Ponti
"""
import re
import os
import logging
import xml.etree.ElementTree as xml
from datetime import datetime

class Processor:
    """The Processor class performs all steps required for registering datasets
    from the assigned dropbox folder."""

    # A transaction object passed by openBIS
    transaction = None

    # The incoming folder to process (a java.io.File object)
    incoming = ""

    # Constructor
    def __init__(self, transaction, logFile):

        self.transaction = transaction
        self.incoming = transaction.getIncoming()

        # Set up logging
        logging.basicConfig(filename=logFile, level=logging.DEBUG)

    def createExperiment(self, expId, expName,
                         expType="LSR_FORTESSA_EXPERIMENT"):
        """Create an experiment with given Experiment ID extended with the addition
        of a string composed from current date and time.

        @param expID, the experiment ID
        @param expName, the experiment name
        @param expType, the experiment type that must already exist; optional,
        default is "LSR_FORTESSA_EXPERIMENT"
        """

        # Make sure to keep the code length within the limits imposed by
        # openBIS for codes
        if len(expId) > 41:
            expId = expId[0:41]

        # Create univocal ID
        expId = expId + "_" + self.getCustomTimeStamp()

        # Create the experiment
        logging.info("Register experiment %s" % expId)
        exp = self.transaction.createNewExperiment(expId, expType)
        if not exp:
            msg = "Could not create experiment " + expId + "!"
            logging.error(msg)
            raise Exception(msg)
        else:
            logging.info("Created experiment with ID " + expId + ".")

        # Store the name
        exp.setPropertyValue("LSR_FORTESSA_EXPERIMENT_NAME", expName)

        return exp

    def createSampleWithGenCode(self, spaceCode,
                                sampleType="LSR_FORTESSA_PLATE"):
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
            logging.error(msg)
            raise Exception(msg)

        return sample

    def formatExpDateForPostgreSQL(self, expDate):
        """Format the experiment date to be compatible with postgreSQL's
        'timestamp' data type

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
            logging.info("Invalid experiment date %s found. " \
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
        """Returns a list of subfolders of the passed incoming directory.

        @return list of subfolders (String)
        """

        incomingStr = self.incoming.getAbsolutePath()
        return [name for name in os.listdir(incomingStr)
                if os.path.isdir(os.path.join(incomingStr, name))]

    def processExperiment(self, experimentNode,
                          openBISExpType="LSR_FORTESSA_EXPERIMENT"):
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

        # Create the experiment (with corrected ID if needed: see above)
        openBISExperiment = self.createExperiment(openBISIdentifier,
                                                  expName, openBISExpType)
        if not openBISExperiment:
            msg = "Could not create experiment " + openBISIdentifier
            logging.error(msg)
            raise Exception(msg)

        # Set the date
        openBISExperiment.setPropertyValue("LSR_FORTESSA_EXPERIMENT_DATE",
                                           expDate)
        # Set the description
        openBISExperiment.setPropertyValue("LSR_FORTESSA_EXPERIMENT_DESCRIPTION",
                                           description)

        # Set the acquisition hardware
        openBISExperiment.setPropertyValue("LSR_FORTESSA_EXPERIMENT_ACQ_HARDWARE",
                                           acqHardware)

        # Set the acquisition software
        openBISExperiment.setPropertyValue("LSR_FORTESSA_EXPERIMENT_ACQ_SOFTWARE",
                                           acqSoftware)

        # Set the experiment owner
        openBISExperiment.setPropertyValue("LSR_FORTESSA_EXPERIMENT_OWNER",
                                           owner)

        # Return the openBIS Experiment object
        return openBISExperiment

    def processFCSFile(self, fcsFileNode, openBISTube, openBISExperiment):
        """Register the FCS File using the parsed properties file

        @param fcsFileNode An XML node corresponding to an FCS file (dataset)
        @param openBISTube  An ISample object representing a Tube or Well
        @param openBISExperiment An ISample object representing an Experiment
        """

        # Dataset type
        datasetType = "LSR_FORTESSA_FCSFILE"

        # Create a new dataset
        dataset = self.transaction.createNewDataSet()
        if not dataset:
            msg = "Could not get or create dataset"
            logging.error(msg)
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
        logging.info("PROCESSFCSFILE: Registering file: " + fileName)

        # Move the file
        self.transaction.moveFile(fileName, dataset)


    def processTray(self, trayNode, openBISExperiment):
        """Register a Tray (Plate) based on the Tray XML node
        and an IExperimentUpdatable object

        @param trayNode An XML node corresponding to a Tray (Plate)
        @param openBISExperiment An IExperimentUpdatable object
        @param openBISSampleType sample type (default "LSR_FORTESSA_PLATE")
        @return ISample sample, or null
        """

        # openBIS sample type
        openBISSampleType = "LSR_FORTESSA_PLATE"

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
            logging.error(msg)
            raise Exception(msg)

        # Set the experiment for the sample
        openBISTray.setExperiment(openBISExperiment)

        # Set the plate name
        openBISTray.setPropertyValue("LSR_FORTESSA_PLATE_NAME", name)

        # Set the tray geometry
        openBISTray.setPropertyValue("LSR_FORTESSA_PLATE_GEOMETRY", trayGeometry)

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
        @param openBISSpecimenType (default "LSR_FORTESSA_TUBE"), the
        sample type. One of LSR_FORTESSA_TUBE and LSR_FORTESSA_WELL.
        @return ISample sample, or null
        """

        # Get the name
        name = tubeNode.attrib.get("name")

        # openBIS type
        if tubeNode.tag == "Tube":
            openBISSpecimenType = "LSR_FORTESSA_TUBE"
        elif tubeNode.tag == "Well":
            openBISSpecimenType = "LSR_FORTESSA_WELL"
        else:
            msg = "Unknown tube type" + tubeNode.tag
            logging.error(msg)
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
            logging.error(msg)
            raise Exception(msg)

        # Set the experiment to which it belongs
        openBISTube.setExperiment(openBISExperiment)

        # Set the Specimen name as a property
        openBISTube.setPropertyValue("LSR_FORTESSA_SPECIMEN", specimenName)

        # Set the name
        if openBISSpecimenType == "LSR_FORTESSA_TUBE":
            openBISTube.setPropertyValue("LSR_FORTESSA_TUBE_NAME", name)
        elif openBISSpecimenType == "LSR_FORTESSA_WELL":
            openBISTube.setPropertyValue("LSR_FORTESSA_WELL_NAME", name)
        else:
            msg = "Unknown value for openBISSpecimenType."
            logging.error(msg)
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
        openBISSampleType = "LSR_FORTESSA_TUBESET"

        # Get the identifier of the space all relevant attributes
        openBISSpaceIdentifier = \
            experimentNode.attrib.get("openBISSpaceIdentifier")

        # Create the sample. The Tubeset is configured in openBIS to
        # auto-generate its own identifier.
        openBISTubeSet = self.createSampleWithGenCode(openBISSpaceIdentifier,
                                                      openBISSampleType)
        if not openBISTubeSet:
            msg = "Could not get or create TubeSet"
            logging.error(msg)
            raise Exception(msg)

        # Set the experiment for the sample
        openBISTubeSet.setExperiment(openBISExperiment)

        logging.info("PROCESS_TUBESET: Created new TubeSet " \
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
                logging.error(msg)
                raise Exception(msg)

            # Process an Experiment XML node and get/create an IExperimentUpdatable
            openBISExperiment = self.processExperiment(experimentNode,
                                                       "LSR_FORTESSA_EXPERIMENT")

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
                            logging.error(msg)
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
                                logging.error(msg)
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
                            logging.error(msg)
                            raise Exception(msg)

                        # The only information we need from the Specimen is its
                        # name to associate to the Wells as property
                        specimenNameProperty = specimenNode.attrib.get("name")

                        for wellNode in specimenNode:

                            # The child of a Specimen is a Tube
                            if wellNode.tag != "Well":
                                msg = "Expected Well node!"
                                logging.error(msg)
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
                                    logging.error(msg)
                                    raise Exception(msg)

                                # Process the FCS file node
                                self.processFCSFile(fcsNode, openBISWell,
                                                    openBISExperiment)

                else:

                    msg = "The Node must be either a Specimen or a Tray"
                    logging.error(msg)
                    raise Exception(msg)

        # Log that we are finished with the registration
        logging.info("REGISTER: Registration completed")


    def run(self):
        """Run the registration."""

        # Make sure that incoming is a folder
        if not self.incoming.isDirectory():
            msg = "Incoming MUST be a folder!"
            logging.error(msg)
            raise Exception(msg)

        # Log
        logging.info("Incoming folder: " + self.incoming.getAbsolutePath())

        # There must be just one subfolder: the user subfolder
        subFolders = self.getSubFolders()
        if len(subFolders) != 1:
            msg = "Expected user subfolder!"
            logging.error(msg)
            raise Exception(msg)

        # Set the user folder
        userFolder = os.path.join(self.incoming.getAbsolutePath(),
                                  subFolders[0])

        # In the user subfolder we must find the data_structure.ois file
        dataFileName = os.path.join(userFolder, "data_structure.ois")
        if not os.path.exists(dataFileName):
            msg = "File data_structure.ois not found!"
            logging.error(msg)
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
            logging.info("* * * Processing: " + propertiesFile)

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
    dbPath = "../core-plugins/flow/1/dss/drop-boxes/BDLSRFortessaDropbox"

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
