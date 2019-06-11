import re
import os
import logging
from datetime import datetime
from __builtin__ import None, True
import xml.etree.ElementTree as xml


class Processor:
    """The Processor class performs all steps required for registering datasets
    from the assigned dropbox folder."""

    # Constructor
    def __init__(self, transaction, prefix, version, logDir):

        # Store arguments
        self._transaction = transaction
        self._prefix = prefix
        self._version = version
        self._incoming = transaction.getIncoming()

        # Set up logging
        self._logger = self._setup_logger(logDir, prefix)

        # Store the transaction time stamp
        self._transactionTimeStamp = self._getCurrentTimeStampMS()

        # Keep track of the total number of samples in the transaction
        self._transactionSampleCount = 0

    def _supportIndexSorting(self, tubeSampleType):
        """Return true if the experiment with given prefix supports index sorting.
        
        @param tubeSampleType: Type of the sample Tube. 
        """
        prefix = tubeSampleType[:-5]
        if prefix in ["FACS_ARIA", "INFLUX", "MOFLO_XDP", "S3E"]:
            return True
        elif prefix in ["LSR_FORTESSA"]:
            return False
        else:
            raise Exception("Unknown prefix!")

    def _collectionNameFromIdentifier(self, openBISCollectionIdentifier):
        """Converts the collection identifier to a human-friendly string for
        the NAME property.
    
        @param openBISCollectionIdentifier Identifier of the collection object.
        @return string Human-friendly collection name.
        """
        try:
            collectionName = openBISCollectionIdentifier[
                            openBISCollectionIdentifier.rfind('/') + 1:].replace(
                "_", " ").capitalize()
        except:
            collectionName = "COLLECTION"

        return collectionName

    def _createSampleWithManagedCode(self,
                                     spaceCode,
                                     openBISCollection,
                                     sampleType,
                                     setExperiment=True):
        """Create a sample with automatically generated code.
    
        Depending on whether project samples are enabled in openBIS, the sample
        code will be created accordingly.
        
        If project samples are enabled, the code will be in the form: /SPACE/PROJECT/CODE
        If project samples are not enabled, the code will be in the form: /SPACE/CODE
    
        @param spaceCode The code of space (the space must exist).
        @param openBISCollection The openBIS Collection object (must exist).
        @param sampleType Sample type.
        @param setExperiment (optional, default = True) Set to true, to assign the
               newly created sample to the openBISCollection collection.
        @return sample Created ISample
        """

        if self._transaction.serverInformation.get('project-samples-enabled') == 'true':

            # Build sample identifier
            identifier = openBISCollection.getExperimentIdentifier()
            project_identifier = identifier[:identifier.rfind('/')]
            identifier = project_identifier + "/" + self._getUniqueSampleCode(sampleType)

        else:

            # Make sure there are not slashes in the spaceCode
            spaceCode = spaceCode.replace("/", "")

            # Build sample identifier
            identifier = "/" + spaceCode + "/" + self._getUniqueSampleCode(sampleType)

        # Inform
        self._logger.info("Creating sample of type " + sampleType + " with (unique) identifier " + identifier)

        # Create the sample
        sample = self._transaction.createNewSample(identifier, sampleType)

        # Set the experiment (collection)?
        if setExperiment:
            sample.setExperiment(openBISCollection)

        return sample

    def _dictToXML(self, d):
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

    def _formatExpDateForPostgreSQL(self, expDate):
        """Format the experiment date to be compatible with postgreSQL's
        'timestamp' data type.
    
        @param Date stored in the FCS file, in the form 01-JAN-2013
        @return Date in the form 2013-01-01
        """

        monthMapper = {'JAN': '01', 'FEB': '02', 'MAR': '03', 'APR': '04',
                       'MAY': '05', 'JUN': '06', 'JUL': '07', 'AUG': '08',
                       'SEP': '09', 'OCT': '10', 'NOV': '11', 'DEC': '12'}

        # Separate the date into day, month, and year
        tryOtherFormat = False
        try:
            day, month, year = expDate.split("-")
        except ValueError:
            tryOtherFormat = True

        if tryOtherFormat:
            try:
                day, month, year = expDate.split(" ")
            except ValueError:
                month = -1

        # Try mapping the month to digits (e.g. "06"). If the mapping does
        # not work, return "NOT_FOUND"
        month = monthMapper.get(month.upper(), "NOT_FOUND")

        # Build the date in the correct format. If the month was not found,
        # return 01-01-1970
        if month == "NOT_FOUND":
            return "1970-01-01"
        else:
            return year + "-" + month + "-" + day

    def _getCurrentTimeStampMS(self):
        """Create an univocal time stamp based on the current date and time
        (works around incomplete API of Jython 2.5).
        """

        t = datetime.now()
        return unicode(t.strftime("%y%d%m%H%M%S%f"))

    def _getUniqueSampleCode(self, sampleType):
        """Return a unique sample code based on the sample type,
        the transaction time stamp, and the global transaction
        sample count."""

        self._transactionSampleCount += 1
        return sampleType + "_" + self._transactionTimeStamp + "_" + str(self._transactionSampleCount)

    def _getOrCreateCollection(self,
                               openBISCollectionIdentifier):
        """Retrieve or register an openBIS Collection with given identifier.
    
        @param openBISCollectionIdentifier The collection openBIS identifier.
        @return IExperiment collection
        """

        # Try retrieving the collection
        collection = self._transaction.getExperiment(openBISCollectionIdentifier)

        # If the collection does not exist, create it
        if collection is None:

            # Create a new collection of type "COLLECTION"
            collection = self._transaction.createNewExperiment(
                openBISCollectionIdentifier, "COLLECTION")
            if collection is not None:
                # Set the collection name
                collectionName = self._collectionNameFromIdentifier(openBISCollectionIdentifier)
                collection.setPropertyValue("$NAME", collectionName)

        return collection

    def _getSubFolders(self, incoming):
        """Return a list of subfolders of the passed incoming directory.
    
        @param incoming Incoming folder.
        @return list of subfolders (String)
        """

        incomingStr = incoming.getAbsolutePath()
        return [name for name in os.listdir(incomingStr)
                if os.path.isdir(os.path.join(incomingStr, name))]

    def _processExperimentNode(self,
                               experimentNode,
                               openBISExperimentSampleType,
                               machineName):
        """Process an experiment node.
    
        The ExperimentNode maps to an openBIS Experiment Sample.
    
        The {...}_EXPERIMENT SAMPLE object has following structure:
    
                PARENTS  : samples of type ORGANIZATION_UNIT (tags)
    
                CHILDREN : samples of types {...}_TUBESET and {...}_TUBE
                           and, depending on the acquisition station,
                           also {...}_PLATE  and {...}_WELL
    
                CONTAINED: none
    
                DATASETS: datasets of type ATTACHMENT (several file extensions)
    
        @param experimentNode An XML node corresponding to {...}_EXPERIMENT (sample).
        @param openBISExperimentSampleType Type of the experiment sample.
        @param machineName Human-friendly name of the acquisition machine.
        @return tuple with a Sample of specified type {...}_EXPERIMENT and the
        corresponding Collection.
        """

        #
        # Extract attributes
        #

        # Get the openBIS openBISCollection identifier
        openBISCollectionIdentifier = experimentNode.attrib.get("openBISCollectionIdentifier")

        # Get the openBIS identifier
        openBISIdentifier = experimentNode.attrib.get("openBISIdentifier")

        # Make sure to keep the code length within the limits imposed by
        # openBIS for codes
        if len(openBISIdentifier) > 80:
            openBISIdentifier = openBISIdentifier[0:80]

        # Create univocal ID
        openBISIdentifier = openBISIdentifier + "_" + self._getCurrentTimeStampMS()

        # Get the experiment name
        expName = experimentNode.attrib.get("name")

        # Get the experiment date and reformat it to be compatible
        # with postgreSQL
        expDate = self._formatExpDateForPostgreSQL(experimentNode.attrib.get("date"))
        if expDate == "1970/01/01":
            self._logger.info("Invalid experiment date %s found. " \
                              "Reverting to 1970/01/01." % expDate)

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

        # Get comma-separated tag list
        tagList = experimentNode.attrib.get("tags")

        #
        # Create needed/requested openBIS objects
        #

        # Get or create the openBISCollection with given identifier
        openBISCollection = self._getOrCreateCollection(openBISCollectionIdentifier)

        if openBISCollection is None:
            msg = "Failed creating openBISCollection with ID " + \
                  openBISCollectionIdentifier + "."
            self._logger.error(msg)
            raise Exception(msg)

        # Make sure to create a new sample of type openBISExperimentSampleType
        openBISExperimentSample = self._transaction.createNewSample(openBISIdentifier,
                                                                    openBISExperimentSampleType)

        if openBISExperimentSample is None:
            msg = "Could not create " + openBISExperimentSampleType + \
                  " sample wit id " + openBISIdentifier
            self._logger.error(msg)
            raise Exception(msg)

        # Set the openBISCollection
        openBISExperimentSample.setExperiment(openBISCollection)

        # Add tags (create them if needed)
        if tagList != None and tagList != "":
            openBISExperimentSample = self._registerTags(openBISExperimentSample,
                                                   tagList)

        # Add the attachments
        if attachments is not None:

            if not self._registerAttachmentsToCollection(attachments,
                                                   openBISCollection,
                                                   openBISExperimentSample):
                # Error
                msg = "Adding attachments failed!"
                self._logger.error(msg)
                raise Exception(msg)

        #
        # Store properties
        #

        # Store the name (in both the {...}_EXPERIMENT_NAME
        # and $NAME properties). $NAME is used by the ELN-LIMS
        # user interface.
        openBISExperimentSample.setPropertyValue(
            "$NAME", expName)
        openBISExperimentSample.setPropertyValue(
            openBISExperimentSampleType + "_NAME", expName)

        # Set the experiment version
        openBISExperimentSample.setPropertyValue(
            openBISExperimentSampleType + "_VERSION", str(self._version))

        # Set the date
        openBISExperimentSample.setPropertyValue(
            openBISExperimentSampleType + "_DATE", expDate)

        # Set the description
        openBISExperimentSample.setPropertyValue(
            openBISExperimentSampleType + "_DESCRIPTION", description)

        # Set the acquisition hardware
        openBISExperimentSample.setPropertyValue(
            openBISExperimentSampleType + "_ACQ_HARDWARE", acqHardware)

        # Set the acquisition hardware friendly name
        openBISExperimentSample.setPropertyValue(
            openBISExperimentSampleType + "_ACQ_HARDWARE_FRIENDLY_NAME", machineName)

        # Set the acquisition software
        openBISExperimentSample.setPropertyValue(
            openBISExperimentSampleType + "_ACQ_SOFTWARE", acqSoftware)

        # Set the experiment owner
        openBISExperimentSample.setPropertyValue(
            openBISExperimentSampleType + "_OWNER", owner)

        #
        # Return
        #

        # Return the openBIS Experiment Sample object and the openBISCollection
        return openBISExperimentSample, openBISCollection

    def _processSpecimenNode(self,
                             specimenNode,
                             specimens,
                             openBISCollection,
                             openBISSpecimenSampleType,
                             specimenName):
        """Register a TubeSet (virtual tube container).
    
        The SpecimenNode maps to an openBIS {...}_SPECIMEN sample.
    
        The {...}_SPECIMEN SAMPLE object has following structure:
    
                PARENTS  : none
    
                CHILDREN : samples of type {...}_WELL or {...}_TUBE
    
                CONTAINED: none
    
                DATASETS: none
    
        @param specimenNode An XML node corresponding to a Specimen. 
        @param openBISCollection A Collection Sample object
        @param openBISSpecimenSampleType  The Specimen sample type
        @param openBISExperimentSampleIdentifier The identifier of the
               {...}_EXPERIMENT sample.
        @param specimenName The name of the Specimen.
        @return ISample sample, or null
        """

        # Get the identifier of the space all relevant attributes
        openBISSpaceIdentifier = specimenNode.attrib.get("openBISSpaceIdentifier")

        # If the Specimen object already exists, return it; otherwise,
        # create a new one.
        if specimenName in specimens:
            self._logger.info("Reusing Specimen " + specimenName)
            return specimens[specimenName]

        # Create the sample. The Specimen is configured in openBIS to
        # auto-generate its own identifier.
        openBISSpecimen = self._createSampleWithManagedCode(openBISSpaceIdentifier,
                                                            openBISCollection,
                                                            openBISSpecimenSampleType,
                                                            setExperiment=True)
        # Confirm creation
        if not openBISSpecimen:
            msg = "Could not get or create Specimen"
            self._logger.error(msg)
            raise Exception(msg)

        # Inform
        self._logger.info("Created new Specimen " \
                          "with identifier %s, sample type %s" \
                          % (openBISSpecimen.getSampleIdentifier(),
                             openBISSpecimenSampleType))

        # Set the name of the Specimen
        openBISSpecimen.setPropertyValue("$NAME", specimenName)

        # Return the openBIS ISample object
        return openBISSpecimen

    def _processTrayNode(self,
                         trayNode,
                         openBISCollection,
                         openBISExperimentSampleIdentifier,
                         openBISTraySampleType):
        """Register a Tray (Plate) based on the Tray XML node.
    
    
        The {...}_SPECIMEN SAMPLE object has following structure:
    
                PARENTS  : {...}_EXPERIMENT
    
                CHILDREN : samples of type {...}_WELL
    
                CONTAINED: none
    
                DATASETS: none
    
        @param trayNode An XML node corresponding to a Tray (Plate).
        @param openBISCollection An IExperimentUpdatable object.
        @param openBISExperimentSampleIdentifier The identifier of the {...}_EXPERIMENT sample.        
        @param openBISTraySampleType Tray sample type.
        @return ISample sample, or None.
        """

        # Get the identifier of the space all relevant attributes
        openBISSpaceIdentifier = trayNode.attrib.get("openBISSpaceIdentifier")

        # Get the tray name
        name = trayNode.attrib.get("name")

        # Get the tray geometry
        trayGeometry = trayNode.attrib.get("trayGeometry")

        # Create the sample. The Plate is configured in openBIS to
        # auto-generate its own identifier.
        openBISTray = self._createSampleWithManagedCode(openBISSpaceIdentifier,
                                                        openBISCollection,
                                                        openBISTraySampleType,
                                                        setExperiment=True)
        if not openBISTray:
            msg = "Could not create plate sample."
            self._logger.error(msg)
            raise Exception(msg)

        # Set the parent sample of type {...}_EXPERIMENT
        openBISTray.setParentSampleIdentifiers([openBISExperimentSampleIdentifier])

        # Set the plate name
        openBISTray.setPropertyValue(openBISTraySampleType + "_NAME", name)

        # Set the $NAME property to be compatible with ELN
        openBISTray.setPropertyValue("$NAME", name)

        # Set the tray geometry
        openBISTray.setPropertyValue(openBISTraySampleType + "_GEOMETRY", trayGeometry)

        # Return the openBIS ISample object
        return openBISTray

    def _processTube(self,
                     tubeNode,
                     openBISCollection,
                     openBISTubeSampleType,
                     openBISExperimentSample,
                     openBISSpecimenSample,
                     openBISTubeSetSample):
        """Register a Tube (as a child of a Specimen) based on the Tube XML node.
    
        The {...}_TUBE SAMPLE object has following structure:
    
                PARENTS  : {...}_EXPERIMENT, {...}_SPECIMEN, {...}_TUBESET
    
                CHILDREN : none
    
                CONTAINED: none
    
                DATASETS: {...}_FCSFILE (with corresponding .FCS files)
    
    
        @param tubeNode An XML node corresponding to a Tube. 
        @param openBISCollection The IExperiment to which the Tube belongs
        @param openBISTubeSampleType The Tube sample type.
        @param openBISExperimentSample The openBIS Experiment sample (parent).
        @param openBISSpecimenSample The openBIS Specimen sample (parent).
        @param openBISTubeSetSample The openBIS TubeSet sample (parent).
        @return ISample sample, or null
        """

        # Get the name
        name = tubeNode.attrib.get("name")

        # Build the openBIS Identifier
        openBISSpaceIdentifier = \
            tubeNode.attrib.get("openBISSpaceIdentifier")

        # Create the sample. The Tube/Well is configured in openBIS to
        # auto-generate its own identifier.
        openBISTube = self._createSampleWithManagedCode(openBISSpaceIdentifier,
                                                        openBISCollection,
                                                        openBISTubeSampleType,
                                                        setExperiment=True)

        if not openBISTube:
            msg = "Could not create TUBE sample with auto-generated identifier"
            self._logger.error(msg)
            raise Exception(msg)

        # Set the name property
        openBISTube.setPropertyValue(openBISTubeSampleType + "_NAME", name)

        # Set the $NAME property to be compatible with ELN
        openBISTube.setPropertyValue("$NAME", name)

        # Does the tube have an "indexSort" attribute?
        if self._supportIndexSorting(openBISTubeSampleType):
            indexSort = tubeNode.attrib.get("indexSort")
            if indexSort is not None:
                openBISTube.setPropertyValue(openBISTubeSampleType + "_ISINDEXSORT", indexSort)

        # Set the parents
        openBISTube.setParentSampleIdentifiers([
            openBISExperimentSample.getSampleIdentifier(),
            openBISSpecimenSample.getSampleIdentifier(),
            openBISTubeSetSample.getSampleIdentifier()
            ])

        # Return the openBIS Tube sample
        return openBISTube

    def _processTubeSetNode(self,
                            experimentNode,
                            openBISCollection,
                            openBISTubeSetSampleType,
                            openBISExperimentSampleIdentifier):
        """Register a TubeSet (virtual tube container).
    
        The TubeSetNode maps to an openBIS {...}_TUBESET sample.
    
        The {...}_TUBESET SAMPLE object has following structure:
    
                PARENTS  : sample of type {...}_EXPERIMENT.
    
                CHILDREN : samples of type {...}_TUBE
    
                CONTAINED: none
    
                DATASETS: none
    
        @param experimentNode An XML node corresponding to a (virtual) TubeSet.
        @param openBISCollection A Collection Sample object
        @param openBISTubeSetSampleType  The TubeSet sample type
        @param openBISExperimentSampleIdentifier The identifier of the
               {...}_EXPERIMENT sample.
        @return ISample sample, or null
        """

        # Get the identifier of the space all relevant attributes
        openBISSpaceIdentifier = \
            experimentNode.attrib.get("openBISSpaceIdentifier")

        # Create the sample. The Tubeset is configured in openBIS to
        # auto-generate its own identifier.
        openBISTubeSet = self._createSampleWithManagedCode(openBISSpaceIdentifier,
                                                           openBISCollection,
                                                           openBISTubeSetSampleType,
                                                           setExperiment=True)
        # Confirm creation
        if not openBISTubeSet:
            msg = "Could not get or create TubeSet"
            self._logger.error(msg)
            raise Exception(msg)

        # Inform
        self._logger.info("Created new TubeSet " \
                          "with identifier %s, sample type %s" \
                          % (openBISTubeSet.getSampleIdentifier(),
                             openBISTubeSetSampleType))

        # Set the parent sample of type {...}_EXPERIMENT
        openBISTubeSet.setParentSampleIdentifiers([openBISExperimentSampleIdentifier])

        # Return the openBIS ISample object
        return openBISTubeSet

    def _processWell(self,
                     wellNode,
                     openBISCollection,
                     openBISWellSampleType,
                     openBISExperimentSample,
                     openBISSpecimenSample,
                     openBISPlateSample):
        """Register a Well based on the Well XML node.
    
        The {...}_WELL SAMPLE object has following structure:
    
                PARENTS  : {...}_EXPERIMENT, {...}_SPECIMEN, {...}_PLATE
    
                CHILDREN : none
    
                CONTAINED: none
    
                DATASETS: {...}_FCSFILE (with corresponding .FCS files)
    
    
        @param wellNode An XML node corresponding to a Well. 
        @param openBISCollection The IExperiment to which the Tube belongs
        @param openBISWellSampleType The Well sample type.
        @param openBISExperimentSample The openBIS Experiment sample (parent).
        @param openBISSpecimenSample The openBIS Specimen sample (parent).
        @param openBISPlateSample The openBIS Plate sample (parent).
        @return ISample sample, or null
        """

        # Get the name
        name = wellNode.attrib.get("name")

        # Build the openBIS Identifier
        openBISSpaceIdentifier = wellNode.attrib.get("openBISSpaceIdentifier")

        # Create the sample. The Tube/Well is configured in openBIS to
        # auto-generate its own identifier.
        openBISWell = self._createSampleWithManagedCode(openBISSpaceIdentifier,
                                                        openBISCollection,
                                                        openBISWellSampleType,
                                                        setExperiment=True)

        if not openBISWell:
            msg = "Could not create WELL sample with auto-generated identifier"
            self._logger.error(msg)
            raise Exception(msg)

        # Set the name property
        openBISWell.setPropertyValue(openBISWellSampleType + "_NAME", name)

        # Set the $NAME property to be compatible with ELN
        openBISWell.setPropertyValue("$NAME", name)

        # Set the parents
        openBISWell.setParentSampleIdentifiers([
            openBISExperimentSample.getSampleIdentifier(),
            openBISSpecimenSample.getSampleIdentifier(),
            openBISPlateSample.getSampleIdentifier()
            ])

        # Return the openBIS Tube sample
        return openBISWell

    def _processFCSFile(self,
                        fcsFileNode,
                        openBISDataSetType,
                        openBISSample,
                        openBISCollection):
        """Register the FCS File using the parsed properties file.
    
        @param fcsFileNode An XML node corresponding to an FCS file (dataset).
        @param openBISDataSetType The type of the DataSet.
        @param openBISSample An ISample object representing a Tube or Well.
        @param openBISCollection The openBIS Collection.
        """

        # Create a new dataset
        dataset = self._transaction.createNewDataSet()
        if not dataset:
            msg = "Could not get or create dataset"
            self._logger.error(msg)
            raise Exception(msg)

        # Set the dataset type
        dataset.setDataSetType(openBISDataSetType)

        # Assign the dataset to the sample
        dataset.setSample(openBISSample)

        # Set the file type
        dataset.setFileFormatType("FCS")

        # Get the parameter node
        for parameterNode in fcsFileNode:

            if parameterNode.tag != "FCSFileParamList":
                msg = "Expected FSC File Parameter List node!"
                self._logger.error(msg)
                raise Exception(msg)

            parametersXML = self._dictToXML(parameterNode.attrib)

            # Store the parameters in the LSR_FORTESSA_FCSFILE_PARAMETERS property
            dataset.setPropertyValue(openBISDataSetType + "_PARAMETERS", parametersXML)

            # Log the parameters
            self._logger.info("FCS file parameters (XML): " + str(parametersXML))

        # Assign the file to the dataset (we will use the absolute path)
        fileName = fcsFileNode.attrib.get("relativeFileName")
        fileName = os.path.join(self._transaction.getIncoming().getAbsolutePath(), fileName)

        # Log
        self._logger.info("Registering file: " + fileName)

        # Move the file
        self._transaction.moveFile(fileName, dataset)

    def run(self):
        """Run the registration."""

        # Make sure that incoming is a folder
        if not self._transaction.getIncoming().isDirectory():
            msg = "Incoming MUST be a folder!"
            self._logger.error(msg)
            raise Exception(msg)

        # Log
        self._logger.info("Incoming folder: " + self._transaction.getIncoming().getAbsolutePath())

        # There must be just one subfolder: the user subfolder
        subFolders = self._getSubFolders(self._transaction.getIncoming())
        if len(subFolders) != 1:
            msg = "Expected user subfolder!"
            self._logger.error(msg)
            raise Exception(msg)

        # Set the user folder
        userFolder = os.path.join(self._transaction.getIncoming().getAbsolutePath(), subFolders[0])

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
                propertiesFile = os.path.join(self._transaction.getIncoming().getAbsolutePath(), line)
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
            self._register(tree)

    def _register(self, tree):
        """Register the Experiment using the parsed properties file.
    
        @param tree ElementTree parsed from the properties XML file.
        """

        # Keep track of the Specimens already created since they can be
        # common to different plates and across plates and tubes
        specimens = {}

        # Some sample types we will need
        openBISDataSetType = self._prefix + "_FCSFILE"
        openBISExperimentSampleType = self._prefix + "_EXPERIMENT"
        openBISTraySampleType = self._prefix + "_PLATE"
        openBISTubeSampleType = self._prefix + "_TUBE"
        openBISTubeSetSampleType = self._prefix + "_TUBESET"
        openBISSpecimenSampleType = self._prefix + "_SPECIMEN"
        openBISWellSampleType = self._prefix + "_WELL"

        # Get the root node (obitXML)
        rootNode = tree.getroot()

        # Check the tag
        if rootNode.tag != "obitXML":
            msg = "Unexpected properties root node tag '" + \
                  rootNode.tag + "'. Invalid file. Cannot process."
            self._logger.error(msg)
            raise Exception(msg)

        # Make sure that we have the expected version of the properties file
        file_version = rootNode.attrib.get("version")
        if file_version is None or file_version < self._version:
            msg = "PROCESSOR::_register(): Expected properties file version " + \
                  str(self._version) + ". This file is obsolete. Cannot process."
            self._logger.error(msg)
            raise Exception(msg)

        # Store the machine name
        machineName = rootNode.attrib.get("machineName")
        if machineName is None:
            machineName = ""

        # Create a virtual TubeSet: an experiment only has 0 or 1 TubeSets.
        openBISTubeSetSample = None

        # Iterate over the children (Experiment nodes that map to {...}_EXPERIMENT samples)
        for experimentNode in rootNode:

            # The tag of the immediate children of the root experimentNode
            # must be Experiment
            if experimentNode.tag != "Experiment":
                msg = "Expected Experiment node, found " + experimentNode.tag
                self._logger.error(msg)
                raise Exception(msg)

            # Process an Experiment XML node and get/create an IExperimentUpdatable
            openBISExperimentSample, openBISCollection = \
                self._processExperimentNode(
                    experimentNode,
                    openBISExperimentSampleType,
                    machineName)

            # Process children of the Experiment
            for experimentChildNode in experimentNode:

                # The child of an Experiment can be a Tray or a Specimen
                experimentChildNodeType = experimentChildNode.tag

                if experimentChildNodeType == "Specimen":

                    # A specimen is a direct child of an experiment if there
                    # is no plate, and the FCS files are therefore associated
                    # to tubes. In this case, we create a virtual TubeSet
                    # sample container (one for all Tubes in the experiment).
                    if openBISTubeSetSample is None:
                        openBISTubeSetSample = self._processTubeSetNode(experimentNode,
                                                                  openBISCollection,
                                                                  openBISTubeSetSampleType,
                                                                  openBISExperimentSample.getSampleIdentifier())

                    # Now we process the Specimen node
                    specimenNameProperty = experimentChildNode.attrib.get("name")
                    openBISSpecimenSample = self._processSpecimenNode(experimentChildNode,
                                                                specimens,
                                                                openBISCollection,
                                                                openBISSpecimenSampleType,
                                                                specimenNameProperty)

                    # If this is a new Specimen, add it to the specimens dictionary
                    if specimenNameProperty not in specimens:
                        specimens[specimenNameProperty] = openBISSpecimenSample

                    # Now iterate over the children of the Specimen
                    for tubeNode in experimentChildNode:

                        # The child of a Specimen is a Tube
                        if tubeNode.tag != "Tube":
                            msg = "Expected Tube node!"
                            self._logger.error(msg)
                            raise Exception(msg)

                        # Process the tube node and get the openBIS object
                        openBISTubeSample = self._processTube(tubeNode,
                                                        openBISCollection,
                                                        openBISTubeSampleType,
                                                        openBISExperimentSample,
                                                        openBISSpecimenSample,
                                                        openBISTubeSetSample)

                        # Now process the FCS file
                        for fcsFileNode in tubeNode:

                            # The child of a Tube is an FCSFile
                            if fcsFileNode.tag != "FCSFile":
                                msg = "Expected FSC File node!"
                                self._logger.error(msg)
                                raise Exception(msg)

                            # Process the FCS file node
                            self._processFCSFile(fcsFileNode,
                                           openBISDataSetType,
                                           openBISTubeSample,
                                           openBISCollection)

                elif experimentChildNodeType == "Tray":

                    # Process the tray node and get the openBIS object
                    openBISTraySample = self._processTrayNode(experimentChildNode,
                                                        openBISCollection,
                                                        openBISExperimentSample.getSampleIdentifier(),
                                                        openBISTraySampleType)

                    # Now iterate over the children of the Tray
                    for specimenNode in experimentChildNode:

                        # The child of a Tray is a Specimen
                        if specimenNode.tag != "Specimen":
                            msg = "Expected Specimen node!"
                            self._logger.error(msg)
                            raise Exception(msg)

                        # Now we process the Specimen node
                        specimenNameProperty = specimenNode.attrib.get("name")
                        openBISSpecimenSample = self._processSpecimenNode(experimentChildNode,
                                                                    specimens,
                                                                    openBISCollection,
                                                                    openBISSpecimenSampleType,
                                                                    specimenNameProperty)

                        # If this is a new Specimen, add it to the specimens dictionary
                        if specimenNameProperty not in specimens:
                            specimens[specimenNameProperty] = openBISSpecimenSample

                        for wellNode in specimenNode:

                            # The child of a Specimen is a Tube
                            if wellNode.tag != "Well":
                                msg = "Expected Well node!"
                                self._logger.error(msg)
                                raise Exception(msg)

                            # Process the tube node and get the openBIS object
                            openBISWellSample = self._processWell(wellNode,
                                                            openBISCollection,
                                                            openBISWellSampleType,
                                                            openBISExperimentSample,
                                                            openBISSpecimenSample,
                                                            openBISTraySample)

                            # Now process the FCS file
                            for fcsFileNode in wellNode:

                                # The child of a Tube is an FCSFile
                                if fcsFileNode.tag != "FCSFile":
                                    msg = "Expected FSC File node!"
                                    self._logger.error(msg)
                                    raise Exception(msg)

                                # Process the FCS file node
                                self._processFCSFile(fcsFileNode,
                                                     openBISDataSetType,
                                                     openBISWellSample,
                                                     openBISCollection)

                else:

                    msg = "The Node must be either a Specimen or a Tray"
                    self._logger.error(msg)
                    raise Exception(msg)

        # Log that we are finished with the registration
        self._logger.info("Registration completed")

    def _registerAttachmentsToCollection(self,
                                         attachments,
                                         openBISCollection,
                                         openBISExperimentSample):
        """Register a list of files to the collection.
    
        @param attachments Comma-separated list of file names.
        @param openBISCollection openBIS Collection object.
        @param openBISExperimentSample openBIS Experiment Sample object.
        """

        # Extract all relative file names
        if type(attachments) is str:
            attachmentFiles = attachments.split(";")
        elif type(attachments) is list:
            attachmentFiles = attachments
        else:
            return False

        for f in attachmentFiles:

            # This is an additional security step
            if f == '':
                continue

            # Build the full path
            attachmentFilePath = os.path.join(self._transaction.getIncoming().getAbsolutePath(), f)

            # Extract the file name
            attachmentFileName = os.path.basename(attachmentFilePath)

            # Create a dataset of type ATTACHMENT and add it to the
            # {...}_EXPERIMENT sample and the containing COLLECTION
            attachmentDataSet = self._transaction.createNewDataSet("ATTACHMENT")
            self._transaction.moveFile(attachmentFilePath, attachmentDataSet)
            attachmentDataSet.setPropertyValue("$NAME", attachmentFileName)
            attachmentDataSet.setSample(openBISExperimentSample)

        return True

    def _registerTags(self,
                      openBISExperimentSample,
                      tagList):
        """Register the tags as parent samples of type ORGANIZATION_UNIT.
    
        @param openBISExperimentSample openBIS Experiment Sample object.
        @param tagList Comma-separated list of tag names.
        """

        # Make sure tagList is not None
        if tagList is None:
            return openBISExperimentSample

        # Collect the parent sample identifiers
        tagSampleIdentifiers = []

        # Get the individual tag names (with no blank spaces)
        tags = ["".join(t.strip()) for t in tagList.split(",")]

        # Process all tags
        for tag in tags:
            if len(tag) == 0:
                continue

            # The tag (a sample of type "ORGANIZATION_UNIT") is expected to exist.
            # If it does not exist, we skip creation, since we do not have NAME
            # and DESCRIPTION to create a meaningful one.
            sample = self._transaction.getSample(tag)
            if sample is not None:
                tagSampleIdentifiers.append(tag)

        # Add tag samples as parent
        openBISExperimentSample.setParentSampleIdentifiers(tagSampleIdentifiers)

        return openBISExperimentSample

    def _setup_logger(self, log_dir_path, logger_name, level=logging.DEBUG):
        """
        Sets up the logger.
        
        @param log_dir_path: Full path to the log folder.
        @param logger_name: Name of the logger.
        @param level: Debug level (optional, default = logging.DEBUG) 
        @return Logger object.
        """

        # Make sure the logs subforder exist
        if not os.path.exists(log_dir_path):
            os.makedirs(log_dir_path)

        # Path for the log file
        log_filename = os.path.join(log_dir_path, "log.txt")

        # Set up logging
        logging.basicConfig(filename=log_filename, level=level,
                            format='%(asctime)-15s %(levelname)s: %(message)s')
        logger = logging.getLogger(logger_name)

        return logger
