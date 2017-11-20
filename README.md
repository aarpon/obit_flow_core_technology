# openBIS Importer Toolset :: Flow Cytometry Core Technology

The openBIS Importer toolset is a tightly integrated collection of tools that allows for the semi-automated, semi-unsupervised registration of annotated datasets into openBIS directly from the acquisition stations; but it also extends openBIS itself with custom data viewers and server-side core plug-ins packaged into two new core technologies for **flow cytometry** and **microscopy**.

To enable the flow core technology in openBIS , add the following line to ``openbis/servers/core-plugins/core-plugins.properties``:

``enabled-modules = flow, shared``

The [`shared` module](https://github.com/aarpon/obit_shared_core_technology) is optional but recommended.

## User manuals and administration guides

oBIT website: https://wiki-bsse.ethz.ch/display/oBIT
