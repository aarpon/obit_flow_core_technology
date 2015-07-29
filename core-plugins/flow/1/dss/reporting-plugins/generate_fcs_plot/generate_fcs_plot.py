# -*- coding: utf-8 -*-

'''
Aggregation plug-in to generate FCS plots.

The first time a scatterplot is generated for an FCS file, the data is read
and stored in a CSV file that is registered in openBIS.
Later plots will use the CSV file directly.

@author: Aaron Ponti
'''


# Plug-in entry point
def aggregate(parameters, tableBuilder):

    # Get the parameters
    code = parameters.get("code")

    # Get the entity type
    paramX = parameters.get("paramX")

    # Get the entity code
    paramY = parameters.get("paramY")

    # Add the table headers
    tableBuilder.addHeader("Success")
    tableBuilder.addHeader("Message")

    # Store the results in the table
    row = tableBuilder.addRow()
    row.setCell("Success", True)
    row.setCell("Message", "You requested the generation of a plot " + 
                "for the file with code " + code + " and parameters (" 
                + paramX + ", " + paramY + ").")
