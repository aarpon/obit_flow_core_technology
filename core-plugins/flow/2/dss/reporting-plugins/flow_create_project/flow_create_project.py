# -*- coding: utf-8 -*-

# Ingestion service: create a project with user-defined name in given space

def process(transaction, parameters, tableBuilder):
    """Create a project with user-defined name in given space.
    
    """
    # Prepare the return table
    tableBuilder.addHeader("success")
    tableBuilder.addHeader("message")

    # Add a row for the results
    row = tableBuilder.addRow()

    # Retrieve parameters from client
    spaceCode = parameters.get("spaceCode")
    projectCode = parameters.get("projectCode")

    # Build project id
    projectId = "/" + spaceCode + "/" + projectCode

    # Make sure that the project does not already exist
    if transaction.getProjectForUpdate(projectId) is None:

        # Create the project
        project = transaction.createNewProject(projectId)

        # Set the results
        if project is not None:
            
            success = "true"
            message = "Project " + projectId + " successfully created."
        
        else:
        
            success = "false"
            message = "Could not create project " + projectId + "."

    else:
        
        success = "false"
        message = "The project " + projectId + " exists already."

    
    # Add the results to current row
    row.setCell("success", success)
    row.setCell("message", message)
