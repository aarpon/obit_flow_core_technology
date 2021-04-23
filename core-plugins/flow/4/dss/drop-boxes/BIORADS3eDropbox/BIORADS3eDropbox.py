# -*- coding: utf-8 -*-

"""
@author: Aaron Ponti
"""

from Processor import Processor


def process(transaction):
    """Dropbox entry point.

    @param transaction, the transaction object
    """

    #
    # Run registration
    #
    prefix = "S3E"
    version = 2
    logDir = "../core-plugins/flow/4/dss/drop-boxes/BIORADS3eDropbox/logs"

    processor = Processor(transaction, prefix, version, logDir)
    processor.run()
