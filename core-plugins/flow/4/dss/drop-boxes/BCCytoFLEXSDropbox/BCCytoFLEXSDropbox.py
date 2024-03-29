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
    prefix = "CYTOFLEX_S"
    version = 2
    logDir = "../core-plugins/flow/4/dss/drop-boxes/BCCytoFLEXSDropbox/logs"

    processor = Processor(transaction, prefix, version, logDir)
    processor.run()
