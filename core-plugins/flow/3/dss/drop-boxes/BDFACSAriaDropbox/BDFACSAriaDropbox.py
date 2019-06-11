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
    prefix = "FACS_ARIA"
    version = 2
    logDir = "../core-plugins/flow/3/dss/drop-boxes/BDFACSAriaDropbox/logs"

    processor = Processor(transaction, prefix, version, logDir)
    processor.run()
