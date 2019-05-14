# -*- coding: utf-8 -*-

"""
@author: Aaron Ponti
"""

from flow_dropbox_lib import register, setup_logger


def process(transaction):
    """Dropbox entry point.

    @param transaction, the transaction object
    """

    #
    # Run registration
    #
    prefix = "INFLUX"
    version = 2
    logger = setup_logger("../core-plugins/flow/3/dss/drop-boxes/BDInfluxDropbox/logs", prefix)
    register(transaction, prefix, version, logger)