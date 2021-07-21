import logging
import os
import sys
import argparse
import re
import time
import csv
import datetime
import shutil
import unittest
import locale
import copy
import glob
import subprocess
import ast
import xmlrunner
from tabulate import tabulate
from conf import merge_spec
from conf import settings
from tools import tasks
from pods.papi import papi


VERBOSITY_LEVELS = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warning': logging.WARNING,
    'error': logging.ERROR,
    'critical': logging.CRITICAL
}

_CURR_DIR = os.path.dirname(os.path.realpath(__file__))

def parse_arguments():
    """
    Parse command line arguments.
    """
    parser = argparse.ArgumentParser(prog=__file__, formatter_class=
                                     argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--version', action='version', version='%(prog)s 0.2')
    group = parser.add_argument_group('test selection options')
    group.add_argument('--k8s', action='store_true', help='execute Kubernetes tests')
    group.add_argument('--openstack', action='store_true', help='Run VSPERF with openstack')
    args = vars(parser.parse_args())
    return args

def main():
    """Main function.
    """
    args = parse_arguments()

    # configure settings

    settings.load_from_dir(os.path.join(_CURR_DIR, 'conf'))

    # define the timestamp to be used by logs and results
    date = datetime.datetime.fromtimestamp(time.time())
    timestamp = date.strftime('%Y-%m-%d_%H-%M-%S')
    settings.setValue('LOG_TIMESTAMP', timestamp)

    results_dir = "results_" + timestamp
    results_path = os.path.join(settings.getValue('LOG_DIR'), results_dir)
    settings.setValue('RESULTS_PATH', results_path)

    # create results directory
    if not os.path.exists(results_path):
        os.makedirs(results_path)

    papiref = papi.Papi()
    print("Ok.. We are ready")


if __name__ == "__main__":
    main()
