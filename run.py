# -*- coding: utf-8 -*-
"""Performs all analysis steps for all cases specified in the
configuration file.

"""
# Standard modules
import json
import logging
import multiprocessing
import os

from faultmap import config_setup
from faultmap.gaincalc import weightcalc
from faultmap.data_processing import result_reconstruction
from faultmap.data_processing import trend_extraction
from faultmap.noderank import noderankcalc
from faultmap.graphreduce import reducegraph
from plotting.plotter import plotdraw
from db_functions import select_all_from_table_new

# TODO: Move to class object
# TODO: Perform analysis on scenario level inside class object


def run_weightcalc(configloc, writeoutput, mode, case, case_id, robust,single_entropies,fftcalc,do_multiprocessing):
    #with open(os.path.join(configloc, "config_weightcalc" + ".json")) as f:
    #    weightcalc_config = json.load(f)
    #f.close()

    # Flag indicating whether single signal entropy values for each
    # signal involved should be calculated
    #single_entropies = weightcalc_config["calc_single_entropies"]
    # Flag indicating whether
    #fftcalc = weightcalc_config["fft_calc"]
    #do_multiprocessing = weightcalc_config["multiprocessing"]

    if robust:
        try:
            weightcalc(
                mode, case, case_id, writeoutput, single_entropies, fftcalc, do_multiprocessing
            )
        except:
            raise RuntimeError("Weight calculation failed for case: " + case)
    else:
        weightcalc(
            mode, case, case_id, writeoutput, single_entropies, fftcalc, do_multiprocessing
        )

    return None


def run_createarrays(writeoutput, mode, case, case_id, robust):

    if robust:
        try:
            # Needs to execute twice for nosigtest cases if derived from
            # sigtest cases
            # TODO: Remove this requirement
            result_reconstruction(mode, case, case_id, writeoutput)
            result_reconstruction(mode, case, case_id, writeoutput)
        except:
            raise RuntimeError("Array creation failed for case: " + case)
    else:
        result_reconstruction(mode, case, case_id, writeoutput)
        result_reconstruction(mode, case, case_id, writeoutput)

    return None


def run_trendextraction(writeoutput, mode, case, case_id, robust):

    if robust:
        try:
            trend_extraction(mode, case, case_id, writeoutput)
        except:
            raise RuntimeError("Trend extraction failed for case: " + case)
    else:
        trend_extraction(mode, case, case_id, writeoutput)

    return None


def run_noderank(writeoutput, mode, case, case_id, robust):

    if robust:
        try:
            noderankcalc(mode, case, case_id, writeoutput)
        except:
            raise RuntimeError("Node ranking failed for case: " + case)
    else:
        noderankcalc(mode, case, case_id, writeoutput)

    return None


def run_graphreduce(writeoutput, mode, case, case_id, robust):

    if robust:
        try:
            reducegraph(mode, case, case_id, writeoutput)
        except:
            raise RuntimeError("Graph reduction failed for case: " + case)
    else:
        reducegraph(mode, case, case_id, writeoutput)


def run_plotting(writeoutput, mode, case, robust):

    if robust:
        try:
            plotdraw(mode, case, writeoutput)
        except:
            raise RuntimeError("Plotting failed for case: " + case)
    else:
        plotdraw(mode, case, writeoutput)

    return None


def run_all(mode, robust=False):
    #_, configloc, _, _ = config_setup.get_locations(mode)
    #with open(os.path.join(configloc, "config_full" + ".json")) as f:
    #    fullrun_config = json.load(f)
    #f.close()



    sql = ''' SELECT * FROM cases'''
    case_info = select_all_from_table_new(sql)
    fullrun_config_new = {'mode': 'cases', 'writeoutput': True, 'cases': [i[1] for i in case_info]}

    # Flag indicating whether calculated results should be written to disk
    writeoutput = fullrun_config_new["writeoutput"]
    # Provide the mode and case names to calculate
    mode = fullrun_config_new["mode"]
    cases = fullrun_config_new["cases"]

    for case in case_info:
        logging.info("Now attempting case: " + case[1])
        if case[7] == 1:
            run_weightcalc(configloc, writeoutput, mode, case[1], case[0], robust, bool(case[8]), bool(case[9]), bool(case[10]))
        if case[2] == 1 :
            run_createarrays(writeoutput, mode, case[1], case[0], robust)
        if case[6] == 1:
            run_trendextraction(writeoutput, mode, case[1], case[0], robust)
        if case[4] == 1:
            run_noderank(writeoutput, mode, case[1], case[0], robust)
        if case[3] == 1:
            run_graphreduce(writeoutput, mode, case[1], case[0], robust)
        if case[5] == 1 :
            run_plotting(writeoutput, mode, case[1], robust)
        logging.info("Done with case: " + case[1])


if __name__ == "__main__":
    multiprocessing.freeze_support()
    logging.basicConfig(level=logging.INFO)

    run_all("cases")
