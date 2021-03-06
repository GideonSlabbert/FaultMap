# -*- coding: utf-8 -*-
"""This module calculates the gains (weights) of edges connecting
variables in the digraph.

Calculation of both Pearson's correlation and transfer entropy is supported.
Transfer entropy is calculated according to the global average of local
entropies method.
All weights are optimized with respect to time shifts between the time series
data vectors (i.e. cross-correlated).

The delay giving the maximum weight is returned, together with the maximum
weights.

All weights are tested for significance.
The Pearson's correlation weights are tested for significance according to
the parameters presented by Bauer2005.
The transfer entropy weights are tested for significance using a non-parametric
rank-order method using surrogate data generated according to the iterative
amplitude adjusted Fourier transform method (iAAFT).

"""

# Standard libraries
import csv
import json
import logging
import multiprocessing
import os
import time

import h5py
import numpy as np
import pandas as pd

from faultmap import data_processing, config_setup
from test import datagen
from faultmap import gaincalc_oneset
from faultmap.gaincalculators import CorrWeightcalc, TransentWeightcalc
from db_functions import convert_db_info_to_dict_weightcalc


class WeightcalcData(object):
    """Creates a data object from files or functions for use in
    weight calculation methods.

    """

    def __init__(
        self,
        mode,
        case,
        case_id,
        single_entropies,
        fftcalc,
        do_multiprocessing,
        use_gpu,
    ):
        """
        Parameters
        ----------
        mode : str
            Either 'tests' or 'cases'. Tests data are generated dynamically and
            stored in specified folders. Case data are read from file and
            stored under organized headings in the saveloc directory specified
            in config.json.
        case : str
            The name of the case that is to be run. Points to dictionary in
            either test or case config files.
        single_entropies : bool
            Flags whether the entropies of single signals should be calculated.
        fftcalc : bool
            Indicates whether the FFT of all individual signals should be
            calculated.
        do_multiprocessing : bool
            Indicates whether the weight calculation operations should run in
            parallel processing mode where all available CPU cores
            are utilized.

        """
        # Get file locations from configuration file
        self.saveloc, self.caseconfigdir, self.casedir, self.infodynamicsloc = config_setup.runsetup(
            mode, case
        )
        # Load case config file
 #       with open(
  #          os.path.join(self.caseconfigdir, case + "_weightcalc" + ".json")
  #      ) as configfile:
  #          self.caseconfig = json.load(configfile)
  #      configfile.close()
        self.caseconfig = convert_db_info_to_dict_weightcalc(case_id)


        # Get data type
        self.datatype = self.caseconfig["datatype"]
        # Get scenarios
        self.scenarios = self.caseconfig["scenarios"]
        # Get methods
        self.methods = self.caseconfig["methods"]

        self.do_multiprocessing = do_multiprocessing
        self.use_gpu = use_gpu

        self.casename = case

        # Flag for calculating single signal entropies
        self.single_entropies = single_entropies
        # Flag for calculating FFT of all signals
        self.fftcalc = fftcalc

    def scenariodata(self, scenario):
        """Retrieves data particular to each scenario for the case being
        investigated.

        Parameters
        ----------
            scenario : str
                Name of scenario to retrieve data for. Should be defined in
                config file.

        """
        print("The scenario name is: " + scenario)

        self.settings_set = self.caseconfig[scenario]["settings"]

    def setsettings(self, scenario, settings_name):
        if "use_connections" in self.caseconfig[settings_name]:
            self.connections_used = self.caseconfig[settings_name][
                "use_connections"
            ]
        else:
            self.connections_used = False
        if "transient" in self.caseconfig[settings_name]:
            self.transient = self.caseconfig[settings_name]["transient"]
            if "transient_method" in self.caseconfig[settings_name]:
                self.transient_method = self.caseconfig[settings_name][
                    "transient_method"
                ]
            else:
                self.transient_method = "legacy"
        else:
            self.transient = False
            logging.info("Defaulting to single time region analysis")
        if "normalise" in self.caseconfig[settings_name]:
            self.normalise = self.caseconfig[settings_name]["normalise"]
        else:
            self.normalise = False
            logging.info("Defaulting to no normalisation")
        if "detrend" in self.caseconfig[settings_name]:
            self.detrend = self.caseconfig[settings_name]["detrend"]
        else:
            self.detrend = False
            logging.info("Defaulting to no detrending")
        self.sigtest = self.caseconfig[settings_name]["sigtest"]
        if self.sigtest:
            # The transfer entropy threshold calculation method be either
            # 'sixsigma' or 'rankorder'
            self.thresh_method = self.caseconfig[settings_name][
                "thresh_method"
            ]
            # The transfer entropy surrogate generation method be either
            # 'iAAFT' or 'random_shuffle'
            self.surr_method = self.caseconfig[settings_name]["surr_method"]
        if "allthresh" in self.caseconfig[settings_name]:
            self.allthresh = self.caseconfig[settings_name]["allthresh"]
        else:
            self.allthresh = False

        # Get sampling rate and unit name
        self.sampling_rate = self.caseconfig[settings_name]["sampling_rate"]
        self.sampling_unit = self.caseconfig[settings_name]["sampling_unit"]
        # Get starting index
        if "startindex" in self.caseconfig[settings_name]:
            self.startindex = self.caseconfig[settings_name]["startindex"]
        else:
            self.startindex = 0

        # Get parameters for Kraskov method
        if "transfer_entropy_kraskov" in self.methods:
            self.additional_parameters = self.caseconfig[settings_name][
                "additional_parameters"
            ]

        # Get parameters for kernel method
        if "transfer_entropy_kernel" in self.methods:
            if "kernel_width" in self.caseconfig[settings_name]:
                self.kernel_width = self.caseconfig[settings_name][
                    "kernel_width"
                ]
            else:
                self.kernel_width = None

        if self.datatype == "file":
            # Get path to time series data input file in standard format
            # described in documentation under "Input data formats"
            raw_tsdata = os.path.join(
                self.casedir, "data", self.caseconfig[scenario]["data"]
            )

            # Retrieve connection matrix
            if self.connections_used:
                # Get connection (adjacency) matrix
                connection_loc = os.path.join(
                    self.casedir,
                    "connections",
                    self.caseconfig[scenario]["connections"],
                )
                self.connectionmatrix, _ = data_processing.read_connectionmatrix(
                    connection_loc
                )

            # Read data into Pandas dataframe
            raw_df = pd.read_csv(raw_tsdata)
            raw_df["Time"] = pd.to_datetime(raw_df["Time"], unit="s")
            raw_df.set_index("Time", inplace=True)

            self.variables = list(raw_df.keys())
            self.timestamps = np.asarray(
                raw_df.index.astype(np.int64) // 10 ** 9
            )
            self.headerline = ["Time"] + [var for var in self.variables]

            self.inputdata_raw = np.asarray(raw_df)

            # Convert timeseries data in CSV file to H5 data format
            # datapath = data_processing.csv_to_h5(self.saveloc, raw_tsdata,
            #                                      scenario, self.casename)
            # Read variables from orignal CSV file
            # self.variables = data_processing.read_variables(raw_tsdata)
            # self.timestamps = data_processing.read_timestamps(raw_tsdata)
            # # Get inputdata from H5 table created
            # self.inputdata_raw = np.array(h5py.File(os.path.join(
            #     datapath, scenario + '.h5'), 'r')[scenario])
            # self.headerline = np.genfromtxt(raw_tsdata, delimiter=',',
            #                                 dtype='str')[0, :]

        elif self.datatype == "function":
            raw_tsdata_gen = self.caseconfig[scenario]["datagen"]
            if self.connections_used:
                connectionloc = self.caseconfig[scenario]["connections"]
                # Get the variables and connection matrix
                self.variables, self.connectionmatrix = getattr(
                    datagen, connectionloc
                )()
            # TODO: Store function arguments in scenario config file
            params = self.caseconfig[settings_name]["datagen_params"]
            # Get inputdata
            self.inputdata_raw = getattr(datagen, raw_tsdata_gen)(params)
            self.inputdata_raw = np.asarray(self.inputdata_raw)

            self.timestamps = np.arange(
                0,
                len(self.inputdata_raw[:, 0]) * self.sampling_rate,
                self.sampling_rate,
            )

            self.headerline = ["Time"]
            [self.headerline.append(variable) for variable in self.variables]

        # Perform normalisation
        # Retrieve scaling limits from file
        if self.normalise == "skogestad":
            # Get scaling parameters
            if "scalelimits" in self.caseconfig[scenario]:
                scaling_loc = os.path.join(
                    self.casedir,
                    "scalelimits",
                    self.caseconfig[scenario]["scalelimits"],
                )
                scalingvalues = data_processing.read_scalelimits(scaling_loc)
            else:
                raise NameError(
                    "Scale limits reference missing from " "configuration file"
                )
        else:
            scalingvalues = None

        self.inputdata_normstep = data_processing.normalise_data(
            self.headerline,
            self.timestamps,
            self.inputdata_raw,
            self.variables,
            self.saveloc,
            self.casename,
            scenario,
            self.normalise,
            self.methods,
            scalingvalues,
        )

        # Get delay type
        if "delaytype" in self.caseconfig[settings_name]:
            self.delaytype = self.caseconfig[settings_name]["delaytype"]
        else:
            self.delaytype = "datapoints"

        # Get bias correction parameter
        if "bias_correct" in self.caseconfig[scenario]:
            self.bias_correct = self.caseconfig[scenario]["bias_correct"]
        else:
            self.bias_correct = False

        # Get size of sample vectors for tests
        # Must be smaller than number of samples
        self.testsize = self.caseconfig[settings_name]["testsize"]

        # Get number of delays to test
        test_delays = self.caseconfig[scenario]["test_delays"]

        if "bidirectional_delays" in self.caseconfig[scenario].keys():
            self.bidirectional_delays = self.caseconfig[scenario][
                "bidirectional_delays"
            ]
        else:
            self.bidirectional_delays = False

        if self.bidirectional_delays is True:
            delay_range = range(-test_delays, test_delays + 1)
        else:
            delay_range = range(test_delays + 1)

        # Define intervals of delays
        if self.delaytype == "datapoints":
            self.delays = delay_range

        elif self.delaytype == "intervals":
            # Test delays at specified intervals
            self.delayinterval = self.caseconfig[settings_name][
                "delay_interval"
            ]

            self.delays = [(val * self.delayinterval) for val in delay_range]

        if "causevarindexes" in self.caseconfig[scenario]:
            self.causevarindexes = self.caseconfig[scenario]["causevarindexes"]
        else:
            self.causevarindexes = "all"
        if self.causevarindexes == "all":
            self.causevarindexes = range(len(self.variables))
        if "affectedvarindexes" in self.caseconfig[scenario]:
            self.affectedvarindexes = self.caseconfig[scenario][
                "affectedvarindexes"
            ]
        else:
            self.affectedvarindexes = "all"
        if self.affectedvarindexes == "all":
            self.affectedvarindexes = range(len(self.variables))

        if "bandgap_filtering" in self.caseconfig[scenario]:
            bandgap_filtering = self.caseconfig[scenario]["bandgap_filtering"]
        else:
            bandgap_filtering = False
        if bandgap_filtering:
            low_freq = self.caseconfig[scenario]["low_freq"]
            high_freq = self.caseconfig[scenario]["high_freq"]
            self.inputdata_bandgapfiltered = data_processing.bandgapfilter_data(
                raw_tsdata,
                self.inputdata_normstep,
                self.variables,
                low_freq,
                high_freq,
                self.saveloc,
                self.casename,
                scenario,
            )
            self.inputdata_originalrate = self.inputdata_bandgapfiltered
        else:
            self.inputdata_originalrate = self.inputdata_normstep

        # Perform detrending
        # Detrending should be performed after normalisation and band gap filtering

        self.inputdata_originalrate = data_processing.detrend_data(
            self.headerline,
            self.timestamps,
            self.inputdata_originalrate,
            self.saveloc,
            self.casename,
            scenario,
            self.detrend,
        )

        # Subsample data if required
        # Get sub_sampling interval
        self.sub_sampling_interval = self.caseconfig[settings_name][
            "sub_sampling_interval"
        ]
        # TODO: Use proper pandas.tseries.resample techniques
        # if it will really add any functionality
        # TODO: Investigate use of forward-backward Kalman filters
        self.inputdata = self.inputdata_originalrate[
            0 :: self.sub_sampling_interval
        ]

        if self.transient:
            self.boxsize = self.caseconfig[settings_name]["boxsize"]
            if self.transient_method == "legacy":
                self.boxnum = self.caseconfig[settings_name]["boxnum"]
            elif self.transient_method == "robust":
                self.boxoverlap = self.caseconfig[settings_name]["boxoverlap"]

        else:
            self.boxnum = 1  # Only a single box will be used
            self.boxsize = self.inputdata.shape[0] * self.sampling_rate
            # This box should now return the same size
            # as the original data file - but it does not play a role at all
            # in the actual box determination for the case of boxnum = 1

        try:
            self.transient_method
        except:
            self.transient_method = None

        if self.transient_method == "legacy" or self.transient_method is None:
            # Get box start and end dates
            self.boxdates = data_processing.split_tsdata(
                self.timestamps,
                self.sampling_rate * self.sub_sampling_interval,
                self.boxsize,
                self.boxnum,
            )
            data_processing.write_boxdates(
                self.boxdates, self.saveloc, self.casename, scenario
            )

            # Generate boxes to use
            self.boxes = data_processing.split_tsdata(
                self.inputdata,
                self.sampling_rate * self.sub_sampling_interval,
                self.boxsize,
                self.boxnum,
            )

        elif self.transient_method == "robust":
            # Get box start and end dates

            df = pd.DataFrame(self.inputdata)
            df.index = pd.to_datetime(self.timestamps, unit="s")
            df.columns = self.variables

            freq_string = str(self.sampling_rate) + "S"

            self.boxes, self.boxdates = data_processing.get_continous_boxes(
                df, self.boxsize, self.boxoverlap, freq_string
            )

            data_processing.write_boxdates(
                self.boxdates, self.saveloc, self.casename, scenario
            )

            self.boxnum = len(self.boxdates)

        # Select which of the boxes to evaluate
        if self.transient:
            if "boxindexes" in self.caseconfig[scenario]:
                if self.caseconfig[scenario]["boxindexes"] == "range":
                    self.boxindexes = range(
                        self.caseconfig[scenario]["boxindexes_start"],
                        self.caseconfig[scenario]["boxindexes_end"] + 1,
                    )
                else:
                    self.boxindexes = self.caseconfig[scenario]["boxindexes"]
            else:
                self.boxindexes = "all"
            if self.boxindexes == "all":
                self.boxindexes = range(self.boxnum)
        else:
            self.boxindexes = [0]

        if len(self.boxindexes) > 1:
            self.generate_diffs = True
        else:
            self.generate_diffs = False

        # Calculate delays in indexes as well as time units
        if self.delaytype == "datapoints":
            self.actual_delays = [
                (delay * self.sampling_rate * self.sub_sampling_interval)
                for delay in self.delays
            ]
            self.sample_delays = self.delays
        elif self.delaytype == "intervals":
            self.actual_delays = [
                int(round(delay / self.sampling_rate)) * self.sampling_rate
                for delay in self.delays
            ]
            self.sample_delays = [
                int(round(delay / self.sampling_rate)) for delay in self.delays
            ]

        # Create descriptive dictionary for later use
        # This will need to be approached slightly differently to allow for
        # different formats under the same "plant"
        #        self.descriptions = data_processing.descriptive_dictionary(
        #            os.path.join(self.casedir, 'data', 'tag_descriptions.csv'))

        # FFT the data and write back in format that can be analysed in
        # TOPCAT in a plane plot

        if self.fftcalc:
            data_processing.fft_calculation(
                self.headerline,
                self.inputdata_originalrate,
                self.variables,
                self.sampling_rate,
                self.sampling_unit,
                self.saveloc,
                self.casename,
                scenario,
            )


def writecsv_weightcalc(filename, items, header):
    """CSV writer customized for use in weightcalc function."""

    with open(filename, "w", newline="") as f:
        csv.writer(f).writerow(header)
        csv.writer(f).writerows(items)


def calc_weights(weightcalcdata, method, scenario, writeoutput):
    """Determines the maximum weight between two variables by searching through
    a specified set of delays.

    Parameters
    ----------
        method : str
        Can be one of the following:
        'cross_correlation'
        'partial_correlation' -- does not support time delays
        'transfer_entropy_kernel'
        'transfer_entropy_kraskov'

    TODO: Fix partial correlation method to make use of time delays

    """

    if method == "cross_correlation":
        weightcalculator = CorrWeightcalc(weightcalcdata)
    elif method == "transfer_entropy_kernel":
        weightcalculator = TransentWeightcalc(weightcalcdata, "kernel")
    elif method == "transfer_entropy_kraskov":
        weightcalculator = TransentWeightcalc(weightcalcdata, "kraskov")
    elif method == "transfer_entropy_discrete":
        weightcalculator = TransentWeightcalc(weightcalcdata, "discrete")
    # elif method == 'partial_correlation':
    #     weightcalculator = PartialCorrWeightcalc(weightcalcdata)
    else:
        raise ValueError("Method not recognized")

    if weightcalcdata.sigtest:
        sigstatus = "sigtested"
    elif not weightcalcdata.sigtest:
        sigstatus = "nosigtest"

    if method == "transfer_entropy_kraskov":
        if weightcalcdata.additional_parameters["auto_embed"]:
            embedstatus = "autoembedding"
        else:
            embedstatus = "naive"
    else:
        embedstatus = "naive"

    vardims = len(weightcalcdata.variables)
    startindex = weightcalcdata.startindex
    size = weightcalcdata.testsize

    cause_dellist = []
    affected_dellist = []
    for index in range(vardims):
        if index not in weightcalcdata.causevarindexes:
            cause_dellist.append(index)
            logging.info("Deleted column " + str(index))
        if index not in weightcalcdata.affectedvarindexes:
            affected_dellist.append(index)
            logging.info("Deleted row " + str(index))

    if weightcalcdata.connections_used:
        newconnectionmatrix = weightcalcdata.connectionmatrix
    else:
        newconnectionmatrix = np.ones((vardims, vardims))
    # Substitute columns not used with zeros in connectionmatrix
    for cause_delindex in cause_dellist:
        newconnectionmatrix[:, cause_delindex] = np.zeros(vardims)
    # Substitute rows not used with zeros in connectionmatrix
    for affected_delindex in affected_dellist:
        newconnectionmatrix[affected_delindex, :] = np.zeros(vardims)

    # Initiate headerline for weightstore file
    # Create "Delay" as header for first row
    headerline = ["Delay"]
    for affectedvarindex in weightcalcdata.affectedvarindexes:
        affectedvarname = weightcalcdata.variables[affectedvarindex]
        headerline.append(affectedvarname)

    # Define filename structure for CSV file containing weights between
    # a specific causevar and all the subsequent affectedvars
    def filename(weightname, boxindex, causevar):
        boxstring = "box{:03d}".format(boxindex)

        filedir = config_setup.ensure_existence(
            os.path.join(weightstoredir, weightname, boxstring), make=True
        )

        filename = "{}.csv".format(causevar)

        return os.path.join(filedir, filename)

    # Store the weight calculation results in similar format as original data

    # Define weightstoredir up to the method level
    weightstoredir = config_setup.ensure_existence(
        os.path.join(
            weightcalcdata.saveloc,
            "weightdata",
            weightcalcdata.casename,
            scenario,
            method,
            sigstatus,
            embedstatus,
        ),
        make=True,
    )

    if weightcalcdata.single_entropies:
        # Initiate headerline for single signal entropies storage file
        signalent_headerline = weightcalcdata.variables
        # Define filename structure for CSV file

        def signalent_filename(name, boxindex):
            return signalent_filename_template.format(
                weightcalcdata.casename, scenario, name, boxindex
            )

        signalentstoredir = config_setup.ensure_existence(
            os.path.join(weightcalcdata.saveloc, "signal_entropies"), make=True
        )

        signalent_filename_template = os.path.join(
            signalentstoredir, "{}_{}_{}_box{:03d}.csv"
        )

    for boxindex in weightcalcdata.boxindexes:
        box = weightcalcdata.boxes[boxindex]

        # Calculate single signal entropies - do not worry about
        # delays, but still do it according to different boxes
        if weightcalcdata.single_entropies:
            # Calculate single signal entropies of all variables
            # and save output in similar format to
            # standard weight calculation results
            signalentlist = []
            for varindex, _ in enumerate(weightcalcdata.variables):
                vardata = box[:, varindex][startindex : startindex + size]
                entropy = data_processing.calc_signalent(
                    vardata, weightcalcdata
                )
                signalentlist.append(entropy)

            # Write the signal entropies to file - one file for each box
            # Each file will only have one line as we are not
            # calculating for different delays as is done for the case of
            # variable pairs.

            # Need to add another axis to signalentlist in order to make
            # it a sequence so that it can work with writecsv_weightcalc
            signalentlist = np.asarray(signalentlist)
            signalentlist = signalentlist[np.newaxis, :]

            writecsv_weightcalc(
                signalent_filename("signal_entropy", boxindex + 1),
                signalentlist,
                signalent_headerline,
            )

        # Start parallelising code here
        # Create one process for each causevarindex

        ###########################################################

        non_iter_args = [
            weightcalcdata,
            weightcalculator,
            box,
            startindex,
            size,
            newconnectionmatrix,
            method,
            boxindex,
            filename,
            headerline,
            writeoutput,
        ]

        # Run the script that will handle multiprocessing
        gaincalc_oneset.run(non_iter_args, weightcalcdata.do_multiprocessing)

        ########################################################

    return None


def weightcalc(
    mode,
    case,
    case_id,
    writeoutput=False,
    single_entropies=False,
    fftcalc=False,
    do_multiprocessing=False,
    use_gpu=False,
):
    """Reports the maximum weight as well as associated delay
    obtained by shifting the affected variable behind the causal variable a
    specified set of delays.

    Parameters
    ----------
        mode : str
            Either 'tests' or 'cases'. Tests data are generated dynamically and
            stored in specified folders. Case data are read from file
            and stored under organized headings in the saveloc directory
            specified in config.json.
        case : str
            The name of the case that is to be run. Points to dictionary in
            either test or case config files.
        single_entropies : bool
            Flags whether the entropies of single signals should be calculated.
        fftcalc : bool
            Indicates whether the FFT of all individual signals should be
            calculated.
        do_multiprocessing : bool
            Indicates whether the weight calculation operations should run in
            parallel processing mode where all available CPU cores
            are utilized.

    Notes
    -----
        Supports calculating weights according to either correlation or
        transfer entropy metrics.

    """

    weightcalcdata = WeightcalcData(
        mode, case, case_id, single_entropies, fftcalc, do_multiprocessing, use_gpu
    )

    for scenario in weightcalcdata.scenarios:
        logging.info("Running scenario {}".format(scenario))
        # Update scenario-specific fields of weightcalcdata object
        weightcalcdata.scenariodata(scenario)
        for settings_name in weightcalcdata.settings_set:
            weightcalcdata.setsettings(scenario, settings_name)
            logging.info("Now running settings {}".format(settings_name))

            for method in weightcalcdata.methods:
                logging.info("Method: " + method)

                start_time = time.clock()
                calc_weights(weightcalcdata, method, scenario, writeoutput)
                end_time = time.clock()
                print(end_time - start_time)


if __name__ == "__main__":
    multiprocessing.freezeSupport()
