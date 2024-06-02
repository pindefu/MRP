from arcgis.gis import GIS
from arcgis.features import FeatureCollection
from arcgis.features.analysis import create_watersheds
import threading
import traceback
from datetime import datetime
import logging
import time
import json
import os


logger = None
batch_size = 2500
num_failed_records = 0
num_succeeded_records = 0


def get_config(in_file):
    with open(in_file) as config:
        param_dict = json.load(config)

    return param_dict

def get_logger(l_dir, t_filename, s_time):
    global logger

    logger = logging.getLogger(__name__)
    logger.setLevel(1)

    # Set Logger Time
    logger_date = datetime.fromtimestamp(s_time).strftime("%Y_%m_%d")
    logger_time = datetime.fromtimestamp(s_time).strftime("%H_%M_%S")

    # Debug Handler for Console Checks - logger.info(msg)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    logger.addHandler(console_handler)

    # Ensure Logs Directory Exists
    # l_dir = os.path.join(t_dir, "logs", logger_date)
    # if not os.path.exists(l_dir):
    #     os.makedirs(l_dir)

    # Log Handler for Reports - logger.info(msg)
    l_file_name = "Log_{}_{}_{}.txt".format(t_filename, logger_date, logger_time)
    l_dir_file_path = os.path.join(l_dir, l_file_name)
    log_handler = logging.FileHandler(l_dir_file_path, "w")
    log_handler.setLevel(logging.INFO)
    logger.addHandler(log_handler)

    logger.info("Script Started: {} - {}".format(logger_date, logger_time))

    return logger, l_dir, l_file_name

def calculate_watershed_for_featureset(sites_layer, watershedFeatureLayer, adjPointsFeatureLayer, fset, records_per_request, thread_id):
    logger.info("Thread ID: {}".format(thread_id))
    context = {"overwrite": False}
    num_of_results = 0
    # calculate the number of loops required to process all the features in the feature set
    num_of_loops = len(fset) // records_per_request + 1
    for i in range(0, num_of_loops):
        try:
            logger.info("Thread {} Loop: {}".format(thread_id, i))
            # get the lab ids for the features in the batch
            lab_ids = [f.attributes["Lab_ID"] for f in fset[i * records_per_request: (i + 1) * records_per_request]]

            # format the lab ids as a string with single quotes
            lab_ids_str = ",".join(["'{}'".format(x) for x in lab_ids])
            logger.info("\tLab IDs: {}".format(lab_ids_str))

            sites_q_results = sites_layer.query(where = "Lab_ID IN ({})".format(lab_ids_str), out_fields="*",
                                        return_all_records = True, return_geometry=True,
                                        order_by_fields="Lab_ID {}".format(order_direction))

            sitefc = FeatureCollection(sites_q_results.to_dict())

            # calculate the watershed for the site using ArcGIS Python API create_watersheds
            output_fc = create_watersheds(sitefc, context=context)

            num_of_results += len(output_fc["watershed_layer"].layer.featureSet.features)
            logger.info("\t{} watersheds calculated by thread {}. Appending to the feature layer...".format(num_of_results, thread_id))

            # append the calculated watershed to the watershedFeatureLayer
            addResults_watershed = watershedFeatureLayer.edit_features(adds=output_fc["watershed_layer"].layer.featureSet.features)
            logger.info("\tWatershed addResults: {}".format(addResults_watershed))

            # append the calculated adjPoints to the adjPointsFeatureLayer
            addResults_adjPoint = adjPointsFeatureLayer.edit_features(adds=output_fc["snap_pour_pts_layer"].layer.featureSet.features)
            logger.info("\tAdjPoints addResults: {}".format(addResults_adjPoint))

        except Exception:
            logger.info(traceback.format_exc())
        finally:
            #sleep for 2 minutes
            time.sleep(120)
            continue

if __name__ == "__main__":

    # Get Start Time
    start_time = time.time()

    # Get Script Directory
    this_dir = os.path.split(os.path.realpath(__file__))[0]
    this_filename = os.path.split(os.path.realpath(__file__))[1]

    # Collect Configured Parameters
    parameters = get_config(os.path.join(this_dir, './config/config_watershed.json'))
    # Get Logger & Log Directory
    log_folder = parameters["log_folder"]
    logger, log_dir, log_file_name = get_logger(log_folder, this_filename, start_time)

    the_portal = parameters['the_portal']
    portal_url = the_portal['url']
    the_username = the_portal['user']
    the_password = the_portal['pass']
    gis = GIS(portal_url, the_username, the_password)

    sitesItemId = parameters['sitesConfig']["itemId"]
    sites_layerId = parameters['sitesConfig']["layerId"]

    outputWatershedItemId = parameters['outputWatershedLayerConfig']["itemId"]

    try:
        # get the layers and tables
        sitesItem=gis.content.get(sitesItemId)
        sites_layer = sitesItem.layers[sites_layerId]
        order_direction = parameters["order_direction"]

        records_per_request = parameters["records_per_request"]
        number_of_threads = parameters["number_of_threads"]

        outputWatershedItem = gis.content.get(outputWatershedItemId)
        watershedFeatureLayer = outputWatershedItem.layers[0]
        adjPointsFeatureLayer = outputWatershedItem.layers[1]

        resp_watershed = watershedFeatureLayer.query("1=1", out_fields="Lab_ID", return_all_records=True, return_geometry=False)
        calculated_sites = [f.attributes["Lab_ID"] for f in resp_watershed.features]
        # join the text values in the list to a comma separated string with single quotes
        calculated_sites_str = ",".join(["'{}'".format(x) for x in calculated_sites])

        # return the id of the sites that have not been calculated
        sites_resp = sites_layer.query(where = "Lab_ID NOT IN ({})".format(calculated_sites_str), out_fields="Lab_ID",
                                       return_all_records = True, return_geometry=False,
                                       order_by_fields="Lab_ID {}".format(order_direction))

        layer_definition = sites_layer.properties
        num_of_features = len(sites_resp.features)
        # exit if there are no more sites to calculate
        if num_of_features == 0:
            exit()

        records_per_thread = num_of_features // number_of_threads + 1
        threads = list()
        for i in range(0, number_of_threads):
            fset = sites_resp.features[i * records_per_thread: (i + 1) * records_per_thread]
            x = threading.Thread(target=calculate_watershed_for_featureset, args=(sites_layer, watershedFeatureLayer, adjPointsFeatureLayer, fset, records_per_request, i))
            threads.append(x)
            x.start()

        for index, thread in enumerate(threads):
            thread.join()

    except Exception:
        logger.info(traceback.format_exc())

    finally:
        # Log Run Time
        logger.info('\n\nProgram Run Time: {0} Minutes'.format(round(((time.time() - start_time) / 60), 2)))
