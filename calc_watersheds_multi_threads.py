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

def calculate_watershed_for_featureset(sites_layer, key_field, watershedFeatureLayer, adjPointsFeatureLayer, lab_ids_allocated, records_per_request, seconds_between_requests, search_parameters, thread_id):
    logger.info("Thread ID: {}".format(thread_id))
    context = {"overwrite": False}
    num_of_results = 0
    # calculate the number of loops required to process in this thread
    num_of_loops = (len(lab_ids_allocated) // records_per_request)
    # if the remainder is greater than 0, add 1 to the number of loops
    if (len(lab_ids_allocated) % records_per_request) > 0:
        num_of_loops += 1

    for i in range(0, num_of_loops):
        try:
            logger.info("Thread {} Loop: {}".format(thread_id, i))
            # get the lab ids for the lab ids in the request
            lab_ids = lab_ids_allocated[i * records_per_request: (i + 1) * records_per_request]
            if len(lab_ids) == 0:
                return num_of_results

            # format the lab ids as a string with single quotes
            lab_ids_str = ",".join(["'{}'".format(x) for x in lab_ids])
            logger.info("\tThread ID: {} Lab IDs: {}".format(thread_id, lab_ids_str))

            sites_q_results = sites_layer.query(where = "{} IN ({})".format(key_field, lab_ids_str), out_fields="*",
                                        return_all_records = True, return_geometry=True)

            logger.info("\tThread ID: {}: {} sites found".format(thread_id, len(sites_q_results.features)))
            sitefc = FeatureCollection(sites_q_results.to_dict())

            # calculate the watershed for the sitefc
            if "search_distance" in search_parameters and "search_units" in search_parameters and search_parameters["search_distance"] >=0:
                search_distance = search_parameters["search_distance"]
                search_units = search_parameters["search_units"]
                output_fc = create_watersheds(sitefc, context=context, generalize=False, search_distance=search_distance, search_units=search_units)
            else:
                # logger.info("\tThread ID: {}: Using default search distance and units".format(thread_id))
                output_fc = create_watersheds(sitefc, context=context, generalize=False)

            num_watersheds_generated = len(output_fc["watershed_layer"].layer.featureSet.features)

            num_of_results += num_watersheds_generated
            logger.info("\tThread ID: {}: {} -> {} watersheds calculated. ".format(thread_id, num_watersheds_generated, num_of_results))

            if num_watersheds_generated > 0:
                # append the calculated watershed to the watershedFeatureLayer
                addResults_watershed = watershedFeatureLayer.edit_features(adds=output_fc["watershed_layer"].layer.featureSet.features)
                # the results is in this format:
                # {'addResults': [{'objectId': 19245, 'uniqueId': 19245, 'globalId': '191564D7-A52E-49B8-9DC9-071E3F40FF94', 'success': True}, {'objectId': 19246, 'uniqueId': 19246, 'globalId': '31758F40-91BE-4168-91DC-04F1157CF10B', 'success': True}, {'objectId': 19247, 'uniqueId': 19247, 'globalId': '02504392-67E0-48EC-A030-1AD27562DCAE', 'success': True}], 'updateResults': [], 'deleteResults': []}

                # log the number of successful results with success=True and number of failed results
                num_succeeded_records = len(list(filter(lambda d: d["success"] == True, addResults_watershed["addResults"])))
                num_failed_records = num_watersheds_generated - num_succeeded_records
                logger.info("\tThread ID: {}: add watersheds: {} succeeded, {} failed".format(thread_id, num_succeeded_records, num_failed_records))

                # append the calculated adjPoints to the adjPointsFeatureLayer
                num_adjPoints_generated = len(output_fc["snap_pour_pts_layer"].layer.featureSet.features)
                addResults_adjPoint = adjPointsFeatureLayer.edit_features(adds=output_fc["snap_pour_pts_layer"].layer.featureSet.features)
                # log the number of successful results with success=True and number of failed results
                num_succeeded_records = len(list(filter(lambda d: d["success"] == True, addResults_adjPoint["addResults"])))
                num_failed_records = num_adjPoints_generated - num_succeeded_records
                logger.info("\tThread ID: {}: add AdjPoints: {} succeeded, {} failed".format(thread_id, num_succeeded_records, num_failed_records))

        except Exception:
            logger.info("Thread {} exception {}".format(thread_id, traceback.format_exc()))
            time.sleep(60)
        finally:
            time.sleep(seconds_between_requests)
            continue

        return num_of_results

if __name__ == "__main__":

    # Get Start Time
    start_time = time.time()

    # Get Script Directory
    this_dir = os.path.split(os.path.realpath(__file__))[0]
    this_filename = os.path.split(os.path.realpath(__file__))[1]

    # Collect Configured Parameters
    parameters = get_config(os.path.join(this_dir, './config/config_watersheds.json'))
    # Get Logger & Log Directory
    log_folder = parameters["log_folder"]
    logger, log_dir, log_file_name = get_logger(log_folder, this_filename, start_time)

    the_portal = parameters['the_portal']
    portal_url = the_portal['url']
    the_username = the_portal['user']
    the_password = the_portal['pass']
    gis = GIS(portal_url, the_username, the_password)

    # Get Parameters about multi-threading
    records_per_request = parameters["records_per_request"]
    number_of_threads = parameters["number_of_threads"]
    seconds_between_requests = parameters["seconds_between_requests"]
    logger.info("Number of Threads: {}".format(number_of_threads))
    logger.info("Records per Request: {}".format(records_per_request))
    logger.info("Seconds Between Requests: {}".format(seconds_between_requests))

    # Get the Search Parameters for the Watershed Calculation
    search_parameters = parameters["search_parameters"]

    try:

        for task in parameters["tasks"]:
            logger.info("\n ----------- Task: {} skip: {} -----------".format(task["name"], task["skip"]))
            if task["skip"] == True:
                continue

            sitesItemId = task['sitesConfig']["itemId"]
            sites_layerId = task['sitesConfig']["layerId"]
            key_field = task['sitesConfig']["key_field"]
            outputWatershedItemId = task['outputWatershedLayerConfig']["itemId"]
            sWhere = task['sitesConfig']["where"]
            order_direction = task["order_direction"]

            sitesItem=gis.content.get(sitesItemId)
            sites_layer = sitesItem.layers[sites_layerId]

            outputWatershedItem = gis.content.get(outputWatershedItemId)
            watershedFeatureLayer = outputWatershedItem.layers[0]
            adjPointsFeatureLayer = outputWatershedItem.layers[1]

            # return all the id of the sites
            sites_resp = sites_layer.query(where = sWhere, out_fields=key_field, return_distinct_values = True,
                                           return_all_records = True, return_geometry=False)
            all_sites = [f.attributes[key_field] for f in sites_resp.features]
            logger.info("all_sites: {}".format(all_sites))
            len_all_sites = len(all_sites)

            # return all the unique Lab Ids of the sites that have already been calculated
            watershed_resp = watershedFeatureLayer.query("1=1", out_fields=key_field, return_distinct_values = True,
                                                        return_all_records=True, return_geometry=False)
            calculated_sites = [f.attributes[key_field] for f in watershed_resp.features]
            logger.info("calculated_sites: {}".format(calculated_sites))
            len_calculated_sites = len(calculated_sites)

            # get the lab ids of the sites that have not been calculated
            sites_to_calculate = list(set(all_sites) - set(calculated_sites))

            logger.info("sites_to_calculate: {}".format(sites_to_calculate))

            num_of_features = len(sites_to_calculate)
            logger.info("total {}, calculated {}, remaining {}".format(len_all_sites, len_calculated_sites, num_of_features))

            # Go to the next task if there are no more sites to calculate
            if num_of_features == 0:
                continue

            records_per_thread = (num_of_features // number_of_threads)
            # if the remainder is greater than 0, add 1 to the records per thread
            if (num_of_features % number_of_threads) > 0:
                records_per_thread += 1

            logger.info("Records per Thread: {}".format(records_per_thread))

            threads = list()
            for i in range(0, number_of_threads):
                # get the lab ids from sites_to_calculate for the current thread
                lab_ids_allocated = sites_to_calculate[i * records_per_thread: (i + 1) * records_per_thread]
                logger.info("Thread {}: Lab IDs: {}".format(i, lab_ids_allocated))
                x = threading.Thread(target=calculate_watershed_for_featureset, args=(sites_layer, key_field, watershedFeatureLayer, adjPointsFeatureLayer, lab_ids_allocated, records_per_request, seconds_between_requests, search_parameters, i))
                threads.append(x)
                x.start()

            for index, thread in enumerate(threads):
                thread.join()


    except Exception:
        logger.info(traceback.format_exc())

    finally:
        # Log Run Time
        logger.info('\n\nProgram Run Time: {0} Minutes'.format(round(((time.time() - start_time) / 60), 2)))
