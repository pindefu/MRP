from arcgis.gis import GIS
from arcgis.features import FeatureLayerCollection
from arcgis.features import FeatureCollection
from arcgis.geometry import Polygon, Geometry
from arcgis.features.analysis import create_watersheds
import arcpy
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

def alter_tracking(item, tracking_state):
    if item == None:
        return

    logger.info("\n\n{} Editor tracking on {}\n".format(tracking_state, item.title))
    flc = FeatureLayerCollection.fromitem(item)
    cap = flc.properties["editorTrackingInfo"]
    # logger.info("\n ... existng editor tracking property {}\n".format(cap))

    if tracking_state == "Disable":
        cap["enableEditorTracking"] = False

    else:
        cap["enableEditorTracking"] = True

    alter_response = ""
    try:
        alter_response = flc.manager.update_definition({"editorTrackingInfo": cap})
    except Exception:
        logger.info("Exception {}".format(traceback.format_exc()))
    finally:
        logger.info("Change tracking result: {}\n\n".format(alter_response))


def run_update(the_func):
    def wrapper(*args, **kwargs):
        global batch_size
        global num_failed_records
        global num_succeeded_records

        # Run Function & Collect Update List
        edit_list = the_func(*args)
        num_total_records = len(edit_list)

        if edit_list:
            operation = kwargs.get("operation", None)
            # Batch Update List Into Smaller Sets
            # batch_size = kwargs.get("batch", None)
            use_global_ids = kwargs.get("use_global_ids", False)
            if not batch_size:
                batch_size = 1000
            update_sets = [
                edit_list[x : x + batch_size]
                for x in range(0, len(edit_list), batch_size)
            ]
            # logger.info("\nProcessing {} Batch(es)\n".format(len(update_sets)))

            if operation in ["update", "add"] and kwargs.get("track") is not None:
                try:
                    alter_tracking(kwargs.get("track"), "Disable")
                except RuntimeError:
                    logger.info("Alter Tracking - RunTime Error. Passing Until Testing Proves Otherwise . . .\n\n")
                    pass

            # Push Edit Batches
            try:
                for update_set in update_sets:
                    try:
                        keyStr = ""
                        if operation == "update":
                            edit_result = kwargs.get("update").edit_features(updates=update_set, use_global_ids=use_global_ids)
                            keyStr = "updateResults"
                        else:  # add
                            edit_result = kwargs.get("add").edit_features(adds=update_set)
                            keyStr = "addResults"


                        totalRecords = len(edit_result[keyStr])

                        print("updateResults {}: {}".format(totalRecords, edit_result[keyStr]))

                        succeeded_records = len(list(filter(lambda d: d["success"] == True,edit_result[keyStr],)))
                        logger.info("\nBatch Edit Results: {} of {} succeeded".format(succeeded_records, totalRecords))
                        num_succeeded_records = num_succeeded_records + succeeded_records
                        if totalRecords > succeeded_records:
                            failed_records = list(filter(lambda d: d["success"] == False,edit_result[keyStr],))
                            num_failed_records = num_failed_records + len(failed_records)
                            logger.info("\Failed records: {}".format(failed_records))


                    except Exception:
                        logger.info(traceback.format_exc())
            except Exception:
                logger.info(traceback.format_exc())
            finally:
                logger.info(" \n\n Summary: Total records {}, succeeded records {}, failed records {}".format(num_total_records, num_succeeded_records, num_failed_records))

                if operation in ["add", "update"]:
                    try:
                        alter_tracking(kwargs.get("track"), "Enable")
                    except RuntimeError:
                        logger.info("Alter Tracking - RunTime Error. Passing Until Testing Proves Otherwise . . .")
                        pass

        else:
            logger.info("Returned List Was Empty. No Edits Performed.")

    return wrapper

def calculate_watershed(sites_layer, watershedFeatureLayer, adjPointsFeatureLayer, order_direction):
    context = {"overwrite": False}
    while True:
        resp_watershed = watershedFeatureLayer.query("1=1", out_fields="Lab_ID", return_all_records=True, return_geometry=False)
        calculated_sites = [f.attributes["Lab_ID"] for f in resp_watershed.features]
        # join the text values in the list to a comma separated string with single quotes
        calculated_sites_str = ",".join(["'{}'".format(x) for x in calculated_sites])

        # return only one record
        sites_resp = sites_layer.query(where = "Lab_ID NOT IN ({})".format(calculated_sites_str), out_fields="*",
                                       return_all_records = False, return_geometry=True,
                                       order_by_fields="Lab_ID {}".format(order_direction), result_record_count=1)

        # exit if there are no more sites to calculate
        if len(sites_resp.features) == 0:
            return

        # print the lab_id of the site to be calculated
        print("Lab_ID: {}".format(sites_resp.features[0].attributes["Lab_ID"]))


        sitefc = FeatureCollection(sites_resp.to_dict())

        # calculate the watershed for the site using ArcGIS Python API create_watersheds
        output_fc = create_watersheds(sitefc, context=context)

        print("watershed calculated. Appending to the feature layer...")

        # append the calculated watershed to the watershedFeatureLayer
        addResults_watershed = watershedFeatureLayer.edit_features(adds=output_fc["watershed_layer"].layer.featureSet.features)
        print("Watershed addResults: {}".format(addResults_watershed))

        # append the calculated adjPoints to the adjPointsFeatureLayer
        addResults_adjPoint = adjPointsFeatureLayer.edit_features(adds=output_fc["snap_pour_pts_layer"].layer.featureSet.features)
        print("AdjPoints addResults: {}".format(addResults_adjPoint))


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

    sitesItemId = parameters['sitesConfig']["itemId"]
    sites_layerId = parameters['sitesConfig']["layerId"]

    outputWatershedItemId = parameters['outputWatershedLayerConfig']["itemId"]

    try:
        # get the layers and tables
        sitesItem=gis.content.get(sitesItemId)
        sites_layer = sitesItem.layers[sites_layerId]
        order_direction = parameters["order_direction"]

        outputWatershedItem = gis.content.get(outputWatershedItemId)
        watershedFeatureLayer = outputWatershedItem.layers[0]
        adjPointsFeatureLayer = outputWatershedItem.layers[1]

        calculate_watershed(sites_layer, watershedFeatureLayer, adjPointsFeatureLayer, order_direction)
    except Exception:
        logger.info(traceback.format_exc())

    finally:
        # Log Run Time
        logger.info('\n\nProgram Run Time: {0} Minutes'.format(round(((time.time() - start_time) / 60), 2)))
