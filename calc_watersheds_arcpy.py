import arcgis
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
import sys
import arcpy
from arcgis.geometry import Geometry

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

def calculate_watershed_for_featureset(samples_layer, key_field, watershedFeatureLayer, adjPointsFeatureLayer, lab_ids_allocated, records_per_request, seconds_between_requests, search_parameters, thread_id):
    logger.info("Thread ID: {}".format(thread_id))

    target_sr = watershedFeatureLayer.container.properties.spatialReference.wkid
    # arcpy.agolservices.Watershed can't use memory feature class, so we use in_memory workspace
    arcpy.env.overwriteOutput = True
    arcpy.env.workspace = "in_memory"
    arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(target_sr)

    num_of_results = 0
    # calculate the number of loops required to process in this thread
    num_of_loops = (len(lab_ids_allocated) // records_per_request)
    # if the remainder is greater than 0, add 1 to the number of loops
    if (len(lab_ids_allocated) % records_per_request) > 0:
        num_of_loops += 1

    watershed_attribute_fields_to_include = []
    Pourpoint_attribute_fields_to_include = []
    fields_to_skip = ["OBJECTID", "Shape", "Shape_Length", "Shape_Area"]
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

            samples_q_results = samples_layer.query(where = "{} IN ({})".format(key_field, lab_ids_str), out_fields="*",
                                        return_all_records = True, return_geometry=True, out_sr = target_sr)

            logger.info("\tThread ID: {}: {} samples found".format(thread_id, len(samples_q_results.features)))
            # User Arcpy
            #my_sr = arcpy.SpatialReference(samples_q_results.spatial_reference["wkid"])

            # check if the in_memory samples feature class exists, if it does, delete it
            fc_name = "samples{}".format(thread_id)
            if arcpy.Exists(r"in_memory\{}".format(fc_name)):
                arcpy.management.Delete(fc_name)

            # create a feature class from the samples_q_results features. The geometries need to be Arcpy geometries
            arcpy_samples = arcpy.management.CreateFeatureclass("in_memory", fc_name, "POINT", spatial_reference = target_sr)
            arcpy.AddField_management(arcpy_samples, key_field, "TEXT")

            with arcpy.da.InsertCursor(arcpy_samples, [key_field, "SHAPE@"]) as cursor:
                for sample in samples_q_results.features:
                    #print(sample.attributes[key_field])
                    cursor.insertRow([sample.attributes[key_field], Geometry(sample.geometry).as_arcpy])
            del cursor

            out_result = arcpy.agolservices.Watershed(
                InputPoints=arcpy_samples,
                PointIDField=key_field,
                SnapDistance="",
                SnapDistanceUnits="Meters",
                DataSourceResolution="FINEST",
                Generalize=False,
                ReturnSnappedPoints=True
            )

            sleep_interval = 3
            time.sleep(sleep_interval)
            seconds_slept = sleep_interval
            while out_result.status < 4:
                #print(out_result.status)
                time.sleep(sleep_interval)
                seconds_slept += sleep_interval

            print("seconds_slept: {}".format(seconds_slept))

            # Delete the in_memory feature class
            arcpy.management.Delete(arcpy_samples)

            out_watersheds_fc = out_result.getOutput(0)
            out_pourpoints_fc = out_result.getOutput(1)

            # get the number of features in the output watersheds feature class
            num_features_generated = int(arcpy.management.GetCount(out_watersheds_fc)[0])
            if num_features_generated > 0:
                num_of_results += num_features_generated
                logger.info("\tThread ID: {}: {} -> {} watersheds calculated. ".format(thread_id, num_features_generated, num_of_results))

                # project the output  feature class to the target spatial reference
                out_watersheds_fc_projected = arcpy.management.Project(out_watersheds_fc, "in_memory\watersheds_projected", target_sr)
                out_pourpoints_fc_projected = arcpy.management.Project(out_pourpoints_fc, "in_memory\pourpoints_projected", target_sr)

                if len(watershed_attribute_fields_to_include) == 0:
                    fieldList = arcpy.ListFields(out_watersheds_fc_projected)
                    for f in fieldList:
                        if f.type != "Geometry" and f.name not in fields_to_skip:
                            watershed_attribute_fields_to_include.append(f.name)

                if len(Pourpoint_attribute_fields_to_include) == 0:
                    fieldList = arcpy.ListFields(out_pourpoints_fc_projected)
                    for f in fieldList:
                        if f.type != "Geometry" and f.name not in fields_to_skip:
                            Pourpoint_attribute_fields_to_include.append(f.name)

                save_to_feature_layer(out_watersheds_fc_projected, watershedFeatureLayer, thread_id, watershed_attribute_fields_to_include, key_field)
                save_to_feature_layer(out_pourpoints_fc_projected, adjPointsFeatureLayer, thread_id, Pourpoint_attribute_fields_to_include, key_field)

            arcpy.management.Delete(out_watersheds_fc)
        except Exception:
            logger.info("Thread {} exception {}".format(thread_id, traceback.format_exc()))
        finally:
            time.sleep(seconds_between_requests)
            continue

        return num_of_results

def save_to_feature_layer(out_fc, targetFeatureLayer, thread_id, fields_to_include, key_field):

    # read from the output feature class
    list_out_features = []
    with arcpy.da.SearchCursor(out_fc, ["SHAPE@"] + fields_to_include, where_clause="1=1") as cursor:
        for row in cursor:
            geom = row[0]
            geom_json_str = geom.JSON
            # covert geom_json_str to json
            geom_json = json.loads(geom_json_str)
            new_feature = {"geometry": geom_json}
            new_feature["attributes"] = {}
            for i in range(1, len(row)):
                new_feature["attributes"][fields_to_include[i - 1]] = row[i]

            #new_feature["attributes"][key_field] = new_feature["attributes"]["PourPtID"]
            list_out_features.append(new_feature)
    del cursor

    num_features_generated = len(list_out_features)

    if num_features_generated > 0:
        logger.info("\tFeatures to add: {}".format(list_out_features))
        # append
        addResults_watershed = targetFeatureLayer.edit_features(adds=list_out_features)
        # the results is in this format:
        # {'addResults': [{'objectId': 19245, 'uniqueId': 19245, 'globalId': '191564D7-A52E-49B8-9DC9-071E3F40FF94', 'success': True}, {'objectId': 19246, 'uniqueId': 19246, 'globalId': '31758F40-91BE-4168-91DC-04F1157CF10B', 'success': True}, {'objectId': 19247, 'uniqueId': 19247, 'globalId': '02504392-67E0-48EC-A030-1AD27562DCAE', 'success': True}], 'updateResults': [], 'deleteResults': []}

        # log the number of successful results with success=True and number of failed results
        num_succeeded_records = len(list(filter(lambda d: d["success"] == True, addResults_watershed["addResults"])))
        num_failed_records = num_features_generated - num_succeeded_records
        logger.info("\tThread ID: {}: add features: {} succeeded, {} failed".format(thread_id, num_succeeded_records, num_failed_records))
        if num_failed_records > 0:
            logger.info(list_out_features)
            logger.info(addResults_watershed)



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
    logger.info("Python version {}".format(sys.version))
    logger.info("ArcGIS Python API version {}".format(arcgis.__version__))

    the_portal = parameters['the_portal']
    use_ArcGIS_Pro = the_portal['use_ArcGIS_Pro']

    if use_ArcGIS_Pro:
        gis = GIS("pro")
    else:
        portal_url = the_portal['url']
        the_username = the_portal['user']
        the_password = the_portal['pass']
        gis = GIS(portal_url, the_username, the_password)


    # Get Parameters about multi-threading
    records_per_request = parameters["records_per_request"]
    number_of_threads = 1 # parameters["number_of_threads"]
    seconds_between_requests = 0 # parameters["seconds_between_requests"]
    #logger.info("Number of Threads: {}".format(number_of_threads))
    logger.info("Records per Request: {}".format(records_per_request))
    #logger.info("Seconds Between Requests: {}".format(seconds_between_requests))

    # Get the Search Parameters for the Watershed Calculation
    search_parameters = {} # parameters["search_parameters"]

    try:

        for task in parameters["tasks"]:
            logger.info("\n ----------- Task: {} skip: {} -----------".format(task["name"], task["skip"]))
            if task["skip"] == True:
                continue

            samplesItemId = task['inputLayerConfig']["itemId"]
            samples_layerId = task['inputLayerConfig']["layerId"]
            key_field = task['inputLayerConfig']["key_field"]
            outputWatershedItemId = task['outputWatershedLayerConfig']["itemId"]
            watershed_layer_id = task['outputWatershedLayerConfig']["watershed_layer_id"]
            adjPoints_layer_id = task['outputWatershedLayerConfig']["snapped_points_layer_id"]
            output_key_field = task['outputWatershedLayerConfig']["key_field"]
            sWhere = task['inputLayerConfig']["where"]

            samplesItem=gis.content.get(samplesItemId)
            samples_layer = samplesItem.layers[samples_layerId]

            outputWatershedItem = gis.content.get(outputWatershedItemId)

            watershedFeatureLayer = outputWatershedItem.layers[watershed_layer_id]
            adjPointsFeatureLayer = outputWatershedItem.layers[adjPoints_layer_id]

            # return all the id of the samples
            samples_resp = samples_layer.query(where = sWhere, out_fields=[key_field], return_distinct_values = True,
                                           return_all_records = True, return_geometry=False)
            all_samples = [f.attributes[key_field] for f in samples_resp.features]
            logger.info("all_samples: {}".format(all_samples))
            len_all_samples = len(all_samples)

            # truncate the watersheds feature layer
            #watershedFeatureLayer.delete_features(where="1=1")
            # return all the unique Ids of the samples that have already been calculated
            watershed_resp = watershedFeatureLayer.query("1=1", out_fields=output_key_field, return_distinct_values = True,
                                                        return_all_records=True, return_geometry=False)
            calculated_samples = [f.attributes[output_key_field] for f in watershed_resp.features]
            logger.info("calculated_samples: {}".format(calculated_samples))
            len_calculated_samples = len(calculated_samples)

            # get the lab ids of the samples that have not been calculated
            samples_to_calculate = list(set(all_samples) - set(calculated_samples))

            logger.info("samples_to_calculate: {}".format(samples_to_calculate))

            num_of_features = len(samples_to_calculate)
            logger.info("total {}, calculated {}, remaining {}".format(len_all_samples, len_calculated_samples, num_of_features))

            # Go to the next task if there are no more samples to calculate
            if num_of_features == 0:
                continue

            records_per_thread = (num_of_features // number_of_threads)
            # if the remainder is greater than 0, add 1 to the records per thread
            if (num_of_features % number_of_threads) > 0:
                records_per_thread += 1

            logger.info("Records per Thread: {}".format(records_per_thread))

            threads = list()
            for i in range(0, number_of_threads):
                # get the lab ids from samples_to_calculate for the current thread
                lab_ids_allocated = samples_to_calculate[i * records_per_thread: (i + 1) * records_per_thread]
                calculate_watershed_for_featureset(samples_layer, key_field, watershedFeatureLayer, adjPointsFeatureLayer, lab_ids_allocated, records_per_request, seconds_between_requests, search_parameters, i)
            #     logger.info("Thread {}: Lab IDs: {}".format(i, lab_ids_allocated))
            #     x = threading.Thread(target=calculate_watershed_for_featureset, args=(samples_layer, key_field, watershedFeatureLayer, adjPointsFeatureLayer, lab_ids_allocated, records_per_request, seconds_between_requests, search_parameters, i))
            #     threads.append(x)
            #     x.start()

            # for index, thread in enumerate(threads):
            #     thread.join()


    except Exception:
        logger.info(traceback.format_exc())

    finally:
        # Log Run Time
        logger.info('\n\nProgram Run Time: {0} Minutes'.format(round(((time.time() - start_time) / 60), 2)))
