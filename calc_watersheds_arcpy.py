import arcgis
from arcgis.gis import GIS
import arcpy

# from arcgis.features.analysis import create_watersheds
import traceback
from datetime import datetime
import logging
import time
import json
import os
import sys

logger = None


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


def calculate_watershed_for_featureset(
    samples_layer,
    key_field,
    watershedFeatureLayer,
    adjPointsFeatureLayer,
    lab_ids_allocated,
    records_per_request,
    seconds_between_requests,
):

    target_sr = watershedFeatureLayer.container.properties.spatialReference.wkid
    # arcpy.agolservices.Watershed can't use memory feature class, so we use in_memory workspace
    arcpy.env.overwriteOutput = True
    arcpy.env.workspace = "in_memory"
    arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(target_sr)

    num_of_results = 0
    # calculate the number of loops required to process
    num_of_loops = len(lab_ids_allocated) // records_per_request
    # if the remainder is greater than 0, add 1 to the number of loops
    if (len(lab_ids_allocated) % records_per_request) > 0:
        num_of_loops += 1

    for i in range(0, num_of_loops):
        try:
            logger.info("Loop: {}".format(i))
            # get the lab ids for the lab ids in the request
            lab_ids = lab_ids_allocated[
                i * records_per_request : (i + 1) * records_per_request
            ]
            if len(lab_ids) == 0:
                return num_of_results

            # format the lab ids as a string with single quotes
            lab_ids_str = ",".join(["'{}'".format(x) for x in lab_ids])
            logger.info("\tLab IDs: {}".format(lab_ids_str))

            samples_q_results = samples_layer.query(
                where="{} IN ({})".format(key_field, lab_ids_str),
                out_fields="*",
                return_all_records=True,
                return_geometry=True,
                out_sr=target_sr,
            )

            logger.info("\t{} samples found".format(len(samples_q_results.features)))

            # check if the in_memory samples feature class exists, if it does, delete it
            fc_name = "samples"
            arcpy_samples = r"in_memory\{}".format(fc_name)
            if arcpy.Exists(arcpy_samples):
                arcpy.management.Delete(arcpy_samples)

            samples_sdf = samples_q_results.sdf

            # create a feature class from the spatial dataframe
            samples_sdf.spatial.to_featureclass(
                location=r"in_memory\{}".format(fc_name)
            )

            out_result = arcpy.agolservices.Watershed(
                InputPoints=r"in_memory\{}".format(fc_name),
                PointIDField=key_field,
                SnapDistance="",
                SnapDistanceUnits="Meters",
                DataSourceResolution="FINEST",
                Generalize=False,
                ReturnSnappedPoints=True,
            )

            sleep_interval = 3
            time.sleep(sleep_interval)
            seconds_slept = sleep_interval
            while out_result.status < 4:
                # print(out_result.status)
                time.sleep(sleep_interval)
                seconds_slept += sleep_interval

            print("seconds_slept: {}".format(seconds_slept))

            # Delete the in_memory feature class
            arcpy.management.Delete(arcpy_samples)

            out_watersheds_fc = out_result.getOutput(0)
            out_pourpoints_fc = out_result.getOutput(1)

            # get the number of features in the output watersheds feature class
            num_features_generated = int(
                arcpy.management.GetCount(out_watersheds_fc)[0]
            )
            if num_features_generated > 0:
                num_of_results += num_features_generated
                logger.info(
                    "\t{} -> {} watersheds calculated. ".format(
                        num_features_generated, num_of_results
                    )
                )

                save_to_feature_layer(out_watersheds_fc, watershedFeatureLayer)
                save_to_feature_layer(out_pourpoints_fc, adjPointsFeatureLayer)

            # delete the out_result
            arcpy.management.Delete(out_result)
        except Exception:
            logger.info("Exception {}".format(traceback.format_exc()))
        finally:
            time.sleep(seconds_between_requests)
            continue


def save_to_feature_layer(out_fc, targetFeatureLayer):

    fs = arcpy.FeatureSet(out_fc)
    fset = arcgis.features.FeatureSet.from_arcpy(fs)

    num_features_generated = len(fset.features)

    if num_features_generated > 0:
        # append
        addResults = targetFeatureLayer.edit_features(adds=fset.features)

        # log the number of successful results with success=True and number of failed results
        num_succeeded_records = len(
            list(filter(lambda d: d["success"] == True, addResults["addResults"]))
        )
        num_failed_records = num_features_generated - num_succeeded_records
        logger.info(
            "\tAdd features: {} succeeded, {} failed".format(
                num_succeeded_records, num_failed_records
            )
        )
        if num_failed_records > 0:
            logger.info(fset.features)
            logger.info(addResults)


if __name__ == "__main__":

    # Get Start Time
    start_time = time.time()

    # Get Script Directory
    this_dir = os.path.split(os.path.realpath(__file__))[0]
    this_filename = os.path.split(os.path.realpath(__file__))[1]

    # Collect Configured Parameters
    parameters = get_config(os.path.join(this_dir, "./config/config_watersheds.json"))
    # Get Logger & Log Directory
    log_folder = parameters["log_folder"]
    logger, log_dir, log_file_name = get_logger(log_folder, this_filename, start_time)
    logger.info("Python version {}".format(sys.version))
    logger.info("ArcGIS Python API version {}".format(arcgis.__version__))

    the_portal = parameters["the_portal"]
    use_ArcGIS_Pro = the_portal["use_ArcGIS_Pro"]

    if use_ArcGIS_Pro:
        gis = GIS("pro")
    else:
        portal_url = the_portal["url"]
        the_username = the_portal["user"]
        the_password = the_portal["pass"]
        gis = GIS(portal_url, the_username, the_password)

    records_per_request = parameters["records_per_request"]
    seconds_between_requests = 0  # parameters["seconds_between_requests"]
    logger.info("Records per Request: {}".format(records_per_request))
    # logger.info("Seconds Between Requests: {}".format(seconds_between_requests))

    try:

        for task in parameters["tasks"]:
            logger.info(
                "\n ----------- Task: {} skip: {} -----------".format(
                    task["name"], task["skip"]
                )
            )
            if task["skip"] == True:
                continue

            samplesItemId = task["inputLayerConfig"]["itemId"]
            samples_layerId = task["inputLayerConfig"]["layerId"]
            key_field = task["inputLayerConfig"]["key_field"]
            outputWatershedItemId = task["outputWatershedLayerConfig"]["itemId"]
            watershed_layer_id = task["outputWatershedLayerConfig"][
                "watershed_layer_id"
            ]
            adjPoints_layer_id = task["outputWatershedLayerConfig"][
                "snapped_points_layer_id"
            ]
            output_key_field = "PourPtID"
            sWhere = task["inputLayerConfig"]["where"]

            samplesItem = gis.content.get(samplesItemId)
            samples_layer = samplesItem.layers[samples_layerId]

            outputWatershedItem = gis.content.get(outputWatershedItemId)

            watershedFeatureLayer = outputWatershedItem.layers[watershed_layer_id]
            adjPointsFeatureLayer = outputWatershedItem.layers[adjPoints_layer_id]

            # return all the id of the samples
            samples_resp = samples_layer.query(
                where=sWhere,
                out_fields=[key_field],
                return_distinct_values=True,
                return_all_records=True,
                return_geometry=False,
            )
            all_samples = [f.attributes[key_field] for f in samples_resp.features]
            logger.info("all_samples: {}".format(all_samples))
            len_all_samples = len(all_samples)

            # return all the unique Ids of the samples that have already been calculated
            watershed_resp = watershedFeatureLayer.query(
                "1=1",
                out_fields=output_key_field,
                return_distinct_values=True,
                return_all_records=True,
                return_geometry=False,
            )
            calculated_samples = [
                f.attributes[output_key_field] for f in watershed_resp.features
            ]
            logger.info("calculated_samples: {}".format(calculated_samples))
            len_calculated_samples = len(calculated_samples)

            # get the lab ids of the samples that have not been calculated
            samples_to_calculate = list(set(all_samples) - set(calculated_samples))

            logger.info("samples_to_calculate: {}".format(samples_to_calculate))

            num_of_features = len(samples_to_calculate)
            logger.info(
                "total {}, calculated {}, remaining {}".format(
                    len_all_samples, len_calculated_samples, num_of_features
                )
            )

            # Go to the next task if there are no more samples to calculate
            if num_of_features == 0:
                continue

            calculate_watershed_for_featureset(
                samples_layer,
                key_field,
                watershedFeatureLayer,
                adjPointsFeatureLayer,
                samples_to_calculate,
                records_per_request,
                seconds_between_requests,
            )

    except Exception:
        logger.info(traceback.format_exc())

    finally:
        # Log Run Time
        logger.info(
            "\n\nProgram Run Time: {0} Minutes".format(
                round(((time.time() - start_time) / 60), 2)
            )
        )
