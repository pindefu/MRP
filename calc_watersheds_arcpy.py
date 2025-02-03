import arcgis
from arcgis.gis import GIS
import arcpy
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

    arcpy.env.overwriteOutput = True
    arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(target_sr)
    sleep_interval = 3

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
            logger.info("\tKey IDs: {}".format(lab_ids_str))

            # create an arcpy feature set by querying the samples layer
            arcpy_samples_fset = arcpy.FeatureSet(
                samples_layer.url,
                where_clause="{} IN ({})".format(key_field, lab_ids_str),
            )

            num_samples = int(arcpy.management.GetCount(arcpy_samples_fset)[0])
            logger.info("\tCalculating watersheds for {} points".format(num_samples))

            out_result = arcpy.agolservices.Watershed(
                InputPoints=arcpy_samples_fset,
                PointIDField=key_field,
                SnapDistance="",
                SnapDistanceUnits="Meters",
                DataSourceResolution="FINEST",
                Generalize=False,
                ReturnSnappedPoints=True,
            )

            seconds_slept = 0
            while out_result.status < 4:
                # print(out_result.status)
                time.sleep(sleep_interval)
                seconds_slept += sleep_interval

            print("\tTime used: {} seconds".format(seconds_slept))

            out_watersheds_fc = out_result.getOutput(0)
            out_pourpoints_fc = out_result.getOutput(1)

            # get the number of features in the output watersheds feature class
            num_features_generated = int(
                arcpy.management.GetCount(out_watersheds_fc)[0]
            )
            if num_features_generated > 0:
                num_of_results += num_features_generated
                logger.info(
                    "\t{} watersheds calculated. Total watersheds {} ".format(
                        num_features_generated, num_of_results
                    )
                )

                save_to_feature_layer(
                    out_watersheds_fc, watershedFeatureLayer, "Watersheds"
                )
                save_to_feature_layer(
                    out_pourpoints_fc, adjPointsFeatureLayer, "PourPoints"
                )

            # delete the out_result
            arcpy.management.Delete(out_result)
        except Exception:
            logger.info("Exception {}".format(traceback.format_exc()))
        finally:
            time.sleep(seconds_between_requests)
            continue


def save_to_feature_layer(out_fc, targetFeatureLayer, lyr_name=None):

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
            "\tAdd {}: {} succeeded, {} failed".format(
                lyr_name, num_succeeded_records, num_failed_records
            )
        )
        if num_failed_records > 0:
            logger.info("\tFailed to add {}".format(lyr_name))
            logger.info(fset.features)
            logger.info(addResults)


def print_envs():
    logger.info("Python version {}".format(sys.version))
    logger.info("ArcGIS Python API version {}".format(arcgis.__version__))
    logger.info("ArcPy version {}".format(arcpy.GetInstallInfo()["Version"]))


def connect_to_portal(parameters):
    the_portal = parameters["the_portal"]
    use_ArcGIS_Pro = the_portal["use_ArcGIS_Pro"]

    if use_ArcGIS_Pro:
        return GIS("pro")
    else:
        portal_url = the_portal["url"]
        the_username = the_portal["user"]
        the_password = the_portal["pass"]
        return GIS(portal_url, the_username, the_password)


def updateItemProperties(
    result_item, input_item, task, tags_to_inject, gis, completedOn
):
    # check if you have the right to update the item
    try:
        me = gis.users.me
        userFullName = me.fullName
        newDescription = "<B>Watersheds calculated by: {} on {} using the following parameters:</b>  \n<br>".format(
            userFullName, completedOn
        )
        newDescription += "<ul><li>Input point item: <a href='https://usgs.maps.arcgis.com/home/item.html?id={}' target='_blank'>{}</a></li>".format(
            input_item.id, input_item.title
        )
        newDescription += "<li>Input point layer: <a href='https://usgs.maps.arcgis.com/home/item.html?id={}&sublayer={}#data' target='_blank'>{}</a></li>".format(
            input_item.id,
            task["inputLayerConfig"]["layerId"],
            input_item.layers[task["inputLayerConfig"]["layerId"]].properties.name,
        )
        newDescription += "<li>key_field: {}</li>".format(
            task["inputLayerConfig"]["key_field"]
        )
        newDescription += "<li>Where: {}</li>".format(task["inputLayerConfig"]["where"])
        newDescription += "</ul>"
        if result_item.description is not None:
            newDescription += "\n\n<br><br>" + result_item.description
        props = {"description": newDescription}

        if tags_to_inject and len(tags_to_inject) > 0:
            # Check if the tags in tags_to_inject are already in the item
            # if not, add them
            tags = result_item.tags
            # drop empty tags
            tags = [tag for tag in tags if tag]
            for tag in tags_to_inject:
                if tag not in tags:
                    tags.append(tag)
            props["tags"] = tags

        update_response = result_item.update(item_properties=props)
        logger.info(
            "Update result item metadata: {}".format(
                "Successful" if update_response else "Failed"
            )
        )

    except Exception as e:
        if e.args[0].find("403") > 0 and e.args[0].find("permissions") > 0:
            print("User does not have permissions to update the metadata of the item")
        else:
            print("Error updating item description: {}".format(e))


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
    print_envs()
    gis = connect_to_portal(parameters)

    tags_to_inject = parameters["tags_to_inject"]
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
            # logger.info("All samples: {}".format(all_samples))
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
            # logger.info("Calculated samples: {}".format(calculated_samples))
            len_calculated_samples = len(calculated_samples)

            # get the lab ids of the samples that have not been calculated
            samples_to_calculate = list(set(all_samples) - set(calculated_samples))

            logger.info("Samples to calculate: {}".format(samples_to_calculate))

            num_of_features = len(samples_to_calculate)
            logger.info(
                "\n\nTotal {}, Calculated {}, Remaining {}".format(
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

            # Update the description of the output item outputWatershedItem
            updateItemProperties(
                outputWatershedItem,
                samplesItem,
                task,
                tags_to_inject,
                gis,
                datetime.now(),
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
