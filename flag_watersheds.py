from arcgis.gis import GIS
from arcgis.features import FeatureLayerCollection
from arcgis.features.analysis import create_watersheds
import traceback
from datetime import datetime
import logging
import time
import json
import os
import math
import arcpy
from arcgis.geometry import Polygon, Geometry, areas_and_lengths
from arcgis.geometry import Point, buffer, LengthUnits, AreaUnits

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



def flag_size(taskItem, taskLyrTble, rule_config):
    logger.info("\n ------- Flagging by size ------- \n")
    # Query the layer to get all values of the area_field.
    fld_area = rule_config["area_field"]
    log_transform = rule_config["log_transform"]
    cutoff_large = rule_config["cutoff_large"]
    cutoff_small = rule_config["cutoff_small"]
    field_to_calc = rule_config["field_to_calc"]

    query_result = taskLyrTble.query(where="1=1", out_fields="{}".format(fld_area), return_geometry=False, return_all_records=True, order_by_fields="{} ASC".format(fld_area))

    # extract the area_field values from the query result, logaritmized the values, and store them in a list

    sorted_area_values = []
    log_transform = False
    for f in query_result.features:
        if log_transform:
            sorted_area_values.append(math.log(f.attributes[fld_area]))
        else:
            sorted_area_values.append(f.attributes[fld_area])


    # get the cutoff values
    cutoff_large_value = sorted_area_values[int(len(sorted_area_values) * cutoff_large)]
    cutoff_small_value = sorted_area_values[int(len(sorted_area_values) * cutoff_small)]

    if log_transform:
        cutoff_large_value = math.exp(cutoff_large_value)
        cutoff_small_value = math.exp(cutoff_small_value)

    logger.info("Cutoff Large Value: {}".format(cutoff_large_value))
    logger.info("Cutoff Small Value: {}".format(cutoff_small_value))

    # Arcade doesn't work here. Let's use SQL instead
    # write an SQL expression to update the field. If the area is larger than the cutoff_large_value, set the field to 2. If the area is smaller than the cutoff_small_value, set the field to 1. Otherwise, set the field to 0

    calc_sql_expresison = "CASE WHEN {} > {} THEN 2 WHEN {} < {} THEN 1 ELSE 0 END".format(fld_area, cutoff_large_value, fld_area, cutoff_small_value)
    print("field: {} = Calc Expression: {}".format(field_to_calc, calc_sql_expresison))
    calc_field_response = taskLyrTble.calculate(where="1=1", calc_expression={"field": field_to_calc, "sqlExpression" : calc_sql_expresison})
    print("Calc Field Response: {}".format(calc_field_response))

    # arcade_expression = "if ($feature.AreaSqKm > " + str(cutoff_large_value) + ") { return 2 } else if ($feature.AreaSqKm < " + str(cutoff_small_value) + ") {return 1 } else { return 0 }"
    # arcpy.management.CalculateField(
    #     in_table=taskLyrTble,
    #     field=field_to_calc,
    #     expression=arcade_expression,
    #     expression_type="ARCADE",
    #     enforce_domains="NO_ENFORCE_DOMAINS"
    # )

def flag_elongated_with_circularity_ratio(taskItem, taskLyrTble, rule_config):
    cutoff_large = rule_config["cutoff_large"]
    cutoff_small = rule_config["cutoff_small"]
    # Query the layer to get globalid and the AreaSqKm fields, and return the geometries.
    fld_objectId = taskLyrTble.properties.objectIdField
    query_result = taskLyrTble.query(where="1=1", out_fields=[fld_objectId, "Circularity_Ratio"], return_geometry=False, return_all_records=True, order_by_fields="{} ASC".format("Circularity_Ratio"))

    n_features = len(query_result.features)
    logger.info("Total number of features: {}".format(n_features))
    # The features are already sorted by Circularity_Ratio
    # loop through the query result, and calculate the percentile for each feature, and flag the feature based on the cutoff values
    list_to_update = []
    for i, f in enumerate(query_result.features):
        new_attributes = {fld_objectId: f.attributes[fld_objectId]}
        if i < n_features * cutoff_small:
            new_attributes["Flag_Elongated"] = 2
        elif i > n_features * cutoff_large:
            new_attributes["Flag_Elongated"] = 1
        else:
            new_attributes["Flag_Elongated"] = 0
        list_to_update.append({"attributes": new_attributes})

    save_to_featurelayer(list_to_update,update=taskLyrTble, track=None, item=taskItem, operation="update", use_global_ids=False)

def flag_elongated(taskItem, taskLyrTble, rule_config):
    logger.info("\n ------- Flagging enlongated polygons ------- \n")

    cutoff_large = rule_config["cutoff_large"]
    cutoff_small = rule_config["cutoff_small"]
    # Query the layer to get globalid and the AreaSqKm fields, and return the geometries.
    fld_objectId = taskLyrTble.properties.objectIdField
    query_result = taskLyrTble.query(where="1=1", out_fields=[fld_objectId, "AreaSqKm"], return_geometry=True, return_all_records=True)

    # for each feature in the query result, calculate the perimeter of the geometry and the area of the geometry.
    logger.info("Calculating circularity ratios")
    for f in query_result.features:
        area_sq_km = f.attributes["AreaSqKm"]

        # use ArcGIS Python API Geometry class to calculate the perimeter
        geom = f.geometry
        perimeter_km = Polygon(geom).length / 1000
        output_results = areas_and_lengths(polygons=[geom], length_unit=9036, area_unit = {'areaUnit': 'esriSquareKilometers'},  calculation_type='GEODESIC')
        perimeter_km = output_results["lengths"][0]

        # Use Arcpy to calculate the perimeter of the geometry
        # shp = Geometry(f.geometry).as_arcpy
        # output_result = arcpy.management.CalculateGeometryAttributes(in_features=shp, geometry_property="LENGTH_GEODESIC", length_unit="KILOMETERS")
        # perimeter_km = output_result.getOutput(0)

        # calculate the circularity ratio, which is the area devided by the perimeter squared
        circularity_ratio = area_sq_km / (perimeter_km ** 2)
        f.attributes["circularity_ratio"] = circularity_ratio

    logger.info("Calculating cutoff values")
    # get the list of circularity ratios, sort them, and get the cutoff value by percentiles
    list_circularity_ratios = [f.attributes["circularity_ratio"] for f in query_result.features]
    list_circularity_ratios.sort()
    num_values = len(list_circularity_ratios)
    # get the cutoff values
    cutoff_large_value = list_circularity_ratios[int(num_values * cutoff_large)]
    cutoff_small_value = list_circularity_ratios[int(num_values * cutoff_small)]

    logger.info("Cutoff Upper Value: {}".format(cutoff_large_value))
    logger.info("Cutoff Lower Value: {}".format(cutoff_small_value))

    # build the list of features to update the field based on the circularity ratio
    list_to_update = []
    for f in query_result.features:
        new_attributes = {fld_objectId: f.attributes[fld_objectId]}
        if f.attributes["circularity_ratio"] > cutoff_large_value:
            new_attributes["Flag_Elongated"] = 2
        elif f.attributes["circularity_ratio"] < cutoff_small_value:
            new_attributes["Flag_Elongated"] = 1
        else:
            new_attributes["Flag_Elongated"] = 0
        list_to_update.append({"attributes": new_attributes})

    save_to_featurelayer(list_to_update,update=taskLyrTble, track=None, item=taskItem, operation="update", use_global_ids=False)

@run_update
def save_to_featurelayer(process_list):
    logger.info("\tFeatures info to update: {}".format(process_list))
    return process_list

def flag_multiparts(taskItem, taskLyrTble, rule_config):
    logger.info("\n ------- Flagging multipart polygons ------- \n")
    # Query the layer to get globalid and return the geometries.
    fld_objectId = taskLyrTble.properties.objectIdField
    query_result = taskLyrTble.query(where="1=1", out_fields=[fld_objectId, "DataResolution"], return_geometry=True, return_all_records=True)

    # for each feature in the query result, calculate the number of parts in the geometry
    iCount_multipart = 0
    list_to_update = []
    for f in query_result.features:
        new_attributes = {fld_objectId: f.attributes[fld_objectId]}
        geom = f.geometry
        num_parts = len(geom["rings"])
        if num_parts == 1:
            new_attributes["Flag_Multipart"] = 0
        else:
            # use arcpy to buffer the geometry by the half of the DEM resolution with the dissolve option
            # check the resulting geometry to see if it is multipart
            # if the number of parts is larger than 1, set the Flag_Multipart field to 1, otherwise set the Flag_Multipart field to 0
            dataResolution = f.attributes["DataResolution"]
            buffer_distance = float(dataResolution) / 2
            shp = Geometry(f.geometry).as_arcpy
            logger.info("\nBuffering & dissolving object ID: {} with {} rings".format(f.attributes[fld_objectId], num_parts))
            output_result = arcpy.analysis.Buffer(in_features=shp, out_feature_class="in_memory/buffered_geom", buffer_distance_or_field=" {} Meters".format(buffer_distance), dissolve_option="ALL")
            buffered_fc = output_result.getOutput(0)
            # use cursor to get the first feature
            with arcpy.da.SearchCursor(buffered_fc, ["SHAPE@"]) as cursor:
                for row in cursor:
                    buffered_geom = row[0]
                    if buffered_geom.isMultipart:
                        new_attributes["Flag_Multipart"] = 1
                        iCount_multipart += 1
                        logger.info("{} Multipart found. Object ID {}".format(iCount_multipart, f.attributes[fld_objectId]))
                    else:
                        new_attributes["Flag_Multipart"] = 0

            # delete the cursor
            del cursor
            # delete the buffered feature class
            arcpy.management.Delete(buffered_fc)
        list_to_update.append({"attributes": new_attributes})

    logger.info("Total {} multipart polygons found".format(iCount_multipart))
    save_to_featurelayer(list_to_update,update=taskLyrTble, track=None, item=taskItem, operation="update", use_global_ids=False)

if __name__ == "__main__":

    # Get Start Time
    start_time = time.time()

    # Get Script Directory
    this_dir = os.path.split(os.path.realpath(__file__))[0]
    this_filename = os.path.split(os.path.realpath(__file__))[1]

    # Collect Configured Parameters
    parameters = get_config(os.path.join(this_dir, './config/config_flag_watersheds.json'))
    # Get Logger & Log Directory
    log_folder = parameters["log_folder"]
    logger, log_dir, log_file_name = get_logger(log_folder, this_filename, start_time)

    the_portal = parameters['the_portal']
    portal_url = the_portal['url']
    the_username = the_portal['user']
    the_password = the_portal['pass']
    gis = GIS(portal_url, the_username, the_password)


    try:
        rules_to_run = parameters['rules_to_run']

        tasks = parameters['tasks']
        for task in tasks:
            logger.info("\n\nStarting task: {}\n".format(task["name"]))
            bSkip = task.get("skip", False)
            if bSkip:
                logger.info("Task is skipped")
                continue

            taskItemId = task["itemId"]
            taskItemIsLayer = task["isLayer"]
            taskLyrTblId = task["LyrTblid"]

            taskItem=gis.content.get(taskItemId)
            # print out the title of the item
            logger.info("Task Item Title: {}".format(taskItem.title))
            taskLyrTble = None
            if taskItemIsLayer:
                taskLyrTble = taskItem.layers[taskLyrTblId]
            else:
                taskLyrTble = taskItem.tables[taskLyrTblId]

            if not rules_to_run["flag_size"]["skip"]:
                flag_size(taskItem, taskLyrTble, rules_to_run["flag_size"])

            if not rules_to_run["flag_multiparts"]["skip"]:
                flag_multiparts(taskItem, taskLyrTble, rules_to_run["flag_multiparts"])

            if not rules_to_run["flag_elongated"]["skip"]:
                flag_elongated_with_circularity_ratio(taskItem, taskLyrTble, rules_to_run["flag_elongated"])


    except Exception:
        logger.info(traceback.format_exc())

    finally:
        # Log Run Time
        logger.info('\n\nProgram Run Time: {0} Minutes'.format(round(((time.time() - start_time) / 60), 2)))
