import sys
import arcgis
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
import pandas as pd
from arcgis.geometry import Polygon, Geometry, areas_and_lengths
from arcgis.geometry import Point, buffer, LengthUnits, AreaUnits

logger = None
batch_size = 1000
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
        num_succeeded_records = 0
        num_failed_records = 0

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

            # Push Edit Batches
            try:
                for update_set in update_sets:
                    try:
                        keyStr = ""
                        if operation == "update":
                            edit_result = kwargs.get("update").edit_features(
                                updates=update_set, use_global_ids=use_global_ids
                            )
                            keyStr = "updateResults"
                        else:  # add
                            edit_result = kwargs.get("add").edit_features(
                                adds=update_set
                            )
                            keyStr = "addResults"

                        totalRecords = len(edit_result[keyStr])

                        succeeded_records = len(
                            list(
                                filter(
                                    lambda d: d["success"] == True,
                                    edit_result[keyStr],
                                )
                            )
                        )
                        logger.info(
                            "\tBatch edit results: {} of {} succeeded".format(
                                succeeded_records, totalRecords
                            )
                        )
                        num_succeeded_records = (
                            num_succeeded_records + succeeded_records
                        )
                        if totalRecords > succeeded_records:
                            failed_records = list(
                                filter(
                                    lambda d: d["success"] == False,
                                    edit_result[keyStr],
                                )
                            )
                            num_failed_records = num_failed_records + len(
                                failed_records
                            )
                            logger.info("\Failed records: {}".format(failed_records))

                    except Exception:
                        logger.info(traceback.format_exc())
            except Exception:
                logger.info(traceback.format_exc())
            finally:
                logger.info(
                    " \n\tSummary: Total records {}, succeeded records {}, failed records {}".format(
                        num_total_records, num_succeeded_records, num_failed_records
                    )
                )

        else:
            logger.info("Returned List Was Empty. No Edits Performed.")

    return wrapper


integer_field_def = {
    "type": "esriFieldTypeInteger",
    "nullable": True,
    "editable": True,
    "visible": True,
    "defaultValue": None,
}
rounded_domain_def = {
    "domain": {
        "type": "codedValue",
        "codedValues": [
            {"name": "Missing Value", "code": -1},
            {"name": "No", "code": 0},
            {"name": "Rounded to Degree", "code": 1},
            {"name": "Rounded to Minute", "code": 2},
            {"name": "Rounded to 0.5 Minute", "code": 3},
        ],
    }
}

corners_domain_def = {
    "domain": {
        "type": "codedValue",
        "codedValues": [
            {"name": "Missing Latidue or Longitude", "code": -1},
            {"name": "No", "code": 0},
            {"name": "Yes", "code": 1},
        ],
    }
}

landforms_domain_def = {
    "domain": {
        "type": "codedValue",
        "codedValues": [
            {"name": "Flat", "code": 1},
            {"name": "Peak", "code": 2},
            {"name": "Ridge", "code": 3},
            {"name": "Shoulder", "code": 4},
            {"name": "Spur", "code": 5},
            {"name": "Slope", "code": 6},
            {"name": "Hollow", "code": 7},
            {"name": "Footslope", "code": 8},
            {"name": "Valley", "code": 9},
            {"name": "Pit", "code": 10},
        ],
    }
}

field_to_calc_x = "Flag_RoundedX"
field_to_calc_y = "Flag_RoundedY"
field_to_calc_corner = "Flag_Corners"
field_to_calc_landforms = "Flag_Landforms"


def add_flag_fields(taskItem, taskLyr):
    global field_to_calc_x, field_to_calc_y, field_to_calc_corner, field_to_calc_landforms
    logger.info("\n ------- Adding fields to the layer ------- \n")
    # Add Flag_RoundedX, Flag_RoundedY, Flag_Corners, Flag_Landforms, and Flagged fields to the layer
    # Check if each of the fields already exist in the layer, if not, add them
    fields_to_add = []
    if not any(f["name"] == field_to_calc_x for f in taskLyr.properties.fields):
        field_to_calc_x_def = {
            "name": field_to_calc_x,
            "alias": field_to_calc_x.replace("_", " "),
        }

        # merge with the integer_field_def and rounded_domain_def, and create a new field definition
        field_to_calc_x_def = {
            **field_to_calc_x_def,
            **integer_field_def,
            **rounded_domain_def,
        }
        field_to_calc_x_def["domain"]["name"] = "{}_Domain".format(field_to_calc_x)
        fields_to_add.append(field_to_calc_x_def)

    if not any(f["name"] == field_to_calc_y for f in taskLyr.properties.fields):
        field_to_calc_y_def = {
            "name": field_to_calc_y,
            "alias": field_to_calc_y.replace("_", " "),
        }

        field_to_calc_y_def = {
            **field_to_calc_y_def,
            **integer_field_def,
            **rounded_domain_def,
        }
        field_to_calc_y_def["domain"]["name"] = "{}_Domain".format(field_to_calc_y)
        fields_to_add.append(field_to_calc_y_def)

    if not any(f["name"] == field_to_calc_corner for f in taskLyr.properties.fields):
        field_to_calc_corner_def = {
            "name": field_to_calc_corner,
            "alias": field_to_calc_corner.replace("_", " "),
        }
        field_to_calc_corner_def = {
            **field_to_calc_corner_def,
            **integer_field_def,
            **corners_domain_def,
        }
        field_to_calc_corner_def["domain"]["name"] = "{}_Domain".format(
            field_to_calc_corner
        )
        fields_to_add.append(field_to_calc_corner_def)

    if not any(f["name"] == field_to_calc_landforms for f in taskLyr.properties.fields):
        field_to_calc_landforms_def = {
            "name": field_to_calc_landforms,
            "alias": field_to_calc_landforms.replace("_", " "),
        }
        field_to_calc_landforms_def = {
            **field_to_calc_landforms_def,
            **integer_field_def,
            **landforms_domain_def,
        }
        field_to_calc_landforms_def["domain"]["name"] = "{}_Domain".format(
            field_to_calc_landforms
        )
        fields_to_add.append(field_to_calc_landforms_def)

    if len(fields_to_add) > 0:
        field_names = [f["name"] for f in fields_to_add]
        logger.info("Fields to add: {}".format(field_names))
        add_fields_response = taskLyr.manager.add_to_definition(
            {"fields": fields_to_add}
        )
        logger.info("\tAdd Fields Response: {}".format(add_fields_response))
    else:
        logger.info("\tAll fields already exist in the layer")


def flag_rounded(taskItem, taskLyr, task):
    global field_to_calc_x, field_to_calc_y, field_to_calc_corner, field_to_calc_landforms
    logger.info("\n ------- Flagging points with lat/long rounded ------- \n")
    latitide_field = task["latitide_field"]
    longitude_field = task["longitude_field"]
    where = None
    if "where" in task:
        where = task["where"]

    if where is None or where == "":
        where = "1=1"

    oid_field = taskLyr.properties.objectIdField

    # Query the layer to get all values of the latitide_field and longitude_field
    query_result = taskLyr.query(
        where=where,
        out_fields="{},{},{}".format(oid_field, latitide_field, longitude_field),
        return_geometry=False,
        return_all_records=True,
    )

    list_to_update = []
    num_flagged_x = 0
    num_flagged_y = 0
    num_flagged = 0
    # for each feature in the query result, check if the latitide_field and longitude_field are rounded to the nearest integer, the nearest minute, or the nearest 30 second
    for f in query_result.features:
        new_attributes = {oid_field: f.attributes[oid_field]}
        lat = f.attributes[latitide_field]
        lon = f.attributes[longitude_field]
        if lat is None:
            new_attributes[field_to_calc_y] = -1
        else:
            new_attributes[field_to_calc_y] = check_rounded(lat)

        if lon is None:
            new_attributes[field_to_calc_x] = -1
        else:
            new_attributes[field_to_calc_x] = check_rounded(lon)

        if new_attributes[field_to_calc_y] != 0:
            num_flagged_y += 1

        if new_attributes[field_to_calc_x] != 0:
            num_flagged_x += 1

        if new_attributes[field_to_calc_x] > 0 or new_attributes[field_to_calc_y] > 0:
            num_flagged += 1

        list_to_update.append({"attributes": new_attributes})

    logger.info("\tNumber of flagged longitude: {}".format(num_flagged_x))
    logger.info("\tNumber of flagged latitude: {}".format(num_flagged_y))
    logger.info("\tNumber of flagged points: {}".format(num_flagged))

    if num_flagged == 0:
        calc_field_response = taskLyr.calculate(
            where=where,
            calc_expression=[
                {"field": field_to_calc_x, "sqlExpression": 0},
                {"field": field_to_calc_y, "sqlExpression": 0},
            ],
        )
        logger.info("Calculate field response: {}".format(calc_field_response))
    else:
        save_to_featurelayer(
            list_to_update,
            update=taskLyr,
            track=None,
            item=taskItem,
            operation="update",
            use_global_ids=False,
        )


# check if the latitide_field and longitude_field are rounded to the nearest integer, the nearest minute, or the nearest 30 second
def check_rounded(v):
    if v is None:
        return 0

    tolerance_min = 0.0000001
    tolerance_max = 1 - tolerance_min
    # Extract the fractional degrees
    fractional_degrees = abs(v) % 1

    # Check if the value is rounded to the nearest degree
    if fractional_degrees < tolerance_min or fractional_degrees > tolerance_max:
        return 1
    else:
        # Convert the fractional degrees into minutes (1 degree = 60 minutes)
        minutes = fractional_degrees * 60
        fractional_minutes = minutes % 1
        # Check if the minutes value is close to an integer (within a small tolerance)
        if fractional_minutes < tolerance_min or fractional_minutes > tolerance_max:
            return 2
        else:
            # Check if the minutes value is close to a multiple of 0.5 (within a small tolerance)
            fractional_half_minutes = (minutes * 2) % 1
            if (
                fractional_half_minutes < tolerance_min
                or fractional_half_minutes > tolerance_max
            ):
                return 3
    return 0


@run_update
def save_to_featurelayer(process_list):
    global num_failed_records, num_succeeded_records, num_total_records
    num_total_records = num_succeeded_records = num_failed_records = 0
    logger.info("\n\tSaving to feature layer\n")
    # logger.info("\tFeatures info to update: {}".format(process_list))
    return process_list


def flag_cornered(taskItem, taskLyr, task):
    global field_to_calc_x, field_to_calc_y, field_to_calc_corner, field_to_calc_landforms
    logger.info(
        "\n ------- Flagging points snapped to the corners of USGS 3.5' topographic quadrangle maps ------- \n"
    )
    latitide_field = task["latitide_field"]
    longitude_field = task["longitude_field"]

    where = None
    if "where" in task:
        where = task["where"]

    if where is None or where == "":
        where = "1=1"

    oid_field = taskLyr.properties.objectIdField

    # Query the layer to get all values of the latitide_field and longitude_field
    query_result = taskLyr.query(
        where=where,
        out_fields="{},{},{}".format(oid_field, latitide_field, longitude_field),
        return_geometry=False,
        return_all_records=True,
    )

    list_to_update = []
    num_flagged = 0
    # for each feature in the query result, check if the latitide_field and longitude_field are rounded to the nearest integer, the nearest minute, or the nearest 30 second
    for f in query_result.features:
        new_attributes = {oid_field: f.attributes[oid_field]}
        lat = f.attributes[latitide_field]
        lon = f.attributes[longitude_field]

        if lat is None or lon is None:
            new_attributes[field_to_calc_corner] = -1
        else:
            new_attributes[field_to_calc_corner] = (
                1
                if (check_cornered(lat, False) == 1 and check_cornered(lon, True))
                else 0
            )

        if new_attributes[field_to_calc_corner] != 0:
            num_flagged += 1

        list_to_update.append({"attributes": new_attributes})

    logger.info("\tNumber of flagged points: {}".format(num_flagged))

    if num_flagged == 0:
        calc_field_response = taskLyr.calculate(
            where=where,
            calc_expression={"field": field_to_calc_corner, "sqlExpression": 0},
        )
        logger.info("Calculate field response: {}".format(calc_field_response))
    else:
        save_to_featurelayer(
            list_to_update,
            update=taskLyr,
            track=None,
            item=taskItem,
            operation="update",
            use_global_ids=False,
        )


def flag_landforms(taskItem, taskLyr, task):
    global field_to_calc_x, field_to_calc_y, field_to_calc_corner, field_to_calc_landforms
    logger.info("\n ------- Flagging points with landforms ------- \n")
    field_to_calc_landforms = "Flag_Landforms"
    where = task["where"]
    oid_field = taskLyr.properties.objectIdField

    # read the taskLayer into a spatial dataframe
    sdf = taskLyr.query(where=where, out_fields=[oid_field]).sdf

    # check if the in memory points feature already exists, it it does, delete it
    if arcpy.Exists("memory/points"):
        arcpy.Delete_management("memory/points")

    # conver the sdf to a feature class in memory
    sdf.spatial.to_featureclass(location="memory/points")

    in_rasters = task["rules_to_run"]["flag_landforms"]["in_rasters"]
    # loop through the in_rasters, and build the string to pass to the ExtractMultiValuesToPoints tool.
    # The string includes the in rasters and an out field name for each raster
    raster_string = ""
    for i, raster in enumerate(in_rasters):
        if i > 0:
            raster_string += ";"
        raster_string += "{} {} ".format(raster, "tmp_landform_{}".format(i + 1))

    arcpy.sa.ExtractMultiValuesToPoints(
        in_point_features="memory/points",
        in_rasters=raster_string,
        bilinear_interpolate_values="NONE",
    )

    # get the object id field name from the memory/points feature class
    oid_field_fc = arcpy.ListFields("memory/points", "*", "OID")[0].name

    # convert the points feature class back to a spatial dataframe
    sdf_fc = pd.DataFrame.spatial.from_featureclass("memory/points")
    # print(sdf_fc)

    # Loop through rows in the sdf.
    # For each row, set field_to_calc_landforms the value of any of the out fields that are not null
    list_to_update = []
    num_flagged = 0
    for index, row in sdf_fc.iterrows():
        # get the oid_field value from the row
        oid_value = row[oid_field_fc]
        new_attributes = {oid_field: oid_value}
        landform = 0
        for i, raster in enumerate(in_rasters):
            v = row["tmp_landform_{}".format(i + 1)]
            if pd.notna(v) and v is not None:
                landform = v
                break

        new_attributes[field_to_calc_landforms] = landform
        if new_attributes[field_to_calc_landforms] > 0:
            num_flagged += 1

        list_to_update.append({"attributes": new_attributes})

    if arcpy.Exists("memory/points"):
        arcpy.Delete_management("memory/points")

    logger.info("\tNumber of flagged points: {}".format(num_flagged))

    if num_flagged == 0:
        calc_field_response = taskLyr.calculate(
            where=where,
            calc_expression={"field": field_to_calc_landforms, "sqlExpression": 0},
        )
        logger.info("Calculate field response: {}".format(calc_field_response))
    else:
        save_to_featurelayer(
            list_to_update,
            update=taskLyr,
            track=None,
            item=taskItem,
            operation="update",
            use_global_ids=False,
        )


def check_cornered(v, isLongitude):
    orig_long_in_minutes = -6693.75
    orig_lat_in_minutes = 2681.25
    cell_size = 3.75
    tolerance_min = 0.0000001
    tolerance_max = cell_size - tolerance_min

    fractional_minutes = None
    if isLongitude:
        fractional_minutes = abs(v * 60 - orig_long_in_minutes) % cell_size
    else:
        fractional_minutes = abs(v * 60 - orig_lat_in_minutes) % cell_size

    if fractional_minutes < tolerance_min or fractional_minutes > tolerance_max:
        return 1
    else:
        return 0


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


def updateItemProperties(result_item, task, tags_to_inject, gis, completedOn):
    # check if you have the right to update the item
    try:
        me = gis.users.me
        userFullName = me.fullName
        newDescription = "<B>Input points flagged by: {} on {} using the following parameters:</b>  \n<br>".format(
            userFullName, completedOn
        )
        newDescription += "<ul>"
        newDescription += "<li>Latitide field: {} and longitude field: {}</li>".format(
            task["latitide_field"], task["longitude_field"]
        )
        newDescription += "<li>Where filter: {}</li>".format(task["where"])

        if not task["rules_to_run"]["flag_rounded"]["skip"]:
            newDescription += "<li>Flag points with longitude and latitude rounded to the nearest degree, minute, and half minute: Yes </li>"

        if not task["rules_to_run"]["flag_corners"]["skip"]:
            newDescription += "<li>Flag points snapped to the corners of USGS 3.75' topographic quadrangle maps: Yes </li>"

        if not task["rules_to_run"]["flag_landforms"]["skip"]:
            newDescription += "<li>Flag points with landforms: Yes </li>"
            newDescription += "<li>Landform rasters: {} </li>".format(
                task["rules_to_run"]["flag_landforms"]["in_rasters"]
            )

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
    parameters = get_config(
        os.path.join(this_dir, "./config/config_flag_sample_points.json")
    )
    # Get Logger & Log Directory
    log_folder = parameters["log_folder"]
    logger, log_dir, log_file_name = get_logger(log_folder, this_filename, start_time)
    print_envs()
    gis = connect_to_portal(parameters)

    try:

        tasks = parameters["tasks"]
        tags_to_inject = parameters["tags_to_inject"]
        for task in tasks:
            logger.info("\n\nStarting task: {}\n".format(task["name"]))
            bSkip = task.get("skip", False)
            if bSkip:
                logger.info("Task is skipped")
                continue

            taskItemId = task["itemId"]
            lyrId = task["lyrId"]

            taskItem = gis.content.get(taskItemId)
            # print out the title of the item
            logger.info("Task Item Title: {}".format(taskItem.title))
            taskLyr = taskItem.layers[lyrId]

            # Add the fields to the layer
            add_flag_fields(taskItem, taskLyr)
            rules_to_run = task["rules_to_run"]

            if not rules_to_run["flag_rounded"]["skip"]:
                flag_rounded(taskItem, taskLyr, task)

            if not rules_to_run["flag_corners"]["skip"]:
                flag_cornered(taskItem, taskLyr, task)

            if not rules_to_run["flag_landforms"]["skip"]:
                flag_landforms(taskItem, taskLyr, task)

            updateItemProperties(taskItem, task, tags_to_inject, gis, datetime.now())

    except Exception:
        logger.info(traceback.format_exc())

    finally:
        # Log Run Time
        logger.info(
            "\n\nProgram Run Time: {0} Minutes".format(
                round(((time.time() - start_time) / 60), 2)
            )
        )
