from arcgis.gis import GIS
from arcgis.features import FeatureLayerCollection
import traceback
from datetime import datetime
import logging
import time
import json
import os
import pandas as pd
import arcpy
import sys
import arcgis

logger = None
batch_size = 2500
num_failed_records = 0
num_succeeded_records = 0

integer_field_def = {
    "type": "esriFieldTypeInteger",
    "nullable": True,
    "editable": True,
    "visible": True,
    "defaultValue": None,
}

size_domain_def = {
    "domain": {
        "type": "codedValue",
        "codedValues": [
            {"name": "No", "code": 0},
            {"name": "Too Small", "code": 1},
            {"name": "Too Large", "code": 2},
        ],
    }
}

shape_domain_def = {
    "domain": {
        "type": "codedValue",
        "codedValues": [
            {"name": "No", "code": 0},
            {"name": "Too Round", "code": 1},
            {"name": "Too Long", "code": 2},
        ],
    }
}

multipart_domain_def = {
    "domain": {
        "type": "codedValue",
        "codedValues": [{"name": "No", "code": 0}, {"name": "Yes", "code": 1}],
    }
}

field_to_calc_size = "Flag_Size"
field_to_calc_shape = "Flag_Elongated"
field_to_calc_multipart = "Flag_Multipart"


def add_flag_fields(taskItem, taskLyr):
    global field_to_calc_size, field_to_calc_shape, field_to_calc_multipart
    logger.info("\n ------- Adding fields to the layer ------- \n")
    # Check if each of the fields already exist in the layer, if not, add them
    fields_to_add = []
    if not any(f["name"] == field_to_calc_size for f in taskLyr.properties.fields):
        field_to_calc_size_def = {
            "name": field_to_calc_size,
            "alias": field_to_calc_size.replace("_", " "),
        }

        # merge with the integer_field_def and rounded_domain_def, and create a new field definition
        field_to_calc_size_def = {
            **field_to_calc_size_def,
            **integer_field_def,
            **size_domain_def,
        }
        field_to_calc_size_def["domain"]["name"] = "{}_Domain".format(
            field_to_calc_size
        )
        fields_to_add.append(field_to_calc_size_def)

    if not any(f["name"] == field_to_calc_shape for f in taskLyr.properties.fields):
        field_to_calc_shape_def = {
            "name": field_to_calc_shape,
            "alias": field_to_calc_shape.replace("_", " "),
        }

        field_to_calc_shape_def = {
            **field_to_calc_shape_def,
            **integer_field_def,
            **shape_domain_def,
        }
        field_to_calc_shape_def["domain"]["name"] = "{}_Domain".format(
            field_to_calc_shape
        )
        fields_to_add.append(field_to_calc_shape_def)

    if not any(f["name"] == field_to_calc_multipart for f in taskLyr.properties.fields):
        field_to_calc_multipart_def = {
            "name": field_to_calc_multipart,
            "alias": field_to_calc_multipart.replace("_", " "),
        }
        field_to_calc_multipart_def = {
            **field_to_calc_multipart_def,
            **integer_field_def,
            **multipart_domain_def,
        }
        field_to_calc_multipart_def["domain"]["name"] = "{}_Domain".format(
            field_to_calc_multipart
        )
        fields_to_add.append(field_to_calc_multipart_def)

    if len(fields_to_add) > 0:
        field_names = [f["name"] for f in fields_to_add]
        logger.info("\tFields to add: {}".format(field_names))
        add_fields_response = taskLyr.manager.add_to_definition(
            {"fields": fields_to_add}
        )
        logger.info("\tAdd Fields Response: {}".format(add_fields_response))
    else:
        logger.info("\tAll fields already exist in the layer")


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


def flag_size(task, taskItem, taskLyr, rule_config):
    logger.info("\n ------- Flagging by size ------- \n")

    cutoff_large = rule_config["cutoff_large"]
    cutoff_small = rule_config["cutoff_small"]
    field_to_calc = field_to_calc_size

    sWhere = task.get("where", "1=1")
    fld_objectId, watershed_fc = read_featureLayer_to_featureClass(taskLyr, sWhere)

    if watershed_fc is None:
        return

    # use arcpy to calculate the geodesic areas
    logger.info("\tCalculating geodesic area for each feature")
    arcpy.management.CalculateGeometryAttributes(
        in_features=watershed_fc,
        geometry_property=[
            ["area_geodesic_sqkm", "AREA_GEODESIC"],
        ],
        area_unit="SQUARE_KILOMETERS",
    )

    cutoff_large_value, cutoff_small_value = calc_cutoff_values(
        watershed_fc, cutoff_large, cutoff_small, "area_geodesic_sqkm"
    )

    calc_expression = "2 if !area_geodesic_sqkm! > {} else 1 if !area_geodesic_sqkm! < {} else 0".format(
        cutoff_large_value, cutoff_small_value
    )

    list_to_update = calc_flag_values(
        watershed_fc, fld_objectId, field_to_calc, calc_expression
    )

    save_to_featurelayer(
        list_to_update,
        sWhere,
        field_to_calc,
        update=taskLyr,
        track=None,
        item=taskItem,
        operation="update",
        use_global_ids=False,
    )


def read_featureLayer_to_featureClass(taskLyr, sWhere, outFields=[]):
    logger.info("\tRead feature Layer to feature class")
    fld_objectId = taskLyr.properties.objectIdField
    outFields.append(fld_objectId)
    watershed_sdf = taskLyr.query(
        where=sWhere,
        out_fields=outFields,
        return_geometry=True,
        return_all_records=True,
    ).sdf

    if watershed_sdf.empty:
        logger.info("\tNo features found in the layer")
        return None, None
    else:
        logger.info("\tNumber of features found: {}".format(len(watershed_sdf)))

        # Save the spatial dataframe to a feature class in the in_memory workspace
        watershed_fc = "in_memory/watershed_fc"
        # delete the feature class if it already exists
        if arcpy.Exists(watershed_fc):
            arcpy.management.Delete(watershed_fc)

        watershed_sdf.spatial.to_featureclass(location=watershed_fc)
        return fld_objectId, watershed_fc


def calc_cutoff_values(fc, cutoff_large, cutoff_small, field_of_values):
    logger.info("\tCalculating cutoff values")
    # get the list of values, sort them, and get the cutoff value by percentiles
    list_values = [row[0] for row in arcpy.da.SearchCursor(fc, [field_of_values])]
    list_values.sort()
    num_values = len(list_values)
    # get the cutoff values
    cutoff_large_value = list_values[int(num_values * cutoff_large)]
    cutoff_small_value = list_values[int(num_values * cutoff_small)]

    logger.info("\t  Cutoff Upper Value: {}".format(cutoff_large_value))
    logger.info("\t  Cutoff Lower Value: {}".format(cutoff_small_value))

    return cutoff_large_value, cutoff_small_value


def calc_flag_values(
    fc,
    fld_objectId,
    field_to_calc,
    calc_expression,
    expression_type="PYTHON3",
):

    # Use arcpy calculate field to calculate the flag value based on the circularity ratio
    logger.info("\tCalculating flag values")
    arcpy.management.CalculateField(
        in_table=fc,
        field=field_to_calc,
        expression=calc_expression,
        expression_type=expression_type,
        field_type="SHORT",
    )

    # Read the features with flag value <> 0 to a dataframe, only keep the objectid and the flag field,
    out_sdf = pd.DataFrame.spatial.from_featureclass(
        fc,
        fields=[fld_objectId, field_to_calc],
        where_clause="{} <> 0".format(field_to_calc),
    )

    # Delete the feature class if it exists
    if arcpy.Exists(fc):
        arcpy.management.Delete(fc)

    # convert the spatial dataframe to a feature set
    fs = out_sdf.spatial.to_featureset()
    list_to_update = []
    # for each feature in the feature set, create a dictionary with the object and the flag value
    for f in fs.features:
        new_attributes = {fld_objectId: f.attributes[fld_objectId]}
        new_attributes[field_to_calc] = f.attributes[field_to_calc]
        list_to_update.append({"attributes": new_attributes})

    num_flagged = len(list_to_update)
    logger.info("\tNumber of flagged records: {}".format(num_flagged))

    return list_to_update


def flag_elongated(task, taskItem, taskLyr, rule_config):
    logger.info("\n ------- Flagging enlongated polygons ------- \n")

    cutoff_large = rule_config["cutoff_large"]
    cutoff_small = rule_config["cutoff_small"]
    field_to_calc = field_to_calc_shape
    sWhere = task.get("where", "1=1")
    fld_objectId, watershed_fc = read_featureLayer_to_featureClass(taskLyr, sWhere)

    if watershed_fc is None:
        return

    # use arcpy to calculate the length and area of the geometry in geodesic units
    logger.info("\tCalculating geodesic area and perimeter for each feature")
    arcpy.management.CalculateGeometryAttributes(
        in_features=watershed_fc,
        geometry_property=[
            ["length_geodesic_km", "PERIMETER_LENGTH_GEODESIC"],
            ["area_geodesic_sqkm", "AREA_GEODESIC"],
        ],
        length_unit="KILOMETERS",
        area_unit="SQUARE_KILOMETERS",
    )

    # Use arcpy calculate field to Calculate the circularity ratio, which is the area divided by the perimeter squared
    logger.info("\tCalculating circularity ratio for each feature")
    arcpy.management.CalculateField(
        in_table=watershed_fc,
        field="circularity_ratio",
        expression="!area_geodesic_sqkm! / (!length_geodesic_km!**2)",
        expression_type="PYTHON3",
        field_type="DOUBLE",
    )

    cutoff_large_value, cutoff_small_value = calc_cutoff_values(
        watershed_fc, cutoff_large, cutoff_small, "circularity_ratio"
    )

    calc_expression = "1 if !circularity_ratio! > {} else 2 if !circularity_ratio! < {} else 0".format(
        cutoff_large_value, cutoff_small_value
    )

    list_to_update = calc_flag_values(
        watershed_fc, fld_objectId, field_to_calc, calc_expression
    )

    save_to_featurelayer(
        list_to_update,
        sWhere,
        field_to_calc,
        update=taskLyr,
        track=None,
        item=taskItem,
        operation="update",
        use_global_ids=False,
    )


@run_update
def save_to_featurelayer(list_to_update, sWhere, field_to_calc):

    # Set the default values to 0
    logger.info("\tSetting the defaut values to 0")
    calc_field_response = taskLyr.calculate(
        where=sWhere,
        calc_expression=[{"field": field_to_calc, "sqlExpression": 0}],
    )
    logger.info("\t  Result: {}".format(calc_field_response))

    logger.info("\tUpdating the flagged records")
    return list_to_update


def flag_multiparts(task, taskItem, taskLyr, rule_config):
    logger.info("\n ------- Flagging multipart polygons ------- \n")

    field_to_calc = field_to_calc_multipart
    # field_resolution = rule_config["resolution_field"]
    resolution_meters = rule_config["resolution_meters"]
    sWhere = task.get("where", "1=1")

    logger.info("\tQuerying the layer to get the geometries")
    fld_objectId, watershed_fc = read_featureLayer_to_featureClass(taskLyr, sWhere)

    if watershed_fc is None:
        return

    list_to_update = []

    # use arcpy to calculate the number of parts in the geometry using Arcade
    logger.info("\tCalculating number of rings")
    arcpy.management.CalculateField(
        in_table=watershed_fc,
        field="num_parts",
        expression="Count(Geometry($feature).rings)",
        expression_type="ARCADE",
        field_type="SHORT",
    )

    # Query the feature class where num_parts > 1 to a new feature class in the in_memory workspace
    logger.info("\tQuerying polygons with more than one ring")
    sWhere_mpart = "num_parts > 1"
    multipart_fc = "in_memory/multipart_fc"
    if arcpy.Exists(multipart_fc):
        arcpy.management.Delete(multipart_fc)

    arcpy.analysis.Select(watershed_fc, multipart_fc, sWhere_mpart)

    # Count multipart polygons
    result = arcpy.GetCount_management(multipart_fc)
    count = int(result.getOutput(0))
    if count == 0:
        logger.info("\tNo multipart polygons found")
    else:
        logger.info("\tNumber of multi-ring polygons found: {}".format(count))
        logger.info(
            "\tBuffering them the half of the resolution to see if they are still multi-rings"
        )

        # arcpy.management.CalculateField(
        #     in_table=multipart_fc,
        #     field=field_resolution,
        #     expression="{} + ' Meters'".format(field_resolution),
        #     expression_type="PYTHON3",
        # )

        buffered_fc = "in_memory/buffered_fc"
        if arcpy.Exists(buffered_fc):
            arcpy.management.Delete(buffered_fc)

        arcpy.analysis.Buffer(
            in_features=multipart_fc,
            out_feature_class=buffered_fc,
            buffer_distance_or_field="{} Meters".format(resolution_meters / 2),
            dissolve_option="NONE",
            method="PLANAR",
        )

        calc_expression = "IIF(Count(Geometry($feature).rings) > 1, 1, 0)"
        list_to_update = calc_flag_values(
            buffered_fc, fld_objectId, field_to_calc, calc_expression, "ARCADE"
        )

    save_to_featurelayer(
        list_to_update,
        sWhere,
        field_to_calc,
        update=taskLyr,
        track=None,
        item=taskItem,
        operation="update",
        use_global_ids=False,
    )


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


if __name__ == "__main__":

    # Get Start Time
    start_time = time.time()

    # Get Script Directory
    this_dir = os.path.split(os.path.realpath(__file__))[0]
    this_filename = os.path.split(os.path.realpath(__file__))[1]

    # Collect Configured Parameters
    parameters = get_config(
        os.path.join(this_dir, "./config/config_flag_watersheds.json")
    )
    # Get Logger & Log Directory
    log_folder = parameters["log_folder"]
    logger, log_dir, log_file_name = get_logger(log_folder, this_filename, start_time)

    print_envs()
    gis = connect_to_portal(parameters)

    try:
        rules_to_run = parameters["rules_to_run"]

        tasks = parameters["tasks"]
        for task in tasks:
            logger.info("\n\nStarting task: {}\n".format(task["name"]))
            bSkip = task.get("skip", False)
            if bSkip:
                logger.info("Task is skipped")
                continue

            taskItemId = task["itemId"]
            taskLyrId = task["lyrId"]

            taskItem = gis.content.get(taskItemId)
            # print out the title of the item
            logger.info("Task Item Title: {}".format(taskItem.title))
            taskLyr = taskItem.layers[taskLyrId]

            # Add the fields to the layer
            add_flag_fields(taskItem, taskLyr)

            if not rules_to_run["flag_size"]["skip"]:
                flag_size(task, taskItem, taskLyr, rules_to_run["flag_size"])

            if not rules_to_run["flag_elongated"]["skip"]:
                flag_elongated(task, taskItem, taskLyr, rules_to_run["flag_elongated"])

            if not rules_to_run["flag_multiparts"]["skip"]:
                flag_multiparts(
                    task, taskItem, taskLyr, rules_to_run["flag_multiparts"]
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
