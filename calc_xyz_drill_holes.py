from arcgis.gis import GIS
import math
import traceback
from datetime import datetime
import logging
import time
import json
import os
import arcpy
import sys
import arcgis
from arcgis.geometry import Point, project

logger = None
batch_size = 2500
num_failed_records = 0
num_succeeded_records = 0
num_total_records = 0


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


@run_update
def save_to_featurelayer(process_list):
    global num_failed_records, num_succeeded_records, num_total_records
    num_total_records = num_succeeded_records = num_failed_records = 0
    logger.info("\n\tSaving to feature layer\n")
    # logger.info("\tFeatures info to update: {}".format(process_list))
    return process_list


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


def updateItemProperties(result_item, rules_to_run, tags_to_inject, gis, completedOn):
    # check if you have the right to update the item
    try:
        me = gis.users.me
        userFullName = me.fullName
        newDescription = "<B>Watersheds flagged by: {} on {} using the following parameters:</b>  \n<br>".format(
            userFullName, completedOn
        )
        newDescription += "<ul>"

        if not rules_to_run["flag_size"]["skip"]:
            newDescription += "<li>Flag Size: Cut values are {} for too large and {} for too small </li>".format(
                rules_to_run["flag_size"]["cutoff_large"],
                rules_to_run["flag_size"]["cutoff_small"],
            )

        if not rules_to_run["flag_elongated"]["skip"]:
            newDescription += "<li>Flag Elongated: Cut values are {} for too long and {} for too round</li>".format(
                rules_to_run["flag_elongated"]["cutoff_large"],
                rules_to_run["flag_elongated"]["cutoff_small"],
            )

        if not rules_to_run["flag_multiparts"]["skip"]:
            newDescription += "<li>Flag Multipart: Resolution is {} meters</li>".format(
                rules_to_run["flag_multiparts"]["resolution_meters"]
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


def drill_hole_end_point(lat, lon, elev, depth, sections):
    """
    Calculate the latitude, longitude, and elevation of the end point of a drill hole.

    Parameters:
    lat (float): Latitude of the entry point (decimal degrees)
    lon (float): Longitude of the entry point (decimal degrees)
    elev (float): Elevation of the entry point (meters above sea level)
    depth (float): Target depth to compute position for (meters)
    sections (list): List of sections in format [[start_depth, dip, azimuth], ...]

    Returns:
    dict: {"latitude": new_lat, "longitude": new_lon, "elevation": new_elev}
    """
    rad = math.pi / 180  # Convert degrees to radians
    current_lat = lat
    current_lon = lon
    current_elev = elev
    remaining_depth = depth

    # Process each section in order until the target depth is reached
    for i in range(len(sections) - 1):
        section_start, dip, azimuth = sections[i]
        section_end = sections[i + 1][0]  # Next section start

        if section_start >= depth:
            break  # Stop processing if we've reached the target depth

        # Determine how much of this section to process
        section_depth = min(section_end, depth) - section_start

        if section_depth <= 0:
            continue  # Skip sections that are above the input depth

        # Convert angles to radians
        dip_rad = dip * rad

        # Compute vertical displacement correctly
        vertical_depth = section_depth * math.cos(dip_rad)  # Proper accumulation

        # Update elevation correctly
        current_elev += vertical_depth

        # Reduce remaining depth
        remaining_depth -= section_depth

        # Stop if we've reached the target depth
        if remaining_depth <= 0:
            break

    return {
        "x": current_lon,
        "y": current_lat,
        "z": current_elev,
    }


if __name__ == "__main__":

    # Get Start Time
    start_time = time.time()

    # Get Script Directory
    this_dir = os.path.split(os.path.realpath(__file__))[0]
    this_filename = os.path.split(os.path.realpath(__file__))[1]

    # Collect Configured Parameters
    parameters = get_config(
        os.path.join(this_dir, "./config/config_calc_xyz_drill_holes.json")
    )
    # Get Logger & Log Directory
    log_folder = parameters["log_folder"]
    logger, log_dir, log_file_name = get_logger(log_folder, this_filename, start_time)

    print_envs()
    gis = connect_to_portal(parameters)

    try:
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

            # read the unique hole ids from the taskLyr
            resp_holeIDs = taskLyr.query(
                where="Calculated is null or Calculated = 0",
                out_fields="holeID",
                return_geometry=False,
                return_distinct_values=True,
            )

            hole_ids = [h.attributes["holeID"] for h in resp_holeIDs.features]
            logger.info("Unique Hole IDs: {}".format(hole_ids))

            if len(hole_ids) == 0:
                logger.info("No hole IDs to process")
                continue

            surveyItemId = task["survey_info"]["itemId"]
            surveyItem = gis.content.get(surveyItemId)
            surveyTable = surveyItem.tables[task["survey_info"]["tableId"]]

            # read the survey table, order by holeID and LENGTH ascending
            sWhere = "holeID in ('{}')".format("','".join(hole_ids))
            resp_survey = surveyTable.query(
                where=sWhere,
                out_fields="holeID, nLength, DIP, bearing, ELEVATION",
                return_geometry=False,
                order_by_fields="holeID ASC, LENGTH ASC",
            )
            logger.info("Survey Table Query Results: {}".format(resp_survey.features))

            # organize the survey table into a dict in the format of {holeID: {sections: [[LENGTH, DIP, bearing], [LENGTH, DIP, bearing] ...], top_z: ELEVATION}}
            survey_dict = {}
            for f in resp_survey.features:
                holeID = f.attributes["holeID"]
                length = f.attributes["nLength"]
                dip = f.attributes["DIP"]
                bearing = f.attributes["bearing"]
                elevation = f.attributes["ELEVATION"]
                if holeID not in survey_dict:
                    survey_dict[holeID] = {"sections": [], "top_z": elevation}
                survey_dict[holeID]["sections"].append([length, dip, bearing])

            logger.info("Survey Dict: {}".format(survey_dict))

            # Loop through the hole IDs and calculate the xyz coordinates
            list_to_update = []
            for hId in hole_ids:
                # query the task layer for the hole ID to get the object id, midpoint length, the latitude, and longitude
                sWhere = (
                    "(Calculated is null or Calculated = 0) and holeID = '{}'".format(
                        hId
                    )
                )
                resp_hole = taskLyr.query(
                    where=sWhere,
                    out_fields="ObjectId, midpoint_ft, LON_WGS84, LAT_WGS84",
                    order_by_fields="midpoint_ft ASC",
                    return_geometry=False,
                )

                if len(resp_hole.features) == 0:
                    logger.info("No features found for hole ID: {}".format(hId))
                    continue
                    # get the object id, midpoint length, latitude, and longitude

                # get the survey data for the hole ID
                sections = survey_dict[hId]["sections"]
                top_z = survey_dict[hId]["top_z"]

                for f in resp_hole.features:
                    oid = f.attributes["ObjectId"]
                    midpoint_ft = f.attributes["midpoint_ft"]
                    lon = f.attributes["LON_WGS84"]
                    lat = f.attributes["LAT_WGS84"]

                    previous_x = previous_y = previous_z = 0

                    if midpoint_ft == 0:
                        # if the midpoint length is 0, its elevation is the elevation of the first survey point
                        list_to_update.append(
                            {
                                "attributes": {
                                    "OBJECTID": oid,
                                    "Calculated": 1,
                                    "lon_caculated": lon,
                                    "lat_caculated": lat,
                                    "elev_calculated": top_z,
                                }
                            }
                        )
                        previous_x = lon
                        previous_y = lat
                        previous_z = top_z
                    else:
                        # calcualte the xyz coordinates
                        # entry_lat if previous_x is not None, else lat
                        entry_lat = lat if previous_y is not None else lat
                        entry_lon = lon if previous_x is not None else lon
                        entry_elev = previous_z if previous_z is not None else top_z

                        target_depth = midpoint_ft / 3.28084  # Convert feet to meters

                        xyz = drill_hole_end_point(
                            lat, lon, entry_elev, target_depth, sections
                        )
                        list_to_update.append(
                            {
                                "attributes": {
                                    "OBJECTID": oid,
                                    "Calculated": 1,
                                    "lon_caculated": xyz["x"],
                                    "lat_caculated": xyz["y"],
                                    "elev_calculated": xyz["z"],
                                }
                            }
                        )
                        previous_x = xyz["x"]
                        previous_y = xyz["y"]
                        previous_z = xyz["z"]

            logger.info("List to update: {}".format(list_to_update))

            # Loop through the list to build the point geometry with z for each record
            for i in range(len(list_to_update)):
                # project the lat and lon to the same spatial reference as the task layer
                logger.info(
                    "projecting {}".format(list_to_update[i]["attributes"]["OBJECTID"])
                )
                pnt_wgs84 = Point(
                    {
                        "x": list_to_update[i]["attributes"]["lon_caculated"],
                        "y": list_to_update[i]["attributes"]["lat_caculated"],
                        "spatialReference": {"wkid": 4326},
                    }
                )

                pnt_webmerc = project(
                    geometries=[pnt_wgs84], in_sr=4326, out_sr=102100
                )[0]
                pnt_webmerc.z = list_to_update[i]["attributes"]["elev_calculated"]
                pnt_webmerc.spatialReference = {"wkid": 102100}
                list_to_update[i]["geometry"] = pnt_webmerc

            # save the updates to the task layer with update
            save_to_featurelayer(
                list_to_update,
                operation="update",
                update=taskLyr,
                use_global_ids=False,
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
