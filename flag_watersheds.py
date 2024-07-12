from arcgis.gis import GIS
from arcgis.features import FeatureLayerCollection
from arcgis.features.analysis import create_watersheds
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


def run_rules_to_flag(taskLyrTble, areaField, rules_to_run):

    


    for field_name in element_fields:
        field_name_lower = field_name.lower()
        field_exists = False
        element_field_name = None
        for field in existing_fields:
            if field["name"].lower() == field_name_lower:
                # check if the field is a numeric field
                if field["type"] != "esriFieldTypeDouble" and field["type"] != "esriFieldTypeInteger":
                    print("Field {} is not a numeric field".format(field_name))
                    break
                else:
                    field_exists = True
                    element_field_name = field["name"]
                    break

        if not field_exists:
            print("Field {} does not exist or is not numeric in the target layer or table".format(field_name))
            continue

        new_field_name = "{}_Clarke".format(element_field_name)
        # check if the new_field_name already exists in the target layer or table, ignore case
        new_field_name_lower = new_field_name.lower()
        new_field_exists = False
        for field in existing_fields:
            if field["name"].lower() == new_field_name_lower:
                new_field_exists = True
                raw_Clarke_fields_lookup[field_name] = field["name"]
                break

        # if the new field does not exist, add it to the fields_to_add list
        if not new_field_exists:
            new_field = {
                "name": new_field_name,
                "type": "esriFieldTypeDouble",
                "alias": "{} Clarke".format(element_field_name),
                "nullable": True,
                "editable": True,
                "visible": True
            }
            fields_to_add.append(new_field)
            raw_Clarke_fields_lookup[field_name] = new_field_name


    logger.info("Fields to add: {}".format(fields_to_add))
    logger.info("raw_Clarke_fields_lookup: {}".format(raw_Clarke_fields_lookup))

    # add the fields to the target layer or table
    if len(fields_to_add) > 0:
        add_fields_response = taskLyrTble.manager.add_to_definition({"fields": fields_to_add})
        logger.info("Add Fields Response: {}".format(add_fields_response))
    else:
        logger.info("No fields to add")

    # Calculate the Clarke values for each field in element_fields list: field_value / crustal_abundance
    for fld in raw_Clarke_fields_lookup:
        field_name = fld
        new_field_name = raw_Clarke_fields_lookup[field_name]
        logger.info("To calculate field: {}".format(new_field_name))

        crustal_abundance = reference_dict[field_name]
        if crustal_abundance is None or crustal_abundance == 0:
            logger.info("Skipping. The reference crustal abundance of null or 0")
            continue
        else:
            calc_sql_expresison = "{}/{}".format(field_name, crustal_abundance)
            logger.info("field: {} = Calc Expression: {}".format(new_field_name, calc_sql_expresison))
            calc_field_response = taskLyrTble.calculate(where="1=1", calc_expression={"field": new_field_name, "sqlExpression" : calc_sql_expresison})

            logger.info("Calc Field Response: {}".format(calc_field_response))

if __name__ == "__main__":

    # Get Start Time
    start_time = time.time()

    # Get Script Directory
    this_dir = os.path.split(os.path.realpath(__file__))[0]
    this_filename = os.path.split(os.path.realpath(__file__))[1]

    # Collect Configured Parameters
    parameters = get_config(os.path.join(this_dir, './config/flag_watersheds.json'))
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
            taskLyrTble = None
            if taskItemIsLayer:
                taskLyrTble = taskItem.layers[taskLyrTblId]
            else:
                taskLyrTble = taskItem.tables[taskLyrTblId]

            areaField = rules_to_run["areaField"]

            run_rules_to_flag(taskLyrTble, areaField, rules_to_run)

    except Exception:
        logger.info(traceback.format_exc())

    finally:
        # Log Run Time
        logger.info('\n\nProgram Run Time: {0} Minutes'.format(round(((time.time() - start_time) / 60), 2)))
