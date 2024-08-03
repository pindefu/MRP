from arcgis.gis import GIS
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


def calc_Clarke_values(taskLyrTble, element_fields, reference_dict, fieldMapping):
    # get the fields of the target layer or table
    existing_fields = taskLyrTble.properties.fields

    # For each field in element_fields list, check if it exists in the target layer or table, ignore case
    # If it does not exist, print out the missing field
    # If it does exist, create a field definition with the name field_name_Clarke, and add the field to a list of fields to add
    fields_to_add = []
    raw_Clarke_fields_lookup = {}
    org_element_fields_lookup = {}


    # Test purpose only: Use the first field in the element_fields list
    # element_fields = element_fields[:1]

    # filter element_fields to find out whose names are in the fieldMapping keys
    fieldMapping_keys = list(fieldMapping.keys())
    element_fields = [element_field for element_field in element_fields if element_field in fieldMapping_keys]

    for reference_field_name in element_fields:
        target_field_names = []
        # get the field name in the target layer or table
        # if fieldMapping is provided, use the field name in the fieldMapping
        element_field_name_in_target = None
        if fieldMapping is not None:
            element_field_name_in_target = fieldMapping[reference_field_name]
        else:
            reference_field_name_lower = reference_field_name.lower()
            for field in existing_fields:
                if field["name"].lower() == reference_field_name_lower:
                    element_field_name_in_target = field["name"]

        if element_field_name_in_target == None:
            print("Error: Field {} doesn't have corresponding field in the target layer or table".format(reference_field_name))
            continue
        else:
            for field in existing_fields:
                if field["name"] == element_field_name_in_target:
                    if field["type"] != "esriFieldTypeDouble" and field["type"] != "esriFieldTypeInteger":
                        print("Error: Field {} is not a numeric field. Skpped".format(field["name"]))
                    else:
                        print("Field {} is a numeric field".format(field["name"]))
                        target_field_names.append(field["name"])


        # Loop through target_field_names and add each field to the fields_to_add list
        for target_field_name in target_field_names:
            new_field_name = "{}_Clarke".format(target_field_name)
            # check if the new_field_name already exists in the target layer or table, ignore case
            new_field_name_lower = new_field_name.lower()
            new_field_exists = False
            for field in existing_fields:
                if field["name"].lower() == new_field_name_lower:
                    new_field_exists = True
                    raw_Clarke_fields_lookup[new_field_name] = reference_field_name
                    org_element_fields_lookup[new_field_name] = target_field_name
                    break

            # if the new field does not exist, add it to the fields_to_add list
            if not new_field_exists:
                new_field = {
                    "name": new_field_name,
                    "type": "esriFieldTypeDouble",
                    "alias": "{} Clarke".format(target_field_name),
                    "nullable": True,
                    "editable": True,
                    "visible": True
                }
                fields_to_add.append(new_field)
                raw_Clarke_fields_lookup[new_field_name] = reference_field_name
                org_element_fields_lookup[new_field_name] = target_field_name


    logger.info("Fields to add: {}".format(fields_to_add))
    logger.info("raw_Clarke_fields_lookup: {}".format(raw_Clarke_fields_lookup))

    # add the fields to the target layer or table
    if len(fields_to_add) > 0:
        add_fields_response = taskLyrTble.manager.add_to_definition({"fields": fields_to_add})
        logger.info("Add Fields Response: {}".format(add_fields_response))
    else:
        logger.info("No fields to add")

    # Calculate the Clarke values for each field in field mapping list: field_value / crustal_abundance

    for fld in fieldMapping:
        field_name = fieldMapping[fld]
        new_field_name = "{}_Clarke".format(field_name)
        logger.info("To calculate field: {}".format(new_field_name))

        crustal_abundance = reference_dict[fld]
        if crustal_abundance is None or crustal_abundance == 0:
            logger.info("Skipping. The reference crustal abundance of null or 0")
            continue
        else:
            calc_sql_expresison = "ROUND({}/{}, 3)".format(field_name, crustal_abundance)
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
    parameters = get_config(os.path.join(this_dir, './config/config_clarke_value_calc.json'))
    # Get Logger & Log Directory
    log_folder = parameters["log_folder"]
    logger, log_dir, log_file_name = get_logger(log_folder, this_filename, start_time)

    the_portal = parameters['the_portal']
    portal_url = the_portal['url']
    the_username = the_portal['user']
    the_password = the_portal['pass']
    gis = GIS(portal_url, the_username, the_password)


    try:
        # get the layers and tables
        referenceItemId = parameters['clarke_reference_config']["itemId"]
        referenceTableId = parameters['clarke_reference_config']["tableId"]

        referenceItemItem=gis.content.get(referenceItemId)
        referenceTable = referenceItemItem.tables[referenceTableId]

        # Query the reference table and get the reference values
        reference_resp = referenceTable.query(where = "1=1", out_fields=["field", "crustal_abundance"], return_all_records = True, return_geometry=False)

        # build the lookup from field to the crustal abundance
        reference_dict = {f.attributes["field"]: f.attributes["crustal_abundance"] for f in reference_resp.features}

        # build the list of fields
        element_fields = list(reference_dict.keys())

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

            fieldMapping = None
            if "fieldMapping" in task:
                fieldMapping = task["fieldMapping"]

            calc_Clarke_values(taskLyrTble, element_fields, reference_dict, fieldMapping)

    except Exception:
        logger.info(traceback.format_exc())

    finally:
        # Log Run Time
        logger.info('\n\nProgram Run Time: {0} Minutes'.format(round(((time.time() - start_time) / 60), 2)))
