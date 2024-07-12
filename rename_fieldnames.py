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


# For any fields names ending with _temp, delete the field
def delete_temp_fields(taskLyrTble):
    existing_fields = taskLyrTble.properties.fields
    fields_to_delete = []

    for field in existing_fields:
        if field["name"].lower().find("_temp") > 0:
            fields_to_delete.append(field["name"])

    logger.info("Fields to delete: {}".format(fields_to_delete))

    # delete the fields
    if len(fields_to_delete) > 0:
        fields_to_delete_json = [{"name": f} for f in fields_to_delete]
        delete_fields_response = taskLyrTble.manager.delete_from_definition({"fields": fields_to_delete_json})
        logger.info("Delete Fields Response: {}".format(delete_fields_response))


# For any string type field names that includes _ppm or _pct, add a new Double type field with the same name but with _temp suffix
# Calculate the new field value by converting the original field value to a double type
# Delete the original field
def change_string_fields_to_double(taskLyrTble):
    existing_fields = taskLyrTble.properties.fields
    fields_to_add = []
    fields_to_delete = []

    for field in existing_fields:
        if field["type"] == "esriFieldTypeString" and (field["name"].lower().find("_ppm") > 0 or field["name"].lower().find("_pct") > 0):
            new_field_name = "{}_temp".format(field["name"])
            fields_to_delete.append(field["name"])
            # check if the new_field_name already exists in the target layer or table, ignore case
            new_field_name_lower = new_field_name.lower()
            new_field_exists = False
            for field1 in existing_fields:
                if field1["name"].lower() == new_field_name_lower:
                    new_field_exists = True
                    break

            # if the new field does not exist, add it to the fields_to_add list
            if not new_field_exists:
                new_field = {
                    "name": new_field_name,
                    "type": "esriFieldTypeDouble",
                    "alias": "{} Temp".format(field["name"]),
                    "nullable": True,
                    "editable": True,
                    "visible": True
                }
                fields_to_add.append(new_field)

    logger.info("Fields to add: {}".format(fields_to_add))
    logger.info("Fields to delete: {}".format(fields_to_delete))

    # add the fields to the target layer or table
    if len(fields_to_add) > 0:
        add_fields_response = taskLyrTble.manager.add_to_definition({"fields": fields_to_add})
        logger.info("Add Fields Response: {}".format(add_fields_response))
    else:
        logger.info("No fields to add")

    # calculate the new field values
    # for field_name in fields_to_delete:
    #     new_field_name = "{}_temp".format(field_name)
    #     calc_sql_expresison = "CAST({} AS FLOAT)".format(field_name)
    #     logger.info("field: {} = Calc Expression: {}".format(new_field_name, calc_sql_expresison))
    #     calc_field_response = taskLyrTble.calculate(where="1=1", calc_expression={"field": new_field_name, "sqlExpression" : calc_sql_expresison})

    #     logger.info("Calc Field Response: {}".format(calc_field_response))


    # change the original field list to this json format:
    #     {
    #     "fields" : [
    #     {
    #     "name" : "POP90_SQMI"
    #     }
    # ]
    # }

    # delete the original fields
    if len(fields_to_delete) > 0:
        fields_to_delete_json = [{"name": f} for f in fields_to_delete]
        delete_fields_response = taskLyrTble.manager.delete_from_definition({"fields": fields_to_delete_json})
        logger.info("Delete Fields Response: {}".format(delete_fields_response))


# For any fields  with name ending _temp, add a new field with the same name but without the _temp suffix,
# and copy the values from the _temp field to the new field
# Delete the _temp field
def add_original_fieldnames(taskLyrTble):
    existing_fields = taskLyrTble.properties.fields
    fields_to_add = []
    fields_to_delete = []

    for field in existing_fields:
        if field["name"].lower().find("_temp") > 0:
            new_field_name = field["name"].replace("_temp", "")
            fields_to_delete.append(field["name"])
            # check if the new_field_name already exists in the target layer or table, ignore case
            new_field_name_lower = new_field_name.lower()
            new_field_exists = False
            for field1 in existing_fields:
                if field1["name"].lower() == new_field_name_lower:
                    new_field_exists = True
                    break

            # if the new field does not exist, add it to the fields_to_add list
            if not new_field_exists:
                new_field = {
                    "name": new_field_name,
                    "type": field["type"],
                    "alias": new_field_name,
                    "nullable": True,
                    "editable": True,
                    "visible": True
                }
                fields_to_add.append(new_field)

    logger.info("Fields to add: {}".format(fields_to_add))
    logger.info("Fields to delete: {}".format(fields_to_delete))

    # add the fields to the target layer or table
    if len(fields_to_add) > 0:
        add_fields_response = taskLyrTble.manager.add_to_definition({"fields": fields_to_add})
        logger.info("Add Fields Response: {}".format(add_fields_response))

    # calculate the new field values
    for field_name in fields_to_delete:
        new_field_name = field_name.replace("_temp", "")
        calc_sql_expresison = "{}".format(field_name)
        logger.info("field: {} = Calc Expression: {}".format(new_field_name, calc_sql_expresison))
        calc_field_response = taskLyrTble.calculate(where="1=1", calc_expression={"field": new_field_name, "sqlExpression" : calc_sql_expresison})

        logger.info("Calc Field Response: {}".format(calc_field_response))


# For any field with the alias of ObjectId2 Temp, change the alias to its field name
def change_field_alias(taskLyrTble):
    existing_fields = taskLyrTble.properties.fields
    fields_to_change_alias = []

    for field in existing_fields:
        if field["alias"] == "ObjectId2 Temp":
            fields_to_change_alias.append(field["name"])

    logger.info("Fields to change alias: {}".format(fields_to_change_alias))

    # change the alias of the fields
    if len(fields_to_change_alias) > 0:
        fields_to_change_alias_json = [{"name": f, "alias": f} for f in fields_to_change_alias]
        change_alias_response = taskLyrTble.manager.update_definition({"fields": fields_to_change_alias_json})
        logger.info("Change Alias Response: {}".format(change_alias_response))


def calc_Clarke_values(taskLyrTble, element_fields, reference_dict):
    # get the fields of the target layer or table
    existing_fields = taskLyrTble.properties.fields

    # For each field in element_fields list, check if it exists in the target layer or table, ignore case
    # If it does not exist, print out the missing field
    # If it does exist, create a field definition with the name field_name_Clarke, and add the field to a list of fields to add
    fields_to_add = []
    raw_Clarke_fields_lookup = {}

    # Test purpose only: Use the first field in the element_fields list
    # element_fields = element_fields[:1]

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

            delete_temp_fields(taskLyrTble)
            # add_original_fieldnames(taskLyrTble)
            # change_field_alias(taskLyrTble)
            # change_string_fields_to_double(taskLyrTble)
            # calc_Clarke_values(taskLyrTble, element_fields, reference_dict)

    except Exception:
        logger.info(traceback.format_exc())

    finally:
        # Log Run Time
        logger.info('\n\nProgram Run Time: {0} Minutes'.format(round(((time.time() - start_time) / 60), 2)))
