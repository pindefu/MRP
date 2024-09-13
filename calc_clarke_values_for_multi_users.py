from arcgis.gis import GIS
import traceback
from datetime import datetime
import logging
import time
import json
import os
import math

logger = None
batch_size = 2000
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

    # Debug Handler for Console Checks - print(msg)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    logger.addHandler(console_handler)

    # Ensure Logs Directory Exists
    # l_dir = os.path.join(t_dir, "logs", logger_date)
    # if not os.path.exists(l_dir):
    #     os.makedirs(l_dir)

    # Log Handler for Reports - print(msg)
    l_file_name = "Log_{}_{}_{}.txt".format(t_filename, logger_date, logger_time)
    l_dir_file_path = os.path.join(l_dir, l_file_name)
    log_handler = logging.FileHandler(l_dir_file_path, "w")
    log_handler.setLevel(logging.INFO)
    logger.addHandler(log_handler)

    print("Script Started: {} - {}".format(logger_date, logger_time))

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

        else:
            logger.info("Returned List Was Empty. No Edits Performed.")

    return wrapper


def calc_Clarke_values(taskLyrTble, element_fields, reference_dict, fieldMapping):
    # get the fields of the target layer or table
    existing_fields = taskLyrTble.properties.fields

    # For each field in element_fields list, check if it exists in the target layer or table, ignore case
    # If it does not exist, print out the missing field
    # If it does exist, create a field definition with the name field_name_Clarke, and add the field to a list of fields to add
    fields_to_add = []
    raw_Clarke_fields_lookup = {}
    org_element_fields_lookup = {}

    # If no fieldMapping is provided, use the keys in the reference_dict as the element_fields
    if fieldMapping is None or len(fieldMapping) == 0:
        # Build the fieldMapping from the reference_dict, with the key and values as the field name
        fieldMapping = {k: k for k in reference_dict.keys()}

    # filter element_fields to find out whose names are in the fieldMapping keys
    fieldMapping_keys = list(fieldMapping.keys())
    element_fields = [element_field for element_field in element_fields if element_field in fieldMapping_keys]

    print("Checking if these fields exist: {}".format(element_fields))
    for reference_field_name in element_fields:
        target_field_names = []
        # get the field name in the target layer or table
        # if fieldMapping is provided, use the field name in the fieldMapping
        element_field_name_in_target = None
        if fieldMapping is not None:
            reference_field_name_lower = fieldMapping[reference_field_name].lower()
            for field in existing_fields:
                if field["name"].lower() == reference_field_name_lower:
                    element_field_name_in_target = field["name"]
        else:
            reference_field_name_lower = reference_field_name.lower()
            for field in existing_fields:
                if field["name"].lower() == reference_field_name_lower:
                    element_field_name_in_target = field["name"]

        if element_field_name_in_target == None:
            print("Error: Field {} doesn't have corresponding field in the target layer or table".format(reference_field_name))
            # remove the field from the fieldMapping
            fieldMapping.pop(reference_field_name)
            continue
        else:
            for field in existing_fields:
                if field["name"] == element_field_name_in_target:
                    if field["type"] != "esriFieldTypeDouble" and field["type"] != "esriFieldTypeInteger":
                        print("Error: Field {} is not a numeric field. Skpped".format(field["name"]))
                        fieldMapping.pop(reference_field_name)
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


    print("Fields to add: {}".format(fields_to_add))
    print("raw_Clarke_fields_lookup: {}".format(raw_Clarke_fields_lookup))

    # add the fields to the target layer or table
    if len(fields_to_add) > 0:
        add_fields_response = taskLyrTble.manager.add_to_definition({"fields": fields_to_add})
        print("Add Fields Response: {}".format(add_fields_response))
    else:
        print("No fields to add")

    # Calculate the Clarke values for each field in field mapping list: field_value / crustal_abundance
    print("Calculating Clarke values")
    for fld in fieldMapping:
        field_name = fieldMapping[fld]
        new_field_name = "{}_Clarke".format(field_name)
        print("To calculate field: {}".format(new_field_name))

        crustal_abundance = reference_dict[fld]
        if crustal_abundance is None or crustal_abundance == 0:
            print("Skipping. The reference crustal abundance of null or 0")
            continue
        else:
            calc_sql_expresison = "ROUND({}/{}, 3)".format(field_name, crustal_abundance)
            # print("field: {} = Calc Expression: {}".format(new_field_name, calc_sql_expresison))
            calc_field_response = taskLyrTble.calculate(where="{} is null".format(new_field_name), calc_expression={"field": new_field_name, "sqlExpression" : calc_sql_expresison})

            # print("Calc Field Response: {}".format(calc_field_response))

    return fieldMapping

@run_update
def save_to_featurelayer(process_list):
    logger.info("\tFeatures info to update: {}".format(process_list))
    return process_list


def calc_Clarke_value_Tiers(taskLyrTble, element_fields, reference_dict, fieldMapping):
# get the fields of the target layer or table
    existing_fields = taskLyrTble.properties.fields
    fields_to_add = []

    # If no fieldMapping is provided, use the keys in the reference_dict as the element_fields
    if fieldMapping is None or len(fieldMapping) == 0:
        # Build the fieldMapping from the reference_dict, with the key and values as the field name
        fieldMapping = {k: k for k in reference_dict.keys()}

    # filter element_fields to find out whose names are in the fieldMapping keys
    fieldMapping_keys = list(fieldMapping.keys())
    element_fields = [element_field for element_field in element_fields if element_field in fieldMapping_keys]

    new_field_names_to_add = []
    element_New_Field_Lookup = {}
    print("Checking if the ..._Clarke_Tier fields exist: {}".format(element_fields))
    for from_field_name in element_fields:
        mapped_field_name = fieldMapping[from_field_name]
        new_field_name = "{}_Clarke_Tier".format(mapped_field_name)
        # check if it matches any names of the existing_fields
        bExists = False
        for field in existing_fields:
            if field["name"].lower() == new_field_name.lower():
                bExists = True
                element_New_Field_Lookup[from_field_name] = field["name"]
                break

        if not bExists:
            new_field_names_to_add.append(new_field_name)
            element_New_Field_Lookup[from_field_name] = new_field_name

    # Add the fields to the target layer or table. The field type is integer
    if len(new_field_names_to_add) == 0:
        print("No ..._Clarke_Tier fields to add")
    else:
        print("Fields to add: {}".format(new_field_names_to_add))
        for new_field_name in new_field_names_to_add:
            new_field = {
                "name": new_field_name,
                "type": "esriFieldTypeInteger",
                "alias": new_field_name.replace("_", " "),
                "nullable": True,
                "editable": True,
                "visible": True,
                'domain': {
                    'type': 'codedValue',
                    'name': '{}_Domain'.format(new_field_name),
                    'codedValues': [
                        {
                            'name': 'L0 - Less than background',
                            'code': 0
                        },
                        {
                            'name': 'L1 - Background',
                            'code': 1
                        },
                        {
                            'name': 'L2 - 2-3x background',
                            'code': 2
                        },
                        {
                            'name': 'L3 - 4-7x background',
                            'code': 3
                        },
                        {
                            'name': 'L4 - 8-15x background',
                            'code': 4
                        },
                        {
                            'name': 'L5 - >15x background',
                            'code': 5
                        }
                    ]
                }
            }
            fields_to_add.append(new_field)

        add_fields_response = taskLyrTble.manager.add_to_definition({"fields": fields_to_add})
        print("Add Fields Response: {}".format(add_fields_response))

    # Calculate the Tier values for each new field in element_New_Field_Lookup
    print("To calculate  Tier values")
    for fld in element_New_Field_Lookup:
        new_field_name = element_New_Field_Lookup[fld]
        clarke_field_name = "{}_Clarke".format(fieldMapping[fld])
        print("To calculate field: {}".format(new_field_name))
        # Here is the logic to calculate the Tier value based on the _Clarke value
        # if the value is null, set the Tier value to null
        # if the value is less than 0.5, set the Tier value to 0
        # if the value is less than 1.5, set the Tier value to 1
        # if the value is less than 3.5, set the Tier value to 2
        # if the value is less than 7.5, set the Tier value to 3
        # if the value is less than 15.5, set the Tier value to 4
        # else, set the Tier value to 5
        # The Tier value is an integer field
        # Write the above logic in SQL expression
        calc_sql_expresison = "CASE WHEN {} IS NULL THEN 0 WHEN {} < 0.5 THEN 0 WHEN {} < 1.5 THEN 1 WHEN {} < 3.5 THEN 2 WHEN {} < 7.5 THEN 3 WHEN {} < 15.5 THEN 4 ELSE 5 END".format(clarke_field_name, clarke_field_name, clarke_field_name, clarke_field_name, clarke_field_name, clarke_field_name)

        # print("field: {} = Calc Expression: {}".format(new_field_name, calc_sql_expresison))
        calc_field_response = taskLyrTble.calculate(where="1=1", calc_expression={"field": new_field_name, "sqlExpression" : calc_sql_expresison})

        # print("Calc Field Response: {}".format(calc_field_response))

def delete_clarke_fields(taskLyrTble):
    existing_fields = taskLyrTble.properties.fields

    fields_to_delete = []

    for field in existing_fields:
        # find any field with Clarke in the name
        if "_Tier" in field["name"] or "_Clarke_nDev" in field["name"] or "_Clarke_Pct" in field["name"]:
            fields_to_delete.append({"name": field["name"]})

    if len(fields_to_delete) > 0:
        delete_fields_response = taskLyrTble.manager.delete_from_definition({"fields": fields_to_delete})
        print("Delete Fields Response: {}".format(delete_fields_response))

if __name__ == "__main__":

    # Get Start Time
    start_time = time.time()

    # Get Script Directory
    this_dir = os.path.split(os.path.realpath(__file__))[0]
    this_filename = os.path.split(os.path.realpath(__file__))[1]

    # Collect Configured Parameters
    parameters = get_config(os.path.join(this_dir, './config/config_clarke_value_multi_users.json'))
    # Get Logger & Log Directory
    log_folder = parameters["log_folder"]
    logger, log_dir, log_file_name = get_logger(log_folder, this_filename, start_time)

    the_portal = parameters['the_portal']
    portal_url = the_portal['url']
    the_username = the_portal['user']
    the_password = the_portal['pass']
    gis = GIS(portal_url, the_username, the_password)


    try:
        # Query the config table, and build the parameters for the calculation
        config_item = parameters['config_item']
        config_table_itemId = config_item['itemId']
        config_table_item = gis.content.get(config_table_itemId)
        isLayer = config_item['isLayer']
        LyrTblid = config_item['LyrTblid']
        config_lyr_tbl = None
        if isLayer:
            config_lyr_tbl = config_table_item.layers[LyrTblid]
        else:
            config_lyr_tbl = config_table_item.tables[LyrTblid]


        config_resp = config_lyr_tbl.query(where = "skip = null or skip = 'No' ", out_fields=["*"], return_all_records = True, return_geometry=False)

        if len(config_resp.features) == 0:
            print("No configuration to process")
            exit()
        else:
            parameters['tasks'] = []
            # loop through the features and build the parameters
            for f in config_resp.features:
                task = {}
                task["clarke_reference_config"] = {
                    "itemId": f.attributes["reference_item_id"],
                    "tableId": f.attributes["reference_table_id"]
                }
                task["name"] = f.attributes["name"]
                task["itemId"] = f.attributes["item_id"]
                task["isLayer"] = f.attributes["layer_or_table"] == "Layer"
                task["LyrTblid"] = f.attributes["layer_or_table_id"]
                task["fieldMapping"] = f.attributes["field_mapping"]
                task["output_option"] = f.attributes["output_option"]
                parameters['tasks'].append(task)

        if "tasks" not in parameters:
            print("No tasks to process")
            exit()
        tasks = parameters['tasks']
        for task in tasks:
            print("\n\nTo run the config: {}\n".format(task["name"]))

            # get the layers and tables
            referenceItemId = task['clarke_reference_config']["itemId"]
            referenceTableId = task['clarke_reference_config']["tableId"]

            referenceItemItem=gis.content.get(referenceItemId)
            referenceTable = referenceItemItem.tables[referenceTableId]

            # Query the reference table and get the reference values
            reference_resp = referenceTable.query(where = "1=1", out_fields=["field", "crustal_abundance"], return_all_records = True, return_geometry=False)

            # build the lookup from field to the crustal abundance
            reference_dict = {f.attributes["field"]: f.attributes["crustal_abundance"] for f in reference_resp.features}

            # build the list of fields
            element_fields = list(reference_dict.keys())

            taskItemId = task["itemId"]
            taskItemIsLayer = task["isLayer"]
            taskLyrTblId = task["LyrTblid"]

            taskItem=gis.content.get(taskItemId)
            taskLyrTble = None

            result_item = taskItem
            if task["output_option"] is not None and task["output_option"] == "Create new":
                print("Copy the input layer/table ...")

                cloned_items = gis.content.clone_items(items=[taskItem], search_existing_items=False)
                print("Cloned item id: {}".format(cloned_items[0].id))
                result_item = cloned_items[0]
                if taskItemIsLayer:
                    taskLyrTble = cloned_items[0].layers[taskLyrTblId]
                else:
                    taskLyrTble = cloned_items[0].tables[taskLyrTblId]

            fieldMapping = {}
            if "fieldMapping" in task:
                s_fieldMapping = task["fieldMapping"]
                if s_fieldMapping is not None and len(s_fieldMapping) > 0:
                    fieldMapping = json.loads(s_fieldMapping)

            #delete_clarke_fields(taskLyrTble)

            updated_fieldMapping = calc_Clarke_values(taskLyrTble, element_fields, reference_dict, fieldMapping)
            calc_Clarke_value_Tiers(taskLyrTble, element_fields, reference_dict, updated_fieldMapping)
            #calc_Clarke_value_percentiles_nthStdDev(taskLyrTble, taskItem, element_fields, reference_dict, fieldMapping)

            print("Calculation completed for the config: {}".format(task["name"]))
            print("Result item id: {}".format(result_item.id))
            result_item

    except Exception:
        print(traceback.format_exc())

    finally:
        # Log Run Time
        print('\n\nProgram Run Time: {0} Minutes'.format(round(((time.time() - start_time) / 60), 2)))
