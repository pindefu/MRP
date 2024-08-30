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
            print("field: {} = Calc Expression: {}".format(new_field_name, calc_sql_expresison))
            calc_field_response = taskLyrTble.calculate(where="{} is null".format(new_field_name), calc_expression={"field": new_field_name, "sqlExpression" : calc_sql_expresison})

            print("Calc Field Response: {}".format(calc_field_response))


def calc_Clarke_value_percentiles_nthStdDev(taskLyrTble, taskItem, element_fields, reference_dict, fieldMapping):
    # If no fieldMapping is provided, use the keys in the reference_dict as the element_fields
    if fieldMapping is None or len(fieldMapping) == 0:
        # Build the fieldMapping from the reference_dict, with the key and values as the field name
        fieldMapping = {k: k for k in reference_dict.keys()}

    # filter element_fields to find out whose names are in the fieldMapping keys
    fieldMapping_keys = list(fieldMapping.keys())
    selected_element_fields = [element_field for element_field in element_fields if element_field in fieldMapping_keys]

    # ******************************************************* #
    # Test purpose only: Use the first field in the element_fields list
    # selected_element_fields = selected_element_fields[:1]
    # ******************************************************* #
    object_id_field = taskLyrTble.properties.objectIdField

    # query the layer to return the objectid field and corrsponding clarke fields of the selected_element_fields
    selected_clarke_fields = ["{}_Clarke".format(fieldMapping[element_field]) for element_field in selected_element_fields]
    # create a new list of fields to use as the out_fields parameter
    out_fields = [object_id_field] + selected_clarke_fields
    logger.info("Out Fields: {}".format(out_fields))
    query_result = taskLyrTble.query(where="1=1", out_fields=out_fields, return_all_records=True, return_geometry=False)

    for clarke_fld_name in selected_clarke_fields:
        pct_fld_name = "{}_Pct".format(clarke_fld_name)
        nDev_fld_name = "{}_nDev".format(clarke_fld_name)
        clarke_log_fld_name = "{}_Log".format(clarke_fld_name)
        logger.info("Processing fields: {}, {}, {}".format(clarke_fld_name, pct_fld_name, nDev_fld_name))

        # Calculate the percentiles first
        # sort the features by the clarke_fld_name
        query_result.features.sort(key=lambda x: x.attributes[clarke_fld_name])
        num_values = len(query_result.features)
        # loop through the features and calculate the percentile for each feature
        logger.info("Calculating Percentiles for each feature")
        i = 1
        for f in query_result.features:
            f.attributes[pct_fld_name] = round(i / num_values, 3)
            i += 1
            f.attributes[clarke_log_fld_name] = math.log(f.attributes[clarke_fld_name])

        # Calculate the mean and standard deviation of the clarke_log_fld_name
        logger.info("Calculating Mean and Standard Deviation for field: {}".format(clarke_log_fld_name))
        clarke_log_values = [f.attributes[clarke_log_fld_name] for f in query_result.features]
        mean_value = sum(clarke_log_values) / num_values
        sum_of_squares = sum([(x - mean_value) ** 2 for x in clarke_log_values])
        std_dev = (sum_of_squares / num_values) ** 0.5
        logger.info("Mean: {}, Standard Deviation: {}".format(mean_value, std_dev))
        logger.info("Calculating nth Standard Deviation for each feature")
        for f in query_result.features:
            val = f.attributes[clarke_log_fld_name]
            f.attributes[nDev_fld_name] = calc_nth_standard_dev(mean_value, std_dev, val)
            # remove the clarke_log_fld_name and the clarke_fld_name
            f.attributes.pop(clarke_log_fld_name)
            f.attributes.pop(clarke_fld_name)

    logger.info("Features info to update: {}".format(query_result.features))
    # Update the features
    logger.info("Updating the features")
    save_to_featurelayer(query_result.features,update=taskLyrTble, track=None, item=taskItem, operation="update", use_global_ids=False)

@run_update
def save_to_featurelayer(process_list):
    logger.info("\tFeatures info to update: {}".format(process_list))
    return process_list


# Write a function to calcualte the n, as determined by the input value is >= mean + n * std_dev and <= mean + (n+1) * std_dev
def calc_nth_standard_dev(mean_value, std_dev, value):
    if value > mean_value:
        return math.floor((value - mean_value) / std_dev)
    else:
        return 0 - math.floor((mean_value - value) / std_dev)


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
            print("\n\nStarting task: {}\n".format(task["name"]))
            bSkip = task.get("skip", False)
            if bSkip:
                print("Task is skipped")
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

            #calc_Clarke_values(taskLyrTble, element_fields, reference_dict, fieldMapping)
            calc_Clarke_value_percentiles_nthStdDev(taskLyrTble, taskItem, element_fields, reference_dict, fieldMapping)

    except Exception:
        print(traceback.format_exc())

    finally:
        # Log Run Time
        print('\n\nProgram Run Time: {0} Minutes'.format(round(((time.time() - start_time) / 60), 2)))
