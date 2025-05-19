"""
Script documentation
"""

import arcpy
import pandas as pd
from arcgis.features import GeoAccessor
import os
import json


global x_field, y_field, z_field, length_field, dip_field, bearing_field, hole_id_field
# Required field names in the borehole line feature class
hole_id_field = "holeID"

# In the lab table
# hole_id_field = "holeID"
from_len_field = "from_m"  # from_m or from_ft
to_len_field = "to_m"  # to_m or to_ft


def read_lab_data_to_df(lab_table):
    # Read the lab data into a pandas DataFrame
    if lab_table.lower().endswith(".csv"):
        lab_df = pd.read_csv(lab_table)
    else:
        lab_df = GeoAccessor.from_table(lab_table, skip_nulls=True)

    # Check if the collar data is empty
    if lab_df.empty:
        arcpy.AddError("Lab data is empty.")
        return None

    # order the lab data by hole id and from length ascending
    lab_df = lab_df.sort_values([hole_id_field, from_len_field], ascending=[True, True])

    # add the fields to the lab_df: from_x, from_y, from_z, to_x, to_y, to_z
    lab_df["from_x"] = None
    lab_df["from_y"] = None
    lab_df["from_z"] = None
    lab_df["to_x"] = None
    lab_df["to_y"] = None
    lab_df["to_z"] = None

    return lab_df


def read_borehole_data_to_dict(borehole_lines_FC):
    borehole_sdf = GeoAccessor.from_featureclass(
        borehole_lines_FC, fields=[hole_id_field, "SHAPE@"]
    )
    # Check if the survey data is empty
    if borehole_sdf.empty:
        arcpy.AddError("Borehole line layer is empty.")
        return None

    borehole_dict = {}
    for index, row in borehole_sdf.iterrows():
        hole_id = row[hole_id_field]
        # organize the borehole data into a dict in the format of
        # {holeID: {segments: [[x, y, z, m], [x, y, z, m] ...],
        # get the polyline geometry
        if not row["SHAPE@"]:
            arcpy.AddWarning(f"Hole id {hole_id} has no geometry.")
            continue
        borehole_shp_str = row["SHAPE@"].JSON
        borehole_shp_json = json.loads(borehole_shp_str)
        borehole_dict[hole_id] = {"segments": borehole_shp_json["paths"][0]}

    return borehole_dict


def check_Existence_and_Fields(table, required_fields, tbl_Title):
    # Check if the input table is valid
    if not arcpy.Exists(table):
        arcpy.AddError(f"Input {tbl_Title} {table} does not exist.")
        return False

    # Check if the table contains the required fields
    flds = arcpy.ListFields(table)
    field_names = [fld.name for fld in flds]
    missing_fields = [field for field in required_fields if field not in field_names]
    if missing_fields:
        arcpy.AddError(f"Missing required fields : {', '.join(missing_fields)}")
        return False

    return True


def create_output_feature_class(out_Lab_segment_Lines_FC, spatial_reference):
    if arcpy.Exists(out_Lab_segment_Lines_FC):
        arcpy.Delete_management(out_Lab_segment_Lines_FC)

    # Create a feature class to store the borehole lines
    arcpy.management.CreateFeatureclass(
        os.path.dirname(out_Lab_segment_Lines_FC),
        os.path.basename(out_Lab_segment_Lines_FC),
        "POLYLINE",
        "",
        "ENABLED",
        "ENABLED",
        spatial_reference,
    )


def script_tool(
    borehole_lines_FC, lab_table, out_Lab_segment_Lines_FC, spatial_reference
):

    if not check_Existence_and_Fields(
        borehole_lines_FC,
        [hole_id_field],
        "Borehole Polyline Feature Class",
    ):
        return

    if not check_Existence_and_Fields(
        lab_table,
        [from_len_field, to_len_field, hole_id_field],
        "Lab Table",
    ):
        return

    create_output_feature_class(out_Lab_segment_Lines_FC, spatial_reference)

    # Read the Borehole feature class to a dictionary
    borehole_dict = read_borehole_data_to_dict(borehole_lines_FC)

    # Read the lab data into a pandas DataFrame
    lab_df = read_lab_data_to_df(lab_table)
    if lab_df is None or lab_df.empty:
        arcpy.AddError("Lab data is empty.")
        return

    calculate_lab_segments(borehole_dict, lab_df)

    save_lab_segments_to_fc(lab_df, out_Lab_segment_Lines_FC, hole_id_field)

    del borehole_dict
    del lab_df


def save_lab_segments_to_fc(lab_df, out_Lab_segment_Lines_FC, hole_id_field):
    # Add shape field to the lab_df
    lab_df["SHAPE@"] = None

    # loop through the columns in lab_df and add them to the feature class
    fields_in_df = lab_df.columns.tolist()

    # loop through fields_in_df and add them to the feature class
    arcpy.AddMessage("\nAdding fields to the feature class...")
    for fld in fields_in_df:
        field_name = fld
        dtype = lab_df[fld].dtype
        field_type = "TEXT"
        max_length = 255  # default

        if pd.api.types.is_integer_dtype(dtype):
            field_type = "LONG"
        elif pd.api.types.is_float_dtype(dtype):
            field_type = "DOUBLE"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            field_type = "DATE"
        elif pd.api.types.is_bool_dtype(dtype):
            field_type = "TEXT"
            max_length = 5
        else:
            field_type = "TEXT"
            if lab_df[fld].dtype == "object" or lab_df[fld].dtype == "string":
                try:
                    max_length = int(lab_df[fld].str.len().max() or 0) + 32
                except Exception:
                    max_length = 255

        arcpy.AddMessage(f"Adding field {field_name} of type {field_type}")
        # add the field to the feature class
        if fld != "SHAPE@":
            if field_type == "TEXT":
                arcpy.AddField_management(
                    out_Lab_segment_Lines_FC,
                    field_name,
                    field_type,
                    field_length=max_length,
                )
            else:
                arcpy.AddField_management(
                    out_Lab_segment_Lines_FC, field_name, field_type
                )

    # Saving the sdf directly to the feature class loses the Z geometry
    # so we need to insert the rows one by one to preserve the Z values
    ins_cursor = arcpy.da.InsertCursor(out_Lab_segment_Lines_FC, fields_in_df)
    arcpy.AddMessage("\nInserting rows into the feature class...")
    # loop through the lab_df and create the lines and insert them into the feature class
    previous_holeID = None
    for index, row in lab_df.iterrows():
        holeID = row[hole_id_field]
        # Check if the holeID is the same as the previous one
        if previous_holeID is None:
            previous_holeID = holeID
            arcpy.AddMessage(f"Inserting rows for hole {holeID}...")
        elif previous_holeID != holeID:
            arcpy.AddMessage(f"Inserting rows for hole {holeID}...")
            previous_holeID = holeID

        from_len = row[from_len_field]
        from_x = row["from_x"]
        from_y = row["from_y"]
        from_z = row["from_z"]
        to_len = row[to_len_field]
        to_x = row["to_x"]
        to_y = row["to_y"]
        to_z = row["to_z"]

        if (
            from_x is None
            or from_y is None
            or from_z is None
            or to_x is None
            or to_y is None
            or to_z is None
        ):
            arcpy.AddWarning(
                f"Skipping hole ID {holeID} length {from_len}-{to_len} due to missing coordinates: "
                f"from_x: {from_x}, from_y: {from_y}, from_z: {from_z}, "
                f"to_x: {to_x}, to_y: {to_y}, to_z: {to_z}"
            )
            pLine = None
        else:
            # create the polyline geometry
            pLine = arcpy.Polyline(
                arcpy.Array(
                    [
                        arcpy.Point(from_x, from_y, from_z, 0),
                        arcpy.Point(to_x, to_y, to_z, to_len - from_len),
                    ]
                ),
                spatial_reference,
                True,
                True,
            )

        # Convert row to list and set the SHAPE@ field to pLine if present
        row_list = list(row)
        if "SHAPE@" in fields_in_df:
            shape_idx = fields_in_df.index("SHAPE@")
            row_list[shape_idx] = pLine
        ins_cursor.insertRow(row_list)

    del ins_cursor

    arcpy.AddMessage(f"Lab segment lines created in {out_Lab_segment_Lines_FC}")
    arcpy.SetParameterAsText(2, out_Lab_segment_Lines_FC)


def interpolate_xyz_at_m(segments, m_len, hole_id):
    # Loop through the segments and find the segment that contains the measured length
    for i in range(len(segments) - 1):
        start_len = segments[i][3]
        end_len = segments[i + 1][3]

        # Check if the m_len is between the start_len and end_len
        if start_len <= m_len <= end_len:
            # Calculate the ratio of the m_len to the segment length
            ratio = (m_len - start_len) / (end_len - start_len)

            # Interpolate the x, y, z values using the ratio
            x = segments[i][0] + ratio * (segments[i + 1][0] - segments[i][0])
            y = segments[i][1] + ratio * (segments[i + 1][1] - segments[i][1])
            z = segments[i][2] + ratio * (segments[i + 1][2] - segments[i][2])

            return x, y, z

    # If the m_len is not found in any segment, extend a padding to accommodate arcpy length round-off
    padding = 0.0001
    if (
        segments is not None
        and end_len is not None
        and end_len <= m_len <= end_len + padding
    ):
        return segments[-1][0], segments[-1][1], segments[-1][2]

    arcpy.AddWarning(f"Length {m_len} not found in any segments of {hole_id}.")
    return None, None, None


def calculate_lab_segments(borehole_dict, lab_df):
    # Loop through the lab data and calculate the from_x, from_y, from_z, to_x, to_y, to_z for each lab segment
    arcpy.AddMessage("Calculating lab segment lines...")
    for index, row in lab_df.iterrows():
        hole_id = row[hole_id_field]
        from_len = row[from_len_field]
        to_len = row[to_len_field]

        # Check if the hole_id exists in the borehole_dict
        if hole_id not in borehole_dict:
            arcpy.AddWarning(f"Hole ID {hole_id} not found in borehole data.")
            continue
        else:
            segments = borehole_dict[hole_id]["segments"]

        from_x, from_y, from_z = interpolate_xyz_at_m(segments, from_len, hole_id)
        to_x, to_y, to_z = interpolate_xyz_at_m(segments, to_len, hole_id)

        # assign the values to the row
        lab_df.at[index, "from_x"] = from_x
        lab_df.at[index, "from_y"] = from_y
        lab_df.at[index, "from_z"] = from_z
        lab_df.at[index, "to_x"] = to_x
        lab_df.at[index, "to_y"] = to_y
        lab_df.at[index, "to_z"] = to_z


if __name__ == "__main__":

    inPro = False

    if inPro:
        borehole_lines_FC = arcpy.GetParameterAsText(0)
        lab_table = arcpy.GetParameterAsText(1)
        out_Lab_segment_Lines_FC = arcpy.GetParameterAsText(2)
    else:
        # Cumo Testing Data
        # borehole_lines_FC = r"C:/Dev/USGS_MRP/3D/test.gdb/Cumo_borehole_line"
        # lab_table = r"C:/Dev/USGS_MRP/3D/lab_cumo.csv"
        # out_Lab_segment_Lines_FC = r"C:/Dev/USGS_MRP/3D/test.gdb/Cumo_lab_segments_3D"

        # Pebble Testing Data
        borehole_lines_FC = r"C:/Dev/USGS_MRP/3D/test.gdb/Pebble_borehole_line"
        lab_table = r"C:/Dev/USGS_MRP/3D/NDM_2020_Pebble_lab.csv"
        out_Lab_segment_Lines_FC = r"C:/Dev/USGS_MRP/3D/test.gdb/Pebble_lab_segments_3D"

    # Set the spatial reference to be the same as the borehole lines feature class
    spatial_reference = arcpy.Describe(borehole_lines_FC).spatialReference

    script_tool(
        borehole_lines_FC, lab_table, out_Lab_segment_Lines_FC, spatial_reference
    )
