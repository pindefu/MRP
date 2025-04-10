"""
Script documentation
"""

import arcpy
import pandas as pd
from arcgis.features import GeoAccessor
import os
import math
import json


global x_field, y_field, z_field, length_field, dip_field, bearing_field, hole_id_field
# Required field names in the borehole line feature class
hole_id_field = "holeID"

# In the lab table
# hole_id_field = "holeID"
GlobalID_field = "GlobalID"
from_len_field = "from_length_m"
to_len_field = "to_length_m"

# in the output feature class
segment_globalID_field = "segment_GlobalID"


def read_lab_data_to_df(lab_table):
    # Read the collar feature class into a pandas DataFrame
    lab_df = GeoAccessor.from_table(
        lab_table,
        fields=[
            hole_id_field,
            from_len_field,
            to_len_field,
            GlobalID_field,
        ],
    )
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

    # Add the holeID and GlobalID fields to the feature class
    arcpy.management.AddField(
        out_Lab_segment_Lines_FC, hole_id_field, "TEXT", field_length=50
    )

    arcpy.management.AddField(
        out_Lab_segment_Lines_FC, "segment_GlobalID", "TEXT", field_length=50
    )
    arcpy.management.AddField(out_Lab_segment_Lines_FC, "from_m", "DOUBLE")
    arcpy.management.AddField(out_Lab_segment_Lines_FC, "from_x", "DOUBLE")
    arcpy.management.AddField(out_Lab_segment_Lines_FC, "from_y", "DOUBLE")
    arcpy.management.AddField(out_Lab_segment_Lines_FC, "from_z", "DOUBLE")
    arcpy.management.AddField(out_Lab_segment_Lines_FC, "to_m", "DOUBLE")
    arcpy.management.AddField(out_Lab_segment_Lines_FC, "to_x", "DOUBLE")
    arcpy.management.AddField(out_Lab_segment_Lines_FC, "to_y", "DOUBLE")
    arcpy.management.AddField(out_Lab_segment_Lines_FC, "to_z", "DOUBLE")


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
        [from_len_field, to_len_field, hole_id_field, GlobalID_field],
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

    create_lab_segments(borehole_dict, lab_df)

    save_lab_segments_to_fc(
        lab_df, out_Lab_segment_Lines_FC, hole_id_field, GlobalID_field
    )


def save_lab_segments_to_fc(
    lab_df, out_Lab_segment_Lines_FC, hole_id_field, GlobalID_field
):

    ins_cursor = arcpy.da.InsertCursor(
        out_Lab_segment_Lines_FC,
        [
            hole_id_field,
            segment_globalID_field,
            "from_m",
            "from_x",
            "from_y",
            "from_z",
            "to_m",
            "to_x",
            "to_y",
            "to_z",
            "SHAPE@",
        ],
    )

    # loop through the lab_df and create the lines and insert them into the feature class
    for index, row in lab_df.iterrows():
        global_id = row[GlobalID_field]
        hole_id = row[hole_id_field]
        from_m = row[from_len_field]
        from_x = row["from_x"]
        from_y = row["from_y"]
        from_z = row["from_z"]
        to_m = row[to_len_field]
        to_x = row["to_x"]
        to_y = row["to_y"]
        to_z = row["to_z"]
        # create the polyline geometry
        pLine = arcpy.Polyline(
            arcpy.Array(
                [
                    arcpy.Point(from_x, from_y, from_z, 0),
                    arcpy.Point(to_x, to_y, to_z, to_m - from_m),
                ]
            ),
            spatial_reference,
            True,
            True,
        )
        # insert the row into the feature class
        ins_cursor.insertRow(
            (
                hole_id,
                global_id,
                from_m,
                from_x,
                from_y,
                from_z,
                to_m,
                to_x,
                to_y,
                to_z,
                pLine,
            )
        )
    del ins_cursor

    arcpy.AddMessage(f"Lab segment lines created in {out_Lab_segment_Lines_FC}")
    arcpy.SetParameterAsText(2, out_Lab_segment_Lines_FC)


def interpolate_xyz_at_m(segments, m_len):
    # Loop through the segments and find the segment that contains the m_len
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

    # If the m_len is not found in any segment, return None
    arcpy.AddWarning(
        f"m_len {m_len} not found in any segment. Returning None for x, y, z."
    )
    return None, None, None


def create_lab_segments(borehole_dict, lab_df):
    # Loop through the lab data and calculate the from_x, from_y, from_z, to_x, to_y, to_z for each lab segment
    for index, row in lab_df.iterrows():
        hole_id = row[hole_id_field]
        arcpy.AddMessage(f"Processing index {index} hole id {hole_id}...")
        # Check if the hole_id exists in the borehole_dict
        if hole_id not in borehole_dict:
            arcpy.AddWarning(f"Hole ID {hole_id} not found in borehole data.")
            continue
        else:
            segments = borehole_dict[hole_id]["segments"]

        from_len = row[from_len_field]
        to_len = row[to_len_field]

        from_x, from_y, from_z = interpolate_xyz_at_m(segments, from_len)
        to_x, to_y, to_z = interpolate_xyz_at_m(segments, to_len)

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
        borehole_lines_FC = r"C:/Users/pind3135/OneDrive - Esri/Documents/ArcGIS/Projects/MRP3/MRP3.gdb/collar_borehole_radius_curvature"
        lab_table = r"C:/Users/pind3135/OneDrive - Esri/Documents/ArcGIS/Projects/MRP3/MRP3.gdb/Cumo_Geochem_BV"
        out_Lab_segment_Lines_FC = r"C:/Users/pind3135/OneDrive - Esri/Documents/ArcGIS/Projects/MRP3/MRP3.gdb/lab_segments_3D"

    # Set the spatial reference to be the same as the borehole lines feature class
    spatial_reference = arcpy.Describe(borehole_lines_FC).spatialReference

    script_tool(
        borehole_lines_FC, lab_table, out_Lab_segment_Lines_FC, spatial_reference
    )
