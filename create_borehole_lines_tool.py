"""
Script documentation
"""

import arcpy
import pandas as pd
from arcgis.features import GeoAccessor
import os
import math


global x_field, y_field, z_field, length_field, dip_field, bearing_field, hole_id_field
# Required field names in the collar feature class
x_field = "Easting_m_UTM11_NAD83HARN1999"
y_field = "Northing_m_UTMNAD83HARN1999"
z_field = "Elevation_mNAVD88"
dip_field_in_collar = "DIP"
bearing_field_in_collar = "Bearing"
hole_id_field = "holeID"

# In the Survey table
length_field = "length_m"
dip_field = "DIP"
bearing_field = "bearing"
# hole_id_field = "holeID"

# In the lab table
from_len_field = "from_length_m"
to_len_field = "to_length_m"
# hole_id_field = "holeID"


def sin_degree(degree):
    result = math.sin(math.radians(degree))

    return result


def cos_degree(degree):
    result = math.cos(math.radians(degree))

    return result


def tan_degree(degree):
    result = math.tan(math.radians(degree))

    return result


def radius_curvature(md_1, incl_1, azm_1, md_2, incl_2, azm_2):
    """http://www.drillingformulas.com/radius-of-curvature-method"""

    delta_md = md_2 - md_1

    # Pluse 0.000001 to handle division of 0 error
    # https://www.netwasgroup.us/services-2/radius-of-curvature.html

    if incl_2 == incl_1 or azm_2 == azm_1:
        delta_ns = (
            delta_md
            * (cos_degree(incl_1) - cos_degree(incl_2))
            * (sin_degree(azm_2) - sin_degree(azm_1))
            * math.pow(180 / math.pi, 2)
            / ((incl_2 - incl_1) * (azm_2 - azm_1) + 0.000001)
        )
        delta_ew = (
            delta_md
            * (cos_degree(incl_1) - cos_degree(incl_2))
            * (cos_degree(azm_1) - cos_degree(azm_2))
            * math.pow(180 / math.pi, 2)
            / ((incl_2 - incl_1) * (azm_2 - azm_1) + 0.000001)
        )
        delta_tvd = (
            delta_md
            * (sin_degree(incl_2) - sin_degree(incl_1))
            * 180
            / ((incl_2 - incl_1) * math.pi + 0.000001)
        )
    else:
        delta_ns = (
            delta_md
            * (cos_degree(incl_1) - cos_degree(incl_2))
            * (sin_degree(azm_2) - sin_degree(azm_1))
            * math.pow(180 / math.pi, 2)
            / ((incl_2 - incl_1) * (azm_2 - azm_1))
        )
        delta_ew = (
            delta_md
            * (cos_degree(incl_1) - cos_degree(incl_2))
            * (cos_degree(azm_1) - cos_degree(azm_2))
            * math.pow(180 / math.pi, 2)
            / ((incl_2 - incl_1) * (azm_2 - azm_1))
        )
        delta_tvd = (
            delta_md
            * (sin_degree(incl_2) - sin_degree(incl_1))
            * 180
            / ((incl_2 - incl_1) * math.pi)
        )

    return [md_2, delta_tvd, delta_ew, delta_ns]


def average_angle(md_1, incl_1, azm_1, md_2, incl_2, azm_2):
    """http://www.drillingformulas.com/angle-averaging-method-in-directional-drilling-calculation"""

    delta_md = md_2 - md_1
    delta_ns = (
        delta_md * sin_degree((incl_1 + incl_2) / 2) * cos_degree((azm_1 + azm_2) / 2)
    )
    delta_ew = (
        delta_md * sin_degree((incl_1 + incl_2) / 2) * sin_degree((azm_1 + azm_2) / 2)
    )
    delta_tvd = delta_md * cos_degree((incl_1 + incl_2) / 2)

    return [md_2, delta_tvd, delta_ew, delta_ns]


def minimum_curvature(md_1, incl_1, azm_1, md_2, incl_2, azm_2):
    """http://www.drillingformulas.com/minimum-curvature-method"""

    beta = math.acos(
        cos_degree(incl_2 - incl_1)
        - sin_degree(incl_1) * sin_degree(incl_2) * (1 - cos_degree(azm_2 - azm_1))
    )

    # Set rf = 1 when beta == 0 to handle division by zero error
    # https://directionaldrillingart.blogspot.com/2015/09/directional-surveying-calculations.html

    if beta == 0:
        rf = 1
    else:
        rf = (2 / beta) * math.tan(beta / 2)

    delta_md = md_2 - md_1
    delta_ew = (
        (delta_md / 2)
        * (
            sin_degree(incl_1) * sin_degree(azm_1)
            + sin_degree(incl_2) * sin_degree(azm_2)
        )
        * rf
    )
    delta_ns = (
        (delta_md / 2)
        * (
            sin_degree(incl_1) * cos_degree(azm_1)
            + sin_degree(incl_2) * cos_degree(azm_2)
        )
        * rf
    )
    delta_tvd = (delta_md / 2) * (cos_degree(incl_1) + cos_degree(incl_2)) * rf

    return [md_2, delta_tvd, delta_ew, delta_ns]


def read_collar_data_to_sdf(collar_FC):
    # Read the collar feature class into a pandas DataFrame
    collar_df = GeoAccessor.from_featureclass(collar_FC)
    collar_df = collar_df[
        [
            x_field,
            y_field,
            z_field,
            hole_id_field,
            dip_field_in_collar,
            bearing_field_in_collar,
        ]
    ]
    # Check if the collar data is empty
    if collar_df.empty:
        arcpy.AddError("Collar data is empty.")
        return None

    return collar_df


def read_survey_data_to_dict(survey_Table, collar_sdf):
    # organize the survey table into a dict in the format of
    # {holeID: {sections: [[LENGTH, DIP, bearing], [LENGTH, DIP, bearing] ...],
    # top_x: xxxx, top_y: yyyyyy, top_z: zzzzzzz}}
    survey_df = GeoAccessor.from_table(survey_Table)
    # Check if the survey data is empty
    if survey_df.empty:
        arcpy.AddError("Survey data is empty.")
        return None

    # order the survey data by hole id and length ascending
    survey_df = survey_df.sort_values(
        [hole_id_field, length_field], ascending=[True, True]
    )

    survey_dict = {}
    for index, row in survey_df.iterrows():
        hole_id = row[hole_id_field]
        if hole_id not in survey_dict:
            # find the collar elevation for this hole
            collar_row = collar_sdf[collar_sdf[hole_id_field] == hole_id]
            if collar_row.empty:
                arcpy.AddWarning(f"Collar data not found for hole {hole_id}.")
                continue
            # add the collar elevation to the survey_dict
            survey_dict[hole_id] = {
                "sections": [],
                "top_x": collar_row[x_field].values[0],
                "top_y": collar_row[y_field].values[0],
                "top_z": collar_row[z_field].values[0],
                "top_dip": collar_row[dip_field_in_collar].values[0],
                "top_bearing": collar_row[bearing_field_in_collar].values[0],
            }

            # if the length is not 0, add a section with length 0, dip and bearing from the top values
            if row[length_field] != 0:
                survey_dict[hole_id]["sections"].append(
                    [
                        0,
                        survey_dict[hole_id]["top_dip"],
                        survey_dict[hole_id]["top_bearing"],
                    ]
                )
        # append the survey data to the survey_dict
        survey_dict[hole_id]["sections"].append(
            [row[length_field], row[dip_field], row[bearing_field]]
        )
    return survey_dict


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


def extend_lengths(survey_dict, lab_Table):

    # read the maximum length from the lab table
    lab_df = pd.DataFrame.spatial.from_table(
        lab_Table, fields=[hole_id_field, to_len_field], skip_nulls=False
    )
    # check if the lab table is empty
    if lab_df.empty:
        arcpy.AddError("Lab data is empty.")
        return survey_dict

    for hole_id in survey_dict.keys():
        #  get the maximum length from the lab table
        #           and check if the maximum length is > the survey max length
        #           If so, add a new section to the survey_dict with the maximum length
        #           and the dip and bearing from the last section of the survey_dict

        lab_hole_df = lab_df[lab_df[hole_id_field] == hole_id]
        if lab_hole_df.empty:
            arcpy.AddWarning(f"Lab data not found for hole {hole_id}.")
            continue
        # get the maximum length from the lab table
        max_to_len = lab_hole_df[to_len_field].max()
        # check if the maximum length is greater than the survey max length
        if max_to_len > survey_dict[hole_id]["sections"][-1][0]:
            # display max_to_len and survey_dict[hole_id]["sections"][-1][0]
            arcpy.AddMessage(
                f"Max length from lab table: {max_to_len} > Survey max length: {survey_dict[hole_id]['sections'][-1][0]}. Extending length."
            )

            # add a new section to the survey_dict with the maximum length
            # and the dip and bearing from the last section of the survey_dict
            survey_dict[hole_id]["sections"].append(
                [
                    max_to_len,
                    survey_dict[hole_id]["sections"][-1][1],
                    survey_dict[hole_id]["sections"][-1][2],
                ]
            )


def script_tool(
    collar_FC, survey_Table, lab_Table, method, out_3D_Polyline_FC, spatial_reference
):

    if not check_Existence_and_Fields(
        collar_FC,
        [
            x_field,
            y_field,
            z_field,
            hole_id_field,
            dip_field_in_collar,
            bearing_field_in_collar,
        ],
        "Collar Feature Class",
    ):
        return
    if not check_Existence_and_Fields(
        survey_Table,
        [length_field, dip_field, bearing_field, hole_id_field],
        "Survey Table",
    ):
        return

    if not check_Existence_and_Fields(
        lab_Table, [from_len_field, to_len_field, hole_id_field], "Lab Table"
    ):
        return

    # Read the collar data into a pandas DataFrame
    collar_sdf = read_collar_data_to_sdf(collar_FC)
    if collar_sdf.empty:
        arcpy.AddError("Collar data is empty.")
        return

    # Read the survey data into a dictionary
    survey_dict = read_survey_data_to_dict(survey_Table, collar_sdf)

    extend_lengths(survey_dict, lab_Table)

    boreRows = []
    for hole_id in survey_dict.keys():
        arcpy.AddMessage(f"Creating hole {hole_id}...")
        borehole_dict = survey_dict[hole_id]
        borehole_line = create_borehole_line(hole_id, borehole_dict, method)
        boreRows.append((hole_id, borehole_line))

    if arcpy.Exists(out_3D_Polyline_FC):
        arcpy.Delete_management(out_3D_Polyline_FC)

    # Create a feature class to store the borehole lines
    arcpy.management.CreateFeatureclass(
        os.path.dirname(out_3D_Polyline_FC),
        os.path.basename(out_3D_Polyline_FC),
        "POLYLINE",
        "",
        "ENABLED",
        "ENABLED",
        spatial_reference,
    )

    # Add the holeID field to the feature class
    arcpy.management.AddField(
        out_3D_Polyline_FC, hole_id_field, "TEXT", field_length=50
    )

    cursor = arcpy.da.InsertCursor(out_3D_Polyline_FC, [hole_id_field, "SHAPE@"])
    for boreRow in boreRows:
        cursor.insertRow(boreRow)
    del cursor
    arcpy.AddMessage(f"Borehole lines created in {out_3D_Polyline_FC}")
    arcpy.SetParameterAsText(4, out_3D_Polyline_FC)


def create_borehole_line(hole_id, borehole_dict, method):

    x_start = borehole_dict["top_x"]
    y_start = borehole_dict["top_y"]
    z_start = borehole_dict["top_z"]

    sections = borehole_dict["sections"]
    xyz_deltas = [[0, 0, 0, 0]]
    previous_value = [0, 0, 0, 0]

    for i in range(0, len(sections) - 1):
        md_1, dip_1, azm_1 = sections[i]
        md_2, dip_2, azm_2 = sections[i + 1]
        incl_1 = 90 - dip_1
        incl_2 = 90 - dip_2

        if method == "Minimum Curvature":
            new_value = minimum_curvature(md_1, incl_1, azm_1, md_2, incl_2, azm_2)
        elif method == "Average Angle":
            new_value = average_angle(md_1, incl_1, azm_1, md_2, incl_2, azm_2)
        elif method == "Radius of Curvature":
            new_value = radius_curvature(md_1, incl_1, azm_1, md_2, incl_2, azm_2)

        accumulated_value = [
            new_value[0],
            previous_value[1] + new_value[1],
            previous_value[2] + new_value[2],
            previous_value[3] + new_value[3],
        ]

        previous_value = list(accumulated_value)

        xyz_deltas.append(accumulated_value)

    pnt_array = arcpy.Array()

    for xyz_delta in xyz_deltas:
        md = xyz_delta[0]
        tvd = xyz_delta[1]
        ew = xyz_delta[2]
        ns = xyz_delta[3]

        pnt = arcpy.Point()
        pnt.X = x_start + ew  # ew for X and ns for Y
        pnt.Y = y_start + ns
        pnt.Z = z_start + tvd
        pnt.M = md
        pnt_array.add(pnt)

    # display the point array
    # loop through the point array and print the values
    for i in range(pnt_array.count):
        pnt = pnt_array.getObject(i)
        arcpy.AddMessage(f"Point {i}: X={pnt.X}, Y={pnt.Y}, Z={pnt.Z}, M={pnt.M}")

    if pnt_array.count < 2:
        arcpy.AddWarning(
            "Only {} points. Needing at least two points.".format(pnt_array.count)
        )
        return None

    # Create a polyline from the point array
    borehole_line = arcpy.Polyline(pnt_array, None, True)

    return borehole_line


if __name__ == "__main__":

    # collar_FC = arcpy.GetParameterAsText(0)
    # survey_Table = arcpy.GetParameterAsText(1)
    # lab_Table = arcpy.GetParameterAsText(2)
    # method = arcpy.GetParameterAsText(3)
    # out_3D_Polyline_FC = arcpy.GetParameterAsText(4)
    # spatial_reference = arcpy.GetParameterAsText(5)

    collar_FC = r"C:/Users/pind3135/OneDrive - Esri/Documents/ArcGIS/Projects/MRP3/MRP3.gdb/collar_cumo_points"
    survey_Table = r"C:/Users/pind3135/OneDrive - Esri/Documents/ArcGIS/Projects/MRP3/MRP3.gdb/survey_cumo"
    lab_Table = r"C:/Users/pind3135/OneDrive - Esri/Documents/ArcGIS/Projects/MRP3/MRP3.gdb/Cumo_Geochem_BV"
    method = "Radius of Curvature"
    out_3D_Polyline_FC = r"C:/Users/pind3135/OneDrive - Esri/Documents/ArcGIS/Projects/MRP3/MRP3.gdb/borehole_lines"
    spatial_reference = arcpy.SpatialReference(32611)

    # print the input parameters
    arcpy.AddMessage(f"Collar Feature Class: {collar_FC}")
    arcpy.AddMessage(f"Survey Table: {survey_Table}")
    arcpy.AddMessage(f"Lab Table: {lab_Table}")

    script_tool(
        collar_FC,
        survey_Table,
        lab_Table,
        method,
        out_3D_Polyline_FC,
        spatial_reference,
    )
    arcpy.SetParameterAsText(3, "Result")
