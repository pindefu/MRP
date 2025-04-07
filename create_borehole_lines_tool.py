"""
Script documentation

- Tool parameters are accessed using arcpy.GetParameter() or
                                     arcpy.GetParameterAsText()
- Update derived parameter values using arcpy.SetParameter() or
                                        arcpy.SetParameterAsText()
"""

import arcpy
import pandas as pd
from arcgis.features import GeoAccessor

global x_field, y_field, z_field, length_field, dip_field, bearing_field, hole_id_field
x_field = "Easting_m_UTM11_NAD83HARN1999"
y_field = "Northing_m_UTMNAD83HARN1999"
z_field = "Elevation_mNAVD88"
# In the Survey table
length_field = "length_m"
dip_field = "DIP"
bearing_field = "bearing"
hole_id_field = "holeID"

# In the lab table
from_len_field = "from_length_m"
to_len_field = "to_length_m"


def read_collar_data_to_sdf(Collar_FC):
    # Read the collar feature class into a pandas DataFrame
    collar_df = GeoAccessor.from_featureclass(Collar_FC)
    collar_df = collar_df[[x_field, y_field, z_field, hole_id_field]]
    # Check if the collar data is empty
    if collar_df.empty:
        arcpy.AddError("Collar data is empty.")
        return None

    return collar_df


def read_survey_data_to_dict(Survey_Table, collar_sdf):
    # organize the survey table into a dict in the format of
    # {holeID: {sections: [[LENGTH, DIP, bearing], [LENGTH, DIP, bearing] ...],
    # top_x: xxxx, top_y: yyyyyy, top_z: zzzzzzz}}
    survey_df = GeoAccessor.from_table(Survey_Table)
    # Check if the survey data is empty
    if survey_df.empty:
        arcpy.AddError("Survey data is empty.")
        return None

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
            }
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


def script_tool(Collar_FC, Survey_Table, Lab_Table):

    if not check_Existence_and_Fields(
        Collar_FC, [x_field, y_field, z_field, hole_id_field], "Collar Feature Class"
    ):
        return
    if not check_Existence_and_Fields(
        Survey_Table,
        [length_field, dip_field, bearing_field, hole_id_field],
        "Survey Table",
    ):
        return

    if not check_Existence_and_Fields(
        Lab_Table, [from_len_field, to_len_field, hole_id_field], "Lab Table"
    ):
        return

    # Read the collar data into a pandas DataFrame
    collar_sdf = read_collar_data_to_sdf(Collar_FC)
    if collar_sdf.empty:
        arcpy.AddError("Collar data is empty.")
        return

    # Read the survey data into a dictionary
    survey_dict = read_survey_data_to_dict(Survey_Table, collar_sdf)

    # read the unique hole ids from the lab table
    lab_df = pd.DataFrame.spatial.from_table(
        Lab_Table, fields=[hole_id_field], skip_nulls=False
    )

    lab_hole_ids = lab_df[hole_id_field].unique()

    create_boreholes(lab_hole_ids, Lab_Table, survey_dict)

    return


def create_boreholes(lab_hole_ids, Lab_Table, survey_dict):

    arcpy.AddMessage("Creating borehole lines...")

    # display the number of boreholes to be created
    arcpy.AddMessage(f"Number of boreholes to be created: {len(lab_hole_ids)}")

    return

    # Create a new feature class to store the borehole lines
    borehole_fc = arcpy.CreateFeatureclass_management(
        out_path=arcpy.env.workspace,
        out_name="Boreholes",
        geometry_type="POLYLINE",
        spatial_reference=arcpy.Describe(Lab_Table).spatialReference,
    )

    # Add fields to the new feature class
    arcpy.AddField_management(borehole_fc, hole_id_field, "TEXT")
    arcpy.AddField_management(borehole_fc, "Length_m", "DOUBLE")
    arcpy.AddField_management(borehole_fc, "Dip", "DOUBLE")
    arcpy.AddField_management(borehole_fc, "Bearing", "DOUBLE")

    # Create a cursor to insert features into the new feature class
    with arcpy.da.InsertCursor(borehole_fc, ["SHAPE@", hole_id_field]) as cursor:
        for hole_id in lab_hole_ids:
            if hole_id not in survey_dict:
                arcpy.AddWarning(f"Survey data not found for hole {hole_id}.")
                continue

            # Get the collar elevation for this hole
            collar_elevation = survey_dict[hole_id]["top_z"]

            # Create a polyline for each section of the borehole
            for section in survey_dict[hole_id]["sections"]:
                length_m = section[0]
                dip = section[1]
                bearing = section[2]

                # Calculate the end point of the borehole section
                end_x = collar_elevation + length_m * math.sin(math.radians(dip))
                end_y = collar_elevation + length_m * math.cos(math.radians(dip))

                # Create a polyline geometry object
                line = arcpy.Polyline(
                    arcpy.Array(
                        [
                            arcpy.Point(collar_elevation, collar_elevation),
                            arcpy.Point(end_x, end_y),
                        ]
                    )
                )

                # Insert the new feature into the feature class
                cursor.insertRow([line, hole_id])

    # Set the output parameter to the new feature class
    arcpy.SetParameterAsText(3, borehole_fc)


if __name__ == "__main__":

    # Collar_FC = arcpy.GetParameterAsText(0)
    # Survey_Table = arcpy.GetParameterAsText(1)
    # Lab_Table = arcpy.GetParameterAsText(2)

    Collar_FC = r"C:/Users/pind3135/OneDrive - Esri/Documents/ArcGIS/Projects/MRP3/MRP3.gdb/collar_cumo_points"
    Survey_Table = r"C:/Users/pind3135/OneDrive - Esri/Documents/ArcGIS/Projects/MRP3/MRP3.gdb/survey_cumo"
    Lab_Table = r"C:/Users/pind3135/OneDrive - Esri/Documents/ArcGIS/Projects/MRP3/MRP3.gdb/Cumo_Geochem_BV"

    # print the input parameters
    arcpy.AddMessage(f"Collar Feature Class: {Collar_FC}")
    arcpy.AddMessage(f"Survey Table: {Survey_Table}")
    arcpy.AddMessage(f"Lab Table: {Lab_Table}")

    script_tool(Collar_FC, Survey_Table, Lab_Table)
    arcpy.SetParameterAsText(3, "Result")
