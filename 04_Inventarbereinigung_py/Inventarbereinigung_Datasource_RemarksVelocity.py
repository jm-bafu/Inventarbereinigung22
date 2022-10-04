"""
jerome messmer, 04.10.2022
jerome.messmer@bafu.admin.ch

Using this script the national InSAR-inventory can be imported. A copy of this inventory is the created. Using externally
defined lists (csv) defined attribute values from the old inventory can be replaced by defined new ones. The cleaned
inventory is the saved with the correct field types in .gpkg format.
"""

# 1: Import packages, Import datasets, set variables, set directories and create result geopackage
# 1.1: import requested packages:
import geopandas as gpd
import pandas as pd
import os
import fiona
import time
import shutil
import regex as re
print("script started")


# 1.2: set the time (used in filenames):
today_time = time.strftime("%Y%m%d-%H-%M")  # todays date and time


# 1.3: adjust the Mitarbeiterkuerzel for correct path reference:
Mitarbeiterkuerzel = "MJ2022"  # change


# 1.4: set inventory directories
wd = os.path.join("O:\GIS\GEP\RLS\_Mitarbeitende", Mitarbeiterkuerzel, "Inventarbereinigung")  # working directory
inventory_initial = os.path.join(wd, "Inventar_bereinigt_BM2022", "gpkg", "db_InSARCH_V0_clean20012022.gpkg")  # path
# of the inventory before cleansing
inventory_result = os.path.join(wd, "04_Inventarbereinigung_py", "Inventarbereinigung_results", "gpkg",
    "Inventarbereinigung_result_" + today_time + ".gpkg")  # path to store the result after cleansing


# 1.5: copy the inventory (before cleansing) to the result location. All alterations will from here on be performed
# on the copied result-inventory:
shutil.copy(inventory_initial, inventory_result)


# 1.6: list all cantons (used for import of and looping through the inventory, which is grouped by canton:
kantone_list = ["AG", "AI", "AR", "BE", "BL", "BS", "FR", "GE", "GL", "GR", "JU", "LU", "NE", "NW", "OW", "SG", "SH",
                "SO", "SZ", "TG", "TI", "UR", "VD", "VS", "ZG", "ZH"]


# 1.7: Import the InSAR inventory to a dictionary, in which each key stores a canton.
print("RuntimeWarning: Sequential read of iterator was interrupted, will follow - CAN BE IGNORED")
kt_dict = {}
for kanton in kantone_list:
    kt_inventory = gpd.read_file(inventory_result, layer = kanton)
    key = kanton
    kt_dict[key] = kt_inventory


# 2: Cleansing of "simple to cleanse" attributes
# 2.1: Load the lookup tables containing the "old" values and their replacements
xls_dir = os.path.join(wd, "04_Inventarbereinigung_py", "Inventarbereinigung_spreadsheets", "other_fields")
xls = os.listdir(xls_dir)

Process = pd.read_excel(os.path.join(xls_dir, xls[xls.index("Process.xls")]), header = None)
Delimitation = pd.read_excel(os.path.join(xls_dir, xls[xls.index("Delimitation.xls")]), header = None)
Edition_year = pd.read_excel(os.path.join(xls_dir, xls[xls.index("Edition_year.xls")]), header = None)
First_cartographic_version = pd.read_excel(os.path.join(xls_dir, xls[xls.index("First_cartographic_version.xls")]), header = None)
Revisions_list = pd.read_excel(os.path.join(xls_dir, xls[xls.index("Revisions_list.xls")]), header = None)


# 2.2: Define the attribute fields to be changed and link them to the according value lookup-tables:
ChangeFieldsList = ["Process", "Delimitation", "Edition_year", "First_cartographic_version", "Revisions_list"]

FieldDict = {"Process": Process, "Delimitation": Delimitation, "Edition_year": Edition_year,
"First_cartographic_version": First_cartographic_version, "Revisions_list": Revisions_list}


# 2.3: replace the "old" values according to the imported lookup tables for all defined fields and all cantons.
# Alterations are directly made in the result-geopackage:
for key in kt_dict.keys():
    for field in ChangeFieldsList:
        changelist = FieldDict.get(field)
        old_values = list(changelist.iloc[:, 0])
        replacement = list(changelist.iloc[:, 1])
        df = kt_dict.get(key)
        index = df.columns.get_loc(field)
        df.iloc[:, index] = df.iloc[:, index].replace(old_values, replacement)


# 3: Cleansing of attribute Datasource and amendment of Datasource using values in Remarks_velocity
# 3.1 Load the table containing the "old" Datasource values and the according updated Datasource values and additions
# for Remarks velocity:
lookup_table_ds_rmv = pd.read_excel(os.path.join(wd, "04_Inventarbereinigung_py","Inventarbereinigung_spreadsheets",
    "datasource_remarks_velocity", "datasource_remarks_velocity.xls"))  # import table
datasource_init = list(lookup_table_ds_rmv["Datasource_init"])  # Datasource as in the inventory before cleansing
velocity_remarks_append = list(lookup_table_ds_rmv["Velocity_remarks_append"])  # substrings of old Datasource fields,
datasource_res = list(lookup_table_ds_rmv["Datasource_res"])  # replacement values for Datasource
# which are to be appended to velocity remarks

# 3.2 Adjust data format and NA handling for the lookup values:
velocity_remarks_append = list(map(str, velocity_remarks_append))  # homogenize datatype

for lookup_list in [velocity_remarks_append, datasource_init, datasource_res]:  # NA handling
    index = 0
    for i in lookup_list:
        if i == "nan":
            lookup_list[index] = None
        index += 1


# 3.3: Append substrings of old Datasource value, related to velocity information to Remarks_Velocity
for key in kt_dict.keys():
    df = kt_dict.get(key)
    for line, row in enumerate(df.itertuples(), 1):
        if row.Datasource in datasource_init:
            list_index = datasource_init.index(row.Datasource)  # get index of value in lookup list
            if row.Velocity_remarks is None and velocity_remarks_append[list_index] is not None:  # Velocity_remarks
                # is empty but will be added to - no "; " needed
                df._set_value(row.Index, "Velocity_remarks", velocity_remarks_append[list_index]) # update empty
                # Velocity_remarks field with substring from old Datasource field
            elif velocity_remarks_append[list_index] is not None: # Velocity_remarks is not empty and will be
                # appended to
                value = (df.loc[row.Index, "Velocity_remarks"] + "; " + velocity_remarks_append[
                list_index])  # value before replacement + delimiter + addition from old Datasource
                df._set_value(row.Index, "Velocity_remarks", value)  # set value
            elif velocity_remarks_append[list_index] is None:  # nothing to append
                continue
            else:
                print("something went wrong")
        elif row.Datasource is None:  # empty and nothing to add
            continue


# 3.3: Replace old Datasource values
for key in kt_dict.keys():
    df = kt_dict.get(key)
    index = df.columns.get_loc("Datasource")
    df.iloc[:,index] = df.iloc[:,index].replace(datasource_init, datasource_res)



# for key in kt_dict.keys():
#     df = kt_dict.get(key)
#     velocity_remarks = ("Velocity_remarks")

# 3.4 Derive addition to Datasource from Velocity_remarks fields by ,filtering for IPTA or InSAR related strings:

# Regular Expression list to find instances of IPTA (all expressions are case insensitive):
regex_list_ipta =["(?<!too few.*)(?<!no.*)IPTA(?!\?)"] # instances of "IPTA", unless "IPTA" is followed directly by "?",
# or it is preceded by "no" or "too few"

# Regular Expression list to find instances of InSAR related information (all expressions are case insensitive):
regex_list_insar = [
    "TSX",  # instances of "TSX"
    "RSAT",  # instances of "RSAT"
    "RAST",  # instances of "RAST" (common typo)
    "DES(?=[c|_])",  # instances of "DES" directly followed by "c" or "_" ("DESC" or "DES_" but not "geodesy")
    "AS(?=[c|_])",  # instances of "AS" directly followed by "c" or "_" ("AS_" or "ASC_" but not e.g. "faster")
    "(?<!no.*)SAR",  # instances of "SAR" unless its preceded, not necessarily directly, by "no" or "not" (e.g. "no clear
    # inSAR signal" is not considered)
    "COS",  # instances of "COS"
    "ENV",  # instances of "ENV"
    "\d+EV",  # instances of a digit directly followed by "EV"
    "(?<![A-Ia-iK-Zk-z])ERS",  # Instances of "ERS", unless followed by any other letter than "J" (catches JERS but
    # not e.g. "inclinometers")
    "S_(?=[ADOT\d])",  # instances of "S_" followed by "A" or "D" or "O" or "T" or a digit but exclude uncommon other
    # strings such as "GIS_URI" or "VS_BAS"
    "\d+d(?! >|r|\?)",  # instances of one or more digits followed by d, but not "d >" or "dr" or "d?"
    ".TIF",  # instances of .TIF
    "ALO(?!\?|<|ng)",  # instances of "ALO" (including "ALOS") but not "ALONG", "ALO<->" or "ALO?"
    "(?<!no .*)INTERFEROGRAM",  # instances of "interferogram" excluding if no is (not necessarily directly) before,
    # however includes: "noisy interferogram"
]


for key in kt_dict.keys():  # for each canton
    df = kt_dict.get(key)
    for line, row in enumerate(df.itertuples(), 1):  # for each row (polygon)

        for i in regex_list_ipta:  # for all ipta regex expressions
            pattern = re.compile(i, flags = re.IGNORECASE)  # compile the expression as case-insensitive
            if row.Velocity_remarks is None:  # if Velocity_remarks is empty
                continue
            elif pattern.search(row.Velocity_remarks):  # if the regex pattern matches the field value
                if "IPTA" not in str(row.Datasource):  # if "IPTA" is not already in Datasource field
                    value = (str(df.loc[row.Index, "Datasource"]) + "; " + "IPTA")  # value = field value + delimiter
                    # + "IPTA" (newly added
                    df._set_value(row.Index, "Datasource", value)
            else:  # if the regex pattern doesn't match the field value
                continue

        insar_no_duplicates = 0  # used to prevent the multiplicate addition of InSAR to the field if there were
        # mutiple matches of the field value:
        for i in regex_list_insar:  # for all insar regex expressions
            pattern = re.compile(i, flags = re.IGNORECASE)  # compile expression as case-insensitive
            if row.Velocity_remarks is None:  # if Velocity_remarks is empty
                continue
            elif pattern.search(row.Velocity_remarks):  # if the regex pattern matches the field value
                if insar_no_duplicates == 0:  # if "InSAR" was not already added to Datasource field (newly added
                    # "InSAR" strings will not be detected by the next conditional before the loop is finished)
                    if "InSAR" not in str(row.Datasource) and isinstance(df.loc[row.Index, "Datasource"],
                            float) is False:  # If "InSAR" is not already in Datasource and Datasource is no float
                        value = (df.loc[row.Index, "Datasource"] + "; " + "InSAR")  # value = Datasource + Delimiter +
                        # "InSAR"
                        df._set_value(row.Index, "Datasource", value)
                        insar_no_duplicates += 1  # increment variable to prevent multiplicate addition of "InSAR"
                    print(pattern,"  ----  ", row.Velocity_remarks,"  ----  ", value,"  ----  ", row.Datasource)
            else:
                continue


#4: Save the cleansed inventory:

# 4.1: get the data schema of a sample can - can be. The schema specifies the data format of the various columns and
# can be used to correctly export the dataframe

#AR = gpd.read_file(inventory_result, layer = "AR", engine = "fiona")
with fiona.open(inventory_initial, layer = "AR") as data:
    schema = data.schema

# 4.2 Save layers to the result inventory
for key in kt_dict.keys():
    layer = kt_dict.get(key)
    print(key, "saved")
    layer.to_file(inventory_result, layer = key, driver = "GPKG", schema = schema)


print("script terminated")