# jerome messmer, 04.10.2022
# jerome.messmer@bafu.admin.ch
# this script is used to return both unique values before and after the execution of the Inventarbereinigung script.
# The returned unique values can be used both to find values to replace (when executed on the "old" inventory) and to
# control the results of the cleaning (when executed on the "new" inventory)

# import requested packages:
import geopandas as gpd
import pandas as pd
import os
import glob
print("script started")


# set the Mitarbeiterkuerzel for correct path reference:
Mitarbeiterkuerzel = "MJ2022"  # change


# set for which version of the inventory unique values should be returned. the version for which unique values should
# not be returned can be commented (#), the version for which unique values should be returned must not be commented.
# It is using this script, only possible to return unique values for a version once at a time.
# "old"    return unique values for the old (non-cleaned) inventory
# "new"    return unique values for the new (cleaned) inventory
return_inventory = "new"


#set various directories:
wd = os.path.join("O:\GIS\GEP\RLS\_Mitarbeitende", Mitarbeiterkuerzel, "Inventarbereinigung")  # working directory

gpkg_list = glob.glob(os.path.join(wd, "04_Inventarbereinigung_py", "Inventarbereinigung_results", "gpkg", "*.gpkg"))
# list of all saved, cleaned inventories
inventory_new = max(gpkg_list, key = os.path.getctime)  # latest version of the cleaned inventory
uv_suffix = inventory_new[-20:-5]  # name suffix used to link the unique values to the respective inventory
unique_values_new = os.path.join(wd, "02_Unique_Values", "nach_Bereinigung", "gesamtCH", "unique_values_CH" +
    uv_suffix + ".txt")  # file path for the unique values of the new inventory

inventory_old = os.path.join(wd, "04_Inventarbereinigung_py", "Inventar_resource", "gpkg",
    "db_InSARCH_V0_clean20012022.gpkg")  # path to the version of the inventory before cleaning
unique_values_old = os.path.join(wd, "02_Unique_Values", "vor_Bereinigung", "gesamtCH",
    "unique_values_CH_" + "20012022" + ".txt")  # file path for the unique values of the old inventory

if return_inventory == "new":  # set path for further processing according to settings
    inventory = inventory_new
    unique_values = unique_values_new
elif return_inventory == "old":
    inventory = inventory_old
    unique_values = unique_values_old
else:
    print("correctly enter \"new\" or \"old\" to set for which version of the inventory unique values should be "
          "returned")


# list all cantons for reading of the Kantone-InSAR-inventories:
kantone_list = ["AG", "AI", "AR", "BE", "BL", "BS", "FR", "GE", "GL", "GR", "JU", "LU", "NE", "NW", "OW", "SG", "SH",
                "SO", "SZ", "TG", "TI", "UR", "VD", "VS", "ZG", "ZH"]


# Read all Kantone-InSAR-inventories:
print("RuntimeWarning: Sequential read of iterator was interrupted. Resetting iterator - CAN BE IGNORED")
kt_dict = {}
for kanton in kantone_list:
    kt = gpd.read_file(inventory, layer = kanton)
    key = kanton
    kt_dict[key] = kt


# Merge all Kantone-InSAR-inventories to a national dataset:
CH = gpd.GeoDataFrame()
concat_layers = []
for key in kt_dict.keys():
    concat_layers.append(kt_dict.get(key))
CH = pd.concat(concat_layers)


# Remove columns for which unique values should not be returned:
cols = list(CH.columns)
cols.remove("geometry")
cols.remove("UUID")


# Write Unique Values of the entire InSAR-inventory (CH) to a file in the specified folder:
with open(unique_values, 'w') as f:
    for col in cols:
        f.write(50*"#")
        f.write('\nAnzahl der Unique Values für Feld %s'%col)
        f.write(3 * "\n")
        f.write(CH[col].value_counts(dropna = False).to_string() + 50 * "\n")
        f.write(30 * "\n")


print("script terminated, check out unique values here:", unique_values)

