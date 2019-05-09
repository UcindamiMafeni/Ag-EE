# Created by Yixin Sun in December 2019
# overlays Crop Data Layer CLU polygons
# note that the tabulate area function is tricky - it doesn't work if
# the "zones" (in this case the clu polygons) are overlapping - have to iterate
# through each row one by one
# https://gis.stackexchange.com/questions/78448/tabulate-area-for-large-datasets?rq=1

# Settings
import arcpy
import os
import csv
import getpass 


user = getpass.getuser()
if user == "Yixin Sun":
        arcpy.env.workspace = "C:\\Users\\ysun9\\Dropbox\\Energy Water Project\\Data"

arcpy.CheckOutExtension("Spatial")
arcpy.env.overwriteOutput = True


# Local variables:
stump = "C:\\Users\\Yixin Sun\\Documents\\Dropbox\\Energy Water Project\\Data"
cdl_stump = os.path.join(stump, "Spatial Data\\CropLand Data Layer")

clu = os.path.join(stump, "cleaned_spatial\\CLU\\clu_poly\\clu_poly.shp")
clu_proj = os.path.join(stump, "cleaned_spatial\\CLU\\clu_poly\\clu_poly_proj.shp")

clu_cdl_stump = os.path.join(stump, "cleaned_spatial\\cross\\clu_cdl")

# find projection of raster 
# then convert clu to same projection as raster
cdl07 = os.path.join(cdl_stump, "CDL_2007_06", "CDL_2007_06.tif")
dsc = arcpy.Describe(cdl07)
coord_sys = dsc.spatialReference
arcpy.Project_management(clu, clu_proj, coord_sys)
print("Projection done")

# loop through 2007 to 2017 cdl layers
for i in range(2007, 2018):
        print i
        folder = "CDL_" + str(i) + "_06"
        path = folder + ".tif"
        ras = os.path.join(cdl_stump, folder, path)
        out_base = "clu_cdl" + str(i) + ".csv"
        out_dir = os.path.join(clu_cdl_stump, out_base)

        arcpy.CreateTable_management(clu_cdl_stump, out_base)

        arcpy.sa.TabulateArea(clu_proj, "CLU_ID", ras, "Value", r"in_memory\outTable")
        arcpy.AddField_management(r"in_memory\outTable", "Year", "TEXT")
        arcpy.CalculateField_management(r"in_memory\outTable", "Year", str(i), "PYTHON_9.3")
        
        arcpy.CopyRows_management(r"in_memory\outTable", out_dir)

arcpy.CheckInExtension("Spatial")

arcpy.Delete_management(clu_proj)
