# Created by Yixin Sun in December 2019
# overlays Crop Data Layer CLU polygons
# note that the tabulate area function is tricky - it doesn't work if
# the "zones" (in this case the di leases) are overlapping - have to iterate
# through each row one by one
# https://gis.stackexchange.com/questions/78448/tabulate-area-for-large-datasets?rq=1

# Settings
import arcpy
import os
import csv

arcpy.CheckOutExtension("Spatial")

arcpy.env.overwriteOutput = True
arcpy.env.workspace = "C:\\Users\\Yixin Sun\\Documents\\Dropbox\\Energy Water Project\\Data"

# Local variables:
stump = "C:\\Users\\Yixin Sun\\Documents\\Dropbox\\Energy Water Project\\Data"
cdl_stump = os.path.join(stump, "Spatial Data\\CropLand Data Layer")

clu = os.path.join(stump, "cleaned_spatial\\CLU\\clu_poly\\clu_poly.shp")
clu_proj = os.path.join(stump, "cleaned_spatial\\CLU\\clu_poly\\clu_poly_proj.shp")

clu_cdl_stump = os.path.join(stump, "cleaned_spatial\\cross")
clu_cdl_out = os.path.join(clu_cdl_stump, "clu_cdl.csv")

# find projection of raster 
# then convert clu to same projection as raster
cdl07 = os.path.join(cdl_stump, "CDL_2007_06", "CDL_2007_06.tif")
dsc = arcpy.Describe(cdl07)
coord_sys = dsc.spatialReference
arcpy.Project_management(clu, clu_proj, coord_sys)
print("Projection done")

# loop through 2007 to 2017 cdl layers
for i in range(2007, 2017):
	print i
	folder = "CDL_" + str(i) + "_06"
	path = folder + ".tif"
	ras = os.path.join(cdl_stump, folder, path)
	point = folder + ".shp"
	ras_out = os.path.join(cdl_stump, "shape_files", point) 

	# convert raster to point
	arcpy.RasterToPoint_conversion(ras, ras_out)

	# intersect cdl and clu
	# arcpy.Intersect_analysis([ras_out, clu_proj], clu_cdl)

	# arcpy.CopyRows_management(clu_cdl, clu_cdl_out)



arcpy.CheckInExtension("Spatial")