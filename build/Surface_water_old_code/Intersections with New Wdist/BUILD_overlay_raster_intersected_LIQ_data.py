import rasterio
import fiona
import numpy as np
import pandas as pd
import geopandas as gpd
from rasterio.features import rasterize

path_shapefile = 'C:/Users/clohani/Dropbox/New_Water_Districts/Intersected_2014/Intersected_CLU_LIQ.shp'
path_raster = 'C:/Users/clohani/Downloads/CDL_2007_06.tif'

#create ID
shapes = gpd.read_file(path_shapefile)
shapes['id'] = range(1, len(shapes)+1)
shapes.to_file('C:/Users/clohani/Dropbox/New_Water_Districts/Intersected_2014/Intersected_CLU_LIQ_id.shp')

path_shapefile = 'C:/Users/clohani/Dropbox/New_Water_Districts/Intersected_2014/Intersected_CLU_LIQ_id.shp'
path_raster = 'C:/Users/clohani/Downloads/CDL_2007_06.tif'

 def batch_crosswalk(raster_path,
                     path_shapefile,
                     id_var):
    '''
    Create one-to-one relation between pixels and polygon boundaries using one
    raster layer. 
    Returns:
        pandas.DataFrame with indexed data for each cell value and its index
        (or the feature_to_burn) 
    '''

    with rasterio.open(raster_path) as src:
        with fiona.open(path_shapefile, 'r') as shp:
            geoms = [feature['geometry'] for feature in shp]
            index = [feature['properties'][id_var] for feature in shp]

            crosswalk_dict = {}
            for idx, geom in zip(index, geoms):
                geom_rasterize = rasterize([(geom, 1)],
                                           out_shape=src.shape,
                                           transform=src.transform,
                                           all_touched=True,
                                           fill=0,
                                           dtype='uint8')

                crosswalk_dict[idx] = np.where(geom_rasterize == 1)

        return crosswalk_dict

    
    
def batch_extract_process(raster_path,
                          save_path_dir,
                          crosswalk):
    '''
    Extract raster numpy array and map to batch crosswalk.
    '''

    save_path = os.path.join(save_path_dir, 'txt_batch')
    if os.path.exists(save_path):
        print(f'Batch saving path exists in {self.save_path}')
    else:
        os.makedirs(save_path)

    with zipmem.open(raster_path) as src:
        r_array = src.read(1)
        r_array[r_array == src.nodata] = np.nan

        count_dict = []
        for key, value in crosswalk.items():
            array_polygon = r_array[value]
            array_unique_vals, array_count = array_polygon.unique(return_counts=True)
            
            df_polygon = pd.DataFrame({
                'poly_id': key,
                'crop_type': array_unique_vals,
                'count_crop_type': array_count
            })
            
            count_dict.append(df_polygon)

        df = pd.concat(count_dict)
        print(f'Saving extracted data from {path_raster}')
        
        save_dir = os.path.join(self.save_path, 'crop_type_extract.csv')

        df.to_csv(save_dir,
                  index=False)


batch_crosswalk(path_raster, path_shapefile, id_var=id )
