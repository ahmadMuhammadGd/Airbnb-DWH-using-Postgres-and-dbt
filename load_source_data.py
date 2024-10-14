import pandas as pd
from sqlalchemy import create_engine
import geopandas as gpd
import dotenv 
from glob import glob
import os 

dotenv.load_dotenv('./secrets.env')

user = os.getenv('POSTGRES_USER')
password = os.getenv('POSTGRES_PASSWORD')
database = os.getenv('POSTGRES_DB')

host = 'localhost'  
port = '5432' 

raw_files = './raw'
csv_files_list = glob(f"{raw_files}/*.csv")
geojson_files_list = glob(f"{raw_files}/*.geojson")

engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')

for file_path in csv_files_list:    
    filename = file_path.split('/')[-1]
    table_name = filename.replace(".csv", "")
    print(f"Seeding data from file {file_path} into {table_name}:")
    
    i = 1
    for chunk in pd.read_csv(file_path, chunksize=5000):
        if table_name == 'calendar':
            chunk["adjusted_price"] = chunk["adjusted_price"].apply(lambda x: str(x).replace(',', '').replace('$', '') if pd.notnull(x) else x)
            chunk["price"] = chunk["price"].apply(lambda x: str(x).replace(',', '').replace('$', '') if pd.notnull(x) else x)
        
        print(f"[{filename} -> {table_name}] casting chunk: {i}          Done!")
        
        try:
            print(f"[{filename} -> {table_name}] seeding chunk: {i}")
            chunk.to_sql(table_name, engine, if_exists='append', index=False)
            print(f"[{filename} -> {table_name}] seeding chunk: {i}          Done!")
        except Exception as e:
            print(f"Error while seeding chunk {i} from file {filename}: {e}")
        
        i += 1


for path in geojson_files_list:
    filename = path.split('/')[-1]
    table_name = filename.replace(".geojson", "")
    
    try:
        print(f"[{path} --> {table_name}] Reading")
        gdf = gpd.read_file(path)
        
        print(f"[{path} --> {table_name}] to crs")
        gdf = gdf.to_crs(epsg=4326)
        
        print(f"[{path} --> {table_name}] loading")
        gdf.to_postgis(table_name, engine, if_exists='replace')
        
        
        print(f"[{path} --> {table_name}] Done")
    except Exception as e:
            print(f"Error while seeding {path} into {table_name}: {e}")
        