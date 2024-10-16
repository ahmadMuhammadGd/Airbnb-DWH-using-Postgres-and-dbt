import pandas as pd
import dotenv, os, json 
from sqlalchemy import create_engine, text
from tqdm import tqdm

dotenv.load_dotenv('./secrets.env')

user = os.getenv('POSTGRES_USER')
password = os.getenv('POSTGRES_PASSWORD')
database = os.getenv('POSTGRES_DB')

host = 'localhost'
port = '5432'

engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')

def read_config(path:str)->dict:
    with open(path, 'r') as file:
        return json.load(file)

def proecss_calendar(chunk:pd.DataFrame)->pd.DataFrame:
    chunk["adjusted_price"] = chunk["adjusted_price"].apply(lambda x: str(x).replace(',', '').replace('$', '') if pd.notnull(x) else x)
    chunk["price"] = chunk["price"].apply(lambda x: str(x).replace(',', '').replace('$', '') if pd.notnull(x) else x)
    return chunk

def prepare_public_schema(engine):
    stmts = [
        'DROP SCHEMA IF EXISTS public CASCADE;',
        'CREATE SCHEMA IF NOT EXISTS public;',
        'CREATE EXTENSION IF NOT EXISTS postgis;'
    ]
    with engine.begin() as conn:
        for stmt in stmts:
            print(f"Executing: {stmt}")
            conn.execute(text(stmt))
        
    
config = read_config("./load_source_config.json")

prepare_public_schema(engine)

for file_path in config:
    print("*"*100)
    pair = config.get(file_path)
    table_name = pair.get('table_name')
    primary_unique_key = pair.get('primary_unique_key')
    
    add_primary_key = False
    if file_path.endswith("csv"):
        df = pd.read_csv(file_path, nrows=1)
        
        if primary_unique_key not in df.columns:
            add_primary_key = True
            
        print(f"Loading data from {file_path} to {table_name} table ..")
        
        show = False
        for chunk in tqdm(pd.read_csv(
            file_path,
            chunksize=15000
        )):            
            
            if not show:
                print("df before processing")
                print(chunk.head(10))
            if add_primary_key:
                chunk = chunk.reset_index().rename(columns={'index': primary_unique_key})
            
            if table_name == 'calendar':
                chunk = proecss_calendar(chunk)
            
            if not show:
                print("df after processing")
                print(chunk.head(10))
            show = True
            
            chunk.to_sql(
                table_name,
                engine,
                if_exists='append',
                index=False)
            
            
    elif file_path.endswith("geojson"):
        import geopandas as gpd
        
        print(f"reading file {file_path}")
        
        gdf = gpd.read_file(file_path)
        print(gdf.head)
        
        print(f"Loading file {file_path} into {table_name} table")
        
        if primary_unique_key not in gdf.columns:
            add_primary_key = True
        
        if add_primary_key:
            gdf = gdf.reset_index().rename(columns={'index': primary_unique_key})
            
        gdf = gdf.to_crs(epsg=4326)
        gdf.to_postgis(
            table_name,
            engine,
            if_exists='replace',
        )
        
        print(f"Loading file {file_path} into {table_name} table            Done!")
    
    primary_key_alter = text(f"ALTER TABLE {table_name} ADD PRIMARY KEY ({primary_unique_key})")
    with engine.begin() as conn:
        print("Creating the primary key constraint!")
        print(primary_key_alter)
        
        conn.execute(primary_key_alter)
        print(f"Successfully created the primary key {primary_key_alter} for the table {table_name}")