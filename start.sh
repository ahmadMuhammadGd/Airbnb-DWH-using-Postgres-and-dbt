


RAW_DATA_DIR="$PWD/raw"
mkdir -p $RAW_DATA_DIR

curl https://data.insideairbnb.com/spain/catalonia/barcelona/2024-09-06/data/listings.csv.gz                    -lo $RAW_DATA_DIR/listings.csv.gz
curl https://data.insideairbnb.com/spain/catalonia/barcelona/2024-09-06/data/calendar.csv.gz                    -lo $RAW_DATA_DIR/calendar.csv.gz
curl https://data.insideairbnb.com/spain/catalonia/barcelona/2024-09-06/data/reviews.csv.gz                     -lo $RAW_DATA_DIR/reviews.csv.gz
curl https://data.insideairbnb.com/spain/catalonia/barcelona/2024-09-06/visualisations/neighbourhoods.geojson   -lo $RAW_DATA_DIR/neighbourhoods.geojson

gunzip $RAW_DATA_DIR/listings.csv.gz
gunzip $RAW_DATA_DIR/calendar.csv.gz
gunzip $RAW_DATA_DIR/reviews.csv.gz

docker compose up -d

python -m venv dbt-env 
. dbt-env/bin/activate 

pip install -r requirements.txt

python ./load_source_data.py