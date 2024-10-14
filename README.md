## Introduction
The primary goal of this project is to analyze Airbnb listings data for Barcelona, allowing us to construct a data warehouse (DW) that can efficiently support various business queries. By utilizing the dataset, we aim to extract insights that address specific business questions, enabling stakeholders to make informed decisions based on the data.                  
> For more information about this task, check [this repo](https://github.com/ahmedshaaban1999/Data_Engineering_Mentorship/tree/main/level_1/Data_Modeling/projects/airbnb) 

## Business Questions

To further guide the analysis, several specific business questions are posed, such as:

- **Identifying the cheapest**, **most available** listing in October 2024.
- **Determining which listings** received the most reviews in October 2024.
- **Identifying the most** expensive neighborhood in Barcelona.
- **Providing tailored recommendations** for specific user profiles based on their needs and budget.


## Key Deliverables

The project is structured around two key deliverables:

- **Data Warehouse Schema:** A well-defined schema that enables efficient data storage and retrieval.
- **Business Question Answers:** SQL queries that answer the specified business questions, providing insights and recommendations based on the data.                                     
> Check: `./notebook.ipynb` to see results.
    
## Prerequisites:
- Docker & Docker Compose installed
- Python 3.9+
- Linux os (Ubuntu 22.04.5 LTS x86_64 was used in this project)
- gunzip (gzip) 1.10

## How to run

To start, run `sh setup.sh` that handles the following tasks:

- **Data Directory Setup**: Creates a directory structure to organize raw data files.
- **Data Downloading:** Fetches essential CSV and GeoJSON files from the Inside Airbnb website, which includes listings, calendar data, reviews, and geographical data about neighborhoods.
- **Data Uncompression:** Decompresses the downloaded files for further processing.             

Subsequently, another script (`load_source_data.py`) is used to load this data into a PostgreSQL database. This script:

- **Reads** CSV files in chunks to avoid memory issues and processes each chunk before loading it into the database. Special handling is included for price fields to ensure they are stored in a numerical format.
- **Loads** GeoJSON files directly into the database as well, allowing spatial queries to be performed on geographical data.

### profiles.yml
Copy these configurations to your `profiles.yml` 
```yml
# /path/to/profiles.yml
dbt_model:
  outputs:
    dev:
      dbname: bnb
      host: localhost
      pass: passw0rd
      port: 5432
      schema: dwh
      threads: 1
      type: postgres
      user: ahmad
  target: dev
```

## DWH Schema
The schema is designed using an Entity-Relationship Diagram (ERD) that visually represents the relationships between the various tables in the data warehouse. This diagram serves as a blueprint for how data is organized and how different entities interact, making it easier for developers and analysts to understand the structure of the database.
- **Fact Tables**: These contain transactional data related to **listings** and **reviews**, allowing for performance-oriented querying.
- **Dimension Tables**: These provide context to the facts, such as details about hosts, listings, and dates. They enhance the analytical capabilities of the data warehouse by allowing more intuitive filtering and grouping in queries.

```mermaid
erDiagram
    %% FACT TABLES IN THE MIDDLE
    fact_listing {
        bigint _pk PK
        bigint listing_id FK
        bigint host_id FK
        int date FK
        boolean available
        float price
        float adjusted_price
        int minimum_nights
        int maximum_nights
    }

    fact_reviews {
        bigint id PK
        bigint listing_id FK
        bigint host_id FK
        int reviewer_id FK
        int date FK
        string comments
    }

    %% DIMENSION TABLES AROUND THE FACTS
    dim_date {
        date date_day PK
        date prior_date_day
        date next_date_day
        date prior_year_date_day
        date prior_year_over_year_date_day
        int day_of_week
        string day_of_week_name
        string day_of_week_name_short
        int day_of_month
        int day_of_year
        date week_start_date
        date week_end_date
        date prior_year_week_start_date
        date prior_year_week_end_date
        int week_of_year
        date iso_week_start_date
        date iso_week_end_date
        date prior_year_iso_week_start_date
        date prior_year_iso_week_end_date
        int iso_week_of_year
        int prior_year_week_of_year
        int prior_year_iso_week_of_year
        int month_of_year
        string month_name
        string month_name_short
        date month_start_date
        date month_end_date
        date prior_year_month_start_date
        date prior_year_month_end_date
        int quarter_of_year
        date quarter_start_date
        date quarter_end_date
        int year_number
        date year_start_date
        date year_end_date
    }

    dim_host {
        bigint host_id PK
        string host_url
        string host_name
        string host_since
        string host_location
        string host_about
        string host_response_time
        string host_response_rate
        string host_acceptance_rate
        boolean host_is_superhost
        string host_thumbnail_url
        string host_picture_url
        string host_neighbourhood
        int host_listings_count
        int host_total_listings_count
        string host_verifications
        boolean host_has_profile_pic
        boolean host_identity_verified
    }

    dim_listing {
        bigint listing_id PK
        string listing_url
        string source
        string name
        string description
        string neighborhood_overview
        string picture_url
        float latitude
        float longitude
        string property_type
        string room_type
        int accommodates
        int bathrooms
        string bathrooms_text
        int bedrooms
        int beds
        jsonb amenities
        int neighbourhood_id FK
        
    }

    dim_neighbourhood {
        int neighbourhood_id PK
        string neighbourhood_group
        string neighbourhood
        geometry geometry
    }

    dim_reviewer {
        int reviewer_id PK
        string reviewer_name
        string reviewer_first_name
        string reviewer_last_name
        
    }

    %% RELATIONSHIPS
    fact_listing ||--o{ dim_listing: ""
    fact_listing ||--o{ dim_host: ""
    fact_listing ||--o{ dim_date: ""
    
    fact_reviews ||--o{ dim_listing: ""
    fact_reviews ||--o{ dim_reviewer: ""
    fact_reviews ||--o{ dim_date: ""
    fact_reviews ||--o{ dim_host: ""

    dim_listing ||--o{ dim_neighbourhood: ""
```


# Lineage Diagram
The lineage diagram provides a visual representation of the flow of data through various stages of the data pipeline, illustrating how raw data transforms into actionable insights within the data warehouse. It captures the relationships between different components, including data sources, transformations, and the final analytical outputs.
![dbt DAG](./readme_assets/lineage.png)