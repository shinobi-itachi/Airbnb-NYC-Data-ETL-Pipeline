import pandas as pd
from sqlalchemy import create_engine, text

# Database connection parameters
db_params = {
    'database': 'airbnb_nyc',
    'user': 'postgres',
    'password': 'Rohit5363',
    'host': 'localhost',
    'port': '5432'
}

# Create SQLAlchemy engine
engine = create_engine(f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}")

#Create table in airbnb_nyc database
# Create table schema
create_table_query = """
CREATE TABLE IF NOT EXISTS airbnb_listings (
    id SERIAL PRIMARY KEY,
    name TEXT,
    host_id INTEGER,
    host_name TEXT,
    neighbourhood_group TEXT,
    neighbourhood TEXT,
    latitude DECIMAL,
    longitude DECIMAL,
    room_type TEXT,
    price INTEGER,
    minimum_nights INTEGER,
    number_of_reviews INTEGER,
    last_review DATE,
    reviews_per_month DECIMAL,
    calculated_host_listings_count INTEGER,
    availability_365 INTEGER
);
"""

# Execute create table query
with engine.connect() as connection:
    connection.execute(text(create_table_query))

print("Table created successfully!")
