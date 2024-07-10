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
#data loading
# Read the CSV file
csv_file_path = r'C:\Users\rk585\OneDrive\Desktop\AB_NYC_2019.csv'
df = pd.read_csv(csv_file_path , encoding='latin') #encoded with latin because of presence of non-UTF-8 encoded characters in the CSV file.

# Load data into the PostgreSQL table
df.to_sql('airbnb_listings', engine, if_exists='replace', index=False, method='multi')

print("Data loaded successfully!")

#________________extraction, transformation, loading
df.info() #information of table
# Extract data using SQLAlchemy
with engine.connect() as connection:
    df = pd.read_sql('SELECT * FROM airbnb_listings', connection)

print("Data extracted successfully!")

#transform the data
# Normalize the date column
df['last_review_date'] = pd.to_datetime(df['last_review'],dayfirst=True).dt.date
df['last_review_time'] = pd.to_datetime(df['last_review'],dayfirst=True).dt.time
df.drop(columns=['last_review'], inplace=True)

# Calculate additional metrics (e.g., average price per neighborhood)
avg_price_per_neighborhood = df.groupby('neighbourhood')['price'].mean().reset_index()
avg_price_per_neighborhood.rename(columns={'price': 'avg_price'}, inplace=True)

# Merge the new metrics back into the original DataFrame
df = pd.merge(df, avg_price_per_neighborhood, on='neighbourhood', how='left')

# Handle missing values
df.fillna({'reviews_per_month': 0}, inplace=True)
# Calculate the number of listings per host
listings_per_host = df.groupby('host_id')['id'].count().reset_index()
listings_per_host.rename(columns={'id': 'listings_count'}, inplace=True)

# Merge the new metrics back into the original DataFrame
df = pd.merge(df, listings_per_host, on='host_id', how='left')

# Categorize listings by price range
price_bins = [0, 100, 300, float('inf')]
price_labels = ['Low', 'Medium', 'High']
df['price_range'] = pd.cut(df['price'], bins=price_bins, labels=price_labels, include_lowest=True)


print("Data transformation completed successfully!")
