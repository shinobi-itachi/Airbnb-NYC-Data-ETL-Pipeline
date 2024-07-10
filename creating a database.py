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
