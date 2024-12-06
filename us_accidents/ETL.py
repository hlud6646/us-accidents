"""
This script creates the cities and accidents tables in the database.
It is intended to be a minimal ETL pipeline, containing the following steps:

- Read an environment variables to establish a database connection;
- Create a polars data frame from a file;
- Manipulate and select columns;
- Write to db;
- Create some data integrity constraints in the db;
- Clean up.
"""

# %% Imports
import polars as pl
import os
import zipfile
import logging
from sqlalchemy import create_engine, text

from us_accidents import PROJECT_ROOT


# %% Config
os.chdir(PROJECT_ROOT)

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

LOGGER.info("Configuring database connection")
user = "us_accidents_admin"
password = os.environ.get("US_ACCIDENTS_ADMIN_PASSWORD")
db_host = "localhost:5432"
db_name = "us_accidents"
DB_URI = f"postgresql://{user}:{password}@{db_host}/{db_name}"
DB_URI = "postgresql://us_accidents_admin@localhost:5432/us_accidents"
DB_ENGINE = create_engine(DB_URI)

# %% Unzip
LOGGER.info("Checking zip file and extracting if needed")
assert os.path.exists(os.path.join(PROJECT_ROOT, "data", "us-accidents.zip"))
if not os.path.exists(os.path.join(PROJECT_ROOT, "data", "US_Accidents_March23.csv")):
    LOGGER.info("Unzipping the file")
    with zipfile.ZipFile("data/us-accidents.zip", "r") as zip_ref:
        zip_ref.extractall("data/")
else:
    LOGGER.info("File already unzipped")

# %% Load
LOGGER.info("Reading CSV file into polars lazy frame")
df = pl.scan_csv("data/US_Accidents_March23.csv", try_parse_dates=True)

df = df.select(
    # pl.col("ID"),
    pl.col("Severity").alias('severity'),
    pl.col("Start_Time").alias('datetime'),
    pl.col("Start_Lat").alias('lat'),
    pl.col("Start_Lng").alias('lng'),
    pl.col("Weather_Condition").alias('weather_condition'),
    pl.col("City"),
    pl.col("State"),
    pl.col("County")
)

LOGGER.info("Creating lazy frame for cities")
cities_df = df.select(
    pl.col('City'),
    pl.col('State'),
    pl.col('County')
).with_row_index(name='city_id')

# %% Write cities
LOGGER.info("Writing cities to database")
cities_df.collect().write_database(table_name="cities", connection=DB_URI)

# %% Transform accidents
LOGGER.info("Replacing city, county, and state columns with city_id")
df = (
    df.join(
        cities_df,
        on=['City', 'County', 'State'], 
        how='left'
    )
    .select(pl.exclude(['City', 'County', 'State']))
)


# %% Write accidents
LOGGER.info("Writing accidents to database")
df.collect().write_database(table_name="accidents", connection=DB_URI)

LOGGER.info("Removing the unzipped file")
os.remove("data/US_Accidents_March23.csv")


# %% DB constraints
# It feels a bit clunky to do this via sqlalchemy, but since there are only 
# two commands to execute we can avoid a subprocess.
with DB_ENGINE.connect() as conn:
    conn.execute(
        text(
            """
        alter table cities
        add constraint unique_city_id
        unique (city_id);
    """
        )
    )
    conn.commit()
    conn.execute(
        text(
            """
        alter table accidents
        add constraint fK_accidents_city_id
        foreign key (city_id) references cities(city_id);
    """
        )
    )
    conn.commit()





# %% Postscript
# The following quieries describe the non-uniqueness of city names in the US.

# Are city names unique?
cities_df.group_by('City').agg(pl.len().alias('count')).sort('count', descending=True).head(5).collect()
# No.


# Does (name, state) uniquely identify a city?
(
    cities_df
    .group_by('City', 'State')
    .agg(pl.len().alias('count'))
    .sort('count', descending=True)
    .head(5)
    .collect()
)
# No.

# Are county names unique?
cities_df.select(
    pl.col('County'),
    pl.col('State')
).group_by('County').agg(pl.len()).sort('len', descending=True).head(5).collect()
# No.

# Are city, county combos unique?
(
    cities_df
    .group_by('City', 'County')
    .agg(pl.len().alias('count'))
    .sort('count', descending=True)
    .head(5)
    .collect()
)
# No.

# We assume that (city, county, state) uniquely identifies a city.


