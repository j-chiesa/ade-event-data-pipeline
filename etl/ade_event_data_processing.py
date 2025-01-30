import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Import necessary libraries and functions from PySpark
from pyspark.sql import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DoubleType
from pyspark.sql.functions import (
    # Column operations
    col, trim, lower, upper, initcap, concat, lit, split, regexp_replace,
    
    # Date and time functions
    to_timestamp, date_format, year, to_date, unix_timestamp,
    
    # Conditional and null handling
    coalesce, when,
    
    # String and regex functions
    length, regexp_extract,
    
    # Array and collection functions
    explode, transform, array_remove,
    
    # Miscellaneous
    row_number, concat_ws
)

# Define the column names for the dataset
column_names = [
    "name", "start_date", "end_date", "location", "address", "locality",
    "country", "latitude", "longitude", "capacity", "price", "genre",
    "lineup", "state"
]

# Create a schema for the DataFrame using the defined column names, all as StringType
schema = StructType([StructField(name, StringType(), True) for name in column_names])

# Read the CSV file from the specified S3 bucket with the defined schema and options
df = spark.read.option("sep", ",").option("encoding", "UTF-8").option("quote", "\"").csv("s3://ade-data-bucket/ade_event_data/ade_event_data_raw/", header=True, schema=schema)

# Define a list of tuples for renaming and trimming columns
columns = [
    ("name", "EventName"),
    ("start_date", "StartDatetime"),
    ("end_date", "EndDatetime"),
    ("location", "Venue"),
    ("address", "Address"),
    ("latitude", "Latitude"),
    ("longitude", "Longitude"),
    ("capacity", "Capacity"),
    ("price", "Price"),
    ("genre", "Genre"),
    ("lineup", "Lineup"),
    ("state", "State")
]

# Select and rename columns while trimming whitespace
df_cleaned = df.select([trim(col(c[0])).alias(c[1]) for c in columns])

# Filter out events with specific names to exclude them from the dataset
excluding_events = ["amsterdam dance event", "amsterdam dance events"]
df_cleaned = df_cleaned.filter(~lower(col("EventName")).isin(excluding_events))

# Define regex patterns to clean and standardize event names
regex_patterns = [
    r"^(ADE )|( ADE)$",  # Remove "ADE" from the beginning or end
    r"^(ADE: )|( ADE)$",  # Remove "ADE: " from the beginning or " ADE" from the end
    r"@.*$",  # Remove locations followed by "@"
    r"^[^\w]+|[^\w]+$"  # Remove special characters from the beginning or end
]

# Convert event names to uppercase
df_cleaned = df_cleaned.withColumn("EventName", upper(col("EventName")))

# Apply each regex pattern to clean the event names
for pattern in regex_patterns:
    df_cleaned = df_cleaned.withColumn("EventName", trim(regexp_replace(col("EventName"), pattern, "")))
      
# Define columns for splitting datetime strings into separate date and time columns
date_columns = [
    ("StartDatetime", "StartDate", "StartTime"),
    ("EndDatetime", "EndDate", "EndTime")
]

# Split datetime columns into separate date and time columns and standardize the format
for datetime_col, date_col, time_col in date_columns:
    df_cleaned = (df_cleaned
        .withColumn(date_col, to_date(date_format(split(col(datetime_col), "T")[0], "dd-MM-yyyy"), "dd-MM-yyyy"))
        .withColumn(time_col, date_format(split(col(datetime_col), "T")[1], "HH:mm"))
    )

# Calculate the event duration in minutes
df_cleaned = df_cleaned.withColumn(
    "EventDurationMin", 
    (unix_timestamp(concat_ws(" ", col("EndDate"), col("EndTime")), "yyyy-MM-dd HH:mm") - 
     unix_timestamp(concat_ws(" ", col("StartDate"), col("StartTime")), "yyyy-MM-dd HH:mm")) / 60
)

# Add an "Edition" column based on the year of the event's start date
df_cleaned = df_cleaned.withColumn("Edition", year("StartDatetime"))

# Clean and standardize venue and address names
df_cleaned = (df_cleaned
    .withColumn("Venue", initcap(trim(regexp_replace(col("Venue"), r"\(.*\)", ""))))
    .withColumn("Address", initcap(trim(regexp_replace(col("Address"), r"\(.*\)", ""))))
)

# Standardize venue names by selecting the shortest name for each address
windowSpec = Window.partitionBy(col("Address")).orderBy(length(col("Venue")))
df_venue_stand = (df_cleaned
    .withColumn("RowNum", row_number().over(windowSpec))
    .select("Address", "Venue", "RowNum")
    .filter(col("RowNum") == 1)
)

# Join the cleaned DataFrame with the standardized venue names
df_cleaned = (df_cleaned.alias("a")
    .join(df_venue_stand.alias("b"), on="Address", how="left")
    .select(
        "a.*", 
        col("b.Venue").alias("VenueName")
    )
)

# Convert latitude and longitude columns to DoubleType
df_cleaned = (df_cleaned
    .withColumns({
        "Latitude": col("Latitude").cast(DoubleType()),
        "Longitude": col("Longitude").cast(DoubleType())
    })
)

# Add columns to indicate if an event is sold out or cancelled
state_columns = [
    ("IsSoldOut", "SOLD OUT"),
    ("IsCancelled", "CANCELLED")
]

for column, state in state_columns:
    df_cleaned = df_cleaned.withColumn(column, when(upper(col("State")).contains(state), True).otherwise(False))

# Define regex patterns for extracting presale and door prices
regex_presale = r"(Presale):\s*(\d{1,3},\d{2})"
regex_door = r"(Door):\s*(\d{1,3},\d{2})"

# Extract and clean presale and door prices
df_cleaned = (df_cleaned
    .withColumns({
        "PricePresale": when(col("Price") == "Free entrance", "0,00")
                        .otherwise(when(regexp_extract(col("Price"), regex_presale, 2) == "", None)
                                   .otherwise(regexp_extract(col("Price"), regex_presale, 2))
                        ),

        "PriceDoor": when(col("Price") == "Free entrance", "0,00")
                    .otherwise(when(regexp_extract(col("Price"), regex_door, 2) == "", None)
                               .otherwise(regexp_extract(col("Price"), regex_door, 2))
                    )
    })
    .withColumns({
        "PricePresale": regexp_replace(coalesce(col("PricePresale"), col("PriceDoor")), r",", ".").cast(DecimalType(4,2)),
        "PriceDoor": regexp_replace(coalesce(col("PriceDoor"), col("PricePresale")), r",", ".").cast(DecimalType(4,2))
    })
)

# Convert the capacity column to IntegerType
df_cleaned = df_cleaned.withColumn("Capacity", col("Capacity").cast(IntegerType()))

# Clean and standardize the genre column
df_cleaned = (df_cleaned
    .withColumn("Genre", regexp_replace(col("Genre"), r"\.", ","))  # Replace dots with commas
    .withColumn("Genre", split(col("Genre"), ","))  # Split by commas
    .withColumn("Genre", transform("Genre", lambda x: trim(upper(x))))  # Trim and convert to uppercase
    .withColumn("Genre", transform(col("Genre"), lambda x: regexp_replace(x, r"[^a-zA-Z0-9\s,]", "")))  # Remove special characters except commas
    .withColumn("Genre", array_remove(col("Genre"), ""))  # Remove empty elements
)

# Clean and standardize the lineup column
df_cleaned = (df_cleaned
    .withColumn("Lineup", regexp_replace(col("Lineup"), "[\u2019\uFFFD']", ""))  # Remove special characters
    .withColumn("Lineup", regexp_replace(col("Lineup"), r"\(.*?\)", ""))  # Remove text within parentheses
    .withColumn("Lineup", regexp_replace(col("Lineup"), r"(?i)\s(?:vs|en|b2b)\s*", ", "))  # Standardize separators
    .withColumn("Lineup", upper(col("Lineup")))  # Convert to uppercase
    .withColumn("Lineup", split(col("Lineup"), ","))  # Split by commas
    .withColumn("Lineup", transform("Lineup", lambda x: trim(x)))  # Trim each element
    .withColumn("Lineup", array_remove(col("Lineup"), ""))  # Remove empty elements
)

# Select the final set of columns for the cleaned DataFrame
df_cleaned = df_cleaned.select(
    col("Edition"), 
    col("EventName"), 
    col("StartDate"), 
    col("StartTime"), 
    col("EndDate"), 
    col("EndTime"), 
    col("EventDurationMin"),
    col("VenueName"), 
    col("Address"), 
    col("Latitude"), 
    col("Longitude"),
    col("PricePresale"),
    col("PriceDoor"),
    col("Capacity"),
    col("Genre"),
    col("Lineup"),
    col("IsSoldOut"),
    col("IsCancelled")
)

# Write the cleaned DataFrame to a Parquet file in the specified S3 bucket, partitioned by "Edition"
df_cleaned.write.mode("overwrite").partitionBy("Edition").parquet("s3://ade-data-bucket/ade_event_data/ade_event_data_clean/")