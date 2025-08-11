# Databricks notebook source
# MAGIC %pip install faker

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from faker import Faker
import random
from builtins import round as python_round

fake = Faker()

# Actual Wyndham brands with realistic ADR ranges
wyndham_brands = [
    ("Wyndham Grand", 180, 350),
    ("Wyndham Hotels & Resorts", 90, 180), 
    ("Ramada", 70, 130),
    ("Days Inn", 60, 100),
    ("Super 8", 55, 85),
    ("Microtel Inn & Suites", 65, 110),
    ("La Quinta", 75, 140),
    ("Baymont", 70, 120),
    ("Wingate", 85, 150),
    ("Howard Johnson", 65, 115),
    ("Travelodge", 60, 105),
    ("AmericInn", 80, 130),
    ("Hawthorn Suites", 95, 165)
]

# US states with tourism weight (higher weight = more hotels)
states_weighted = [
    ("FL", 15), ("CA", 12), ("TX", 10), ("NY", 8), ("NV", 6),
    ("NC", 5), ("VA", 5), ("GA", 4), ("SC", 4), ("TN", 4),
    ("AZ", 3), ("OH", 3), ("PA", 3), ("IL", 3), ("MI", 3),
    ("WA", 2), ("OR", 2), ("CO", 2), ("UT", 2), ("WI", 2),
    ("MN", 2), ("LA", 2), ("AL", 2), ("MS", 2), ("AR", 2),
    ("OK", 1), ("KS", 1), ("MO", 1), ("IA", 1), ("IN", 1),
    ("KY", 1), ("WV", 1), ("MD", 1), ("DE", 1), ("CT", 1),
    ("RI", 1), ("VT", 1), ("NH", 1), ("ME", 1), ("MT", 1),
    ("ND", 1), ("SD", 1), ("WY", 1), ("ID", 1), ("NM", 1),
    ("AK", 1), ("HI", 3)
]

# Create weighted lists for sampling
states = []
for state, weight in states_weighted:
    states.extend([state] * weight)

market_segments = ["Business", "Leisure", "Extended Stay", "Airport", "Highway"]

# Generate hotel data
hotels = []
for i in range(800):  # More realistic count for demo
    brand_info = random.choice(wyndham_brands)
    brand_name = brand_info[0]
    min_adr, max_adr = brand_info[1], brand_info[2]
    
    state = random.choice(states)
    
    # Room count varies by brand
    if "Grand" in brand_name or "Suites" in brand_name:
        room_count = random.randint(120, 400)
    elif brand_name in ["Super 8", "Days Inn", "Microtel Inn & Suites"]:
        room_count = random.randint(50, 120)
    else:
        room_count = random.randint(80, 200)
    
    # Star rating by brand
    if "Grand" in brand_name:
        star_rating = python_round(random.uniform(4.0, 5.0), 1)
    elif brand_name in ["Super 8", "Days Inn", "Travelodge"]:
        star_rating = python_round(random.uniform(2.0, 3.5), 1)
    else:
        star_rating = python_round(random.uniform(3.0, 4.5), 1)
    
    hotels.append((
        i,
        f"{fake.city()} {brand_name}",
        brand_name,
        fake.city(),
        state,
        float(star_rating),  # Ensure star_rating is a float
        fake.date_between(start_date='-15y', end_date='-1y'),  # Established hotels
        int(room_count),  # Ensure room_count is an integer
        float(python_round(random.uniform(min_adr * 0.8, max_adr * 1.2), 2)),  # Ensure base_adr is a float
        random.choice(market_segments),
        bool(random.choice([True, False])),  # Ensure boolean type
        bool(random.choice([True, False])),  # Ensure boolean type
        bool(random.choice([True, False]))   # Ensure boolean type
    ))

schema = StructType([
    StructField("hotel_id", IntegerType(), False),
    StructField("hotel_name", StringType(), False),
    StructField("brand", StringType(), False),
    StructField("city", StringType(), False),
    StructField("state", StringType(), False),
    StructField("star_rating", DoubleType(), False),
    StructField("open_date", DateType(), False),
    StructField("room_count", IntegerType(), False),
    StructField("base_adr", DoubleType(), False),
    StructField("market_segment", StringType(), False),
    StructField("has_conference_facilities", BooleanType(), False),
    StructField("has_pool", BooleanType(), False),
    StructField("has_fitness_center", BooleanType(), False)
])

df = spark.createDataFrame(hotels, schema)

# Create catalog and schema if they don't exist
spark.sql("CREATE CATALOG IF NOT EXISTS main")
spark.sql("CREATE SCHEMA IF NOT EXISTS main.hotel_inc")

df.write.mode("overwrite").saveAsTable("main.hotel_inc.hotels")

print(f"Generated {df.count()} hotels")
display(df.limit(10))
