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

# COMMAND ----------

# MAGIC %md
# MAGIC # Hotel Data Generation - Wyndham Hotels Scale
# MAGIC Generating realistic hotel data scaled for Wyndham's actual portfolio size and market presence

# COMMAND ----------

# Actual Wyndham brands with realistic ADR ranges (updated for 2025)
wyndham_brands = [
    ("Wyndham Grand", 200, 450),      # Upscale full-service
    ("Wyndham Hotels & Resorts", 85, 220), # Upper midscale
    ("Ramada", 70, 150),              # Midscale
    ("Days Inn", 55, 110),            # Economy
    ("Super 8", 50, 95),              # Economy
    ("Microtel Inn & Suites", 65, 125), # Economy/midscale
    ("La Quinta", 75, 160),           # Midscale
    ("Baymont", 70, 140),             # Midscale
    ("Wingate", 90, 180),             # Upper midscale
    ("Howard Johnson", 60, 130),      # Midscale
    ("Travelodge", 55, 120),          # Economy
    ("AmericInn", 80, 150),           # Midscale
    ("Hawthorn Suites", 100, 200),   # Extended stay
    ("WoodSpring Suites", 60, 110),  # Extended stay economy
    ("Registry Collection", 250, 500) # Luxury
]

# Realistic distribution matching Wyndham's actual brand mix
brand_weights = {
    "Super 8": 25,                    # Largest brand
    "Days Inn": 20,                   # Second largest
    "Ramada": 12,
    "Wyndham Hotels & Resorts": 10,
    "Baymont": 8,
    "La Quinta": 7,
    "Microtel Inn & Suites": 5,
    "Wingate": 4,
    "Travelodge": 3,
    "Howard Johnson": 2,
    "AmericInn": 2,
    "Hawthorn Suites": 1,
    "Wyndham Grand": 0.7,
    "WoodSpring Suites": 0.2,
    "Registry Collection": 0.1
}

# US states with realistic Wyndham distribution (they're everywhere!)
states_weighted = [
    ("TX", 12), ("FL", 10), ("CA", 8), ("NY", 6), ("OH", 5),
    ("PA", 5), ("IL", 4), ("NC", 4), ("GA", 4), ("MI", 4),
    ("VA", 3), ("TN", 3), ("IN", 3), ("WI", 3), ("MO", 3),
    ("AL", 3), ("SC", 3), ("KY", 2), ("LA", 2), ("OK", 2),
    ("AR", 2), ("MS", 2), ("IA", 2), ("KS", 2), ("NE", 2),
    ("WV", 2), ("NM", 2), ("NV", 2), ("AZ", 3), ("CO", 2),
    ("UT", 1), ("OR", 2), ("WA", 3), ("ID", 1), ("MT", 1),
    ("ND", 1), ("SD", 1), ("WY", 1), ("MN", 3), ("ME", 1),
    ("NH", 1), ("VT", 1), ("MA", 2), ("RI", 1), ("CT", 2),
    ("NJ", 3), ("DE", 1), ("MD", 2), ("DC", 1), ("AK", 1), 
    ("HI", 2)
]

# Create weighted lists for sampling
states = []
for state, weight in states_weighted:
    states.extend([state] * weight)

brands = []
for brand, weight in brand_weights.items():
    brands.extend([brand] * int(weight * 100))

market_segments = ["Business", "Leisure", "Extended Stay", "Airport", "Highway"]

# Generate realistic hotel data for a larger portfolio
print("Generating hotel portfolio data...")

hotels = []
for i in range(2400):  # More realistic for demo (Wyndham has ~9000 total)
    # Select brand based on realistic distribution
    brand_name = random.choice(brands)
    
    # Get brand info
    brand_info = None
    for brand_tuple in wyndham_brands:
        if brand_tuple[0] == brand_name:
            brand_info = brand_tuple
            break
    
    if not brand_info:
        continue
        
    min_adr, max_adr = brand_info[1], brand_info[2]
    
    state = random.choice(states)
    
    # Room count varies significantly by brand and location type
    if "Grand" in brand_name or "Registry" in brand_name:
        room_count = random.randint(150, 500)  # Large full-service
    elif "Suites" in brand_name:
        room_count = random.randint(80, 180)   # Extended stay
    elif brand_name in ["Super 8", "Days Inn", "Microtel Inn & Suites"]:
        room_count = random.randint(45, 120)   # Economy properties
    else:
        room_count = random.randint(60, 200)   # Midscale
    
    # Star rating by brand positioning
    if brand_name in ["Wyndham Grand", "Registry Collection"]:
        star_rating = python_round(random.uniform(4.2, 5.0), 1)
    elif brand_name in ["Wyndham Hotels & Resorts", "Wingate", "Hawthorn Suites"]:
        star_rating = python_round(random.uniform(3.5, 4.5), 1)
    elif brand_name in ["Super 8", "Days Inn", "Travelodge", "WoodSpring Suites"]:
        star_rating = python_round(random.uniform(2.0, 3.5), 1)
    else:
        star_rating = python_round(random.uniform(3.0, 4.0), 1)
    
    # Generate realistic city names
    city_name = fake.city()
    
    # Create hotel name that follows Wyndham naming conventions
    if random.random() < 0.7:  # 70% follow location + brand pattern
        hotel_name = f"{city_name} {brand_name}"
    else:  # 30% have descriptive names
        descriptors = ["Airport", "Downtown", "Suites", "Inn", "Resort", "Center", "Plaza"]
        hotel_name = f"{city_name} {random.choice(descriptors)} {brand_name}"
    
    # Market segment based on brand and location factors
    if "Airport" in hotel_name or state in ["FL", "NV", "HI", "CA"]:
        market_segment = random.choices(
            ["Airport", "Business", "Leisure"], 
            weights=[50, 30, 20]
        )[0]
    elif "Suites" in brand_name:
        market_segment = random.choices(
            ["Extended Stay", "Business", "Leisure"], 
            weights=[60, 25, 15]
        )[0]
    elif brand_name in ["Super 8", "Days Inn", "Travelodge"]:
        market_segment = random.choices(
            ["Highway", "Leisure", "Business"], 
            weights=[50, 35, 15]
        )[0]
    else:
        market_segment = random.choice(market_segments)
    
    # Base ADR with regional adjustments
    base_adr = python_round(random.uniform(min_adr * 0.9, max_adr * 1.1), 2)
    
    # Regional ADR adjustments
    if state in ["NY", "CA", "HI", "DC", "MA"]:  # High-cost markets
        base_adr *= random.uniform(1.2, 1.5)
    elif state in ["FL", "NV", "CO"]:  # Tourist markets
        base_adr *= random.uniform(1.1, 1.3)
    elif state in ["MS", "AR", "WV", "AL"]:  # Lower-cost markets
        base_adr *= random.uniform(0.8, 0.95)
    
    base_adr = python_round(base_adr, 2)
    
    # Amenities based on brand tier and property size
    has_pool = random.random() < (0.8 if room_count > 100 else 0.6)
    has_fitness = random.random() < (0.9 if brand_name in ["Wyndham Grand", "Registry Collection"] else 0.7)
    has_conference = random.random() < (0.9 if room_count > 150 else 0.4)
    
    # Realistic open dates (Wyndham has been expanding for decades)
    open_date = fake.date_between(start_date='-25y', end_date='-6m')
    
    hotels.append((
        i,
        hotel_name,
        brand_name,
        city_name,
        state,
        float(star_rating),
        open_date,
        int(room_count),
        float(base_adr),
        market_segment,
        bool(has_conference),
        bool(has_pool),
        bool(has_fitness)
    ))

# Create schema
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

# Write to table
df.write.mode("overwrite").saveAsTable("main.hotel_inc.hotels")

print(f"✅ Generated {df.count():,} hotels")

# COMMAND ----------

# Show summary statistics
print("\n📊 Portfolio Summary:")
brand_summary = df.groupBy("brand").agg(
    count("*").alias("hotel_count"),
    avg("room_count").alias("avg_rooms"),
    avg("base_adr").alias("avg_base_adr"),
    avg("star_rating").alias("avg_rating")
).orderBy(desc("hotel_count"))

print("Brand Distribution:")
brand_summary.show(20, False)

print("\nGeographic Distribution (Top 15 States):")
geo_summary = df.groupBy("state").agg(
    count("*").alias("hotel_count"),
    avg("base_adr").alias("avg_adr")
).orderBy(desc("hotel_count"))

geo_summary.show(15)

print("\nMarket Segment Distribution:")
market_summary = df.groupBy("market_segment").agg(
    count("*").alias("hotel_count"),
    avg("room_count").alias("avg_rooms"),
    avg("base_adr").alias("avg_adr")
).orderBy(desc("hotel_count"))

market_summary.show()

# Calculate total portfolio metrics
total_rooms = df.agg(sum("room_count")).collect()[0][0]
avg_adr = df.agg(avg("base_adr")).collect()[0][0]
avg_rating = df.agg(avg("star_rating")).collect()[0][0]

print(f"\n🏨 Portfolio Totals:")
print(f"Total Hotels: {df.count():,}")
print(f"Total Rooms: {total_rooms:,}")
print(f"Average ADR: ${avg_adr:.2f}")
print(f"Average Rating: {avg_rating:.1f} stars")

print("\n✅ Hotel data generation complete! Data is realistic for Wyndham's scale and brand mix.")
