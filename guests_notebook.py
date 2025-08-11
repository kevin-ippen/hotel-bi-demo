# Databricks notebook source
# MAGIC %pip install faker

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
from pyspark.sql.functions import *
from pyspark.sql.types import *
from faker import Faker
import random
from datetime import date, timedelta

fake = Faker(['en_US'])

# Wyndham Rewards tiers with realistic distribution
loyalty_tiers = [
    ("Blue", 0.60),      # 60% of members
    ("Gold", 0.25),      # 25% of members  
    ("Platinum", 0.12),  # 12% of members
    ("Diamond", 0.03)    # 3% of members
]

# Create weighted list for sampling
tier_list = []
for tier, weight in loyalty_tiers:
    tier_list.extend([tier] * int(weight * 1000))

# Guest segments
guest_segments = ["Business Traveler", "Leisure Traveler", "Extended Stay", "Group/Event"]
communication_preferences = ["Email", "SMS", "Mail", "Phone"]

# Generate guest data
guests = []
for i in range(75000):  # Realistic guest count for demo
    # Demographics
    gender = random.choice(['M', 'F'])
    if gender == 'M':
        first_name = fake.first_name_male()
    else:
        first_name = fake.first_name_female()
    
    last_name = fake.last_name()
    
    # Age distribution skewed toward adults
    age = random.choices(
        range(21, 81), 
        weights=[1 if x < 25 else 3 if x < 65 else 1 for x in range(21, 81)]
    )[0]
    
    # Email based on name
    email_domain = random.choice(['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com'])
    email = f"{first_name.lower()}.{last_name.lower()}@{email_domain}"
    
    # Member since date (weighted toward recent years)
    member_since = fake.date_between(start_date='-8y', end_date='today')
    
    # Loyalty tier (newer members more likely to be Blue)
    days_as_member = (date.today() - member_since).days
    if days_as_member < 365:
        loyalty_tier = random.choices(['Blue', 'Gold'], weights=[85, 15])[0]
    else:
        loyalty_tier = random.choice(tier_list)
    
    guests.append((
        i,
        first_name,
        last_name,
        email,
        fake.phone_number(),
        loyalty_tier,
        member_since,
        age,
        gender,
        fake.address().replace('\n', ', '),
        fake.city(),
        fake.state_abbr(),
        fake.zipcode(),
        random.choice(guest_segments),
        random.choice(communication_preferences),
        random.choice([True, False])  # Marketing opt-in
    ))

schema = StructType([
    StructField("guest_id", IntegerType(), False),
    StructField("first_name", StringType(), False),
    StructField("last_name", StringType(), False),
    StructField("email", StringType(), False),
    StructField("phone", StringType(), False),
    StructField("loyalty_tier", StringType(), False),
    StructField("member_since", DateType(), False),
    StructField("age", IntegerType(), False),
    StructField("gender", StringType(), False),
    StructField("address", StringType(), False),
    StructField("city", StringType(), False),
    StructField("state", StringType(), False),
    StructField("zip_code", StringType(), False),
    StructField("guest_segment", StringType(), False),
    StructField("communication_preference", StringType(), False),
    StructField("marketing_opt_in", BooleanType(), False)
])

df = spark.createDataFrame(guests, schema)
df.write.mode("overwrite").saveAsTable("main.hotel_inc.guests")

print(f"Generated {df.count()} guests")
display(df.limit(10))