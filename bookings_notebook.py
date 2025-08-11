# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import date, timedelta
import random
import math
from builtins import round as python_round

# Get hotel and guest data for realistic relationships
hotels_df = spark.table("main.hotel_inc.hotels")
guests_df = spark.table("main.hotel_inc.guests") 

hotel_data = hotels_df.select("hotel_id", "base_adr", "brand", "market_segment", "room_count").collect()
guest_data = guests_df.select("guest_id", "loyalty_tier", "guest_segment").collect()

# Create lookup dictionaries for performance
hotel_lookup = {row.hotel_id: (row.base_adr, row.brand, row.market_segment, row.room_count) for row in hotel_data}
guest_lookup = {row.guest_id: (row.loyalty_tier, row.guest_segment) for row in guest_data}

# Booking channels with realistic distribution  
channels = [
    ("Brand.com", 0.35),
    ("OTA", 0.40), 
    ("Phone", 0.15),
    ("Walk-in", 0.05),
    ("Corporate", 0.05)
]

channel_list = []
for channel, weight in channels:
    channel_list.extend([channel] * int(weight * 1000))

# Room types
room_types = ["Standard King", "Standard Double", "Suite", "Accessible Room"]

# Generate realistic booking patterns
def generate_bookings():
    booking_id = 0
    start_date = date(2022, 1, 1)
    end_date = date(2025, 8, 1)
    
    current_date = start_date
    while current_date < end_date:
        # Seasonal multiplier
        month = current_date.month
        if month in [6, 7, 8]:  # Summer peak
            seasonal_multiplier = 1.4
        elif month in [11, 12]:  # Holiday season
            seasonal_multiplier = 1.2
        elif month in [1, 2]:   # Winter low
            seasonal_multiplier = 0.7
        else:
            seasonal_multiplier = 1.0
        
        # Day of week effect
        day_of_week = current_date.weekday()
        if day_of_week < 5:  # Monday-Friday
            dow_multiplier = 1.2
        else:  # Weekend
            dow_multiplier = 0.8
        
        # Base bookings per day
        base_bookings = int(400 * seasonal_multiplier * dow_multiplier)
        
        for _ in range(random.randint(int(base_bookings * 0.8), int(base_bookings * 1.2))):
            hotel_id = random.choice(list(hotel_lookup.keys()))
            guest_id = random.choice(list(guest_lookup.keys()))
            
            base_adr, brand, market_segment, room_count = hotel_lookup[hotel_id]
            loyalty_tier, guest_segment = guest_lookup[guest_id]
            
            # Stay length varies by guest segment
            if guest_segment == "Extended Stay":
                stay_length = random.randint(7, 30)
            elif guest_segment == "Business Traveler":
                stay_length = random.randint(1, 4)
            else:  # Leisure
                stay_length = random.randint(2, 7)
            
            check_out = current_date + timedelta(days=stay_length)
            
            # Dynamic pricing based on multiple factors
            price_multiplier = 1.0
            
            # Seasonal pricing
            price_multiplier *= seasonal_multiplier
            
            # Weekend premium for leisure markets
            if day_of_week >= 5 and market_segment == "Leisure":
                price_multiplier *= 1.15
            
            # Loyalty discounts
            if loyalty_tier == "Diamond":
                price_multiplier *= 0.9
            elif loyalty_tier == "Platinum":
                price_multiplier *= 0.95
            elif loyalty_tier == "Gold":
                price_multiplier *= 0.97
            
            # Channel pricing
            channel = random.choice(channel_list)
            if channel == "OTA":
                price_multiplier *= 1.05  # OTA commission built in
            elif channel == "Brand.com":
                price_multiplier *= 0.98  # Direct booking discount
            
            # Add random variance
            price_multiplier *= random.uniform(0.85, 1.15)
            
            room_rate = python_round(base_adr * price_multiplier, 2)
            
            # Booking status (most are confirmed)
            status = random.choices(
                ["Confirmed", "Cancelled", "No-show"],
                weights=[92, 6, 2]
            )[0]
            
            # Lead time (days between booking and stay)
            if guest_segment == "Business Traveler":
                lead_time = random.randint(0, 21)
            else:
                lead_time = random.randint(7, 90)
            
            booking_date = current_date - timedelta(days=lead_time)
            
            yield (
                booking_id,
                hotel_id,
                guest_id,
                booking_date,
                current_date,  # check_in
                check_out,
                room_rate,
                channel,
                random.choice(room_types),
                status,
                stay_length,
                lead_time
            )
            booking_id += 1
        
        current_date += timedelta(days=1)

# Create schema
schema = StructType([
    StructField("booking_id", IntegerType(), False),
    StructField("hotel_id", IntegerType(), False),
    StructField("guest_id", IntegerType(), False),
    StructField("booking_date", DateType(), False),
    StructField("check_in", DateType(), False),
    StructField("check_out", DateType(), False),
    StructField("room_rate", DoubleType(), False),
    StructField("channel", StringType(), False),
    StructField("room_type", StringType(), False),
    StructField("status", StringType(), False),
    StructField("length_of_stay", IntegerType(), False),
    StructField("lead_time_days", IntegerType(), False)
])

print("Generating bookings... this may take a few minutes")

# Generate bookings in chunks to avoid memory issues
chunk_size = 50000
all_bookings = []

booking_generator = generate_bookings()
chunk = []

for booking in booking_generator:
    chunk.append(booking)
    if len(chunk) >= chunk_size:
        chunk_df = spark.createDataFrame(chunk, schema)
        if not all_bookings:
            all_bookings = chunk_df
        else:
            all_bookings = all_bookings.union(chunk_df)
        chunk = []
        print(f"Processed {len(all_bookings.collect())} bookings so far...")

# Process final chunk
if chunk:
    chunk_df = spark.createDataFrame(chunk, schema)
    if not all_bookings:
        all_bookings = chunk_df
    else:
        all_bookings = all_bookings.union(chunk_df)

all_bookings.write.mode("overwrite").saveAsTable("main.hotel_inc.bookings")

print(f"Generated {all_bookings.count()} total bookings")
display(all_bookings.limit(10))
