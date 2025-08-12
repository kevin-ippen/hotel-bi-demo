# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC # Hotel Analytics Aggregations (Corrected for Wyndham Hotels)
# MAGIC - Fixes RevPAR calculations to use consistent methodology
# MAGIC - Ensures proper nightly expansion for accurate metrics
# MAGIC - Adds data quality checks and realistic business rules
# MAGIC - Optimized for Wyndham Hotels scale and operations

# COMMAND ----------

# Common date anchors
current_year = 2025
ytd_start = f'{current_year}-01-01'
print(f"Building analytics views from {ytd_start}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Nightly Bookings View (Foundation)
# MAGIC This is critical for accurate RevPAR, ADR, and occupancy calculations

# COMMAND ----------

# Create nightly expansion view - this is crucial for proper hotel metrics
spark.sql(f"""
CREATE OR REPLACE VIEW main.hotel_inc.v_bookings_nightly AS
WITH booking_nights AS (
  SELECT
    b.booking_id,
    b.hotel_id,
    b.guest_id,
    b.status,
    b.channel,
    b.booking_date,
    b.check_in,
    b.room_rate,            -- nightly rate
    b.length_of_stay,
    b.lead_time_days,
    -- Generate sequence of nights for each booking
    explode(sequence(b.check_in, date_add(b.check_in, b.length_of_stay - 1))) AS business_date
  FROM main.hotel_inc.bookings b
  WHERE b.check_in >= DATE('{ytd_start}')
    AND b.status IN ('Confirmed', 'Cancelled', 'No-show')
)
SELECT
  booking_id,
  hotel_id,
  guest_id,
  status,
  channel,
  booking_date,
  business_date,
  room_rate AS nightly_rate,
  -- Only count room nights and revenue for confirmed stays
  CASE WHEN status = 'Confirmed' THEN 1 ELSE 0 END AS room_night,
  CASE WHEN status = 'Confirmed' THEN room_rate ELSE 0 END AS nightly_revenue,
  lead_time_days
FROM booking_nights
""")

print("✅ Created main.hotel_inc.v_bookings_nightly")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Daily Hotel Performance (Corrected RevPAR)
# MAGIC The foundation for all hotel KPIs - ensures RevPAR = Revenue / Available Room Nights

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW main.hotel_inc.daily_hotel_performance AS
WITH nightly_aggregates AS (
  SELECT
    hotel_id,
    business_date,
    COUNT(DISTINCT booking_id) AS bookings_count,
    SUM(room_night) AS rooms_sold,
    SUM(nightly_revenue) AS room_revenue,
    AVG(CASE WHEN room_night = 1 THEN nightly_rate END) AS avg_rate_sold,
    AVG(lead_time_days) AS avg_lead_time
  FROM main.hotel_inc.v_bookings_nightly
  GROUP BY hotel_id, business_date
),
date_spine AS (
  -- Create complete date range from bookings data
  SELECT DISTINCT business_date 
  FROM main.hotel_inc.v_bookings_nightly
  WHERE business_date >= DATE('{ytd_start}')
    AND business_date <= CURRENT_DATE()
)
SELECT
  h.hotel_id,
  h.hotel_name,
  h.brand,
  h.city,
  h.state,
  h.market_segment,
  h.room_count,
  h.base_adr,
  d.business_date,
  COALESCE(n.bookings_count, 0) AS bookings_count,
  COALESCE(n.rooms_sold, 0) AS rooms_sold,
  COALESCE(n.room_revenue, 0.0) AS room_revenue,
  
  -- ADR = Total Room Revenue / Rooms Sold (industry standard)
  CASE 
    WHEN COALESCE(n.rooms_sold, 0) > 0 
    THEN ROUND(n.room_revenue / n.rooms_sold, 2) 
    ELSE NULL 
  END AS adr,
  
  -- Occupancy % = Rooms Sold / Available Rooms * 100
  CASE 
    WHEN h.room_count > 0 
    THEN ROUND((COALESCE(n.rooms_sold, 0) * 100.0) / h.room_count, 2) 
    ELSE 0 
  END AS occupancy_rate,
  
  -- RevPAR = Total Room Revenue / Available Rooms (industry standard)
  CASE 
    WHEN h.room_count > 0 
    THEN ROUND(COALESCE(n.room_revenue, 0.0) / h.room_count, 2) 
    ELSE 0 
  END AS revpar,
  
  -- Additional metrics
  COALESCE(n.avg_lead_time, 0) AS avg_lead_time,
  CASE 
    WHEN h.base_adr > 0 AND COALESCE(n.rooms_sold, 0) > 0
    THEN ROUND(((n.room_revenue / n.rooms_sold) / h.base_adr - 1) * 100, 2)
    ELSE NULL 
  END AS rate_index,
  
  DATE_TRUNC('month', d.business_date) AS report_month,
  YEAR(d.business_date) AS report_year
  
FROM main.hotel_inc.hotels h
CROSS JOIN date_spine d
LEFT JOIN nightly_aggregates n
  ON h.hotel_id = n.hotel_id 
  AND d.business_date = n.business_date
""")

print("✅ Created main.hotel_inc.daily_hotel_performance")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Portfolio KPIs (Executive Dashboard)

# COMMAND ----------

# Portfolio-level KPIs for executive dashboard
spark.sql(f"""
CREATE OR REPLACE VIEW main.hotel_inc.portfolio_kpis AS
WITH portfolio_metrics AS (
  SELECT
    -- Portfolio totals
    COUNT(DISTINCT hotel_id) AS total_hotels,
    SUM(room_count) AS total_rooms,
    SUM(rooms_sold) AS total_room_nights_sold,
    SUM(room_revenue) AS total_revenue,
    
    -- Portfolio averages (weighted properly)
    SUM(room_revenue) / NULLIF(SUM(room_count), 0) AS portfolio_revpar,
    SUM(room_revenue) / NULLIF(SUM(rooms_sold), 0) AS portfolio_adr,
    (SUM(rooms_sold) * 100.0) / NULLIF(SUM(room_count), 0) AS portfolio_occupancy,
    
    -- Period info
    MIN(business_date) AS period_start,
    MAX(business_date) AS period_end,
    COUNT(DISTINCT business_date) AS days_in_period
    
  FROM main.hotel_inc.daily_hotel_performance
  WHERE business_date >= DATE('{ytd_start}')
)
SELECT 
  *,
  total_revenue / days_in_period AS avg_daily_revenue,
  total_room_nights_sold / days_in_period AS avg_daily_room_nights
FROM portfolio_metrics
""")

print("✅ Created main.hotel_inc.portfolio_kpis")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Brand Performance Analysis

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW main.hotel_inc.brand_performance AS
WITH brand_metrics AS (
  SELECT
    brand,
    COUNT(DISTINCT hotel_id) AS hotel_count,
    COUNT(DISTINCT business_date) AS reporting_days,
    
    -- Totals
    SUM(rooms_sold) AS total_room_nights_sold,
    SUM(room_count) AS total_available_room_nights,
    SUM(room_revenue) AS total_revenue,
    
    -- Proper weighted averages
    SUM(room_revenue) / NULLIF(SUM(room_count), 0) AS brand_revpar,
    SUM(room_revenue) / NULLIF(SUM(rooms_sold), 0) AS brand_adr,
    (SUM(rooms_sold) * 100.0) / NULLIF(SUM(room_count), 0) AS brand_occupancy,
    
    -- Market position
    AVG(rate_index) AS avg_rate_index
    
  FROM main.hotel_inc.daily_hotel_performance
  WHERE business_date >= DATE('{ytd_start}')
  GROUP BY brand
),
portfolio_benchmarks AS (
  SELECT
    SUM(room_revenue) / NULLIF(SUM(room_count), 0) AS portfolio_revpar,
    SUM(room_revenue) / NULLIF(SUM(rooms_sold), 0) AS portfolio_adr,
    (SUM(rooms_sold) * 100.0) / NULLIF(SUM(room_count), 0) AS portfolio_occupancy
  FROM main.hotel_inc.daily_hotel_performance
  WHERE business_date >= DATE('{ytd_start}')
)
SELECT
  b.*,
  p.portfolio_revpar,
  p.portfolio_adr,
  p.portfolio_occupancy,
  
  -- Variance from portfolio
  ROUND(b.brand_revpar - p.portfolio_revpar, 2) AS revpar_variance,
  ROUND(b.brand_adr - p.portfolio_adr, 2) AS adr_variance,
  ROUND(b.brand_occupancy - p.portfolio_occupancy, 2) AS occupancy_variance
  
FROM brand_metrics b
CROSS JOIN portfolio_benchmarks p
ORDER BY b.brand_revpar DESC
""")

print("✅ Created main.hotel_inc.brand_performance")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Geographic Performance

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW main.hotel_inc.geographic_performance AS
SELECT
  state,
  market_segment,
  COUNT(DISTINCT hotel_id) AS hotel_count,
  SUM(room_count) AS total_rooms,
  
  -- Performance metrics
  SUM(room_revenue) AS total_revenue,
  SUM(rooms_sold) AS total_room_nights_sold,
  
  -- Calculated KPIs
  SUM(room_revenue) / NULLIF(SUM(room_count), 0) AS state_revpar,
  SUM(room_revenue) / NULLIF(SUM(rooms_sold), 0) AS state_adr,
  (SUM(rooms_sold) * 100.0) / NULLIF(SUM(room_count), 0) AS state_occupancy,
  
  -- Per hotel metrics
  SUM(room_revenue) / COUNT(DISTINCT hotel_id) AS revenue_per_hotel,
  AVG(room_count) AS avg_rooms_per_hotel
  
FROM main.hotel_inc.daily_hotel_performance
WHERE business_date >= DATE('{ytd_start}')
GROUP BY state, market_segment
HAVING SUM(room_revenue) > 0
ORDER BY total_revenue DESC
""")

print("✅ Created main.hotel_inc.geographic_performance")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Channel & Loyalty Analysis

# COMMAND ----------

# Channel performance with proper revenue attribution
spark.sql(f"""
CREATE OR REPLACE VIEW main.hotel_inc.channel_performance AS
SELECT
  channel,
  COUNT(DISTINCT booking_id) AS total_bookings,
  SUM(nightly_revenue) AS total_revenue,
  SUM(room_night) AS total_room_nights,
  
  -- Channel metrics
  SUM(nightly_revenue) / NULLIF(SUM(room_night), 0) AS channel_adr,
  AVG(lead_time_days) AS avg_lead_time,
  
  -- Revenue share
  ROUND(
    (SUM(nightly_revenue) * 100.0) / 
    SUM(SUM(nightly_revenue)) OVER (), 2
  ) AS revenue_share_pct
  
FROM main.hotel_inc.v_bookings_nightly
WHERE business_date >= DATE('{ytd_start}')
  AND status = 'Confirmed'
GROUP BY channel
ORDER BY total_revenue DESC
""")

# Loyalty performance
spark.sql(f"""
CREATE OR REPLACE VIEW main.hotel_inc.loyalty_performance AS
SELECT
  g.loyalty_tier,
  COUNT(DISTINCT g.guest_id) AS active_guests,
  COUNT(DISTINCT bn.booking_id) AS total_bookings,
  SUM(bn.nightly_revenue) AS total_revenue,
  SUM(bn.room_night) AS total_room_nights,
  
  -- Per guest metrics
  SUM(bn.nightly_revenue) / COUNT(DISTINCT g.guest_id) AS revenue_per_guest,
  COUNT(DISTINCT bn.booking_id) / COUNT(DISTINCT g.guest_id) AS bookings_per_guest,
  SUM(bn.nightly_revenue) / NULLIF(SUM(bn.room_night), 0) AS avg_adr,
  
  -- Revenue share
  ROUND(
    (SUM(bn.nightly_revenue) * 100.0) / 
    SUM(SUM(bn.nightly_revenue)) OVER (), 2
  ) AS revenue_share_pct
  
FROM main.hotel_inc.guests g
LEFT JOIN main.hotel_inc.v_bookings_nightly bn
  ON g.guest_id = bn.guest_id
  AND bn.business_date >= DATE('{ytd_start}')
  AND bn.status = 'Confirmed'
WHERE bn.guest_id IS NOT NULL
GROUP BY g.loyalty_tier
ORDER BY total_revenue DESC
""")

print("✅ Created channel and loyalty performance views")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Operational Views

# COMMAND ----------

# Operational alerts and monitoring
spark.sql(f"""
CREATE OR REPLACE VIEW main.hotel_inc.operational_alerts AS
WITH recent_performance AS (
  SELECT
    hotel_id,
    hotel_name,
    brand,
    city,
    state,
    AVG(occupancy_rate) AS avg_7d_occupancy,
    AVG(adr) AS avg_7d_adr,
    AVG(revpar) AS avg_7d_revpar,
    COUNT(CASE WHEN occupancy_rate < 40 THEN 1 END) AS low_occ_days,
    COUNT(CASE WHEN occupancy_rate > 100 THEN 1 END) AS overbook_days,
    MAX(business_date) AS last_report_date
  FROM main.hotel_inc.daily_hotel_performance
  WHERE business_date >= CURRENT_DATE() - INTERVAL 7 DAYS
  GROUP BY hotel_id, hotel_name, brand, city, state
)
SELECT
  *,
  CASE
    WHEN avg_7d_occupancy < 30 THEN 'Critical - Very Low Occupancy'
    WHEN avg_7d_occupancy < 45 THEN 'Warning - Low Occupancy'
    WHEN low_occ_days >= 4 THEN 'Trend Alert - Occupancy Declining'
    WHEN overbook_days >= 2 THEN 'Capacity Alert - Frequent Overbooking'
    WHEN avg_7d_revpar < 40 THEN 'Revenue Alert - Low RevPAR'
    ELSE 'Normal Operations'
  END AS alert_type,
  
  CASE
    WHEN avg_7d_occupancy < 30 THEN 'High'
    WHEN avg_7d_occupancy < 45 OR low_occ_days >= 4 THEN 'Medium'
    ELSE 'Low'
  END AS alert_priority
  
FROM recent_performance
WHERE avg_7d_occupancy < 60 
   OR low_occ_days >= 3 
   OR overbook_days >= 2
   OR avg_7d_revpar < 50
ORDER BY 
  CASE alert_priority WHEN 'High' THEN 1 WHEN 'Medium' THEN 2 ELSE 3 END,
  avg_7d_occupancy ASC
""")

# Hotels with overbooking issues
spark.sql(f"""
CREATE OR REPLACE VIEW main.hotel_inc.overbooking_analysis AS
SELECT
  hotel_id,
  hotel_name,
  brand,
  city,
  state,
  business_date,
  room_count,
  rooms_sold,
  occupancy_rate,
  room_revenue,
  ROUND(rooms_sold - room_count, 0) AS overbooked_rooms
FROM main.hotel_inc.daily_hotel_performance
WHERE occupancy_rate > 100
ORDER BY business_date DESC, occupancy_rate DESC
""")

# Unoccupied inventory analysis
spark.sql(f"""
CREATE OR REPLACE VIEW main.hotel_inc.unoccupied_inventory AS
SELECT
  hotel_id,
  hotel_name,
  brand,
  city,
  state,
  business_date,
  room_count,
  rooms_sold,
  room_count - rooms_sold AS unoccupied_rooms,
  ROUND(((room_count - rooms_sold) * 100.0) / room_count, 2) AS unoccupied_pct,
  ROUND((room_count - rooms_sold) * base_adr, 2) AS potential_lost_revenue
FROM main.hotel_inc.daily_hotel_performance
WHERE business_date >= CURRENT_DATE() - INTERVAL 30 DAYS
  AND room_count > rooms_sold
ORDER BY unoccupied_pct DESC, potential_lost_revenue DESC
""")

print("✅ Created operational monitoring views")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Dashboard-Ready Datasets

# COMMAND ----------

# Create simplified datasets specifically for dashboard tiles
spark.sql(f"""
CREATE OR REPLACE VIEW main.hotel_inc.kpi_revpar AS
SELECT 
  SUM(room_revenue) / NULLIF(SUM(room_count), 0) AS revpar,
  COUNT(DISTINCT hotel_id) AS hotels_count,
  COUNT(DISTINCT business_date) AS days_count
FROM main.hotel_inc.daily_hotel_performance
WHERE business_date >= DATE('{ytd_start}')
""")

spark.sql(f"""
CREATE OR REPLACE VIEW main.hotel_inc.kpi_occupancy AS
SELECT 
  (SUM(rooms_sold) * 100.0) / NULLIF(SUM(room_count), 0) AS occupancy_pct
FROM main.hotel_inc.daily_hotel_performance
WHERE business_date >= DATE('{ytd_start}')
""")

spark.sql(f"""
CREATE OR REPLACE VIEW main.hotel_inc.kpi_adr AS
SELECT 
  SUM(room_revenue) / NULLIF(SUM(rooms_sold), 0) AS avg_adr
FROM main.hotel_inc.daily_hotel_performance
WHERE business_date >= DATE('{ytd_start}')
  AND rooms_sold > 0
""")

# RevPAR trend for forecasting
spark.sql(f"""
CREATE OR REPLACE VIEW main.hotel_inc.revpar_trend_daily AS
SELECT
  business_date,
  brand,
  SUM(room_revenue) / NULLIF(SUM(room_count), 0) AS revpar,
  SUM(room_revenue) AS total_revenue,
  SUM(room_count) AS total_rooms,
  SUM(rooms_sold) AS total_rooms_sold
FROM main.hotel_inc.daily_hotel_performance
WHERE business_date >= DATE('{ytd_start}')
GROUP BY business_date, brand
ORDER BY business_date, brand
""")

print("✅ Created dashboard-ready KPI datasets")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Data Quality Checks

# COMMAND ----------

# Run data quality checks
print("🔍 Running data quality checks...")

# Check for reasonable RevPAR values
revpar_check = spark.sql("""
SELECT 
  AVG(revpar) as avg_revpar,
  MIN(revpar) as min_revpar,
  MAX(revpar) as max_revpar,
  COUNT(*) as total_records
FROM main.hotel_inc.daily_hotel_performance 
WHERE revpar IS NOT NULL
""").collect()[0]

print(f"RevPAR Quality Check:")
print(f"  Average RevPAR: ${revpar_check.avg_revpar:.2f}")
print(f"  Range: ${revpar_check.min_revpar:.2f} - ${revpar_check.max_revpar:.2f}")
print(f"  Total records: {revpar_check.total_records:,}")

# Check occupancy rates
occ_check = spark.sql("""
SELECT 
  AVG(occupancy_rate) as avg_occupancy,
  COUNT(CASE WHEN occupancy_rate > 100 THEN 1 END) as overbooked_days,
  COUNT(CASE WHEN occupancy_rate < 20 THEN 1 END) as very_low_occ_days
FROM main.hotel_inc.daily_hotel_performance
""").collect()[0]

print(f"\nOccupancy Quality Check:")
print(f"  Average Occupancy: {occ_check.avg_occupancy:.1f}%")
print(f"  Overbooked days: {occ_check.overbooked_days:,}")
print(f"  Very low occupancy days (<20%): {occ_check.very_low_occ_days:,}")

print("\n✅ All corrected analytics views created successfully!")
print("\nKey improvements made:")
print("- ✅ Fixed RevPAR calculation consistency")
print("- ✅ Proper nightly expansion for accurate metrics")
print("- ✅ Added data quality checks")
print("- ✅ Created dashboard-ready datasets")
print("- ✅ Added operational monitoring views")
print("- ✅ Improved geographic and brand analysis")
