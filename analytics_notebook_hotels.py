# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC # Hotel Analytics Aggregations
# MAGIC Creating pre-calculated metrics for dashboard performance

# COMMAND ----------

# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------
# MAGIC %md
# MAGIC # Hotel Analytics Aggregations (Corrected)
# MAGIC - Uses nightly expansion so daily RevPAR & Occupancy are correct
# MAGIC - Preserves hotels with no bookings (where appropriate)
# MAGIC - Avoids double-counting in region rollups
# MAGIC - Guards against divide-by-zero

# COMMAND ----------
# Common date anchors (adjust if needed)
ytd_start = '2024-01-01'

# ---------- Helpers ----------
# Explode bookings into nightly grain so room-nights, revenue, ADR, RevPAR are correct
spark.sql(f"""
CREATE OR REPLACE VIEW main.hotel_inc.v_bookings_nightly AS
WITH base AS (
  SELECT
    b.booking_id,
    b.hotel_id,
    b.guest_id,
    b.status,
    b.channel,
    b.booking_date,
    b.check_in,
    -- If you have check_out, use DATEDIFF(check_out, check_in); else trust length_of_stay
    CASE
      WHEN b.length_of_stay IS NOT NULL AND b.length_of_stay > 0 THEN b.length_of_stay
      ELSE 1
    END AS los,
    b.room_rate,            -- assumed nightly rate
    b.lead_time_days
  FROM main.hotel_inc.bookings b
),
exploded AS (
  SELECT
    booking_id, hotel_id, guest_id, status, channel, booking_date, check_in, los, room_rate, lead_time_days,
    CAST(n AS DATE) AS business_date
  FROM base
  LATERAL VIEW posexplode(sequence(check_in, date_add(check_in, los - 1))) s AS pos, n
)
SELECT
  booking_id, hotel_id, guest_id, status, channel, booking_date,
  business_date,
  room_rate        AS nightly_rate,
  CASE WHEN status = 'Confirmed' THEN 1 ELSE 0 END AS room_night,
  CASE WHEN status = 'Confirmed' THEN room_rate ELSE 0 END AS nightly_revenue,
  lead_time_days
FROM exploded
""")

# COMMAND ----------
# Daily hotel performance (correct nightly math; keep hotels even with no stays)
spark.sql(f"""
CREATE OR REPLACE VIEW main.hotel_inc.daily_hotel_performance AS
WITH nightly AS (
  SELECT
    hotel_id,
    business_date,
    SUM(room_night)                AS rooms_sold,            -- room-nights sold
    SUM(nightly_revenue)          AS room_revenue,          -- revenue at nightly grain
    AVG(NULLIF(nightly_rate,0))   AS adr_sample,            -- per-night average (for reference)
    AVG(CASE WHEN room_night=1 THEN lead_time_days END) AS avg_lead_time
  FROM main.hotel_inc.v_bookings_nightly
  WHERE business_date >= DATE('{ytd_start}')
  GROUP BY hotel_id, business_date
)
SELECT
  h.hotel_id,
  h.hotel_name,
  h.brand,
  h.city,
  h.state,
  h.market_segment,
  h.room_count,               -- assumed static room inventory
  h.base_adr,
  d.business_date,
  COALESCE(n.rooms_sold, 0)                       AS rooms_sold,
  COALESCE(n.room_revenue, 0.0)                   AS room_revenue,
  -- ADR = revenue / room-nights sold (safe divide)
  CASE WHEN COALESCE(n.rooms_sold,0) > 0 THEN ROUND(n.room_revenue / n.rooms_sold, 2) ELSE NULL END AS adr,
  n.avg_lead_time,
  -- Occupancy = room-nights sold / room inventory (per day)
  CASE WHEN h.room_count > 0 THEN ROUND( (COALESCE(n.rooms_sold,0) * 100.0) / h.room_count, 2) ELSE NULL END AS occupancy_rate,
  -- RevPAR = revenue / room inventory (per day)
  CASE WHEN h.room_count > 0 THEN ROUND( COALESCE(n.room_revenue,0.0) / h.room_count, 2) ELSE NULL END AS revpar,
  -- Rate index vs base_adr
  CASE WHEN h.base_adr > 0 AND COALESCE(n.rooms_sold,0) > 0
       THEN ROUND( ( (n.room_revenue / n.rooms_sold) / h.base_adr - 1) * 100, 2)
       ELSE NULL END AS rate_index,
  DATE_TRUNC('month', d.business_date)            AS report_month
FROM main.hotel_inc.hotels h
-- build a date spine from bookings nightly to avoid cross-join calendars
LEFT JOIN (
  SELECT DISTINCT business_date FROM main.hotel_inc.v_bookings_nightly WHERE business_date >= DATE('{ytd_start}')
) d ON 1=1
LEFT JOIN nightly n
  ON h.hotel_id = n.hotel_id
 AND d.business_date = n.business_date
WHERE d.business_date IS NOT NULL
""")

print("Created main.hotel_inc.daily_hotel_performance")

# COMMAND ----------
# Monthly brand performance (aggregate from corrected daily view)
spark.sql("""
CREATE OR REPLACE VIEW main.hotel_inc.monthly_brand_performance AS
SELECT 
  DATE_TRUNC('month', business_date) AS report_month,
  brand,
  COUNT(DISTINCT hotel_id)                             AS hotel_count,
  SUM(rooms_sold)                                      AS total_room_nights_sold,
  SUM(room_count)                                      AS total_room_inventory_days, -- inventory * days counted
  SUM(room_revenue)                                    AS total_revenue,
  ROUND(AVG(occupancy_rate), 2)                        AS avg_occupancy,
  ROUND(AVG(adr), 2)                                   AS avg_adr,
  ROUND(AVG(revpar), 2)                                AS avg_revpar,
  -- portfolio RevPAR computed from totals (not avg of avgs)
  ROUND(SUM(room_revenue) / NULLIF(SUM(room_count),0), 2) AS portfolio_revpar
FROM main.hotel_inc.daily_hotel_performance
GROUP BY DATE_TRUNC('month', business_date), brand
ORDER BY report_month DESC, total_revenue DESC
""")

print("Created main.hotel_inc.monthly_brand_performance")

# COMMAND ----------
# Guest loyalty analysis (rename to YTD unless you remove the date filter)
spark.sql(f"""
CREATE OR REPLACE VIEW main.hotel_inc.guest_loyalty_analysis AS
WITH guest_metrics AS (
  SELECT 
    g.guest_id,
    g.loyalty_tier,
    g.guest_segment,
    g.member_since,
    COUNT(bn.booking_id)                                          AS total_stays,         -- count of stay-nights or stays? choose nights or distinct booking_id
    SUM(bn.nightly_revenue)                                       AS ytd_revenue,
    AVG(CASE WHEN bn.room_night = 1 THEN bn.nightly_rate END)     AS avg_rate_paid,
    AVG(CASE WHEN bn.room_night = 1 THEN 1 END)                   AS avg_los_stub,        -- replace with proper LOS if needed
    MAX(bn.business_date)                                         AS last_stay_date,
    DATEDIFF(CURRENT_DATE(), MAX(bn.business_date))               AS days_since_last_stay
  FROM main.hotel_inc.guests g
  LEFT JOIN main.hotel_inc.v_bookings_nightly bn
    ON g.guest_id = bn.guest_id
   AND bn.business_date >= DATE('{ytd_start}')
  GROUP BY g.guest_id, g.loyalty_tier, g.guest_segment, g.member_since
)
SELECT 
  loyalty_tier,
  guest_segment,
  COUNT(*)                                        AS guest_count,
  AVG(total_stays)                                AS avg_stays_per_guest,
  AVG(ytd_revenue)                                AS avg_ytd_revenue,
  AVG(avg_rate_paid)                              AS avg_rate_paid,
  AVG(avg_los_stub)                               AS avg_length_of_stay, -- if you track LOS at booking-level, recompute properly
  COUNT(CASE WHEN days_since_last_stay <= 90 THEN 1 END)               AS active_guests_90d,
  ROUND(COUNT(CASE WHEN days_since_last_stay <= 90 THEN 1 END) * 100.0 / COUNT(*), 2) AS retention_rate_90d
FROM guest_metrics
WHERE total_stays > 0
GROUP BY loyalty_tier, guest_segment
""")

print("Created main.hotel_inc.guest_loyalty_analysis")

# COMMAND ----------
# Channel performance (monthly)
spark.sql(f"""
CREATE OR REPLACE VIEW main.hotel_inc.channel_performance AS
SELECT 
  DATE_TRUNC('month', bn.business_date)                   AS report_month,
  bn.channel,
  COUNT(DISTINCT bn.booking_id)                           AS bookings,         -- bookings counted once per booking_id
  SUM(bn.nightly_revenue)                                 AS revenue,
  -- ADR from nightly grain
  SUM(bn.nightly_revenue) / NULLIF(SUM(bn.room_night),0)  AS avg_adr,
  AVG(bn.lead_time_days)                                  AS avg_lead_time,
  ROUND(
    COUNT(CASE WHEN b.status = 'Cancelled' THEN 1 END) * 100.0 / 
    NULLIF(COUNT(CASE WHEN b.status IN ('Confirmed','Cancelled') THEN 1 END), 0)
  , 2) AS cancellation_rate
FROM main.hotel_inc.v_bookings_nightly bn
LEFT JOIN main.hotel_inc.bookings b
  ON bn.booking_id = b.booking_id
WHERE bn.business_date >= DATE('{ytd_start}')
GROUP BY DATE_TRUNC('month', bn.business_date), bn.channel
ORDER BY report_month DESC, revenue DESC
""")

print("Created main.hotel_inc.channel_performance")

# COMMAND ----------
# Regional market analysis (no double-counting rooms)
spark.sql(f"""
CREATE OR REPLACE VIEW main.hotel_inc.regional_analysis AS
WITH daily AS (
  SELECT
    state,
    market_segment,
    business_date,
    SUM(room_revenue)                         AS revenue,
    SUM(rooms_sold)                           AS room_nights,
    SUM(room_count)                           AS room_inventory
  FROM main.hotel_inc.daily_hotel_performance
  WHERE business_date >= DATE('{ytd_start}')
  GROUP BY state, market_segment, business_date
),
state_perf AS (
  SELECT
    state,
    market_segment,
    SUM(revenue)                              AS ytd_revenue,
    SUM(room_nights)                          AS ytd_room_nights,
    SUM(room_inventory)                       AS ytd_room_inventory,
    AVG(CASE WHEN room_inventory > 0 THEN (room_nights * 100.0) / room_inventory END) AS avg_occupancy
  FROM daily
  GROUP BY state, market_segment
),
counts AS (
  SELECT state, market_segment, COUNT(DISTINCT hotel_id) AS hotel_count
  FROM main.hotel_inc.hotels
  GROUP BY state, market_segment
)
SELECT
  sp.state,
  sp.market_segment,
  c.hotel_count,
  sp.ytd_revenue,
  sp.ytd_room_nights,
  sp.ytd_room_inventory,
  ROUND(sp.avg_occupancy, 2)                     AS avg_occupancy,
  CASE WHEN sp.ytd_room_inventory > 0
       THEN ROUND(sp.ytd_revenue / sp.ytd_room_inventory, 2) END AS revpar,
  CASE WHEN c.hotel_count > 0
       THEN ROUND(sp.ytd_revenue / c.hotel_count, 2) END AS revenue_per_hotel
FROM state_perf sp
JOIN counts c
  ON sp.state = c.state AND sp.market_segment = c.market_segment
WHERE sp.ytd_revenue > 0
ORDER BY sp.ytd_revenue DESC
""")

print("Created main.hotel_inc.regional_analysis")

# COMMAND ----------
# Executive KPIs (safe divides)
spark.sql("""
CREATE OR REPLACE VIEW main.hotel_inc.executive_kpis AS
WITH cur AS (
  SELECT 
    COUNT(DISTINCT hotel_id) AS active_hotels,
    SUM(rooms_sold)          AS rooms_sold,
    SUM(room_count)          AS available_rooms,
    SUM(room_revenue)        AS total_revenue,
    AVG(occupancy_rate)      AS avg_occupancy,
    AVG(adr)                 AS avg_adr,
    AVG(revpar)              AS avg_revpar
  FROM main.hotel_inc.daily_hotel_performance
  WHERE DATE_TRUNC('month', business_date) = DATE_TRUNC('month', CURRENT_DATE() - INTERVAL 1 MONTH)
),
pr AS (
  SELECT 
    SUM(room_revenue)   AS prior_revenue,
    AVG(occupancy_rate) AS prior_occupancy,
    AVG(adr)            AS prior_adr,
    AVG(revpar)         AS prior_revpar
  FROM main.hotel_inc.daily_hotel_performance
  WHERE DATE_TRUNC('month', business_date) = DATE_TRUNC('month', CURRENT_DATE() - INTERVAL 2 MONTH)
)
SELECT 
  c.*,
  CASE WHEN pr.prior_revenue   IS NOT NULL AND pr.prior_revenue   <> 0 THEN ROUND((c.total_revenue - pr.prior_revenue) / pr.prior_revenue * 100, 2) END AS revenue_growth,
  CASE WHEN pr.prior_occupancy IS NOT NULL THEN ROUND(c.avg_occupancy - pr.prior_occupancy, 2) END AS occupancy_change,
  CASE WHEN pr.prior_adr       IS NOT NULL AND pr.prior_adr       <> 0 THEN ROUND((c.avg_adr - pr.prior_adr) / pr.prior_adr * 100, 2) END AS adr_growth,
  CASE WHEN pr.prior_revpar    IS NOT NULL AND pr.prior_revpar    <> 0 THEN ROUND((c.avg_revpar - pr.prior_revpar) / pr.prior_revpar * 100, 2) END AS revpar_growth
FROM cur c CROSS JOIN pr
""")

print("Created main.hotel_inc.executive_kpis")

# COMMAND ----------
# Operational metrics (unchanged logic, sourced from corrected daily view)
spark.sql(f"""
CREATE OR REPLACE VIEW main.hotel_inc.operational_metrics AS
SELECT 'Portfolio Summary' AS metric_category, 'Total Hotels' AS metric_name, COUNT(DISTINCT hotel_id) AS metric_value, 'count' AS metric_type
FROM main.hotel_inc.hotels
UNION ALL
SELECT 'Portfolio Summary','Total Rooms', SUM(room_count), 'count' FROM main.hotel_inc.hotels
UNION ALL
SELECT 'Performance','YTD Revenue', SUM(room_revenue), 'currency'
FROM main.hotel_inc.daily_hotel_performance
WHERE business_date >= DATE('{ytd_start}')
UNION ALL
SELECT 'Performance','Portfolio Occupancy', AVG(occupancy_rate), 'percentage'
FROM main.hotel_inc.daily_hotel_performance
WHERE business_date >= DATE('{ytd_start}')
UNION ALL
SELECT 'Performance','Portfolio ADR', AVG(adr), 'currency'
FROM main.hotel_inc.daily_hotel_performance
WHERE business_date >= DATE('{ytd_start}')
UNION ALL
SELECT 'Performance','Portfolio RevPAR', AVG(revpar), 'currency'
FROM main.hotel_inc.daily_hotel_performance
WHERE business_date >= DATE('{ytd_start}')
""")

print("Created main.hotel_inc.operational_metrics")

# COMMAND ----------
print("All corrected analytics views created successfully!")
print("\nAvailable views:")
print("- main.hotel_inc.v_bookings_nightly")
print("- main.hotel_inc.daily_hotel_performance")  
print("- main.hotel_inc.monthly_brand_performance")
print("- main.hotel_inc.guest_loyalty_analysis  (YTD)")
print("- main.hotel_inc.channel_performance")
print("- main.hotel_inc.regional_analysis")
print("- main.hotel_inc.executive_kpis")
print("- main.hotel_inc.operational_metrics")


# COMMAND ----------

# Create monthly brand performance summary
monthly_brand_performance = spark.sql("""
    CREATE OR REPLACE VIEW main.hotel_inc.monthly_brand_performance AS
    SELECT 
        DATE_TRUNC('month', business_date) as report_month,
        brand,
        COUNT(DISTINCT hotel_id) as hotel_count,
        SUM(rooms_sold) as total_rooms_sold,
        SUM(room_count) as total_room_inventory,
        SUM(room_revenue) as total_revenue,
        ROUND(AVG(occupancy_rate), 2) as avg_occupancy,
        ROUND(AVG(adr), 2) as avg_adr,
        ROUND(AVG(revpar), 2) as avg_revpar,
        ROUND(SUM(room_revenue) / SUM(room_count), 2) as portfolio_revpar
    FROM main.hotel_inc.daily_hotel_performance
    GROUP BY DATE_TRUNC('month', business_date), brand
    ORDER BY report_month DESC, total_revenue DESC
""")

print("Created monthly_brand_performance view")

# COMMAND ----------

# Create guest loyalty analysis
guest_loyalty_analysis = spark.sql("""
    CREATE OR REPLACE VIEW main.hotel_inc.guest_loyalty_analysis AS
    WITH guest_metrics AS (
        SELECT 
            g.guest_id,
            g.loyalty_tier,
            g.guest_segment,
            g.member_since,
            COUNT(b.booking_id) as total_stays,
            SUM(CASE WHEN b.status = 'Confirmed' THEN b.room_rate * b.length_of_stay ELSE 0 END) as lifetime_revenue,
            AVG(CASE WHEN b.status = 'Confirmed' THEN b.room_rate END) as avg_rate_paid,
            AVG(CASE WHEN b.status = 'Confirmed' THEN b.length_of_stay END) as avg_los,
            MAX(b.check_in) as last_stay_date,
            DATEDIFF(CURRENT_DATE(), MAX(b.check_in)) as days_since_last_stay
        FROM main.hotel_inc.guests g
        LEFT JOIN main.hotel_inc.bookings b ON g.guest_id = b.guest_id
        WHERE b.booking_date >= '2024-01-01'
        GROUP BY g.guest_id, g.loyalty_tier, g.guest_segment, g.member_since
    )
    SELECT 
        loyalty_tier,
        guest_segment,
        COUNT(*) as guest_count,
        AVG(total_stays) as avg_stays_per_guest,
        AVG(lifetime_revenue) as avg_lifetime_revenue,
        AVG(avg_rate_paid) as avg_rate_paid,
        AVG(avg_los) as avg_length_of_stay,
        COUNT(CASE WHEN days_since_last_stay <= 90 THEN 1 END) as active_guests_90d,
        ROUND(COUNT(CASE WHEN days_since_last_stay <= 90 THEN 1 END) * 100.0 / COUNT(*), 2) as retention_rate_90d
    FROM guest_metrics
    WHERE total_stays > 0
    GROUP BY loyalty_tier, guest_segment
""")

print("Created guest_loyalty_analysis view")

# COMMAND ----------

# Create channel performance metrics
channel_performance = spark.sql("""
    CREATE OR REPLACE VIEW main.hotel_inc.channel_performance AS
    SELECT 
        DATE_TRUNC('month', b.check_in) as report_month,
        b.channel,
        COUNT(CASE WHEN b.status = 'Confirmed' THEN 1 END) as bookings,
        SUM(CASE WHEN b.status = 'Confirmed' THEN b.room_rate * b.length_of_stay ELSE 0 END) as revenue,
        AVG(CASE WHEN b.status = 'Confirmed' THEN b.room_rate END) as avg_adr,
        AVG(CASE WHEN b.status = 'Confirmed' THEN b.length_of_stay END) as avg_los,
        AVG(CASE WHEN b.status = 'Confirmed' THEN b.lead_time_days END) as avg_lead_time,
        ROUND(COUNT(CASE WHEN b.status = 'Cancelled' THEN 1 END) * 100.0 / 
              COUNT(CASE WHEN b.status IN ('Confirmed', 'Cancelled') THEN 1 END), 2) as cancellation_rate
    FROM main.hotel_inc.bookings b
    WHERE b.check_in >= '2024-01-01'
    GROUP BY DATE_TRUNC('month', b.check_in), b.channel
    ORDER BY report_month DESC, revenue DESC
""")

print("Created channel_performance view")

# COMMAND ----------

# Create regional market analysis
regional_analysis = spark.sql("""
    CREATE OR REPLACE VIEW main.hotel_inc.regional_analysis AS
    WITH state_performance AS (
        SELECT 
            h.state,
            COUNT(DISTINCT h.hotel_id) as hotel_count,
            h.market_segment,
            SUM(CASE WHEN b.status = 'Confirmed' AND b.check_in >= '2024-01-01' 
                THEN b.room_rate * b.length_of_stay ELSE 0 END) as ytd_revenue,
            COUNT(CASE WHEN b.status = 'Confirmed' AND b.check_in >= '2024-01-01' THEN 1 END) as ytd_bookings,
            AVG(CASE WHEN b.status = 'Confirmed' AND b.check_in >= '2024-01-01' THEN b.room_rate END) as avg_adr,
            SUM(h.room_count) as total_rooms
        FROM main.hotel_inc.hotels h
        LEFT JOIN main.hotel_inc.bookings b ON h.hotel_id = b.hotel_id
        GROUP BY h.state, h.market_segment
    )
    SELECT *,
        ROUND(ytd_revenue / total_rooms, 2) as revpar,
        ROUND(ytd_revenue / hotel_count, 2) as revenue_per_hotel
    FROM state_performance
    WHERE ytd_revenue > 0
    ORDER BY ytd_revenue DESC
""")

print("Created regional_analysis view")

# COMMAND ----------

# Create executive KPI summary
executive_kpis = spark.sql("""
    CREATE OR REPLACE VIEW main.hotel_inc.executive_kpis AS
    WITH current_month AS (
        SELECT 
            COUNT(DISTINCT hotel_id) as active_hotels,
            SUM(rooms_sold) as rooms_sold,
            SUM(room_count) as available_rooms,
            SUM(room_revenue) as total_revenue,
            AVG(occupancy_rate) as avg_occupancy,
            AVG(adr) as avg_adr,
            AVG(revpar) as avg_revpar
        FROM main.hotel_inc.daily_hotel_performance
        WHERE DATE_TRUNC('month', business_date) = DATE_TRUNC('month', CURRENT_DATE() - INTERVAL '1' MONTH)
    ),
    prior_month AS (
        SELECT 
            SUM(room_revenue) as prior_revenue,
            AVG(occupancy_rate) as prior_occupancy,
            AVG(adr) as prior_adr,
            AVG(revpar) as prior_revpar
        FROM main.hotel_inc.daily_hotel_performance
        WHERE DATE_TRUNC('month', business_date) = DATE_TRUNC('month', CURRENT_DATE() - INTERVAL '2' MONTH)
    )
    SELECT 
        c.*,
        ROUND((c.total_revenue - p.prior_revenue) / p.prior_revenue * 100, 2) as revenue_growth,
        ROUND(c.avg_occupancy - p.prior_occupancy, 2) as occupancy_change,
        ROUND((c.avg_adr - p.prior_adr) / p.prior_adr * 100, 2) as adr_growth,
        ROUND((c.avg_revpar - p.prior_revpar) / p.prior_revpar * 100, 2) as revpar_growth
    FROM current_month c
    CROSS JOIN prior_month p
""")

print("Created executive_kpis view")

# COMMAND ----------

# Create operational metrics for dashboard
operational_metrics = spark.sql("""
    CREATE OR REPLACE VIEW main.hotel_inc.operational_metrics AS
    SELECT 
        'Portfolio Summary' as metric_category,
        'Total Hotels' as metric_name,
        COUNT(DISTINCT hotel_id) as metric_value,
        'count' as metric_type
    FROM main.hotel_inc.hotels
    
    UNION ALL
    
    SELECT 
        'Portfolio Summary' as metric_category,
        'Total Rooms' as metric_name,
        SUM(room_count) as metric_value,
        'count' as metric_type
    FROM main.hotel_inc.hotels
    
    UNION ALL
    
    SELECT 
        'Performance' as metric_category,
        'YTD Revenue' as metric_name,
        SUM(room_revenue) as metric_value,
        'currency' as metric_type
    FROM main.hotel_inc.daily_hotel_performance
    WHERE business_date >= '2024-01-01'
    
    UNION ALL
    
    SELECT 
        'Performance' as metric_category,
        'Portfolio Occupancy' as metric_name,
        AVG(occupancy_rate) as metric_value,
        'percentage' as metric_type
    FROM main.hotel_inc.daily_hotel_performance
    WHERE business_date >= '2024-01-01'
    
    UNION ALL
    
    SELECT 
        'Performance' as metric_category,
        'Portfolio ADR' as metric_name,
        AVG(adr) as metric_value,
        'currency' as metric_type
    FROM main.hotel_inc.daily_hotel_performance
    WHERE business_date >= '2024-01-01'
    
    UNION ALL
    
    SELECT 
        'Performance' as metric_category,
        'Portfolio RevPAR' as metric_name,
        AVG(revpar) as metric_value,
        'currency' as metric_type
    FROM main.hotel_inc.daily_hotel_performance
    WHERE business_date >= '2024-01-01'
""")

print("Created operational_metrics view")

# COMMAND ----------

print("All analytics views created successfully!")
print("\nAvailable views:")
print("- main.hotel_inc.daily_hotel_performance")  
print("- main.hotel_inc.monthly_brand_performance")
print("- main.hotel_inc.guest_loyalty_analysis") 
print("- main.hotel_inc.channel_performance")
print("- main.hotel_inc.regional_analysis")
print("- main.hotel_inc.executive_kpis")
print("- main.hotel_inc.operational_metrics")
