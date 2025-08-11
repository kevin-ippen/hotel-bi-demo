# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------
# MAGIC %md
# MAGIC # Hotel Analytics Aggregations
# MAGIC Creating pre-calculated metrics for dashboard performance

# COMMAND ----------
# Create daily hotel performance metrics
daily_hotel_performance = spark.sql("""
    CREATE OR REPLACE VIEW main.hotel_inc.daily_hotel_performance AS
    WITH daily_stats AS (
        SELECT 
            h.hotel_id,
            h.hotel_name,
            h.brand,
            h.city,
            h.state,
            h.market_segment,
            h.room_count,
            h.base_adr,
            b.check_in as business_date,
            COUNT(CASE WHEN b.status = 'Confirmed' THEN 1 END) as rooms_sold,
            COUNT(CASE WHEN b.status = 'Cancelled' THEN 1 END) as cancellations,
            COUNT(CASE WHEN b.status = 'No-show' THEN 1 END) as no_shows,
            SUM(CASE WHEN b.status = 'Confirmed' THEN b.room_rate * b.length_of_stay ELSE 0 END) as room_revenue,
            AVG(CASE WHEN b.status = 'Confirmed' THEN b.room_rate END) as adr,
            AVG(CASE WHEN b.status = 'Confirmed' THEN b.lead_time_days END) as avg_lead_time
        FROM main.hotel_inc.hotels h
        LEFT JOIN main.hotel_inc.bookings b ON h.hotel_id = b.hotel_id
        WHERE b.check_in >= '2024-01-01'
        GROUP BY h.hotel_id, h.hotel_name, h.brand, h.city, h.state, h.market_segment, h.room_count, h.base_adr, b.check_in
    )
    SELECT *,
        ROUND((rooms_sold * 1.0 / room_count) * 100, 2) as occupancy_rate,
        ROUND(room_revenue / room_count, 2) as revpar,
        ROUND((adr / base_adr - 1) * 100, 2) as rate_index
    FROM daily_stats
""")

print("Created daily_hotel_performance view")

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