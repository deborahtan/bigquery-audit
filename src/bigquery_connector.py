# ========================================
    # 5. ECOMMERCE VALIDATION
    # ========================================
    
    def get_ecommerce_validation(self, days_back: int = 7) -> Tuple[pd.DataFrame, str]:
        """
        Validate ecommerce events have required fields populated
        Critical for Woolworths revenue tracking
        """
        sql = f"""
        WITH ecommerce_events AS (
          SELECT 
            DATE(TIMESTAMP_MILLIS(event_timestamp)) as date,
            event_name,
            COUNT(*) as total_events,
            
            -- Check required ecommerce fields
            COUNTIF(event_name = 'purchase' AND purchase_revenue IS NULL) as missing_revenue,
            COUNTIF(event_name IN ('purchase', 'add_to_cart', 'view_item') AND item_name IS NULL) as missing_item_name,
            COUNTIF(event_name IN ('purchase', 'add_to_cart', 'view_item') AND item_price IS NULL) as missing_item_price,
            COUNTIF(event_name IN ('add_to_cart', 'view_cart') AND cart_value_total IS NULL) as missing_cart_value,
            COUNTIF(event_name IN ('add_to_cart', 'view_cart') AND cart_item_total IS NULL) as missing_cart_items,
            
            -- Revenue validation
            SUM(CAST(purchase_revenue AS FLOAT64)) as total_revenue,
            COUNT(DISTINCT CASE WHEN event_name = 'purchase' THEN session_id END) as unique_purchases
            
          FROM {self.full_table_id}
          WHERE DATE(TIMESTAMP_MILLIS(event_timestamp)) >= DATE_SUB(CURRENT_DATE(), INTERVAL {days_back} DAY)
            AND DATE(TIMESTAMP_MILLIS(event_timestamp)) < CURRENT_DATE()
            AND event_name IN ('purchase', 'add_to_cart', 'begin_checkout', 'view_item', 'view_cart')
          GROUP BY date, event_name
        )
        SELECT 
          date,
          event_name,
          total_events,
          missing_revenue,
          missing_item_name,
          missing_item_price,
          missing_cart_value,
          ROUND(missing_revenue / NULLIF(total_events, 0) * 100, 2) as revenue_null_rate,
          ROUND(missing_item_name / NULLIF(total_events, 0) * 100, 2) as item_name_null_rate,
          ROUND(total_revenue, 2) as total_revenue,
          unique_purchases,
          CASE 
            WHEN event_name = 'purchase' AND missing_revenue > 0 THEN '🔴 CRITICAL: Purchases missing revenue'
            WHEN event_name IN ('purchase', 'add_to_cart') AND missing_item_name / NULLIF(total_events, 0) > 0.1 THEN '🟡 WARNING: >10% missing item details'
            WHEN event_name IN ('add_to_cart', 'view_cart') AND missing_cart_value / NULLIF(total_events, 0) > 0.2 THEN '🟡 WARNING: >20% missing cart value'
            ELSE '✅ OK'
          END as validation_status
        FROM ecommerce_events
        WHERE total_events > 0
        ORDER BY date DESC, total_events DESC
        """
        
        return self.query_with_cache(
            query_type="ecommerce",
            sql=sql,
            params={"days_back": days_back}
        )
    
    # ========================================
    # 6. STORE & LOYALTY TRACKING
    # ========================================
    
    def get_store_tracking_health(self, days_back: int = 7) -> Tuple[pd.DataFrame, str]:
        """
        Check if store_name and loyalty_id are being tracked properly
        Critical for Woolworths store-level reporting
        """
        sql = f"""
        SELECT 
          DATE(TIMESTAMP_MILLIS(event_timestamp)) as date,
          COUNT(*) as total_events,
          COUNT(DISTINCT session_id) as total_sessions,
          
          -- Store tracking
          COUNTIF(store_name IS NOT NULL) as events_with_store,
          COUNT(DISTINCT store_name) as unique_stores,
          ROUND(COUNTIF(store_name IS NOT NULL) / COUNT(*) * 100, 2) as store_capture_rate,
          
          -- Loyalty tracking
          COUNTIF(loyalty_id IS NOT NULL) as events_with_loyalty,
          COUNT(DISTINCT loyalty_id) as unique_loyalty_members,
          ROUND(COUNTIF(loyalty_id IS NOT NULL) / COUNT(*) * 100, 2) as loyalty_capture_rate,
          
          CASE 
            WHEN COUNTIF(store_name IS NOT NULL) / COUNT(*) < 0.5 THEN '🟡 WARNING: <50% events have store_name'
            WHEN COUNTIF(loyalty_id IS NOT NULL) / COUNT(*) < 0.2 THEN '🟡 INFO: Low loyalty ID capture'
            ELSE '✅ OK'
          END as tracking_status
          
        FROM {self.full_table_id}
        WHERE DATE(TIMESTAMP_MILLIS(event_timestamp)) >= DATE_SUB(CURRENT_DATE(), INTERVAL {days_back} DAY)
          AND DATE(TIMESTAMP_MILLIS(event_timestamp)) < CURRENT_DATE()
        GROUP BY date
        ORDER BY date DESC
        """
        
        return self.query_with_cache(
            query_type="store_loyalty",
            sql=sql,
            params={"days_back": days_back}
        )
    
    # ========================================
    # 7. PROMOTION TRACKING
    # ========================================
    
    def get_promotion_tracking(self, days_back: int = 7) -> Tuple[pd.DataFrame, str]:
        """
        Check promotion tracking completeness
        """
        sql = f"""
        WITH promotion_events AS (
          SELECT 
            DATE(TIMESTAMP_MILLIS(event_timestamp)) as date,
            event_name,
            COUNT(*) as total_events,
            COUNTIF(promotion_id IS NOT NULL) as events_with_promo_id,
            COUNTIF(promotion_name IS NOT NULL) as events_with_promo_name,
            COUNT(DISTINCT promotion_id) as unique_promotions,
            COUNT(DISTINCT promotion_name) as unique_promo_names
          FROM {self.full_table_id}
          WHERE DATE(TIMESTAMP_MILLIS(event_timestamp)) >= DATE_SUB(CURRENT_DATE(), INTERVAL {days_back} DAY)
            AND DATE(TIMESTAMP_MILLIS(event_timestamp)) < CURRENT_DATE()
            AND event_name IN ('view_promotion', 'select_promotion', 'view_item', 'purchase')
          GROUP BY date, event_name
        )
        SELECT 
          date,
          event_name,
          total_events,
          events_with_promo_id,
          unique_promotions,
          ROUND(events_with_promo_id / NULLIF(total_events, 0) * 100, 2) as promo_capture_rate,
          CASE 
            WHEN event_name IN ('view_promotion', 'select_promotion') AND events_with_promo_id / NULLIF(total_events, 0) < 0.8 THEN '🟡 WARNING: <80% have promotion_id'
            ELSE '✅ OK'
          END as tracking_status
        FROM promotion_events
        ORDER BY date DESC, total_events DESC
        """
        
        return self.query_with_cache(
            query_type="promotion",
            sql=sql,
            params={"days_back": days_back}
        )
    
    # ========================================
    # 8. RECIPE & DIETARY TRACKING
    # ========================================
    
    def get_recipe_tracking(self, days_back: int = 7) -> Tuple[pd.DataFrame, str]:
        """
        Check recipe-related field population
        Unique to Woolworths content tracking
        """
        sql = f"""
        WITH recipe_events AS (
          SELECT 
            DATE(TIMESTAMP_MILLIS(event_timestamp)) as date,
            COUNT(*) as total_events,
            
            -- Recipe fields
            COUNTIF(cuisine IS NOT NULL) as events_with_cuisine,
            COUNTIF(course IS NOT NULL) as events_with_course,
            COUNTIF(dietary_requirement IS NOT NULL) as events_with_dietary,
            COUNTIF(main_ingredient IS NOT NULL) as events_with_ingredient,
            
            COUNT(DISTINCT cuisine) as unique_cuisines,
            COUNT(DISTINCT course) as unique_courses,
            COUNT(DISTINCT dietary_requirement) as unique_dietary
            
          FROM {self.full_table_id}
          WHERE DATE(TIMESTAMP_MILLIS(event_timestamp)) >= DATE_SUB(CURRENT_DATE(), INTERVAL {days_back} DAY)
            AND DATE(TIMESTAMP_MILLIS(event_timestamp)) < CURRENT_DATE()
            AND event_name LIKE '%recipe%'
          GROUP BY date
        )
        SELECT 
          date,
          total_events,
          events_with_cuisine,
          events_with_course,
          events_with_dietary,
          unique_cuisines,
          unique_courses,
          ROUND(events_with_cuisine / NULLIF(total_events, 0) * 100, 2) as cuisine_capture_rate,
          ROUND(events_with_course / NULLIF(total_events, 0) * 100, 2) as course_capture_rate,
          CASE 
            WHEN total_events > 0 AND events_with_cuisine / NULLIF(total_events, 0) < 0.5 THEN '🟡 WARNING: <50% recipe events have cuisine'
            WHEN total_events = 0 THEN '💡 INFO: No recipe events'
            ELSE '✅ OK'
          END as tracking_status
        FROM recipe_events
        ORDER BY date DESC
        """
        
        return self.query_with_cache(
            query_type="recipe",
            sql=sql,
            params={"days_back": days_back}
        )
    
    # ========================================
    # 9. DATA FRESHNESS
    # ========================================
    
    def get_data_freshness(self) -> Tuple[pd.DataFrame, str]:
        """
        Check how recent the data is - critical for real-time reporting
        """
        sql = f"""
        WITH latest_data AS (
          SELECT 
            MAX(TIMESTAMP_MILLIS(event_timestamp)) as latest_event_time,
            COUNT(*) as events_last_hour
          FROM {self.full_table_id}
          WHERE TIMESTAMP_MILLIS(event_timestamp) > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
        )
        SELECT 
          latest_event_time,
          TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), latest_event_time, MINUTE) as minutes_since_last_event,
          events_last_hour,
          CASE 
            WHEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), latest_event_time, MINUTE) > 60 THEN '🔴 CRITICAL: >1hr delay'
            WHEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), latest_event_time, MINUTE) > 30 THEN '🟡 WARNING: >30min delay'
            ELSE '✅ Fresh data'
          END as freshness_status
        FROM latest_data
        """
        
        return self.query_with_cache(
            query_type="freshness",
            sql=sql,
            params={}
        )
    
    # ========================================
    # 10. TOP EVENTS SUMMARY
    # ========================================
    
    def get_top_events(self, days_back: int = 7, limit: int = 20) -> Tuple[pd.DataFrame, str]:
        """
        Get summary of most frequent events
        """
        sql = f"""
        SELECT 
          event_name,
          COUNT(*) as event_count,
          COUNT(DISTINCT session_id) as unique_sessions,
          ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as pct_of_total
        FROM {self.full_table_id}
        WHERE DATE(TIMESTAMP_MILLIS(event_timestamp)) >= DATE_SUB(CURRENT_DATE(), INTERVAL {days_back} DAY)
          AND DATE(TIMESTAMP_MILLIS(event_timestamp)) < CURRENT_DATE()
        GROUP BY event_name
        ORDER BY event_count DESC
        LIMIT {limit}
        """
        
        return self.query_with_cache(
            query_type="top_events",
            sql=sql,
            params={"days_back": days_back, "limit": limit}
        )
    
    # ========================================
    # 11. TRAFFIC SOURCE VALIDATION
    # ========================================
    
    def get_traffic_source_health(self, days_back: int = 7) -> Tuple[pd.DataFrame, str]:
        """
        Check if traffic source tracking is working properly
        """
        sql = f"""
        SELECT 
          DATE(TIMESTAMP_MILLIS(event_timestamp)) as date,
          COUNT(*) as total_events,
          
          -- Source/Medium/Campaign tracking
          COUNTIF(source IS NULL) as null_source,
          COUNTIF(medium IS NULL) as null_medium,
          COUNTIF(campaign IS NULL) as null_campaign,
          
          ROUND(COUNTIF(source IS NULL) / COUNT(*) * 100, 2) as source_null_rate,
          ROUND(COUNTIF(medium IS NULL) / COUNT(*) * 100, 2) as medium_null_rate,
          ROUND(COUNTIF(campaign IS NULL) / COUNT(*) * 100, 2) as campaign_null_rate,
          
          -- Direct traffic check
          COUNTIF(source = '(direct)' AND medium = '(none)') as direct_traffic,
          ROUND(COUNTIF(source = '(direct)' AND medium = '(none)') / COUNT(*) * 100, 2) as direct_traffic_pct,
          
          CASE 
            WHEN COUNTIF(source IS NULL) / COUNT(*) > 0.2 THEN '🔴 CRITICAL: >20% missing source'
            WHEN COUNTIF(source = '(direct)' AND medium = '(none)') / COUNT(*) > 0.5 THEN '🟡 WARNING: >50% direct traffic (check referral exclusions)'
            ELSE '✅ OK'
          END as tracking_status
          
        FROM {self.full_table_id}
        WHERE DATE(TIMESTAMP_MILLIS(event_timestamp)) >= DATE_SUB(CURRENT_DATE(), INTERVAL {days_back} DAY)
          AND DATE(TIMESTAMP_MILLIS(event_timestamp)) < CURRENT_DATE()
        GROUP BY date
        ORDER BY date DESC
        """
        
        return self.query_with_cache(
            query_type="traffic_source",
            sql=sql,
            params={"days_back": days_back}
        )
    
    # ========================================
    # UTILITY METHODS
    # ========================================
    
    def clear_all_cache(self):
        """Clear all cached queries"""
        self.cache_manager.clear_cache()
    
    def get_cache_stats(self) -> dict:
        """Get cache statistics"""
        return self.cache_manager.get_cache_stats()
