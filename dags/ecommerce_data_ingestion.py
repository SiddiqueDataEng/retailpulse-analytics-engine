"""
RetailPulse Analytics Engine - E-commerce Data Ingestion DAG
Orchestrates multi-source e-commerce data collection and processing
"""

import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

# Import custom modules
import sys
sys.path.append('/opt/airflow/scripts')
from data_collectors.shopify_collector import ShopifyCollector
from data_collectors.google_analytics_collector import GoogleAnalyticsCollector
from data_collectors.facebook_ads_collector import FacebookAdsCollector
from data_collectors.email_marketing_collector import EmailMarketingCollector
from utils.snowflake_client import SnowflakeClient
from utils.data_quality_checker import EcommerceDataQualityChecker

# Configuration
SNOWFLAKE_CONN_ID = Variable.get("SNOWFLAKE_CONN_ID", default_var="snowflake_default")
SHOPIFY_STORES = Variable.get("SHOPIFY_STORES", default_var="store1.myshopify.com", deserialize_json=False).split(",")

# DAG Configuration
default_args = {
    'owner': 'retail-analytics-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

dag = DAG(
    'ecommerce_data_ingestion',
    default_args=default_args,
    description='E-commerce data ingestion and processing pipeline',
    schedule_interval=timedelta(hours=1),  # Run every hour
    max_active_runs=1,
    tags=['ecommerce', 'retail', 'analytics'],
)

def extract_shopify_data(**context) -> Dict[str, Any]:
    """Extract data from Shopify stores"""
    collector = ShopifyCollector()
    all_data = {}
    
    for store_url in SHOPIFY_STORES:
        try:
            store_name = store_url.replace('.myshopify.com', '')
            
            # Get orders from last 24 hours
            orders = collector.get_orders(store_url, hours_back=24)
            
            # Get customers updated in last 24 hours
            customers = collector.get_customers(store_url, hours_back=24)
            
            # Get products and inventory
            products = collector.get_products(store_url)
            inventory = collector.get_inventory_levels(store_url)
            
            # Get abandoned checkouts
            abandoned_checkouts = collector.get_abandoned_checkouts(store_url, hours_back=24)
            
            store_data = {
                'store_name': store_name,
                'store_url': store_url,
                'extraction_time': datetime.now().isoformat(),
                'orders': orders,
                'customers': customers,
                'products': products,
                'inventory': inventory,
                'abandoned_checkouts': abandoned_checkouts,
                'source': 'shopify'
            }
            
            all_data[store_name] = store_data
            logging.info(f"Extracted Shopify data for {store_name}: {len(orders)} orders, {len(customers)} customers")
            
        except Exception as e:
            logging.error(f"Error extracting Shopify data for {store_url}: {str(e)}")
            continue
    
    return all_data

def extract_google_analytics_data(**context) -> Dict[str, Any]:
    """Extract data from Google Analytics"""
    collector = GoogleAnalyticsCollector()
    
    try:
        # Get website traffic data
        traffic_data = collector.get_traffic_data(days_back=1)
        
        # Get e-commerce data
        ecommerce_data = collector.get_ecommerce_data(days_back=1)
        
        # Get conversion funnel data
        funnel_data = collector.get_conversion_funnel(days_back=1)
        
        # Get audience data
        audience_data = collector.get_audience_data(days_back=1)
        
        # Get acquisition data
        acquisition_data = collector.get_acquisition_data(days_back=1)
        
        analytics_data = {
            'extraction_time': datetime.now().isoformat(),
            'traffic': traffic_data,
            'ecommerce': ecommerce_data,
            'funnel': funnel_data,
            'audience': audience_data,
            'acquisition': acquisition_data,
            'source': 'google_analytics'
        }
        
        logging.info(f"Extracted Google Analytics data: {len(traffic_data)} sessions")
        return analytics_data
        
    except Exception as e:
        logging.error(f"Error extracting Google Analytics data: {str(e)}")
        return {}

def extract_facebook_ads_data(**context) -> Dict[str, Any]:
    """Extract data from Facebook Ads"""
    collector = FacebookAdsCollector()
    
    try:
        # Get campaign performance
        campaigns = collector.get_campaign_performance(days_back=1)
        
        # Get ad set performance
        ad_sets = collector.get_adset_performance(days_back=1)
        
        # Get ad performance
        ads = collector.get_ad_performance(days_back=1)
        
        # Get audience insights
        audience_insights = collector.get_audience_insights(days_back=7)
        
        facebook_data = {
            'extraction_time': datetime.now().isoformat(),
            'campaigns': campaigns,
            'ad_sets': ad_sets,
            'ads': ads,
            'audience_insights': audience_insights,
            'source': 'facebook_ads'
        }
        
        logging.info(f"Extracted Facebook Ads data: {len(campaigns)} campaigns, {len(ads)} ads")
        return facebook_data
        
    except Exception as e:
        logging.error(f"Error extracting Facebook Ads data: {str(e)}")
        return {}

def extract_email_marketing_data(**context) -> Dict[str, Any]:
    """Extract data from email marketing platforms"""
    collector = EmailMarketingCollector()
    
    try:
        # Get campaign data from Klaviyo
        klaviyo_campaigns = collector.get_klaviyo_campaigns(days_back=1)
        
        # Get email performance metrics
        email_metrics = collector.get_email_metrics(days_back=1)
        
        # Get subscriber data
        subscribers = collector.get_subscriber_data(days_back=1)
        
        # Get automation data
        automations = collector.get_automation_data(days_back=1)
        
        email_data = {
            'extraction_time': datetime.now().isoformat(),
            'campaigns': klaviyo_campaigns,
            'metrics': email_metrics,
            'subscribers': subscribers,
            'automations': automations,
            'source': 'email_marketing'
        }
        
        logging.info(f"Extracted email marketing data: {len(klaviyo_campaigns)} campaigns")
        return email_data
        
    except Exception as e:
        logging.error(f"Error extracting email marketing data: {str(e)}")
        return {}

def validate_data_quality(**context) -> bool:
    """Validate data quality using Great Expectations"""
    checker = EcommerceDataQualityChecker()
    
    # Pull data from previous tasks
    shopify_data = context['ti'].xcom_pull(task_ids='extract_shopify_data')
    analytics_data = context['ti'].xcom_pull(task_ids='extract_google_analytics_data')
    facebook_data = context['ti'].xcom_pull(task_ids='extract_facebook_ads_data')
    email_data = context['ti'].xcom_pull(task_ids='extract_email_marketing_data')
    
    validation_results = []
    
    # Validate each data source
    if shopify_data:
        shopify_validation = checker.validate_shopify_data(shopify_data)
        validation_results.append(shopify_validation)
    
    if analytics_data:
        analytics_validation = checker.validate_analytics_data(analytics_data)
        validation_results.append(analytics_validation)
    
    if facebook_data:
        facebook_validation = checker.validate_facebook_data(facebook_data)
        validation_results.append(facebook_validation)
    
    if email_data:
        email_validation = checker.validate_email_data(email_data)
        validation_results.append(email_validation)
    
    # Check if all validations passed
    all_passed = all(result['success'] for result in validation_results)
    
    if not all_passed:
        failed_validations = [r for r in validation_results if not r['success']]
        logging.warning(f"Data quality validation failed: {failed_validations}")
        
        # Store validation results for monitoring
        context['ti'].xcom_push(key='data_quality_issues', value=failed_validations)
    
    return all_passed

def load_to_snowflake(**context) -> None:
    """Load validated data to Snowflake"""
    snowflake_client = SnowflakeClient()
    
    # Pull data from previous tasks
    shopify_data = context['ti'].xcom_pull(task_ids='extract_shopify_data')
    analytics_data = context['ti'].xcom_pull(task_ids='extract_google_analytics_data')
    facebook_data = context['ti'].xcom_pull(task_ids='extract_facebook_ads_data')
    email_data = context['ti'].xcom_pull(task_ids='extract_email_marketing_data')
    
    try:
        # Load Shopify data
        if shopify_data:
            for store_name, store_data in shopify_data.items():
                snowflake_client.load_shopify_orders(store_data['orders'], store_name)
                snowflake_client.load_shopify_customers(store_data['customers'], store_name)
                snowflake_client.load_shopify_products(store_data['products'], store_name)
                snowflake_client.load_inventory_data(store_data['inventory'], store_name)
            logging.info("Loaded Shopify data to Snowflake")
        
        # Load Google Analytics data
        if analytics_data:
            snowflake_client.load_analytics_data(analytics_data)
            logging.info("Loaded Google Analytics data to Snowflake")
        
        # Load Facebook Ads data
        if facebook_data:
            snowflake_client.load_facebook_ads_data(facebook_data)
            logging.info("Loaded Facebook Ads data to Snowflake")
        
        # Load email marketing data
        if email_data:
            snowflake_client.load_email_marketing_data(email_data)
            logging.info("Loaded email marketing data to Snowflake")
            
    except Exception as e:
        logging.error(f"Error loading data to Snowflake: {str(e)}")
        raise

def trigger_dbt_run(**context) -> None:
    """Trigger dbt transformation run"""
    try:
        # This would typically be done via dbt Cloud API or dbt CLI
        # For now, we'll use a bash operator to run dbt
        logging.info("Triggering dbt transformation run")
        
        # The actual dbt run will be handled by the bash operator
        # This function serves as a placeholder for any pre-processing
        
    except Exception as e:
        logging.error(f"Error triggering dbt run: {str(e)}")
        raise

def update_customer_segments(**context) -> None:
    """Update customer segmentation based on latest data"""
    try:
        snowflake_client = SnowflakeClient()
        
        # Run customer segmentation query
        segmentation_query = """
        MERGE INTO ANALYTICS.CUSTOMER_SEGMENTS AS target
        USING (
            SELECT 
                customer_id,
                CASE 
                    WHEN recency <= 30 AND frequency >= 5 AND monetary >= 1000 THEN 'Champions'
                    WHEN recency <= 60 AND frequency >= 3 AND monetary >= 500 THEN 'Loyal Customers'
                    WHEN recency <= 90 AND frequency >= 2 AND monetary >= 200 THEN 'Potential Loyalists'
                    WHEN recency <= 180 AND frequency = 1 AND monetary >= 100 THEN 'New Customers'
                    WHEN recency > 180 AND frequency >= 2 THEN 'At Risk'
                    WHEN recency > 365 THEN 'Lost Customers'
                    ELSE 'Others'
                END AS segment,
                CURRENT_TIMESTAMP() AS updated_at
            FROM ANALYTICS.CUSTOMER_RFM_SCORES
        ) AS source
        ON target.customer_id = source.customer_id
        WHEN MATCHED THEN 
            UPDATE SET 
                segment = source.segment,
                updated_at = source.updated_at
        WHEN NOT MATCHED THEN 
            INSERT (customer_id, segment, updated_at)
            VALUES (source.customer_id, source.segment, source.updated_at)
        """
        
        snowflake_client.execute_query(segmentation_query)
        logging.info("Updated customer segments")
        
    except Exception as e:
        logging.error(f"Error updating customer segments: {str(e)}")
        raise

# Task definitions
with TaskGroup("data_extraction", dag=dag) as extraction_group:
    
    extract_shopify = PythonOperator(
        task_id='extract_shopify_data',
        python_callable=extract_shopify_data,
        pool='api_pool',
    )
    
    extract_analytics = PythonOperator(
        task_id='extract_google_analytics_data',
        python_callable=extract_google_analytics_data,
        pool='api_pool',
    )
    
    extract_facebook = PythonOperator(
        task_id='extract_facebook_ads_data',
        python_callable=extract_facebook_ads_data,
        pool='api_pool',
    )
    
    extract_email = PythonOperator(
        task_id='extract_email_marketing_data',
        python_callable=extract_email_marketing_data,
        pool='api_pool',
    )

validate_quality = PythonOperator(
    task_id='validate_data_quality',
    python_callable=validate_data_quality,
    dag=dag,
)

load_snowflake = PythonOperator(
    task_id='load_to_snowflake',
    python_callable=load_to_snowflake,
    dag=dag,
)

# dbt transformation
dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='cd /opt/airflow/dbt && dbt run --profiles-dir /opt/airflow/dbt/profiles',
    dag=dag,
)

dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command='cd /opt/airflow/dbt && dbt test --profiles-dir /opt/airflow/dbt/profiles',
    dag=dag,
)

update_segments = PythonOperator(
    task_id='update_customer_segments',
    python_callable=update_customer_segments,
    dag=dag,
)

# Great Expectations validation
run_great_expectations = BashOperator(
    task_id='run_great_expectations',
    bash_command='cd /opt/airflow/great_expectations && great_expectations checkpoint run ecommerce_data_checkpoint',
    dag=dag,
)

# Task dependencies
extraction_group >> validate_quality >> load_snowflake >> dbt_run >> dbt_test >> [update_segments, run_great_expectations]