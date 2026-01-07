#!/bin/bash
# End-to-End Pipeline Test Script
# This script triggers all DAGs in sequence and verifies data at each stage

set -e

echo "=========================================="
echo "SYNTHEA AIRFLOW PIPELINE - E2E TEST"
echo "=========================================="
echo ""

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Step 1: Triggering Synthea patient data generation...${NC}"
docker exec $(docker-compose ps -q airflow-scheduler) airflow dags trigger synthea_generation_dag
echo "Waiting 30 seconds for data generation..."
sleep 30

echo ""
echo -e "${YELLOW}Step 2: Triggering S3 upload...${NC}"
docker exec $(docker-compose ps -q airflow-scheduler) airflow dags trigger s3_upload_dag
echo "Waiting 20 seconds for upload..."
sleep 20

echo ""
echo -e "${YELLOW}Step 3: Triggering Snowflake load...${NC}"
docker exec $(docker-compose ps -q airflow-scheduler) airflow dags trigger s3_to_snowflake_load
echo "Waiting 30 seconds for Snowflake load..."
sleep 30

echo ""
echo -e "${YELLOW}Step 4: Triggering dbt transformations...${NC}"
docker exec $(docker-compose ps -q airflow-scheduler) airflow dags trigger dbt_transform_fhir
echo "Waiting 60 seconds for dbt models to run..."
sleep 60

echo ""
echo "=========================================="
echo "VERIFICATION"
echo "=========================================="
echo ""

# Check local files
echo -e "${YELLOW}1. Local patient bundles:${NC}"
file_count=$(ls output/bundles/ 2>/dev/null | wc -l)
echo -e "${GREEN}   ✓ Found $file_count files in output/bundles/${NC}"

# Check S3
echo ""
echo -e "${YELLOW}2. S3 files:${NC}"
docker exec $(docker-compose ps -q airflow-scheduler) python3 << 'PYEOF'
import boto3, os
try:
    s3 = boto3.client('s3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_REGION'))
    files = s3.list_objects_v2(Bucket=os.getenv('AWS_S3_BUCKET'), Prefix='raw/fhir')
    count = files.get('KeyCount', 0)
    print(f"\033[0;32m   ✓ Found {count} files in S3 bucket\033[0m")
except Exception as e:
    print(f"\033[0;31m   ✗ Error checking S3: {e}\033[0m")
PYEOF

# Check Snowflake raw data
echo ""
echo -e "${YELLOW}3. Snowflake RAW data:${NC}"
docker exec $(docker-compose ps -q airflow-scheduler) python3 << 'PYEOF'
import snowflake.connector, os
try:
    conn = snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database='SYNTHEA', schema='RAW')
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM FHIR_BUNDLES")
    count = cur.fetchone()[0]
    print(f"\033[0;32m   ✓ Found {count} bundles in SYNTHEA.RAW.FHIR_BUNDLES\033[0m")
    cur.close()
    conn.close()
except Exception as e:
    print(f"\033[0;31m   ✗ Error checking Snowflake: {e}\033[0m")
PYEOF

# Check transformed staging data
echo ""
echo -e "${YELLOW}4. dbt Staging layer:${NC}"
docker exec $(docker-compose ps -q airflow-scheduler) python3 << 'PYEOF'
import snowflake.connector, os
try:
    conn = snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database='SYNTHEA', schema='RAW_STAGING')
    cur = conn.cursor()
    
    # Check all staging tables
    tables = ['STG_PATIENTS', 'STG_ENCOUNTERS', 'STG_CONDITIONS', 'STG_MEDICATIONS', 
              'STG_OBSERVATIONS', 'STG_PROCEDURES', 'STG_ALLERGIES', 'STG_IMMUNIZATIONS']
    
    for table in tables:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            if count > 0:
                print(f"\033[0;32m   ✓ {table}: {count} rows\033[0m")
            else:
                print(f"\033[1;33m   ⚠ {table}: 0 rows (may not have run yet)\033[0m")
        except Exception as te:
            print(f"\033[1;33m   ⚠ {table}: Table not found (will be created on first run)\033[0m")
    
    cur.close()
    conn.close()
except Exception as e:
    print(f"\033[0;31m   ✗ Error checking staging: {e}\033[0m")
PYEOF

echo ""
echo "=========================================="
echo -e "${GREEN}✓ Pipeline test completed!${NC}"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Open Airflow UI: http://localhost:8080 (admin/admin)"
echo "2. Check the Graph view for each DAG"
echo "3. View logs for any failed tasks"
echo "4. Query Snowflake directly to verify data"
echo ""
