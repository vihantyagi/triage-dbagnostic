# Dual Database Testing Guide: PostgreSQL & Oracle

This guide explains how to test the Triage machine learning system with both PostgreSQL (Docker) and Oracle databases using the enhanced example notebook.

## Prerequisites

- Docker installed on your Mac
- Python 3.8+ with Jupyter
- Oracle Cloud account (Free Tier works)
- Oracle Instant Client

## Database Setup

### 1. PostgreSQL Docker Setup

#### Start PostgreSQL Container
```bash
# Pull and run PostgreSQL Docker image
docker run --name triage-postgres \
  -e POSTGRES_DB=donors_choose \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  -d postgres:13

# Verify container is running
docker ps
```

#### Test PostgreSQL Connection
```bash
# Connect to verify setup
docker exec -it triage-postgres psql -U postgres -d donors_choose
```

### 2. Oracle Endpoint Setup

#### Install Oracle Instant Client (Mac)
```bash
# Using Homebrew
brew tap InstantClientTap/instantclient
brew install instantclient-basic
brew install instantclient-sqlplus

# Or download manually from Oracle website
# Extract to /usr/local/lib/instantclient_XX_X
```

#### Oracle Autonomous Database Wallet Setup
```bash
# 1. Extract wallet files
mkdir -p ~/oracle_wallet
unzip Wallet_your-db-name.zip -d ~/oracle_wallet

# 2. Set environment variables
export TNS_ADMIN=~/oracle_wallet
export WALLET_LOCATION=~/oracle_wallet

# 3. Verify wallet contents
ls ~/oracle_wallet
# Should contain: tnsnames.ora, sqlnet.ora, cwallet.sso, ewallet.p12, etc.
```

#### Oracle Cloud Connection Details
```python
# For Autonomous Database with wallet
ORACLE_CONNECTION = {
    'service_name': 'your_service_name_high',  # From tnsnames.ora
    'username': 'your_username',
    'password': 'your_password'
}

# Legacy format (for non-autonomous databases)
LEGACY_ORACLE_CONNECTION = {
    'host': 'your-oracle-host.oraclecloud.com',
    'port': 1521,
    'service_name': 'your_service_name',
    'username': 'your_username',
    'password': 'your_password'
}
```

## Environment Setup

### 1. Install Dependencies
```bash
pip install -r requirements.txt
pip install cx_Oracle  # Oracle driver
pip install psycopg2-binary  # PostgreSQL driver
```

### 2. Environment Variables
```bash
# Create .env file
cat > .env << EOF
# PostgreSQL (Docker)
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=donors_choose
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres

# Oracle Cloud (Autonomous Database with wallet)
TNS_ADMIN=~/oracle_wallet
WALLET_LOCATION=~/oracle_wallet
ORACLE_SERVICE=your_service_name_high
ORACLE_USER=your_username
ORACLE_PASSWORD=your_password
EOF
```

## Running the Enhanced Example Notebook

### 1. Start Jupyter
```bash
cd /Users/vihantyagi/CMU/Research/triage
jupyter notebook example/colab/colab_triage_test.ipynb
```

### 2. Database Connection Setup (First Cell)
```python
import os
from sqlalchemy import create_engine
from triage.component.database import PostgreSQLAdapter, OracleAdapter

# PostgreSQL connection (Docker)
pg_connection_string = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
pg_engine = create_engine(pg_connection_string)
pg_adapter = PostgreSQLAdapter(pg_engine)

# Oracle connection (Autonomous Database with wallet)
oracle_connection_string = f"oracle+cx_oracle://{os.getenv('ORACLE_USER')}:{os.getenv('ORACLE_PASSWORD')}@{os.getenv('ORACLE_SERVICE')}"
oracle_engine = create_engine(oracle_connection_string)
oracle_adapter = OracleAdapter(oracle_engine)

print("✅ PostgreSQL connection:", pg_engine.connect().close() or "Success")
print("✅ Oracle connection:", oracle_engine.connect().close() or "Success")
```

### 3. Dual Database Testing Workflow

The notebook follows this pattern for each section:

```python
# 1. Setup data in PostgreSQL
def setup_postgresql_data():
    """Create test data in PostgreSQL"""
    with pg_engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS data"))
        # ... setup tables and data

# 2. Transfer data to Oracle
def transfer_to_oracle():
    """Transfer PostgreSQL data to Oracle with type conversion"""
    # Read from PostgreSQL
    df = pd.read_sql("SELECT * FROM some_table", pg_engine)

    # Convert data types for Oracle compatibility
    # JSONB -> TEXT, arrays -> comma-separated strings

    # Write to Oracle
    df.to_sql('some_table', oracle_engine, if_exists='replace')

# 3. Run comparison queries
def compare_databases(query_name, pg_query, oracle_query=None):
    """Run same query on both databases and compare results"""
    if oracle_query is None:
        oracle_query = pg_query  # Use same query if compatible

    print(f"\n🔍 Testing: {query_name}")

    # PostgreSQL result
    pg_result = pd.read_sql(pg_query, pg_engine)
    print(f"PostgreSQL: {len(pg_result)} rows")

    # Oracle result
    oracle_result = pd.read_sql(oracle_query, oracle_engine)
    print(f"Oracle: {len(oracle_result)} rows")

    # Compare results
    if len(pg_result) == len(oracle_result):
        print("✅ Row counts match")
    else:
        print("❌ Row count mismatch")

    return pg_result, oracle_result
```

### 4. Database Adapter Testing
```python
# Test database-specific SQL generation
timestamps = ['2020-01-01', '2020-02-01', '2020-03-01']

pg_array_query = pg_adapter.format_timestamp_array_query(timestamps)
oracle_array_query = oracle_adapter.format_timestamp_array_query(timestamps)

print("PostgreSQL array query:", pg_array_query)
print("Oracle array query:", oracle_array_query)

# Test in actual queries
pg_query = f"SELECT * FROM data.donations WHERE donation_timestamp IN {pg_array_query}"
oracle_query = f"SELECT * FROM data_donations WHERE donation_timestamp IN {oracle_array_query}"

compare_databases("Timestamp Array Filtering", pg_query, oracle_query)
```

### 5. Triage Experiment Testing
```python
from triage.experiments import SingleThreadedExperiment

# PostgreSQL experiment
pg_experiment = SingleThreadedExperiment(
    config=experiment_config,
    db_engine=pg_engine,
    db_adapter=pg_adapter  # Pass adapter
)

# Oracle experiment (after data transfer)
oracle_experiment = SingleThreadedExperiment(
    config=experiment_config,
    db_engine=oracle_engine,
    db_adapter=oracle_adapter  # Pass adapter
)

# Run both and compare results
pg_experiment.run()
oracle_experiment.run()
```

## Testing Sections

The notebook covers these key areas:

1. **Database Connection & Setup**
   - Verify both database connections
   - Test adapter functionality

2. **Data Exploration (matching colab notebook)**
   - `data.projects` table analysis
   - `data.essays`, `data.resources`, `data.donations` exploration
   - Compare table structures between databases

3. **Cohort and Label Definition**
   - Test cohort queries matching colab example
   - Validate label generation across databases

4. **Feature Engineering**
   - Test feature aggregation queries
   - Validate feature matrix creation using `data.*` tables

5. **Model Training & Evaluation**
   - Run triage experiments on both databases
   - Compare model performance metrics

6. **Results Analysis**
   - Compare prediction results
   - Validate metric calculations

## Troubleshooting

### PostgreSQL Docker Issues
```bash
# Check container logs
docker logs triage-postgres

# Restart container
docker restart triage-postgres

# Reset database
docker exec -it triage-postgres dropdb -U postgres donors_choose
docker exec -it triage-postgres createdb -U postgres donors_choose
```

### Oracle Connection Issues
```bash
# Test Oracle connection
sqlplus username/password@host:port/service_name

# Check Instant Client installation
echo $DYLD_LIBRARY_PATH  # Should include instantclient path
```

### Common Data Type Issues
- **JSONB → TEXT**: Use `json.dumps()` for Oracle storage
- **Arrays → Strings**: Use comma-separated format for Oracle
- **Timestamps**: Ensure consistent format across databases
- **Boolean → Number**: Oracle uses 1/0 for boolean values

## Expected Results

After running the complete notebook:

1. **Schema Compatibility**: Both databases should have equivalent schemas
2. **Data Integrity**: Row counts and key metrics should match
3. **Query Performance**: Oracle may be slower but results should be identical
4. **Model Results**: Model performance should be nearly identical between databases

## Performance Monitoring

```python
import time

def time_query(query, engine, description):
    start = time.time()
    result = pd.read_sql(query, engine)
    duration = time.time() - start
    print(f"{description}: {duration:.2f}s ({len(result)} rows)")
    return result, duration

# Compare query performance
pg_result, pg_time = time_query(query, pg_engine, "PostgreSQL")
oracle_result, oracle_time = time_query(query, oracle_engine, "Oracle")
print(f"Oracle is {oracle_time/pg_time:.1f}x slower")
```

This setup allows comprehensive testing of the database adapter implementation with real dual-database scenarios.