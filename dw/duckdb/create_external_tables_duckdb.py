import duckdb
import os
from pathlib import Path

def create_external_tables_from_bronze(bronze_dir, db_path):
    bronze_path = Path(bronze_dir)
    con = duckdb.connect(str(db_path))

    for entity_dir in bronze_path.iterdir():
        if entity_dir.is_dir():
            entity_name = entity_dir.name
            parquet_pattern = str(entity_dir / '**' / '*.parquet')

            print(f"📦 Creating external table for: {entity_name}")
            try:
                # Create external table
                con.execute(f"""
                    CREATE OR REPLACE TABLE {entity_name} AS
                    SELECT * FROM parquet_scan('{parquet_pattern}', hive_partitioning=1)
                """)
                print(f"✅ External table created: {entity_name}")
            except Exception as e:
                print(f"❌ Failed to create external table for {entity_name}: {e}")

    con.close()
    print(f"\n🚀 External tables created in DuckDB file: {db_path}")
    
    
def create_external_tables_from_silver(silver_dir, db_path):
    silver_path = Path(silver_dir)
    con = duckdb.connect(str(db_path))

    for entity_dir in silver_path.iterdir():
        if entity_dir.is_dir():
            entity_name = entity_dir.name
            parquet_pattern = str(entity_dir / '**' / '*.parquet')

            print(f"📦 Creating external table for: {entity_name}")
            try:
                # Create external table
                con.execute(f"""
                    CREATE OR REPLACE TABLE {entity_name} AS
                    SELECT * FROM parquet_scan('{parquet_pattern}', hive_partitioning=1)
                """)
                print(f"✅ External table created: {entity_name}")
            except Exception as e:
                print(f"❌ Failed to create external table for {entity_name}: {e}")

    con.close()
    print(f"\n🚀 External tables created in DuckDB file: {db_path}")
    
    
    
def main():
    bronze_layer = '../dw/destination_bucket/bronze'
    silver_layer = '../dw/destination_bucket/silver'
    duckdb_bronze = '../dw/duckdb/retail_olap_bronze.duckdb'
    duckdb_silver = '../dw/duckdb/retail_olap_silver.duckdb'
    create_external_tables_from_bronze(bronze_layer, duckdb_bronze)
    create_external_tables_from_silver(silver_layer, duckdb_silver)
    
    
if __name__ == "__main__":
    main()
    


    
