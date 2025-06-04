import os
import glob
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
import argparse


def get_versioned_filename(base_path: str, base_name: str) -> str:
    """
    Generate a versioned Parquet filename like base_name_01.parquet, base_name_02.parquet.
    """
    i = 1
    while True:
        versioned_file = os.path.join(base_path, f"{base_name}_{i:02}.parquet")
        if not os.path.exists(versioned_file):
            return versioned_file
        i += 1


def process_entity_folder(
    entity: str, input_dir: str, output_dir: str, ingestion_date: str
) -> None:
    """
    Process a single entity folder: find latest JSON, convert to Parquet, save with version.
    """
    entity_input_path = os.path.join(input_dir, entity)
    json_files = glob.glob(os.path.join(entity_input_path, "*.json"))

    if not json_files:
        print(f"⚠️  No JSON files found in {entity_input_path}")
        return

    latest_file = max(json_files, key=os.path.getmtime)
    print(f"📄 Processing latest JSON from: {latest_file}")

    try:
        df = pd.read_json(latest_file)

        # Output path: output_dir/entity/ingestion_date=YYYY-MM-DD/
        partition_path = os.path.join(
            output_dir, entity, f"ingestion_date={ingestion_date}"
        )
        os.makedirs(partition_path, exist_ok=True)

        output_file = get_versioned_filename(partition_path, entity)
        table = pa.Table.from_pandas(df)
        pq.write_table(table, output_file)

        print(f"✅ Saved to: {output_file}")

    except Exception as e:
        print(f"❌ Error processing {latest_file}: {e}")


def convert_all_entities(input_dir: str, output_dir: str) -> None:
    """
    Iterate through all subdirectories (entities) and convert latest JSON to versioned Parquet.
    """
    ingestion_date = datetime.now().strftime("%Y-%m-%d")
    subdirs = [
        d for d in os.listdir(input_dir) if os.path.isdir(os.path.join(input_dir, d))
    ]

    if not subdirs:
        print(f"⚠️  No entity subfolders found in {input_dir}")
        return

    for entity in subdirs:
        process_entity_folder(entity, input_dir, output_dir, ingestion_date)

    print("🚀 All conversions done.")


def main():
    input_folder = "dw/source_bucket/json"
    output_folder = "dw/destination_bucket/bronze/"
    convert_all_entities(input_folder, output_folder)


if __name__ == "__main__":
    main()
