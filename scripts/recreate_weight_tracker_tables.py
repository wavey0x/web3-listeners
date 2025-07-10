from sqlalchemy import create_engine, MetaData, text, inspect
import os
import sys
from dotenv import load_dotenv

# Add the parent directory to sys.path
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_dir)

from schemas.weight_tracker import create_tables

load_dotenv()

# Get database URI from environment
DATABASE_URI = os.getenv('DATABASE_URI')
if not DATABASE_URI:
    raise Exception("DATABASE_URI environment variable not set")

# Create engine
engine = create_engine(DATABASE_URI)
metadata = MetaData()

def recreate_tables():
    print("Starting weight tracker table recreation...")
    
    # Drop existing tables if they exist
    with engine.connect() as conn:
        print("Dropping existing weight tracker tables...")
        conn.execute(text("DROP TABLE IF EXISTS weight_changes CASCADE"))
        conn.commit()
        print("Existing weight tracker tables dropped.")
    
    # Create new tables
    print("Creating new weight tracker tables...")
    weight_changes_table = create_tables(metadata)
    metadata.create_all(engine)
    
    # Verify tables were created
    inspector = inspect(engine)
    existing_tables = inspector.get_table_names()
    print("\nExisting tables in database:")
    for table in existing_tables:
        print(f"- {table}")
        columns = inspector.get_columns(table)
        print("  Columns:")
        for column in columns:
            print(f"    - {column['name']}: {column['type']}")
    
    if 'weight_changes' in existing_tables:
        print("\nWeight tracker tables recreated successfully!")
    else:
        print("\nError: Weight tracker tables were not created properly!")
        print("Expected table 'weight_changes' but found:", existing_tables)

if __name__ == "__main__":
    recreate_tables() 