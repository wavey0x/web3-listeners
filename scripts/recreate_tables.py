from sqlalchemy import create_engine, MetaData, text, inspect
import os
import sys
from dotenv import load_dotenv

# Add the parent directory to sys.path
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_dir)

from schemas.resupply_dao import create_tables

load_dotenv()

# Get database URI from environment
DATABASE_URI = os.getenv('DATABASE_URI')
if not DATABASE_URI:
    raise Exception("DATABASE_URI environment variable not set")

# Create engine
engine = create_engine(DATABASE_URI)
metadata = MetaData()

def recreate_tables():
    print("Starting table recreation...")
    
    # Drop existing tables if they exist
    with engine.connect() as conn:
        print("Dropping existing tables...")
        conn.execute(text("DROP TABLE IF EXISTS resupply_votes CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS resupply_proposals CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS votes CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS proposals CASCADE"))
        conn.commit()
        print("Existing tables dropped.")
    
    # Create new tables
    print("Creating new tables...")
    proposals_table, votes_table = create_tables(metadata)
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
    
    if 'resupply_proposals' in existing_tables and 'resupply_votes' in existing_tables:
        print("\nTables recreated successfully!")
    else:
        print("\nError: Tables were not created properly!")
        print("Expected tables 'resupply_proposals' and 'resupply_votes' but found:", existing_tables)

if __name__ == "__main__":
    recreate_tables() 