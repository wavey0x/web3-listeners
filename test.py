from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

# Replace this with your actual database URI
DATABASE_URI = 'postgresql://wavey:kyR6weB6EPURv2qe@95.217.202.159:5432/ybs'

def test_database_connection(uri):
    try:
        # Create an SQLAlchemy engine instance
        engine = create_engine(uri)
        # Connect to the database
        connection = engine.connect()
        print("Database connection is successful!")
        
        # Optionally, perform a simple query (e.g., SELECT 1)
        # This makes sure not only connection but also interaction works
        result = connection.execute("SELECT * FROM stakes")
        print("Query result:", result.scalar())

    except SQLAlchemyError as e:
        print(f"Error connecting to the database: {e}")
    
    finally:
        # Close the connection
        try:
            connection.close()
        except:
            pass

# Test the database connection
test_database_connection(DATABASE_URI)
