from sqlalchemy import Column, String

# Assuming adding to stakes_table for demonstrating:
stakes_table = Table('stakes', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('ybs', String),
                    Column('account', String),
                    Column('amount', Integer),
                    Column('week', Integer),
                    Column('newweight', Integer),
                    Column('netweightchange', Integer),
                    Column('timestamp', Integer),
                    Column('date_str', String),
                    Column('txn_hash', String),
                    Column('block', Integer),
                    Column('event_type', String),  # This is the newly added column
                    autoload_with=engine)