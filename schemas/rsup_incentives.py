from sqlalchemy import Table, Column, Integer, String, Float, BigInteger, MetaData, JSON

def create_tables(metadata):
    """Create tables for RSUP incentives data"""
    
    incentives_table = Table(
        'rsup_incentives',
        metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('epoch', Integer, nullable=False),
        Column('total_incentives', Float, nullable=False),
        Column('convex_amount', Float, nullable=False),
        Column('yearn_amount', Float, nullable=False),
        Column('convex_votes_per_usd', Float, nullable=False),
        Column('yearn_votes_per_usd', Float, nullable=False),
        Column('convex_votes', Float, nullable=False),
        Column('yearn_votes', Float, nullable=False),
        Column('gauge_data', JSON, nullable=False),
        Column('transaction_hash', String, nullable=False),
        Column('block_number', BigInteger, nullable=False),
        Column('timestamp', BigInteger, nullable=False),
        Column('date_str', String, nullable=False)
    )

    return incentives_table 