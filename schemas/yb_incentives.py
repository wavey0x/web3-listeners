from sqlalchemy import Table, Column, Integer, String, Float, BigInteger, MetaData, JSON

def create_tables(metadata):
    """Create tables for YB incentives data"""

    incentives_table = Table(
        'yb_incentives',
        metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('total_incentives', Float, nullable=False),
        Column('votium_amount', Float, nullable=False),
        Column('votemarket_amount', Float, nullable=False),
        Column('votium_votes_per_usd', Float, nullable=False),
        Column('votemarket_votes_per_usd', Float, nullable=False),
        Column('votium_votes', Float, nullable=False),
        Column('votemarket_votes', Float, nullable=False),
        Column('gauge_data', JSON, nullable=False),
        Column('transaction_hash', String, nullable=False),
        Column('block_number', BigInteger, nullable=False),
        Column('timestamp', BigInteger, nullable=False),
        Column('date_str', String, nullable=False),
        Column('period_start', BigInteger, nullable=False)
    )

    return incentives_table
