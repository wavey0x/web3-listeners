from sqlalchemy import Table, Column, Integer, String, Float, BigInteger, MetaData, JSON, UniqueConstraint

def create_tables(metadata):
    """Create unified table for both YB and RSUP incentives data"""

    incentives_table = Table(
        'incentives',
        metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('protocol', String, nullable=False, index=True),  # 'yieldbasis' or 'resupply'
        Column('epoch', Integer, nullable=True),  # For RSUP only
        Column('total_incentives', Float, nullable=False),
        Column('votium_amount', Float, nullable=False),
        Column('votemarket_amount', Float, nullable=False),
        Column('votium_votes_per_usd', Float, nullable=True),
        Column('votemarket_votes_per_usd', Float, nullable=True),
        Column('votium_votes', Float, nullable=False),
        Column('votemarket_votes', Float, nullable=False),
        Column('gauge_data', JSON, nullable=False),
        Column('transaction_hash', String, nullable=False),
        Column('block_number', BigInteger, nullable=False),
        Column('timestamp', BigInteger, nullable=False),
        Column('date_str', String, nullable=False),
        Column('period_start', BigInteger, nullable=False, index=True),
        Column('log_index', Integer, nullable=True),
        UniqueConstraint('protocol', 'transaction_hash', 'log_index', name='uq_incentives_protocol_txn_log')
    )

    return incentives_table
