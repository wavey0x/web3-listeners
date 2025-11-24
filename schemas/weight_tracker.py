from sqlalchemy import Table, Column, Integer, String, Float, DateTime, BigInteger, MetaData, Numeric, UniqueConstraint

def create_tables(metadata):
    """Create tables for weight tracking data"""
    
    weight_changes_table = Table(
        'weight_changes',
        metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('user_address', String, nullable=False),
        Column('old_weight', Numeric(78, 0), nullable=False),  # 78 digits, 0 decimal places
        Column('new_weight', Numeric(78, 0), nullable=False),  # 78 digits, 0 decimal places
        Column('weight_diff', Numeric(78, 0), nullable=False),  # new_weight - old_weight
        Column('block', BigInteger, nullable=False),
        Column('txn_hash', String, nullable=False),
        Column('timestamp', BigInteger, nullable=False),
        Column('date_str', String, nullable=False),
        Column('log_index', Integer, nullable=True),  # To distinguish multiple events in same tx
        UniqueConstraint('txn_hash', 'log_index', name='uq_weight_changes_txn_log')
    )

    return weight_changes_table 