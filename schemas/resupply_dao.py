from sqlalchemy import Table, Column, Integer, String, Float, DateTime, Boolean, MetaData, BigInteger, UniqueConstraint
import enum

class ProposalStatus(enum.Enum):
    OPEN = 'open'
    PASSED = 'passed'
    FAILED = 'failed'
    CANCELLED = 'cancelled'
    EXECUTION_DELAY = 'execution_delay'
    EXECUTABLE = 'executable'
    EXPIRED = 'expired'
    EXECUTED = 'executed'

def create_tables(metadata):
    """Create tables for Resupply DAO data"""
    
    proposals_table = Table(
        'resupply_proposals',
        metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('proposal_id', String, nullable=False),
        Column('status', String, nullable=False, default=ProposalStatus.OPEN.value),
        Column('description', String),
        Column('proposer', String, nullable=False),
        Column('start_time', BigInteger, nullable=False),
        Column('end_time', BigInteger, nullable=False),
        Column('yes_votes', Float, nullable=False),
        Column('no_votes', Float, nullable=False),
        Column('quorum', BigInteger, nullable=False),
        Column('block', BigInteger, nullable=False),
        Column('txn_hash', String, nullable=False),
        Column('voter_address', String, nullable=False),
        Column('timestamp', BigInteger, nullable=False),
        Column('date_str', String, nullable=False),
        Column('last_updated', BigInteger, nullable=False),
        Column('execution_time', BigInteger),
        Column('ending_soon_alert_sent', Boolean, nullable=False, default=False),
        UniqueConstraint('proposal_id', 'voter_address', name='uix_proposal_voter')
    )

    votes_table = Table(
        'resupply_votes',
        metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('proposal_id', String, nullable=False),
        Column('voter', String, nullable=False),
        Column('support', Boolean, nullable=False),
        Column('weight', Float, nullable=False),
        Column('reason', String),
        Column('block', BigInteger, nullable=False),
        Column('txn_hash', String, nullable=False),
        Column('timestamp', BigInteger, nullable=False),
        Column('date_str', String, nullable=False),
        Column('log_index', Integer, nullable=True),  # To distinguish multiple events in same tx
        UniqueConstraint('txn_hash', 'log_index', name='uq_resupply_votes_txn_log')
    )

    return proposals_table, votes_table 