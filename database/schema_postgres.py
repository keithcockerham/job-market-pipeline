from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Text, Boolean, text
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

Base = declarative_base()


class RawJob(Base):
    __tablename__ = 'raw_jobs'
    
    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Source identifiers
    job_id = Column(String(100), index=True)  # Source's job ID
    source = Column(String(50), nullable=False, index=True)  # Indeed, LinkedIn, etc.
    source_url = Column(String(500))  # Original job URL
    
    # Basic job info
    title = Column(String(200))
    company = Column(String(200), index=True)
    location = Column(String(200))
    
    # Salary (as scraped - messy!)
    salary_text = Column(String(200))  # Raw text: "$100k - $130k a year"
    salary_min = Column(Float)  # Parsed minimum
    salary_max = Column(Float)  # Parsed maximum
    salary_currency = Column(String(10))
    salary_period = Column(String(20))  # year, hour, month
    
    # Job details
    description_snippet = Column(Text)  # Short description/summary
    posted_date = Column(String(100))  # Raw text: "3 days ago", "2025-11-20"
    job_type = Column(String(50))  # Full-time, Contract, etc.
    
    # Search context
    search_query = Column(String(200))  # What we searched for
    search_location = Column(String(200))  # Where we searched
    
    # Metadata
    scraped_at = Column(DateTime, default=datetime.utcnow, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Data quality flags
    is_duplicate = Column(Boolean, default=False)
    is_valid = Column(Boolean, default=True)
    validation_errors = Column(Text)  # JSON string of validation issues
    
    def __repr__(self):
        return f"<RawJob(id={self.id}, title='{self.title}', company='{self.company}')>"


class StagingJob(Base):
    __tablename__ = 'staging_jobs'
    
    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Links back to raw
    raw_job_id = Column(Integer, index=True)
    
    # Standardized identifiers
    canonical_job_id = Column(String(100), unique=True, index=True)  # Our unique ID
    source = Column(String(50), nullable=False)
    
    # Normalized job info
    title_normalized = Column(String(200), index=True)
    company_normalized = Column(String(200), index=True)
    location_city = Column(String(100))
    location_state = Column(String(50))
    location_type = Column(String(20))  # Remote, Hybrid, Onsite
    
    # Normalized salary (in USD/year for comparison)
    salary_min_annual = Column(Float)
    salary_max_annual = Column(Float)
    salary_listed = Column(Boolean, default=False)  # Whether salary was in posting
    
    # Job categorization
    seniority_level = Column(String(50))  # Entry, Mid, Senior, Lead
    role_category = Column(String(50))  # Data Scientist, ML Engineer, etc.
    employment_type = Column(String(50))  # Full-time, Contract, etc.
    
    # Parsed posted date
    posted_date_parsed = Column(DateTime)
    days_since_posted = Column(Integer)
    
    # Rich text
    description_full = Column(Text)
    
    # Skills (could be normalized to separate table later)
    skills_extracted = Column(Text)  # JSON array of skills
    
    # Metadata
    is_remote = Column(Boolean, default=False)
    is_active = Column(Boolean, default=True)
    first_seen = Column(DateTime, default=datetime.utcnow)
    last_seen = Column(DateTime, default=datetime.utcnow)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def __repr__(self):
        return f"<StagingJob(id={self.id}, canonical_id='{self.canonical_job_id}')>"


class DataQualityLog(Base):
    __tablename__ = 'data_quality_log'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    raw_job_id = Column(Integer, index=True)
    issue_type = Column(String(100), index=True)  # missing_salary, invalid_date, etc.
    issue_severity = Column(String(20))  # ERROR, WARNING, INFO
    issue_description = Column(Text)
    detected_at = Column(DateTime, default=datetime.utcnow)
    
    def __repr__(self):
        return f"<DataQualityLog(id={self.id}, type='{self.issue_type}')>"


def get_database_url() -> str:
    host = os.getenv('DB_HOST', 'localhost')
    port = os.getenv('DB_PORT', '5432')
    database = os.getenv('DB_NAME', 'job_market')
    user = os.getenv('DB_USER', 'postgres')
    password = os.getenv('DB_PASSWORD', 'postgres')
    
    return f'postgresql://{user}:{password}@{host}:{port}/{database}'


def create_database(db_url: str = None):
    """Create database and all tables"""
    if db_url is None:
        db_url = get_database_url()
    
    engine = create_engine(db_url, echo=False)
    Base.metadata.create_all(engine)
    print(f"✓ Database created successfully")
    print(f"  Connection: {db_url.split('@')[1]}")  # Don't print password
    return engine


def get_session(db_url: str = None):
    """Get database session"""
    if db_url is None:
        db_url = get_database_url()
    
    engine = create_engine(db_url, echo=False)
    Session = sessionmaker(bind=engine)
    return Session()


def test_connection():
    """Test database connection"""
    try:
        db_url = get_database_url()
        engine = create_engine(db_url, echo=False)
        with engine.connect() as connection:
            result = connection.execute(text("SELECT version();"))
            version = result.fetchone()[0]

        print(f"✓ PostgreSQL connection successful!")
        print(f"  Version: {version.split(',')[0]}")
        return True
    except Exception as e:
        print(f"Database connection failed: {e}")
        return False


if __name__ == "__main__":

    if not test_connection():
        print("\nConnection Failed.")
        exit(1)
    
    engine = create_database()
    
    for table_name in Base.metadata.tables.keys():
        print(f"  - {table_name}")

