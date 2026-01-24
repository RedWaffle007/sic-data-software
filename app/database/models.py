"""
SQLAlchemy Database Models
"""
from sqlalchemy import Column, Integer, String, Text, Float, DateTime, JSON, ForeignKey, Index
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.database.database import Base

# ============ MODELS ============

class Dataset(Base):
    """
    Represents a "sheet" - a collection of companies extracted with specific criteria
    """
    __tablename__ = "datasets"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), unique=True, nullable=False, index=True)
    description = Column(Text, nullable=True)
    
    # Extraction criteria
    sic_codes = Column(JSON, nullable=False)  # List of SIC codes
    counties = Column(JSON, nullable=True)    # Optional county filters
    
    # Metadata
    total_companies = Column(Integer, default=0)
    source_file = Column(String(500), nullable=True)  # Original parquet file path
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())
    
    # Relationships
    companies = relationship("Company", back_populates="dataset", cascade="all, delete-orphan")
    analysis = relationship("DatasetAnalysis", back_populates="dataset", uselist=False, cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<Dataset(id={self.id}, name='{self.name}', companies={self.total_companies})>"


class Company(Base):
    """
    Individual company record within a dataset
    """
    __tablename__ = "companies"
    
    id = Column(Integer, primary_key=True, index=True)
    dataset_id = Column(Integer, ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False)
    
    # Core fields
    company_number = Column(String(8), nullable=False, index=True)
    business_name = Column(Text, nullable=False)
    
    # Address
    address_line1 = Column(Text)
    address_line2 = Column(Text)
    town = Column(Text)
    county = Column(String(100), index=True)
    postcode = Column(String(10), index=True)
    
    # PSC & Officers
    person_with_significant_control = Column(Text)
    nature_of_control = Column(Text)
    title = Column(String(50))
    fname = Column(String(100))
    sname = Column(String(100))
    
    # NEW: Enrichment explanation columns
    selected_person_source = Column(Text)  # Why this person was selected
    selected_psc_share_tier = Column(String(20))  # Ownership tier (75-100%, 50-75%, 25-50%)
    selected_psc_nature_of_control = Column(Text)  # Nature of control for selected PSC
    
    position = Column(Text)
    
    # Company details
    sic = Column(Text)
    company_status = Column(String(50))
    company_type = Column(String(100))
    date_of_creation = Column(String(20))
    
    # Additional fields
    website = Column(Text)
    phone = Column(String(50))
    email = Column(String(255))
    website_address = Column(Text)
    address_match = Column(String(50))
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())
    
    # Relationships
    dataset = relationship("Dataset", back_populates="companies")
    
    # Indexes for search performance
    __table_args__ = (
        Index('idx_company_search', 'business_name', 'company_number', 'postcode'),
        Index('idx_dataset_county', 'dataset_id', 'county'),
    )
    
    def __repr__(self):
        return f"<Company(id={self.id}, number='{self.company_number}', name='{self.business_name[:30]}')>"


class DatasetAnalysis(Base):
    """
    Cached analysis results for a dataset (regenerated on edit)
    """
    __tablename__ = "dataset_analysis"
    
    id = Column(Integer, primary_key=True, index=True)
    dataset_id = Column(Integer, ForeignKey("datasets.id", ondelete="CASCADE"), unique=True, nullable=False)
    
    # Summary stats
    total_companies = Column(Integer)
    unique_counties = Column(Integer)
    data_quality_score = Column(Float)
    
    # Detailed breakdowns (JSON)
    regional_distribution = Column(JSON)  # Your region/county breakdown
    county_resolution = Column(JSON)
    missing_data = Column(JSON)
    
    # Timestamp
    generated_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Relationships
    dataset = relationship("Dataset", back_populates="analysis")
    
    def __repr__(self):
        return f"<DatasetAnalysis(dataset_id={self.dataset_id}, quality={self.data_quality_score})>"