from sqlalchemy import Column, VARCHAR, Date, ForeignKey, Numeric, BOOLEAN, Integer
from franchise.database import Base
from sqlalchemy.orm import relationship

class User(Base):
    __tablename__ = 'users'    
    id=Column(Integer, primary_key=True, index=True)
    company_code=Column(VARCHAR(100))
    email=Column(VARCHAR(100))
    distributor_id=Column(VARCHAR(100))
    username=Column(VARCHAR(100))
    password=Column(VARCHAR(100))
    created_at=Column(Date)
    status=Column(BOOLEAN)
    store_name=Column(VARCHAR(50))
    role_id=Column(Integer, ForeignKey('roles.id'))
    location_id=Column(Integer, ForeignKey('locations.location_id'))
    
    roles=relationship("Roles", back_populates="users")
    locations=relationship("Locations", back_populates="users")

class Roles(Base):
    __tablename__ = 'roles'
    
    id=Column(Integer, primary_key=True, index=True)
    roles_name=Column(VARCHAR(100))
    
    users=relationship("User", back_populates="roles")
    
class Locations(Base):
    __tablename__ = 'locations'
    
    location_id=Column(Integer, primary_key=True, index=True)
    location_name=Column(VARCHAR(50))
    
    users=relationship("User", back_populates="locations")