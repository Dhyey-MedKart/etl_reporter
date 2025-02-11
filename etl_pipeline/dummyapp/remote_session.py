from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from .connections import conn_string_remote

engine = create_engine(conn_string_remote())
RemoteSession = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def create_remote_session():
    return RemoteSession()