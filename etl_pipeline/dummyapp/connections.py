from dotenv import load_dotenv
import os

load_dotenv()

DB_local_HOST = os.getenv("DB_local_HOST")
DB_local_PORT = os.getenv("DB_local_PORT")
DB_local_NAME = os.getenv("DB_local_NAME")
DB_local_USER = os.getenv("DB_local_USER")
DB_local_PASS = os.getenv("DB_local_PASS")

DB_remote_HOST = os.getenv("DB_remote_HOST")
DB_remote_PORT = os.getenv("DB_remote_PORT")
DB_remote_NAME = os.getenv("DB_remote_NAME")
DB_remote_USER = os.getenv("DB_remote_USER")
DB_remote_PASS = os.getenv("DB_remote_PASS")



def conn_string_local():
    return f"postgresql://{DB_local_USER}:{DB_local_PASS}@{DB_local_HOST}/{DB_local_NAME}"

def conn_string_remote():
    return f"postgresql://{DB_remote_USER}:{DB_remote_PASS}@{DB_remote_HOST}/{DB_remote_NAME}"


