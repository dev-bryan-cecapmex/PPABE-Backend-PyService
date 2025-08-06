
import os 

class Config:
    SECRET_KEY = os.environ.get("SECRET_KEY")
    SQLALCHEMY_DATABASE_URI = (f"mysql+pymysql://{os.environ.get('DB_USER')}:{os.environ.get('DB_PASSWORD')}@" f"{os.environ.get('DB_HOST')}/{os.environ.get('DB_NAME')}")
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    