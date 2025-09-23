from flask import Flask
from config import Config
from .database.connection import db

# Rutas
from .routes.root import root_bp
from .routes.public import public_bp
from .routes.private import private_bp


def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)
    
    db.init_app(app)
    
    app.register_blueprint(root_bp)
    app.register_blueprint(public_bp, url_prefix="/public")
    app.register_blueprint(private_bp, url_prefix="/private")
    
    return app