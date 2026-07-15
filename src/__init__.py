from flask import Flask
from flask_cors import CORS
from config import Config
from .database.connection import db

from .routes.v1.root import root_bp
from .routes.v1.public import public_bp
from .routes.v1.private import private_bp
from src.services.search_service import SearchService


def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)

    db.init_app(app)

    origins = app.config.get("IP_SERVER_FRONT", [])
    if isinstance(origins, str):
        origins = [origin.strip() for origin in origins.split(",") if origin.strip()]

    print(f"Origenes permitidos: {origins}")

    CORS(
        app,
        resources={r"/*": {"origins": origins}},
        supports_credentials=True,
        allow_headers=["Content-Type", "Authorization"],
        methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    )

    app.register_blueprint(root_bp)
    app.register_blueprint(public_bp, url_prefix="/api/v1/public")
    app.register_blueprint(private_bp, url_prefix="/api/v1/private")

    with app.app_context():
        try:
            print("Inicializando cache de catalogos...")
            SearchService.force_refresh_cache()
            print("Cache inicial cargada correctamente.")
        except Exception as ex:
            print("ERROR cargando cache inicial; continuando sin cache inicial:", ex)

    return app

