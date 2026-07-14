from flask import Flask
from flask_cors import CORS
from config import Config
from .database.connection import db

# Rutas
from .routes.v1.root import root_bp
from .routes.v1.public import public_bp
from .routes.v1.private import private_bp

# Importacion de servicio de refresco de cache 
from src.services.search_service import SearchService

def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)
    
    db.init_app(app)

    # --- Leer entorno e IPs del frontend ---
    # env = app.config["ENVIRONMENT"]
    origins = app.config["IP_SERVER_FRONT"]

    # print(f"🌎 Iniciando entorno: {env}")
    print(f"Origenes permitidos: {origins}")

    # --- Configurar CORS usando los dominios del .env ---
    CORS(
    app,
    resources={r"/*": {"origins": origins}},
    supports_credentials=True,
    allow_headers=["Content-Type", "Authorization"],
    methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"]
)


    # --- Registrar Blueprints ---
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

