from src import create_app
from src.utils.env_validator import validate_env

if __name__ == '__main__':
    validate_env()
    app = create_app()
    app.run(debug=True)