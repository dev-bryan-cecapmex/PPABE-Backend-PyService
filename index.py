import os

from src import create_app
from src.utils.env_validator import validate_env

validate_env()
app = create_app()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', '4001')), debug=False)