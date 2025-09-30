# PPABE-Backend-PyService

## ⚙️ Configuración de Variables de Entorno

### 1. Crear el archivo de variables de entorno

Crea un archivo `.env` en la raíz del proyecto:
```bash
cp .env.example .env
```

### 2.Configura las siguientes variables en tu archivo .env:
```bash
# ========================================
# CONFIGURACIÓN GENERAL
# ========================================
ENVIRONMENT=development
PORT=4000

# ========================================
# SEGURIDAD Y AUTENTICACIÓN
# ========================================
JWT_SECRET=tu_jwt_secret_aqui
SECRET_KEY=tu_secret_key_aqui

# ========================================
# BASE DE DATOS - MySQL
# ========================================
DB_HOST=localhost
DB_PORT=3306
DB_USER=tu_usuario
DB_PASSWORD=tu_contraseña
DB_NAME=nombre_base_datos

# ========================================
# APIS Y SERVICIOS EXTERNOS
# ========================================
# API Backend PAUA
APP_BACK_PAUA=http://ip:puerto/api/
ID_APP=tu_id_app_aqui

# API de Documentos
APP_DOC_API=http://ip:puerto

# ========================================
# SFTP Y ALMACENAMIENTO
# ========================================
SFTP_ROOT=/ruta/al/directorio/raiz/
RUTA_FOLDER_FTP=/CARPETA_PROYECTO/
```