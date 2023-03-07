import os
import uvicorn
from src.api.app import app

PORT = int(os.getenv('PORT', 8000))
LOG_LEVEL = os.getenv('LOG_LEVEL', 'info')
config = uvicorn.Config("__main__:app", host='0.0.0.0', port=PORT, log_level=LOG_LEVEL)
server = uvicorn.Server(config)
server.run()
