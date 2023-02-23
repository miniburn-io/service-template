import os
import uvicorn
from src.api.app import app

PORT = int(os.getenv('PORT', 8000))
LOG_LEVEL = os.getenv('LOG_LEVEL', 'info')
config = uvicorn.Config("__main__:app", port=8000, log_level=LOG_LEVEL)
server = uvicorn.Server(config)
server.run()
