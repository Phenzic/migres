import os
from pathlib import Path
from dotenv import load_dotenv

def load_environment(custom_path=None):
    """
    Load environment variables from .env file
    
    Args:
        custom_path (str, optional): Custom path to .env file
        
    Returns:
        bool: True if environment was loaded successfully
    """
    # Default path is current directory
    default_path = Path('./.env')
    
    # If custom path is provided, use it
    if custom_path:
        env_path = Path(custom_path)
        if env_path.exists():
            load_dotenv(dotenv_path=env_path, override=True)
            return True
    
    # Otherwise use default path
    if default_path.exists():
        load_dotenv(dotenv_path=default_path, override=True)
        return True
    
    return False 