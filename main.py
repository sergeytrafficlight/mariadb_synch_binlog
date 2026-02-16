from src.engine import run
from config.config import MYSQL_SETTINGS, APP_SETTINGS

if __name__ == '__main__':
    run(MYSQL_SETTINGS, APP_SETTINGS)
