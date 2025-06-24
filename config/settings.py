import os
from dataclasses import dataclass
from typing import Dict, Any, Optional
from dotenv import load_dotenv

load_dotenv()


@dataclass
class DatabaseConfig:
    path: str = "trading_bot.db"
    pool_size: int = 10
    timeout: int = 30


@dataclass
class ConnectorConfig:
    connector: str = ""
    api_key: str = ""
    api_secret: str = ""
    sandbox: bool = False
    paper: bool = True
    rate_limit: int = 20
    timeout: int = 30
    tick_type: str = ""

@dataclass
class TradingConfig:
    default_initial_balance: float = 10000.0
    max_concurrent_strategies: int = 5
    strategy_timeout: int = 300
    risk_check_interval: int = 60


@dataclass
class LoggingConfig:
    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    file_path: Optional[str] = None
    max_file_size: int = 10 * 1024 * 1024  # 10MB
    backup_count: int = 5


class Settings:
    def __init__(self):
        self.database = DatabaseConfig(
            path=os.getenv("DB_PATH", "trading_bot.db"),
            pool_size=int(os.getenv("DB_POOL_SIZE", "10")),
            timeout=int(os.getenv("DB_TIMEOUT", "30"))
        )
        self.exchange = os.getenv("EXCHANGE", "KRAKEN")
        self.exchange = ConnectorConfig(
            connector = os.getenv(f'''{self.exchange}_CONNECTOR''', ""),
            api_key=os.getenv(f'''{self.exchange}_API_KEY''', ""),
            api_secret=os.getenv(f'''{self.exchange}_API_SECRET''', ""),
            sandbox=os.getenv(f'''{self.exchange}_SANDBOX''', "false").lower() == "true",
            paper=os.getenv(f'''{self.exchange}_PAPER''', "true").lower() == "true",
            rate_limit=int(os.getenv(f'''{self.exchange}_RATE_LIMIT''', "20")),
            timeout=int(os.getenv(f'''{self.exchange}_TIMEOUT''', "30")),
            tick_type=os.getenv(f'''{self.exchange}_TICK_TYPE''', "spot"),
        )
        
        self.trading = TradingConfig(
            default_initial_balance=float(os.getenv("DEFAULT_INITIAL_BALANCE", "10000.0")),
            max_concurrent_strategies=int(os.getenv("MAX_CONCURRENT_STRATEGIES", "5")),
            strategy_timeout=int(os.getenv("STRATEGY_TIMEOUT", "300")),
            risk_check_interval=int(os.getenv("RISK_CHECK_INTERVAL", "60"))
        )
        
        self.logging = LoggingConfig(
            level=os.getenv("LOG_LEVEL", "INFO"),
            format=os.getenv("LOG_FORMAT", "%(asctime)s - %(name)s - %(levelname)s - %(message)s"),
            file_path=os.getenv("LOG_FILE_PATH"),
            max_file_size=int(os.getenv("LOG_MAX_FILE_SIZE", str(10 * 1024 * 1024))),
            backup_count=int(os.getenv("LOG_BACKUP_COUNT", "5"))
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert settings to dictionary"""
        return {
            "database": self.database.__dict__,
            "connector":  self.connector.__dict__,
            "trading": self.trading.__dict__,
            "logging": self.logging.__dict__,
            "exchanges": self.exchanges.__dict__
        }
    
    def validate(self) -> bool:
        """Validate settings"""
        # Check critical settings
        if self.database.path == "":
            raise ValueError("Database path cannot be empty")
        
        if self.trading.default_initial_balance <= 0:
            raise ValueError("Initial balance must be positive")
        
        if self.trading.max_concurrent_strategies <= 0:
            raise ValueError("Max concurrent strategies must be positive")
        
        return True


# Global settings instance
_settings_instance = None


async def get_settings_service() -> Settings:
    """Factory pour obtenir l'instance du service Market"""
    global _settings_instance

    if _settings_instance is None:
        _settings_instance = Settings()
    return _settings_instance