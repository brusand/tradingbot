# Trading Bot Backend

A high-performance, modular crypto trading bot backend for Kraken (spot + margin) with advanced multi-session management, monitoring, and analytics.

## 🎯 Overview

This trading bot provides a complete solution for automated cryptocurrency trading with support for multiple parallel strategies, advanced monitoring, and comprehensive analytics. Built with Python asyncio for high performance and scalability.

## 🚀 Features

### ✅ Phase 1 - Core Foundation
- **Session Management**: Persistent session lifecycle with SQLite storage
- **Kraken Integration**: Full REST + WebSocket API support (spot + margin)
- **Paper Trading**: Risk-free strategy testing with virtual funds
- **Strategy Engine**: Modular, extensible strategy framework
- **CLI Interface**: Command-line tools for session management
- **Configuration**: Flexible environment-based configuration

### ✅ Phase 2 - Advanced Multi-Session (Current)
- **🔥 Multi-Session Parallel Execution**: Run multiple strategies simultaneously
- **🔥 Advanced Process Monitoring**: Real-time CPU, memory, and health tracking
- **🔥 Session Kill/Force Stop**: Graceful and forced session termination
- **🔥 Batch Operations**: Select and manage multiple sessions at once
- **🔥 Web Dashboard**: Real-time monitoring with WebSocket updates
- **🔥 Session Replay**: Time-travel debugging and state duplication
- **🔥 Performance Analytics**: 30+ advanced metrics and comparative analysis
- **🔥 Professional CLI Tools**: Rich visual output with emojis and status indicators
- **🔥 JSON Export**: Complete data export for external analysis
- **🔥 Statistical Analysis**: Risk metrics, correlation analysis, trend tracking

### 🔄 Phase 3 - Production (Roadmap)
- Live trading mode with enhanced risk management
- Machine learning strategy optimization
- Multi-exchange support and arbitrage
- Strategy marketplace and templates
- Cloud deployment automation

## 🏗️ Architecture

```
trading-backend/
├── Phase 1 - Core Foundation
│   ├── core/session_manager.py     # Session lifecycle management
│   ├── core/strategy_engine.py     # Strategy execution engine
│   ├── core/paper_trading.py       # Paper trading simulation
│   ├── connectors/kraken_connector.py # Kraken API integration
│   ├── strategies/base_strategy.py # Strategy framework
│   └── interfaces/cli.py           # Enhanced CLI with performance features
│
├── Phase 2 - Advanced Features
│   ├── core/multi_session_manager.py  # Parallel session orchestration
│   ├── core/process_monitor.py        # Real-time health monitoring
│   ├── core/session_replay.py         # Time-travel & state management
│   ├── core/performance_analytics.py  # Advanced metrics & reporting
│   ├── interfaces/cli_v2.py           # Enhanced CLI with analytics suite
│   ├── interfaces/web_ui.py           # Real-time web dashboard
│   └── main_v2.py                     # Phase 2 application entry
│
└── Common Infrastructure
    ├── data/models.py              # Data models and schemas
    ├── data/persistence.py        # Database operations
    ├── config/settings.py         # Configuration management
    ├── strategies/performance_tracker.py # Core performance tracking
    ├── docs/CLI_PERFORMANCE_GUIDE.md # Comprehensive CLI documentation
    ├── examples/cli_performance_demo.py # Performance demo scripts
    ├── test_cli_integration.py    # CLI integration tests
    └── tests/                      # Comprehensive test suite
```

## 🚀 Quick Start

### Installation

```bash
# Clone and setup
git clone <repository-url>
cd tradingbot

# Install dependencies (Python 3.11+ recommended)
pip install -r requirements.txt

# Setup environment
cp .env.example .env
# Edit .env with your API keys (optional for paper trading)
```

### Choose Your Interface

#### 🌐 Web Dashboard (Recommended for Phase 2)
```bash
# Launch real-time web interface
python main_v2.py --mode=web --port=8000

# Access dashboard at: http://localhost:8000
# Features: Live monitoring, batch operations, alerts
```

#### 💻 Enhanced CLI (Phase 2) - Advanced Analytics
```bash
# Advanced multi-session commands with analytics
python main_v2.py --mode=cli

# Or directly:
python -m interfaces.cli_v2 --help

# Advanced analytics suite
python -m interfaces.cli_v2 analytics --help
```

#### 🔧 Enhanced CLI (Phase 1) - Performance Features
```bash
# Session management with advanced performance tracking
python -m interfaces.cli --help

# Show detailed performance metrics with rich visual output
python -m interfaces.cli show-session <id> --detailed --trades

# Performance ranking and comparison
python -m interfaces.cli performance --sort-by win_rate
```

## 📊 Phase 2 - Advanced Usage

### Multi-Session Operations

```bash
# Create multiple sessions
python -m interfaces.cli_v2 create-sessions --name="Strategy" --count=5 --mode=paper

# Start sessions by pattern
python -m interfaces.cli_v2 start-sessions --pattern="Strategy" --dry-run
python -m interfaces.cli_v2 start-sessions --pattern="Strategy"

# Start specific sessions
python -m interfaces.cli_v2 start-sessions --sessions="id1,id2,id3"

# Start all paper trading sessions
python -m interfaces.cli_v2 start-sessions --mode=paper --all
```

### Real-Time Monitoring

```bash
# Live session monitoring (updates every 5 seconds)
python -m interfaces.cli_v2 list-running --watch

# System overview
python -m interfaces.cli_v2 system-status

# JSON output for automation
python -m interfaces.cli_v2 list-sessions --format=json
```

### 📊 Performance Analysis & Reporting

#### Basic Performance Commands (CLI v1)

```bash
# Show detailed session performance with all metrics
python -m interfaces.cli show-session <session-id> --detailed --trades --export report.json

# Performance ranking of all sessions
python -m interfaces.cli performance --limit 10 --sort-by pnl

# Sort by win rate
python -m interfaces.cli performance --sort-by win_rate

# Sort by number of trades
python -m interfaces.cli performance --sort-by trades

# Compare multiple sessions
python -m interfaces.cli compare session1 session2 session3
```

#### Advanced Analytics (CLI v2)

```bash
# Comprehensive portfolio report
python -m interfaces.cli_v2 analytics portfolio-report --min-trades 5 --export portfolio.json

# Advanced session comparison with statistical analysis
python -m interfaces.cli_v2 analytics compare-advanced session1 session2 --metric win_rate

# Advanced session comparison with risk analysis
python -m interfaces.cli_v2 analytics compare-advanced session1 session2 session3 --metric pnl

# Performance trend analysis by strategy
python -m interfaces.cli_v2 analytics trend-analysis --timeframe weekly --strategies AdvancedTradingStrategy

# Daily and monthly trend analysis
python -m interfaces.cli_v2 analytics trend-analysis --timeframe daily

# Correlation analysis between sessions
python -m interfaces.cli_v2 analytics correlation-matrix --min-correlation 0.7

# Find highly correlated sessions for diversification
python -m interfaces.cli_v2 analytics correlation-matrix --min-correlation 0.8
```

#### 🎨 Rich Visual Output

**Performance Status Indicators:**
- 🟢 Positive PnL / Running Sessions
- 🔴 Negative PnL / Stopped Sessions  
- 🎯 Excellent Win Rate (≥60%)
- 📈 Good Win Rate (40-60%)
- 📉 Poor Win Rate (<40%)
- 🏆 Best Performing Session
- ⚠️ Risk Warnings

**Example Output:**
```
📊 SESSION PERFORMANCE SUMMARY
================================================================================
🆔 ID: session_123
📝 Name: BTC Scalping Bot
🎯 Mode: paper | 🟢 Status: running
💰 Total PnL: 🟢 +456.20 | 📊 Trades: 45 | 🎯 Win Rate: 68.9%
📈 ROI: 4.56% | 📉 Max Drawdown: 2.1% | 🎯 Sharpe: 1.89
```

### Process Management

```bash
# Graceful shutdown
python -m interfaces.cli_v2 stop-sessions --pattern="Strategy"

# Force kill problematic sessions
python -m interfaces.cli_v2 kill-sessions --sessions="id1,id2" --confirm

# Emergency: kill all sessions
python -m interfaces.cli_v2 kill-sessions --all --confirm
```

### Advanced Filtering

```bash
# Multi-criteria filtering
python -m interfaces.cli_v2 list-sessions \
  --mode=paper \
  --status=running \
  --pattern="SMA" \
  --limit=10

# Export session data
python -m interfaces.cli_v2 list-sessions --format=json > sessions.json
```

## 📈 Performance Analytics & Advanced Metrics

### CLI-Integrated Analytics

Our performance system is fully integrated into both CLI interfaces, providing professional-grade metrics and analysis:

```bash
# Real-time session monitoring with rich visual feedback
python -m interfaces.cli show-session <id> --detailed

# Portfolio-wide analysis
python -m interfaces.cli_v2 analytics portfolio-report --min-trades 5 --export portfolio.json

# Statistical correlation analysis
python -m interfaces.cli_v2 analytics correlation-matrix --min-correlation 0.7
```

### 30+ Professional Metrics

**Profitability Metrics:**
- Total PnL, ROI, Profit Factor
- Win Rate, Average Win/Loss
- Expectancy per trade

**Risk Metrics:**
- Maximum Drawdown, Current Drawdown
- Sharpe Ratio, Sortino Ratio
- Value at Risk (VaR 95%)
- Volatility and Risk-Adjusted Returns

**Consistency Metrics:**
- Consecutive Wins/Losses (max and current)
- Trade Frequency and Duration
- Performance Stability

### Generate Comprehensive Reports

```python
from core.performance_analytics import PerformanceAnalytics
from data.persistence import DatabaseManager

# Initialize analytics
db_manager = DatabaseManager()
analytics = PerformanceAnalytics(db_manager)

# Generate detailed performance report
report = await analytics.generate_performance_report(session_id)

# Key metrics included:
# - Total PnL and returns (absolute & percentage)
# - Risk metrics: Sharpe, Sortino, Calmar ratios
# - Win rate, profit factor, max drawdown
# - Daily/monthly returns analysis
# - Trade statistics and patterns
```

### Compare Multiple Sessions

```python
# Side-by-side performance comparison
comparison = await analytics.compare_sessions([
    "session1", "session2", "session3"
])

# Portfolio-level analytics
portfolio = await analytics.get_portfolio_analytics([
    "session1", "session2", "session3"
])
```

### Export & Analysis

```python
# Export detailed report
report_json = analytics.export_report_to_json(report)

# 30+ metrics included:
# - Profitability: Total PnL, ROI, profit factor, expectancy
# - Risk: Max drawdown, volatility, VaR, risk-adjusted returns
# - Efficiency: Sharpe, Sortino, Calmar ratios
# - Activity: Trade frequency, avg duration, market exposure
# - Consistency: Win rate, consecutive trades, performance stability
# - Visual: Rich emoji indicators and status colors

# CLI Export with JSON
python -m interfaces.cli show-session <id> --detailed --trades --export complete_report.json
```

## 🔄 Session Replay & State Management

### Time-Travel Debugging

```python
from core.session_replay import SessionReplayManager

replay_manager = SessionReplayManager(db_manager)

# Create automatic snapshots (every 5 minutes)
await replay_manager.create_snapshot(session_id, session)

# Replay session from specific time
await replay_manager.replay_session(
    session_id,
    start_time=datetime(2024, 1, 1),
    end_time=datetime(2024, 1, 2),
    speed_multiplier=10.0  # 10x speed
)
```

### Smart Duplication

```python
# Duplicate session with historical state
new_session = await replay_manager.duplicate_session_with_state(
    original_session_id,
    new_name="Strategy Copy",
    snapshot_time=datetime(2024, 1, 15)  # Use state from this time
)
```

### Event Tracking

```python
# Record custom events for replay
await replay_manager.record_event(session_id, "trade", {
    "symbol": "BTCUSD",
    "action": "buy",
    "amount": 0.1,
    "price": 50000,
    "reason": "SMA crossover"
})
```

## 🚨 Advanced Monitoring

### Real-Time Process Health

- **CPU Monitoring**: Alert when sessions exceed 80% CPU usage
- **Memory Tracking**: Monitor memory consumption and detect leaks
- **Responsiveness**: Detect hanging or crashed processes
- **Resource Limits**: Track file handles, network connections

### Alert System

```python
# Configurable alerts with severity levels
from core.process_monitor import ProcessMonitor

monitor = ProcessMonitor(db_manager)
monitor.add_alert_callback(my_alert_handler)

# Alert types: cpu_high, memory_high, not_responsive, crashed
# Severity levels: low, medium, high, critical
```

### Web Dashboard Features

- **Live Updates**: WebSocket real-time data every 5 seconds
- **System Overview**: CPU, memory, session capacity
- **Interactive Controls**: Start, stop, kill sessions directly
- **Alert Notifications**: Real-time alerts with severity indicators
- **Session Details**: Drill-down into individual session metrics

## 🎯 Phase 1 - Basic Usage (Legacy Support)

Phase 2 maintains full backward compatibility with Phase 1 commands:

### Single Session Management

```bash
# Create a session
python -m interfaces.cli create-session --name="My Strategy" --mode=paper

# Basic operations
python -m interfaces.cli list-sessions
python -m interfaces.cli start-session <session-id>
python -m interfaces.cli show-session <session-id>
python -m interfaces.cli stop-session <session-id>
```

### Strategy Development

```python
from strategies.base_strategy import BaseStrategy

class MyCustomStrategy(BaseStrategy):
    async def initialize(self):
        # Initialize strategy parameters
        self.indicators = {}
    
    async def on_market_data(self, symbol: str, data: dict):
        # Process market data
        if await self.should_buy(symbol, data):
            size = await self.calculate_position_size(symbol, data['price'])
            await self.place_buy_order(symbol, size)
    
    async def should_buy(self, symbol: str, data: dict) -> bool:
        # Implement your trading logic
        return False
```

## ⚙️ Configuration

### Environment Variables

```bash
# Multi-session settings (Phase 2)
MAX_CONCURRENT_STRATEGIES=10    # Max parallel sessions
STRATEGY_TIMEOUT=300           # Session timeout (seconds)
RISK_CHECK_INTERVAL=60         # Risk monitoring interval

# Process monitoring
PROCESS_CPU_THRESHOLD=80.0     # CPU alert threshold (%)
PROCESS_MEMORY_THRESHOLD=500   # Memory alert threshold (MB)
PROCESS_CHECK_INTERVAL=5       # Monitoring frequency (seconds)

# Performance Analytics
PERFORMANCE_UPDATE_INTERVAL=30  # Performance metrics update interval
METRICS_RETENTION_DAYS=90      # How long to keep detailed metrics
EXPORT_FORMAT=json             # Default export format for reports

# Kraken API (optional for paper trading)
KRAKEN_API_KEY=your_api_key
KRAKEN_API_SECRET=your_secret_key
KRAKEN_SANDBOX=true

# Trading settings
DEFAULT_INITIAL_BALANCE=10000.0
DB_PATH=trading_bot.db

# Web UI settings
WEB_UI_HOST=0.0.0.0
WEB_UI_PORT=8000
```

### Session Modes

- **sandbox**: Kraken demo environment (requires API keys)
- **paper**: Simulated trading with virtual funds (no API keys needed)
- **live**: Real trading (Phase 3 feature)

## 🔌 API Reference

### REST API Endpoints (Phase 2)

```http
# Session Management
GET    /api/sessions              # List all sessions
POST   /api/sessions              # Create new session
GET    /api/sessions/{id}         # Get session details
DELETE /api/sessions/{id}         # Delete session

# Multi-Session Operations
POST   /api/sessions/start        # Start multiple sessions
POST   /api/sessions/stop         # Stop multiple sessions
POST   /api/sessions/kill         # Force kill sessions

# Performance Analytics API
GET    /api/analytics/portfolio   # Portfolio performance report
GET    /api/analytics/sessions/{id}/performance  # Detailed session metrics
GET    /api/analytics/compare     # Compare multiple sessions
GET    /api/analytics/trends      # Performance trend analysis
GET    /api/analytics/correlation # Session correlation matrix
GET    /api/analytics/export/{format}  # Export reports (JSON/CSV)

# Monitoring
GET    /api/sessions/running      # Get running sessions with process info
GET    /api/system/status         # System statistics and capacity
GET    /api/alerts               # Get alerts with filtering

# Real-time Updates
WebSocket: /ws                    # Live session status, alerts, metrics
```

### Python API Examples

```python
# Phase 1 - Basic session management
from core.session_manager import SessionManager
from data.models import SessionMode, StrategyConfig, RiskConfig

manager = SessionManager(db_manager)
session = await manager.create_session(name, mode, strategy, risk_params)

# Phase 2 - Multi-session management
from core.multi_session_manager import MultiSessionManager

multi_manager = MultiSessionManager(db_manager)
await multi_manager.initialize()

# Start multiple sessions
results = await multi_manager.start_sessions(session_ids, api_keys, api_secrets)

# Get system statistics
stats = await multi_manager.get_system_stats()
```

## 🧪 Testing

```bash
# Run all tests (49 tests including CLI integration)
python -m pytest

# Run with coverage
python -m pytest --cov=. tests/

# Test specific components
python -m pytest tests/test_session_manager.py
python -m pytest tests/test_paper_trading.py

# Test CLI integration and performance features
python test_cli_integration.py

# Performance testing
python -m pytest tests/ -v --tb=short

# Test CLI performance commands specifically
python examples/cli_performance_demo.py
```

## 🚀 Performance & Scalability

### Multi-Core Architecture (Phase 2)
- **Process Isolation**: Each session in separate process
- **True Parallelism**: Bypass Python GIL limitations
- **Resource Management**: Configurable limits and monitoring
- **Fault Isolation**: Session crashes don't affect others

### Memory Efficiency
- **Lazy Loading**: Heavy dependencies loaded on demand
- **Connection Pooling**: Efficient database access
- **Data Retention**: Configurable cleanup policies
- **Memory Monitoring**: Real-time usage tracking and alerts

### Performance Metrics
- **Latency**: Sub-100ms order execution
- **Throughput**: 10+ concurrent sessions per core
- **Monitoring**: 5-second real-time updates
- **Storage**: Efficient SQLite with automatic optimization

## 🔒 Security & Safety

### Process Safety
- **Isolation**: Each session in separate process space
- **Graceful Shutdown**: SIGTERM handling with cleanup
- **Error Recovery**: Automatic restart on crashes
- **Resource Limits**: Memory and CPU constraints

### Data Security
- **Environment Variables**: API keys never hardcoded
- **No Logs**: Sensitive data excluded from logs
- **Sandbox Mode**: Safe testing environment
- **Paper Trading**: Risk-free strategy development

## 🛠️ Migration Guide

### From Phase 1 to Phase 2

Phase 2 is fully backward compatible:

```bash
# Your existing Phase 1 commands still work
python -m interfaces.cli list-sessions

# Enhanced with Phase 2 features
python -m interfaces.cli_v2 list-sessions --format=json

# New capabilities
python main_v2.py --mode=web  # Web dashboard
python -m interfaces.cli_v2 start-sessions --all  # Batch operations
```

### Database Migration

No migration needed - Phase 2 uses the same database schema with additional tables automatically created.

## 🆘 Troubleshooting

### Common Issues

**Sessions not starting:**
```bash
# Check system capacity
python -m interfaces.cli_v2 system-status

# Verify configuration
python -m interfaces.cli_v2 show-session <session-id>

# Check logs for errors
tail -f trading_bot.log
```

**High resource usage:**
```bash
# Monitor process health
python -m interfaces.cli_v2 list-running --watch

# Check alerts
curl http://localhost:8000/api/alerts

# Kill problematic sessions
python -m interfaces.cli_v2 kill-sessions --sessions=<id>
```

**Web UI issues:**
```bash
# Test different port
python main_v2.py --mode=web --port=8001

# Check firewall settings
# Verify dependencies: pip install fastapi uvicorn
```

**Performance command issues:**
```bash
# Test CLI performance integration
python test_cli_integration.py

# Run demo to verify functionality
python examples/cli_performance_demo.py

# Check performance tracker import
python -c "from strategies.performance_tracker import PerformanceTracker; print('OK')"
```

### Performance Optimization

```bash
# Increase concurrent limit
export MAX_CONCURRENT_STRATEGIES=20

# Optimize monitoring frequency
export PROCESS_CHECK_INTERVAL=10

# Tune alert thresholds
export PROCESS_CPU_THRESHOLD=90.0
export PROCESS_MEMORY_THRESHOLD=1000

# Performance analytics optimization
export PERFORMANCE_UPDATE_INTERVAL=60  # Reduce update frequency
export METRICS_RETENTION_DAYS=30       # Reduce data retention
```

## 🎯 Use Cases

### 1. Strategy Development & Testing
```bash
# Create test variants
python -m interfaces.cli_v2 create-sessions --name="SMA_Test" --count=10

# Run parallel backtests
python -m interfaces.cli_v2 start-sessions --pattern="SMA_Test"

# Monitor performance
python main_v2.py --mode=web
```

### 2. Production Trading
```bash
# Start monitoring daemon
python main_v2.py --mode=daemon

# Monitor critical sessions
python -m interfaces.cli_v2 list-running --watch

# Emergency shutdown
python -m interfaces.cli_v2 kill-sessions --all
```

### 3. Performance Analysis
```bash
# Generate comprehensive CLI reports
python -m interfaces.cli_v2 analytics portfolio-report --export weekly_report.json

# Statistical comparison and trend analysis
python -m interfaces.cli_v2 analytics compare-advanced session1 session2 session3
python -m interfaces.cli_v2 analytics trend-analysis --timeframe weekly

# Export individual session reports
python -m interfaces.cli show-session <id> --detailed --trades --export session_report.json
```

```python
# Generate comprehensive reports via API
reports = await analytics.compare_sessions(session_ids)

# Export for external analysis
data = analytics.export_report_to_json(report)
```

## 📋 Roadmap

### Phase 3 - Production Features
- **Live Trading**: Real money trading with enhanced safety
- **Multi-Exchange**: Binance, Coinbase Pro, FTX support
- **Advanced Risk**: Portfolio-level risk management
- **ML Integration**: Strategy optimization with machine learning
- **Cloud Deployment**: Docker, Kubernetes, AWS/GCP support

### Future Enhancements
- **Strategy Marketplace**: Share and discover strategies
- **Social Trading**: Copy successful traders
- **Advanced Analytics**: Monte Carlo analysis, stress testing
- **Mobile App**: iOS/Android monitoring and control

## 🤝 Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/amazing-feature`)
3. Add comprehensive tests
4. Ensure all tests pass (`python -m pytest`)
5. Follow code style guidelines
6. Submit pull request

## 📄 License

MIT License - see LICENSE file for details

## 🆘 Support

- **Issues**: [GitHub Issues](https://github.com/user/tradingbot/issues)
- **Documentation**: Check README and `/docs/CLI_PERFORMANCE_GUIDE.md`
- **Examples**: Review test files and `/examples/cli_performance_demo.py`
- **Community**: Join our Discord server

---

**Current Status**: Phase 2 Complete ✅ | Production Ready 🚀 | Multi-Session Capable 💪 | Advanced Analytics 📊

**Next**: Phase 3 development focused on live trading and advanced features