# Trading Bot Backend v2.0 - Phase 2

A high-performance, multi-session crypto trading bot with advanced monitoring, process management, and analytics.

## 🚀 New Phase 2 Features

### Multi-Session Parallel Execution
- **Parallel Sessions**: Run multiple strategies simultaneously across different processes
- **Capacity Management**: Configurable concurrent session limits with automatic capacity checking
- **Resource Isolation**: Each session runs in its own process for maximum stability

### Advanced Process Management
- **Real-time Monitoring**: CPU, memory, and resource usage tracking per session
- **Health Checks**: Automatic detection of unresponsive or crashed processes
- **Force Kill**: Graceful and forced termination of problematic sessions
- **Smart Alerts**: Configurable thresholds with severity-based notifications

### Session Selection & Batch Operations
- **Multi-select**: Select sessions by ID, name pattern, mode, or status
- **Batch Start/Stop**: Start or stop multiple sessions with a single command
- **Dry Run**: Preview operations before execution
- **Progress Tracking**: Real-time feedback on batch operations

### Enhanced Monitoring Dashboard
- **Web UI**: Modern dashboard with real-time updates
- **WebSocket**: Live session status, alerts, and performance metrics
- **System Overview**: CPU, memory, and session capacity monitoring
- **Interactive Controls**: Start, stop, kill sessions directly from the web interface

### Session Replay & State Management
- **Time Travel**: Replay sessions from any point in time
- **State Snapshots**: Automatic state capture at configurable intervals
- **Smart Duplication**: Create new sessions with historical state
- **Event Tracking**: Complete audit trail of all session activities

### Advanced Analytics
- **Performance Reports**: Comprehensive analysis with 20+ metrics
- **Risk Metrics**: Sharpe ratio, Sortino ratio, max drawdown, volatility
- **Comparative Analysis**: Side-by-side session performance comparison
- **Portfolio Analytics**: Combined performance across multiple sessions

## 🏗️ Architecture Overview

```
Phase 2 Architecture:
├── Multi-Session Manager     # Orchestrates parallel execution
├── Process Monitor          # Real-time health & resource monitoring  
├── Session Replay          # State snapshots & time travel
├── Performance Analytics   # Advanced metrics & reporting
├── Web Dashboard          # Real-time monitoring interface
└── Enhanced CLI v2        # Batch operations & advanced controls
```

## 🚀 Quick Start

### Installation

```bash
# Install Phase 2 dependencies
pip install -r requirements.txt

# The new dependencies include:
# - psutil (process monitoring)
# - fastapi + uvicorn (web UI)
# - pandas + numpy (analytics)
```

### Run Modes

```bash
# 1. Web Dashboard (recommended)
python main_v2.py --mode=web --port=8000
# Access: http://localhost:8000

# 2. Enhanced CLI
python main_v2.py --mode=cli
# Or directly: python -m interfaces.cli_v2

# 3. Daemon Mode (background monitoring)
python main_v2.py --mode=daemon
```

## 📊 Web Dashboard

Access the real-time dashboard at `http://localhost:8000`

**Features:**
- Live session monitoring with CPU/memory usage
- System capacity and resource utilization
- Real-time alerts and notifications
- Interactive session controls (start/stop/kill)
- WebSocket updates every 5 seconds

## 🎯 Enhanced CLI Commands

### Multi-Session Operations

```bash
# Create multiple sessions
python -m interfaces.cli_v2 create-sessions --name="Strategy" --count=5

# Start sessions by pattern
python -m interfaces.cli_v2 start-sessions --pattern="Strategy" --dry-run

# Start specific sessions
python -m interfaces.cli_v2 start-sessions --sessions="id1,id2,id3"

# Start all paper trading sessions
python -m interfaces.cli_v2 start-sessions --mode=paper --all
```

### Process Management

```bash
# List running sessions with process info
python -m interfaces.cli_v2 list-running --format=table

# Watch mode (updates every 5 seconds)
python -m interfaces.cli_v2 list-running --watch

# Force kill sessions
python -m interfaces.cli_v2 kill-sessions --sessions="id1,id2" --confirm

# Kill all running sessions
python -m interfaces.cli_v2 kill-sessions --all
```

### Advanced Filtering

```bash
# Filter sessions by multiple criteria
python -m interfaces.cli_v2 list-sessions --mode=paper --status=running --limit=10

# JSON output for scripting
python -m interfaces.cli_v2 list-sessions --format=json > sessions.json

# System status overview
python -m interfaces.cli_v2 system-status
```

## 🔧 Configuration

### Environment Variables

```bash
# Multi-session settings
MAX_CONCURRENT_STRATEGIES=10    # Max parallel sessions
STRATEGY_TIMEOUT=300           # Session timeout (seconds)
RISK_CHECK_INTERVAL=60         # Risk monitoring interval

# Process monitoring
PROCESS_CPU_THRESHOLD=80.0     # CPU alert threshold (%)
PROCESS_MEMORY_THRESHOLD=500   # Memory alert threshold (MB)
PROCESS_CHECK_INTERVAL=5       # Monitoring frequency (seconds)

# Web UI settings
WEB_UI_HOST=0.0.0.0           # Dashboard host
WEB_UI_PORT=8000              # Dashboard port
```

## 📈 Performance Analytics

### Generate Reports

```python
from core.performance_analytics import PerformanceAnalytics
from data.persistence import DatabaseManager

# Initialize
db_manager = DatabaseManager()
analytics = PerformanceAnalytics(db_manager)

# Generate comprehensive report
report = await analytics.generate_performance_report(session_id)

# Key metrics included:
# - Total PnL and returns
# - Sharpe, Sortino, Calmar ratios
# - Win rate and profit factor
# - Maximum drawdown and duration
# - Daily/monthly returns
# - Risk metrics and volatility
```

### Compare Sessions

```python
# Compare multiple sessions
comparison = await analytics.compare_sessions([
    "session1", "session2", "session3"
])

# Get portfolio analytics
portfolio = await analytics.get_portfolio_analytics([
    "session1", "session2", "session3"
])
```

## 🔄 Session Replay

### Create Snapshots

```python
from core.session_replay import SessionReplayManager

replay_manager = SessionReplayManager(db_manager)

# Automatic snapshots every 5 minutes
await replay_manager.create_snapshot(session_id, session)

# Record events for replay
await replay_manager.record_event(session_id, "trade", {
    "symbol": "BTCUSD",
    "action": "buy",
    "amount": 0.1,
    "price": 50000
})
```

### Replay & Duplicate

```python
# Replay session from specific time
await replay_manager.replay_session(
    session_id,
    start_time=datetime(2024, 1, 1),
    speed_multiplier=10.0  # 10x speed
)

# Duplicate with historical state
new_session = await replay_manager.duplicate_session_with_state(
    original_session_id,
    new_name="Strategy Copy",
    snapshot_time=datetime(2024, 1, 15)
)
```

## 🚨 Monitoring & Alerts

### Process Health Monitoring

- **CPU Usage**: Alert when sessions exceed 80% CPU
- **Memory Usage**: Monitor memory consumption and leaks
- **Responsiveness**: Detect hanging or crashed processes
- **Resource Limits**: Track file handles and network connections

### Alert System

- **Severity Levels**: Low, Medium, High, Critical
- **Real-time Notifications**: WebSocket alerts to dashboard
- **Callback System**: Custom alert handlers
- **Auto-resolution**: Alerts resolve when conditions clear

### System Statistics

```python
# Get comprehensive system stats
stats = await multi_session_manager.get_system_stats()

# Includes:
# - Active sessions count
# - Available capacity
# - Total CPU/memory usage
# - System resource utilization
```

## 🔒 Safety Features

### Process Isolation
- Each session runs in separate process
- Memory leaks contained per session
- Crash isolation - one failure doesn't affect others
- Resource limits per process

### Graceful Shutdown
- SIGTERM handling for clean shutdown
- Session state preservation
- Connection cleanup
- Resource deallocation

### Error Recovery
- Automatic process restart on crash
- Session state recovery from snapshots
- Alert generation on failures
- Fallback mechanisms

## 🎯 Use Cases

### 1. Multiple Strategy Testing
```bash
# Create 10 different strategy configurations
python -m interfaces.cli_v2 create-sessions --name="SMA_Test" --count=10

# Start all in parallel
python -m interfaces.cli_v2 start-sessions --pattern="SMA_Test" --all

# Monitor via web dashboard
python main_v2.py --mode=web
```

### 2. Production Monitoring
```bash
# Run monitoring daemon
python main_v2.py --mode=daemon

# Monitor specific sessions
python -m interfaces.cli_v2 list-running --watch

# Get system status
python -m interfaces.cli_v2 system-status
```

### 3. Performance Analysis
```python
# Compare all paper trading sessions
comparison = await analytics.compare_sessions(paper_session_ids)

# Export detailed report
report_json = analytics.export_report_to_json(report)
```

## 🔧 API Reference

### REST API Endpoints

```http
GET    /api/sessions              # List all sessions
POST   /api/sessions              # Create new session
GET    /api/sessions/{id}         # Get session details
DELETE /api/sessions/{id}         # Delete session

POST   /api/sessions/start        # Start multiple sessions
POST   /api/sessions/stop         # Stop multiple sessions  
POST   /api/sessions/kill         # Force kill sessions

GET    /api/sessions/running      # Get running sessions
GET    /api/system/status         # System statistics
GET    /api/alerts               # Get alerts

WebSocket: /ws                    # Real-time updates
```

### CLI v2 Commands

```bash
# Session management
create-sessions    # Create single or multiple sessions
start-sessions     # Start sessions with selection criteria
stop-sessions      # Stop sessions gracefully or forcefully
kill-sessions      # Force terminate sessions
list-sessions      # List with advanced filtering
list-running       # Show running sessions with process info

# Monitoring
system-status      # Overall system statistics
show-session       # Detailed session information

# Legacy compatibility
duplicate-session  # Duplicate existing session
delete-session     # Delete session
```

## 🚀 Performance Optimizations

### Multi-Core Utilization
- Process-based parallelism for true multi-core usage
- Independent GIL per session process
- Configurable process pool size
- CPU affinity control (future enhancement)

### Memory Management
- Process isolation prevents memory leaks
- Automatic cleanup of old data
- Configurable retention policies
- Memory usage monitoring and alerts

### Resource Efficiency
- Lazy loading of heavy dependencies
- Connection pooling for database access
- WebSocket connection management
- Efficient data serialization

## 📋 Migration from Phase 1

Phase 2 is fully backward compatible with Phase 1:

```bash
# Your existing sessions continue to work
python -m interfaces.cli list-sessions

# Enhanced with new features
python -m interfaces.cli_v2 list-sessions --format=json

# Web UI provides same functionality
python main_v2.py --mode=web
```

## 🛣️ Roadmap to Phase 3

Planned Phase 3 enhancements:
- Live trading mode with enhanced risk management
- Machine learning strategy optimization
- Advanced portfolio rebalancing
- Multi-exchange support
- Strategy marketplace and templates
- Cloud deployment automation

---

**Phase 2 Summary**: Multi-session parallel execution with advanced monitoring, process management, session replay, and comprehensive analytics. Ready for production-scale trading operations.

## 🆘 Troubleshooting

### Common Issues

**Sessions not starting:**
```bash
# Check system capacity
python -m interfaces.cli_v2 system-status

# Verify session configuration
python -m interfaces.cli_v2 show-session <session-id>
```

**High resource usage:**
```bash
# Monitor process health
python -m interfaces.cli_v2 list-running --watch

# Check for alerts
curl http://localhost:8000/api/alerts
```

**Web UI not accessible:**
```bash
# Check if port is available
python main_v2.py --mode=web --port=8001

# Verify firewall settings
# Check logs for binding errors
```