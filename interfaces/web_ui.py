from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import json
from typing import List, Dict, Optional
from datetime import datetime
import uvicorn

from data.models import SessionMode, StrategyConfig, RiskConfig, SessionStatus
from data.persistence import DatabaseManager
from core.multi_session_manager import MultiSessionManager
from core.process_monitor import ProcessAlert


app = FastAPI(title="Trading Bot Dashboard", version="2.0.0")

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global instances
db_manager: Optional[DatabaseManager] = None
multi_session_manager: Optional[MultiSessionManager] = None
websocket_connections: List[WebSocket] = []


@app.on_event("startup")
async def startup_event():
    """Initialize components on startup"""
    global db_manager, multi_session_manager
    
    db_manager = DatabaseManager()
    multi_session_manager = MultiSessionManager(db_manager)
    await multi_session_manager.initialize()
    
    # Add alert callback to broadcast to websockets
    multi_session_manager.process_monitor.add_alert_callback(broadcast_alert)


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    if multi_session_manager:
        await multi_session_manager.shutdown()


async def broadcast_alert(alert: ProcessAlert):
    """Broadcast alert to all connected websockets"""
    message = {
        "type": "alert",
        "data": {
            "session_id": alert.session_id,
            "alert_type": alert.alert_type,
            "severity": alert.severity,
            "message": alert.message,
            "timestamp": alert.timestamp.isoformat()
        }
    }
    
    # Remove disconnected connections
    disconnected = []
    for websocket in websocket_connections:
        try:
            await websocket.send_text(json.dumps(message))
        except:
            disconnected.append(websocket)
    
    for ws in disconnected:
        websocket_connections.remove(ws)


async def broadcast_session_update(session_id: str, event_type: str):
    """Broadcast session status update to all websockets"""
    message = {
        "type": "session_update",
        "data": {
            "session_id": session_id,
            "event_type": event_type,
            "timestamp": datetime.utcnow().isoformat()
        }
    }
    
    disconnected = []
    for websocket in websocket_connections:
        try:
            await websocket.send_text(json.dumps(message))
        except:
            disconnected.append(websocket)
    
    for ws in disconnected:
        websocket_connections.remove(ws)


# WebSocket endpoint for real-time updates
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    websocket_connections.append(websocket)
    
    try:
        while True:
            # Send periodic status updates
            await asyncio.sleep(5)
            
            system_stats = await multi_session_manager.get_system_stats()
            running_sessions = multi_session_manager.list_running_sessions()
            
            status_message = {
                "type": "status_update",
                "data": {
                    "system_stats": system_stats,
                    "running_sessions": [
                        {
                            "session_id": p.session_id,
                            "status": p.status,
                            "cpu_percent": p.cpu_percent,
                            "memory_mb": p.memory_mb,
                            "started_at": p.started_at.isoformat(),
                            "runtime_minutes": (datetime.utcnow() - p.started_at).seconds // 60
                        }
                        for p in running_sessions
                    ],
                    "timestamp": datetime.utcnow().isoformat()
                }
            }
            
            await websocket.send_text(json.dumps(status_message))
            
    except WebSocketDisconnect:
        websocket_connections.remove(websocket)


# API Endpoints

@app.get("/api/sessions")
async def list_sessions(
    mode: Optional[str] = None,
    status: Optional[str] = None,
    limit: Optional[int] = None
):
    """List all sessions with optional filtering"""
    sessions = await multi_session_manager.session_manager.list_sessions()
    
    # Apply filters
    if mode:
        sessions = [s for s in sessions if s.mode.value == mode]
    if status:
        sessions = [s for s in sessions if s.status.value == status]
    if limit:
        sessions = sessions[:limit]
    
    return {
        "sessions": [
            {
                "id": s.id,
                "name": s.name,
                "mode": s.mode.value,
                "status": s.status.value,
                "strategy": s.strategy.name,
                "pairs": s.strategy.pairs,
                "created_at": s.created_at.isoformat(),
                "updated_at": s.updated_at.isoformat(),
                "performance": {
                    "total_pnl": s.performance_metrics.total_pnl,
                    "total_trades": s.performance_metrics.total_trades,
                    "win_rate": s.performance_metrics.win_rate
                }
            }
            for s in sessions
        ]
    }


@app.post("/api/sessions")
async def create_session(session_data: dict):
    """Create a new trading session"""
    try:
        strategy_config = StrategyConfig(
            name=session_data.get("strategy", "SimpleMovingAverage"),
            parameters=session_data.get("strategy_parameters", {
                "short_window": 10,
                "long_window": 30,
                "max_position_size": 0.1
            }),
            timeframe=session_data.get("timeframe", "1h"),
            pairs=session_data.get("pairs", ["BTCUSD"])
        )
        
        risk_config = RiskConfig(
            max_position_size=session_data.get("max_position_size", 0.1),
            stop_loss_pct=session_data.get("stop_loss_pct", 2.0),
            take_profit_pct=session_data.get("take_profit_pct", 4.0),
            max_daily_loss=session_data.get("max_daily_loss", 10.0),
            max_exposure_pct=session_data.get("max_exposure_pct", 50.0)
        )
        
        session = await multi_session_manager.session_manager.create_session(
            name=session_data["name"],
            mode=SessionMode(session_data.get("mode", "paper")),
            strategy=strategy_config,
            risk_params=risk_config
        )
        
        return {"success": True, "session_id": session.id}
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/sessions/start")
async def start_sessions(request_data: dict):
    """Start multiple sessions"""
    try:
        session_ids = request_data["session_ids"]
        api_keys = request_data.get("api_keys", {})
        api_secrets = request_data.get("api_secrets", {})
        
        results = await multi_session_manager.start_sessions(
            session_ids, api_keys, api_secrets
        )
        
        # Broadcast updates
        for session_id in session_ids:
            await broadcast_session_update(session_id, "start_attempt")
        
        return {"success": True, "results": results}
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/sessions/stop")
async def stop_sessions(request_data: dict):
    """Stop multiple sessions"""
    try:
        session_ids = request_data["session_ids"]
        force = request_data.get("force", False)
        
        results = await multi_session_manager.stop_sessions(session_ids, force)
        
        # Broadcast updates
        for session_id in session_ids:
            await broadcast_session_update(session_id, "stop_attempt")
        
        return {"success": True, "results": results}
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/sessions/kill")
async def kill_sessions(request_data: dict):
    """Force kill multiple sessions"""
    try:
        session_ids = request_data["session_ids"]
        
        results = await multi_session_manager.kill_sessions(session_ids)
        
        # Broadcast updates
        for session_id in session_ids:
            await broadcast_session_update(session_id, "kill_attempt")
        
        return {"success": True, "results": results}
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/sessions/running")
async def get_running_sessions():
    """Get all running sessions with process info"""
    try:
        running_processes = multi_session_manager.list_running_sessions()
        
        sessions_data = []
        for process in running_processes:
            session = await multi_session_manager.session_manager.get_session(process.session_id)
            health = multi_session_manager.process_monitor.get_process_health(process.session_id)
            
            session_data = {
                "session_id": process.session_id,
                "name": session.name if session else "Unknown",
                "mode": session.mode.value if session else "unknown",
                "status": process.status,
                "started_at": process.started_at.isoformat(),
                "runtime_minutes": (datetime.utcnow() - process.started_at).seconds // 60,
                "cpu_percent": process.cpu_percent,
                "memory_mb": process.memory_mb
            }
            
            if health:
                session_data.update({
                    "threads_count": health.threads_count,
                    "open_files": health.open_files,
                    "connections": health.connections,
                    "is_responsive": health.is_responsive,
                    "error_count": health.error_count
                })
            
            sessions_data.append(session_data)
        
        return {"running_sessions": sessions_data}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/sessions/{session_id}")
async def get_session_details(session_id: str):
    """Get detailed information about a specific session"""
    try:
        session = await multi_session_manager.session_manager.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        
        # Get process info if running
        process_info = multi_session_manager.get_session_process(session_id)
        health_info = multi_session_manager.process_monitor.get_process_health(session_id)
        alerts = multi_session_manager.process_monitor.get_alerts(session_id)
        
        session_data = {
            "id": session.id,
            "name": session.name,
            "mode": session.mode.value,
            "status": session.status.value,
            "created_at": session.created_at.isoformat(),
            "updated_at": session.updated_at.isoformat(),
            "strategy": {
                "name": session.strategy.name,
                "parameters": session.strategy.parameters,
                "timeframe": session.strategy.timeframe,
                "pairs": session.strategy.pairs
            },
            "risk_params": {
                "max_position_size": session.risk_params.max_position_size,
                "stop_loss_pct": session.risk_params.stop_loss_pct,
                "take_profit_pct": session.risk_params.take_profit_pct,
                "max_daily_loss": session.risk_params.max_daily_loss,
                "max_exposure_pct": session.risk_params.max_exposure_pct
            },
            "performance": {
                "total_pnl": session.performance_metrics.total_pnl,
                "win_rate": session.performance_metrics.win_rate,
                "total_trades": session.performance_metrics.total_trades,
                "winning_trades": session.performance_metrics.winning_trades,
                "losing_trades": session.performance_metrics.losing_trades,
                "max_drawdown": session.performance_metrics.max_drawdown,
                "sharpe_ratio": session.performance_metrics.sharpe_ratio
            },
            "state": session.state
        }
        
        if process_info:
            session_data["process"] = {
                "process_id": process_info.process_id,
                "started_at": process_info.started_at.isoformat(),
                "runtime_minutes": (datetime.utcnow() - process_info.started_at).seconds // 60,
                "cpu_percent": process_info.cpu_percent,
                "memory_mb": process_info.memory_mb
            }
        
        if health_info:
            session_data["health"] = {
                "threads_count": health_info.threads_count,
                "open_files": health_info.open_files,
                "connections": health_info.connections,
                "is_responsive": health_info.is_responsive,
                "error_count": health_info.error_count,
                "last_check": health_info.last_check.isoformat()
            }
        
        session_data["alerts"] = [
            {
                "alert_type": alert.alert_type,
                "severity": alert.severity,
                "message": alert.message,
                "timestamp": alert.timestamp.isoformat(),
                "resolved": alert.resolved
            }
            for alert in alerts
        ]
        
        return session_data
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/system/status")
async def get_system_status():
    """Get overall system status"""
    try:
        system_stats = await multi_session_manager.get_system_stats()
        process_stats = multi_session_manager.process_monitor.get_system_stats()
        
        # Get alerts summary
        all_alerts = multi_session_manager.process_monitor.get_alerts(unresolved_only=False)
        unresolved_alerts = multi_session_manager.process_monitor.get_alerts(unresolved_only=True)
        
        alerts_by_severity = {}
        for alert in unresolved_alerts:
            alerts_by_severity[alert.severity] = alerts_by_severity.get(alert.severity, 0) + 1
        
        return {
            "system_stats": system_stats,
            "process_stats": process_stats,
            "alerts": {
                "total": len(all_alerts),
                "unresolved": len(unresolved_alerts),
                "by_severity": alerts_by_severity
            },
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/alerts")
async def get_alerts(
    session_id: Optional[str] = None,
    unresolved_only: bool = True,
    limit: Optional[int] = None
):
    """Get alerts with optional filtering"""
    try:
        alerts = multi_session_manager.process_monitor.get_alerts(session_id, unresolved_only)
        
        if limit:
            alerts = alerts[:limit]
        
        return {
            "alerts": [
                {
                    "session_id": alert.session_id,
                    "alert_type": alert.alert_type,
                    "severity": alert.severity,
                    "message": alert.message,
                    "timestamp": alert.timestamp.isoformat(),
                    "resolved": alert.resolved
                }
                for alert in alerts
            ]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/sessions/{session_id}")
async def delete_session(session_id: str):
    """Delete a session"""
    try:
        # Stop session if running
        if session_id in [p.session_id for p in multi_session_manager.list_running_sessions()]:
            await multi_session_manager.stop_sessions([session_id], force=True)
        
        # Delete from database
        success = await multi_session_manager.session_manager.delete_session(session_id)
        
        if success:
            await broadcast_session_update(session_id, "deleted")
            return {"success": True}
        else:
            raise HTTPException(status_code=404, detail="Session not found")
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Serve static files (for frontend)
@app.get("/", response_class=HTMLResponse)
async def read_dashboard():
    """Serve the main dashboard HTML"""
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Trading Bot Dashboard</title>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }
            .container { max-width: 1200px; margin: 0 auto; }
            .card { background: white; padding: 20px; margin: 10px 0; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
            .header { text-align: center; color: #333; }
            .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin: 20px 0; }
            .stat { text-align: center; padding: 15px; background: #f8f9fa; border-radius: 6px; }
            .stat-value { font-size: 24px; font-weight: bold; color: #28a745; }
            .stat-label { font-size: 14px; color: #666; margin-top: 5px; }
            .sessions-table { width: 100%; border-collapse: collapse; margin-top: 10px; }
            .sessions-table th, .sessions-table td { padding: 10px; text-align: left; border-bottom: 1px solid #ddd; }
            .sessions-table th { background-color: #f8f9fa; font-weight: bold; }
            .status { padding: 4px 8px; border-radius: 4px; font-size: 12px; font-weight: bold; }
            .status.running { background-color: #d4edda; color: #155724; }
            .status.stopped { background-color: #f8d7da; color: #721c24; }
            .status.created { background-color: #fff3cd; color: #856404; }
            .btn { padding: 8px 16px; border: none; border-radius: 4px; cursor: pointer; margin: 2px; }
            .btn-primary { background-color: #007bff; color: white; }
            .btn-danger { background-color: #dc3545; color: white; }
            .btn-warning { background-color: #ffc107; color: black; }
            .alerts { margin-top: 20px; }
            .alert { padding: 10px; margin: 5px 0; border-radius: 4px; }
            .alert.high { background-color: #f8d7da; color: #721c24; }
            .alert.medium { background-color: #fff3cd; color: #856404; }
            .alert.low { background-color: #d1ecf1; color: #0c5460; }
            .connected { color: #28a745; }
            .disconnected { color: #dc3545; }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="card">
                <h1 class="header">Trading Bot Dashboard v2.0</h1>
                <p style="text-align: center;">
                    WebSocket Status: <span id="ws-status" class="disconnected">Disconnected</span>
                </p>
            </div>
            
            <div class="card">
                <h2>System Statistics</h2>
                <div class="stats" id="system-stats">
                    <!-- Stats will be populated by JavaScript -->
                </div>
            </div>
            
            <div class="card">
                <h2>Running Sessions</h2>
                <table class="sessions-table" id="running-sessions-table">
                    <thead>
                        <tr>
                            <th>Session ID</th>
                            <th>Name</th>
                            <th>Status</th>
                            <th>Runtime</th>
                            <th>CPU %</th>
                            <th>Memory (MB)</th>
                            <th>Actions</th>
                        </tr>
                    </thead>
                    <tbody id="running-sessions-body">
                        <!-- Sessions will be populated by JavaScript -->
                    </tbody>
                </table>
            </div>
            
            <div class="card alerts" id="alerts-section">
                <h2>Recent Alerts</h2>
                <div id="alerts-container">
                    <!-- Alerts will be populated by JavaScript -->
                </div>
            </div>
        </div>
        
        <script>
            let ws;
            let wsStatus = document.getElementById('ws-status');
            
            function connectWebSocket() {
                const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
                ws = new WebSocket(`${protocol}//${window.location.host}/ws`);
                
                ws.onopen = function() {
                    wsStatus.textContent = 'Connected';
                    wsStatus.className = 'connected';
                };
                
                ws.onclose = function() {
                    wsStatus.textContent = 'Disconnected';
                    wsStatus.className = 'disconnected';
                    setTimeout(connectWebSocket, 5000); // Reconnect after 5 seconds
                };
                
                ws.onmessage = function(event) {
                    const data = JSON.parse(event.data);
                    handleWebSocketMessage(data);
                };
            }
            
            function handleWebSocketMessage(data) {
                if (data.type === 'status_update') {
                    updateSystemStats(data.data.system_stats);
                    updateRunningSessions(data.data.running_sessions);
                } else if (data.type === 'alert') {
                    addAlert(data.data);
                }
            }
            
            function updateSystemStats(stats) {
                const container = document.getElementById('system-stats');
                container.innerHTML = `
                    <div class="stat">
                        <div class="stat-value">${stats.active_sessions}/${stats.max_concurrent}</div>
                        <div class="stat-label">Active Sessions</div>
                    </div>
                    <div class="stat">
                        <div class="stat-value">${stats.available_slots}</div>
                        <div class="stat-label">Available Slots</div>
                    </div>
                    <div class="stat">
                        <div class="stat-value">${stats.system_cpu_percent.toFixed(1)}%</div>
                        <div class="stat-label">System CPU</div>
                    </div>
                    <div class="stat">
                        <div class="stat-value">${stats.system_memory_percent.toFixed(1)}%</div>
                        <div class="stat-label">System Memory</div>
                    </div>
                `;
            }
            
            function updateRunningSessions(sessions) {
                const tbody = document.getElementById('running-sessions-body');
                tbody.innerHTML = sessions.map(session => `
                    <tr>
                        <td>${session.session_id.substring(0, 8)}...</td>
                        <td>${session.name || 'Unknown'}</td>
                        <td><span class="status ${session.status}">${session.status}</span></td>
                        <td>${session.runtime_minutes}m</td>
                        <td>${session.cpu_percent.toFixed(1)}%</td>
                        <td>${session.memory_mb.toFixed(1)}</td>
                        <td>
                            <button class="btn btn-warning" onclick="stopSession('${session.session_id}')">Stop</button>
                            <button class="btn btn-danger" onclick="killSession('${session.session_id}')">Kill</button>
                        </td>
                    </tr>
                `).join('');
            }
            
            function addAlert(alert) {
                const container = document.getElementById('alerts-container');
                const alertElement = document.createElement('div');
                alertElement.className = `alert ${alert.severity}`;
                alertElement.innerHTML = `
                    <strong>${alert.alert_type}</strong> - ${alert.message}
                    <br><small>Session: ${alert.session_id.substring(0, 8)}... at ${new Date(alert.timestamp).toLocaleString()}</small>
                `;
                container.insertBefore(alertElement, container.firstChild);
                
                // Keep only last 10 alerts
                while (container.children.length > 10) {
                    container.removeChild(container.lastChild);
                }
            }
            
            async function stopSession(sessionId) {
                try {
                    const response = await fetch('/api/sessions/stop', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ session_ids: [sessionId] })
                    });
                    const result = await response.json();
                    console.log('Stop result:', result);
                } catch (error) {
                    console.error('Error stopping session:', error);
                }
            }
            
            async function killSession(sessionId) {
                if (confirm('Are you sure you want to force kill this session?')) {
                    try {
                        const response = await fetch('/api/sessions/kill', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({ session_ids: [sessionId] })
                        });
                        const result = await response.json();
                        console.log('Kill result:', result);
                    } catch (error) {
                        console.error('Error killing session:', error);
                    }
                }
            }
            
            // Initialize
            connectWebSocket();
            
            // Load initial data
            fetch('/api/system/status')
                .then(response => response.json())
                .then(data => {
                    updateSystemStats(data.system_stats);
                });
            
            fetch('/api/sessions/running')
                .then(response => response.json())
                .then(data => {
                    updateRunningSessions(data.running_sessions);
                });
            
            fetch('/api/alerts?limit=10')
                .then(response => response.json())
                .then(data => {
                    data.alerts.forEach(alert => addAlert(alert));
                });
        </script>
    </body>
    </html>
    """


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)