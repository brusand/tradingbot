"""
TradingCLI GUI avec PySide6
Interface graphique qui utilise le backend Click existant
Résout les problèmes d'event loop asyncio
"""

import sys
import asyncio
import threading
import time
from datetime import datetime
from typing import Optional, Dict, Any
import logging

from PySide6.QtWidgets import (
    QApplication, QMainWindow, QVBoxLayout, QHBoxLayout, 
    QWidget, QTextEdit, QLineEdit, QPushButton, QLabel,
    QTabWidget, QTableWidget, QTableWidgetItem, QSplitter,
    QStatusBar, QMenuBar, QMessageBox, QComboBox, QSpinBox,
    QCheckBox, QGroupBox, QGridLayout, QScrollArea
)
from PySide6.QtCore import QThread, Signal, QTimer, Qt, QObject
from PySide6.QtGui import QFont, QTextCursor, QAction

# Import du CLI existant
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import de toute la logique Click existante
from cli.trading_cli import cli, TradingCLI
import click
from click.testing import CliRunner


class ClickCommandThread(QThread):
    """Thread pour exécuter les commandes Click sans bloquer l'UI"""
    
    command_finished = Signal(str, bool)  # output, success
    log_message = Signal(str)
    
    def __init__(self, command_args: list, cli_obj: TradingCLI):
        super().__init__()
        self.command_args = command_args
        self.cli_obj = cli_obj
        
    def run(self):
        """Exécute la commande Click dans un thread séparé"""
        try:
            runner = CliRunner()
            
            # Créer un contexte avec l'objet CLI
            with cli.make_context('cli', [], obj={'cli': self.cli_obj}) as ctx:
                # Exécuter la commande
                result = runner.invoke(cli, self.command_args, obj={'cli': self.cli_obj}, catch_exceptions=False)
                
                self.command_finished.emit(result.output, result.exit_code == 0)
                
        except Exception as e:
            error_msg = f"❌ Erreur: {str(e)}"
            self.log_message.emit(error_msg)
            self.command_finished.emit(error_msg, False)


class AsyncioEventLoopThread(QThread):
    """Thread dédié pour l'event loop asyncio"""
    
    def __init__(self):
        super().__init__()
        self.loop = None
        
    def run(self):
        """Lance l'event loop asyncio dans un thread dédié"""
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        
        try:
            self.loop.run_forever()
        except Exception as e:
            print(f"Erreur dans l'event loop asyncio: {e}")
        finally:
            self.loop.close()
    
    def stop_loop(self):
        """Arrête l'event loop"""
        if self.loop and self.loop.is_running():
            self.loop.call_soon_threadsafe(self.loop.stop)


class LogHandler(logging.Handler):
    """Handler pour rediriger les logs vers l'interface"""
    
    def __init__(self, log_signal):
        super().__init__()
        self.log_signal = log_signal
        
    def emit(self, record):
        msg = self.format(record)
        self.log_signal.emit(msg)


class TradingGUI(QMainWindow):
    """Interface graphique principale pour le Trading CLI"""
    
    log_received = Signal(str)
    
    def __init__(self):
        super().__init__()
        
        # Initialiser le CLI backend
        self.trading_cli = TradingCLI()
        
        # Thread pour asyncio
        self.asyncio_thread = AsyncioEventLoopThread()
        self.asyncio_thread.start()
        
        # Configuration des logs
        self.setup_logging()
        
        # Interface utilisateur
        self.setup_ui()
        
        # Connecter les signaux
        self.log_received.connect(self.append_log)
        
        # Timer pour mise à jour des métriques
        self.metrics_timer = QTimer()
        self.metrics_timer.timeout.connect(self.update_metrics)
        self.metrics_timer.start(5000)  # Toutes les 5 secondes
        
    def setup_logging(self):
        """Configure les logs pour rediriger vers l'interface"""
        log_handler = LogHandler(self.log_received)
        log_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        
        # Ajouter le handler aux loggers principaux
        loggers = ['PubSubEngine', 'services.kraken_market_service', 'core.strategy_workflow']
        for logger_name in loggers:
            logger = logging.getLogger(logger_name)
            logger.addHandler(log_handler)
            logger.setLevel(logging.DEBUG)
    
    def setup_ui(self):
        """Configure l'interface utilisateur"""
        self.setWindowTitle("🚀 Trading CLI - Interface Graphique")
        self.setGeometry(100, 100, 1400, 900)
        
        # Widget central avec splitter
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        layout = QHBoxLayout(central_widget)
        
        # Splitter principal
        main_splitter = QSplitter(Qt.Horizontal)
        layout.addWidget(main_splitter)
        
        # Panel gauche - Commandes
        self.setup_command_panel(main_splitter)
        
        # Panel droit - Logs et données
        self.setup_data_panel(main_splitter)
        
        # Barre de statut
        self.setup_status_bar()
        
        # Menu
        self.setup_menu()
        
    def setup_command_panel(self, parent):
        """Panel de gauche avec les commandes"""
        command_widget = QWidget()
        command_layout = QVBoxLayout(command_widget)
        
        # Zone de commande manuelle
        cmd_group = QGroupBox("💻 Ligne de commande")
        cmd_layout = QVBoxLayout(cmd_group)
        
        self.command_input = QLineEdit()
        self.command_input.setPlaceholderText("Tapez votre commande ici (ex: session start ema)")
        self.command_input.returnPressed.connect(self.execute_command)
        
        self.execute_btn = QPushButton("🚀 Exécuter")
        self.execute_btn.clicked.connect(self.execute_command)
        
        cmd_layout.addWidget(QLabel("Commande:"))
        cmd_layout.addWidget(self.command_input)
        cmd_layout.addWidget(self.execute_btn)
        
        command_layout.addWidget(cmd_group)
        
        # Commandes rapides
        quick_group = QGroupBox("⚡ Commandes rapides")
        quick_layout = QGridLayout(quick_group)
        
        # Boutons système
        sys_start_btn = QPushButton("🔧 System Start")
        sys_start_btn.clicked.connect(lambda: self.execute_quick_command("system start"))
        
        sys_status_btn = QPushButton("📊 System Status")
        sys_status_btn.clicked.connect(lambda: self.execute_quick_command("system status"))
        
        sys_stop_btn = QPushButton("🛑 System Stop")
        sys_stop_btn.clicked.connect(lambda: self.execute_quick_command("system stop"))
        
        # Boutons sessions
        session_list_btn = QPushButton("📋 List Sessions")
        session_list_btn.clicked.connect(lambda: self.execute_quick_command("session list"))
        
        session_create_btn = QPushButton("➕ Create Session")
        session_create_btn.clicked.connect(self.show_create_session_dialog)
        
        # Layout des boutons
        quick_layout.addWidget(sys_start_btn, 0, 0)
        quick_layout.addWidget(sys_status_btn, 0, 1)
        quick_layout.addWidget(sys_stop_btn, 1, 0)
        quick_layout.addWidget(session_list_btn, 1, 1)
        quick_layout.addWidget(session_create_btn, 2, 0, 1, 2)
        
        command_layout.addWidget(quick_group)
        
        # Formulaire de démarrage de session
        session_group = QGroupBox("🎯 Démarrage de session")
        session_layout = QVBoxLayout(session_group)
        
        self.session_combo = QComboBox()
        self.session_combo.setEditable(True)
        self.session_combo.setPlaceholderText("ID de session (ex: ema)")
        
        self.force_checkbox = QCheckBox("Force (no confirmation)")
        
        start_session_btn = QPushButton("▶️ Start Session")
        start_session_btn.clicked.connect(self.start_session)
        
        stop_session_btn = QPushButton("🛑 Stop Session")
        stop_session_btn.clicked.connect(self.stop_session)
        
        session_layout.addWidget(QLabel("Session ID:"))
        session_layout.addWidget(self.session_combo)
        session_layout.addWidget(self.force_checkbox)
        
        # Layout horizontal pour les boutons Start/Stop
        buttons_layout = QHBoxLayout()
        buttons_layout.addWidget(start_session_btn)
        buttons_layout.addWidget(stop_session_btn)
        session_layout.addLayout(buttons_layout)
        
        command_layout.addWidget(session_group)
        
        # Spacer
        command_layout.addStretch()
        
        # Définir taille minimale
        command_widget.setMinimumWidth(350)
        command_widget.setMaximumWidth(400)
        
        parent.addWidget(command_widget)
        
    def setup_data_panel(self, parent):
        """Panel de droite avec logs et données"""
        data_widget = QTabWidget()
        
        # Tab Logs
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setFont(QFont("Consolas", 9))
        data_widget.addTab(self.log_text, "📋 Logs")
        
        # Tab Métriques
        metrics_widget = QWidget()
        metrics_layout = QVBoxLayout(metrics_widget)
        
        self.metrics_table = QTableWidget()
        self.metrics_table.setColumnCount(2)
        self.metrics_table.setHorizontalHeaderLabels(["Métrique", "Valeur"])
        
        metrics_layout.addWidget(self.metrics_table)
        data_widget.addTab(metrics_widget, "📊 Métriques")
        
        # Tab Sessions
        sessions_widget = QWidget()
        sessions_layout = QVBoxLayout(sessions_widget)
        
        self.sessions_table = QTableWidget()
        self.sessions_table.setColumnCount(4)
        self.sessions_table.setHorizontalHeaderLabels(["ID", "Nom", "Status", "Mode"])
        
        sessions_layout.addWidget(self.sessions_table)
        data_widget.addTab(sessions_widget, "🎯 Sessions")
        
        parent.addWidget(data_widget)
        
    def setup_status_bar(self):
        """Configure la barre de statut"""
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        
        self.status_label = QLabel("🔴 Système arrêté")
        self.status_bar.addWidget(self.status_label)
        
        # Label pour l'heure
        self.time_label = QLabel()
        self.status_bar.addPermanentWidget(self.time_label)
        
        # Timer pour l'heure
        timer = QTimer()
        timer.timeout.connect(self.update_time)
        timer.start(1000)
        
    def setup_menu(self):
        """Configure le menu"""
        menubar = self.menuBar()
        
        # Menu Fichier
        file_menu = menubar.addMenu("📁 Fichier")
        
        export_action = QAction("📤 Exporter logs", self)
        export_action.triggered.connect(self.export_logs)
        file_menu.addAction(export_action)
        
        file_menu.addSeparator()
        
        exit_action = QAction("❌ Quitter", self)
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)
        
        # Menu Aide
        help_menu = menubar.addMenu("❓ Aide")
        
        about_action = QAction("ℹ️ À propos", self)
        about_action.triggered.connect(self.show_about)
        help_menu.addAction(about_action)
        
    def execute_command(self):
        """Exécute une commande saisie manuellement"""
        command = self.command_input.text().strip()
        if not command:
            return
            
        self.append_log(f"📝 Commande: {command}")
        
        # Parser la commande
        args = command.split()
        
        # Exécuter dans un thread
        self.command_thread = ClickCommandThread(args, self.trading_cli)
        self.command_thread.command_finished.connect(self.on_command_finished)
        self.command_thread.log_message.connect(self.append_log)
        self.command_thread.start()
        
        # Vider la ligne de commande
        self.command_input.clear()
        
    def execute_quick_command(self, command: str):
        """Exécute une commande rapide"""
        self.command_input.setText(command)
        self.execute_command()
        
    def start_session(self):
        """Démarre une session"""
        session_id = self.session_combo.currentText().strip()
        if not session_id:
            QMessageBox.warning(self, "Erreur", "Veuillez saisir un ID de session")
            return
            
        command = f"session start {session_id}"
        if self.force_checkbox.isChecked():
            command += " --force"
            
        self.execute_quick_command(command)
    
    def stop_session(self):
        """Arrête une session"""
        session_id = self.session_combo.currentText().strip()
        if not session_id:
            QMessageBox.warning(self, "Erreur", "Veuillez saisir un ID de session")
            return
        
        # Demander confirmation à l'utilisateur
        reply = QMessageBox.question(
            self, 
            "Confirmer l'arrêt", 
            f"Voulez-vous vraiment arrêter la session '{session_id}' ?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            command = f"session stop {session_id}"
            if self.force_checkbox.isChecked():
                command += " --force"
                
            self.execute_quick_command(command)
        
    def show_create_session_dialog(self):
        """Affiche un dialog pour créer une session"""
        # Pour l'instant, utiliser la commande directe
        self.execute_quick_command("session create")
        
    def on_command_finished(self, output: str, success: bool):
        """Appelé quand une commande est terminée"""
        if output.strip():
            self.append_log(output)
            
        if success:
            self.status_label.setText("✅ Commande exécutée")
        else:
            self.status_label.setText("❌ Erreur dans la commande")
            
    def append_log(self, message: str):
        """Ajoute un message aux logs"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        formatted_message = f"[{timestamp}] {message}"
        
        self.log_text.append(formatted_message)
        
        # Auto-scroll vers le bas
        cursor = self.log_text.textCursor()
        cursor.movePosition(QTextCursor.End)
        self.log_text.setTextCursor(cursor)
        
    def update_time(self):
        """Met à jour l'heure dans la barre de statut"""
        current_time = datetime.now().strftime("%H:%M:%S")
        self.time_label.setText(f"🕐 {current_time}")
        
    def update_metrics(self):
        """Met à jour les métriques affichées"""
        # TODO: Récupérer les vraies métriques du système
        metrics = {
            "Status": "Running" if hasattr(self.trading_cli, 'workflow_running') and self.trading_cli.workflow_running else "Stopped",
            "Sessions actives": len(getattr(self.trading_cli, 'sessions_registry', {})),
            "Stratégies": len(getattr(self.trading_cli, 'strategies_registry', {})),
            "Uptime": "N/A"
        }
        
        self.metrics_table.setRowCount(len(metrics))
        
        for i, (key, value) in enumerate(metrics.items()):
            self.metrics_table.setItem(i, 0, QTableWidgetItem(str(key)))
            self.metrics_table.setItem(i, 1, QTableWidgetItem(str(value)))
            
        self.metrics_table.resizeColumnsToContents()
        
    def export_logs(self):
        """Exporte les logs vers un fichier"""
        content = self.log_text.toPlainText()
        filename = f"trading_logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(content)
            QMessageBox.information(self, "Export", f"Logs exportés vers {filename}")
        except Exception as e:
            QMessageBox.critical(self, "Erreur", f"Erreur lors de l'export: {e}")
            
    def show_about(self):
        """Affiche la dialog À propos"""
        QMessageBox.about(self, "À propos", 
                         "🚀 Trading CLI GUI\n\n"
                         "Interface graphique pour le Trading CLI\n"
                         "Utilise PySide6 + Click + AsyncIO\n\n"
                         "© 2025")
        
    def closeEvent(self, event):
        """Gestion de la fermeture de l'application"""
        # Arrêter l'event loop asyncio
        if hasattr(self, 'asyncio_thread'):
            self.asyncio_thread.stop_loop()
            self.asyncio_thread.wait(2000)  # Attendre 2 secondes max
            
        event.accept()


def main():
    """Point d'entrée principal"""
    app = QApplication(sys.argv)
    
    # Définir le style
    app.setStyle('Fusion')
    
    # Créer et afficher la fenêtre principale
    window = TradingGUI()
    window.show()
    
    # Message de bienvenue
    window.append_log("🚀 Trading CLI GUI démarré")
    window.append_log("💡 Utilisez les boutons ou tapez des commandes")
    
    sys.exit(app.exec())


if __name__ == "__main__":
    main()