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
from typing import Optional, Dict, Any, List
import logging
import yaml
import os
import pandas as pd

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
        
        # Charger la configuration
        self.config_data = self.load_config()
        
        # Thread pour asyncio
        self.asyncio_thread = AsyncioEventLoopThread()
        self.asyncio_thread.start()
        
        # Configuration des logs
        self.setup_logging()
        
        # Interface utilisateur
        self.setup_ui()
        
        # Charger les sessions dans la combo box
        self.load_sessions()
        
        # Connecter les signaux
        self.log_received.connect(self.append_log)
        
        # Timer pour mise à jour des métriques
        self.metrics_timer = QTimer()
        self.metrics_timer.timeout.connect(self.update_metrics)
        self.metrics_timer.start(5000)  # Toutes les 5 secondes
        
        # Timer pour rafraîchissement automatique de la DataFrame
        self.dataframe_timer = QTimer()
        self.dataframe_timer.timeout.connect(self.auto_refresh_dataframe)
        self.dataframe_timer.start(10000)  # Toutes les 10 secondes
    
    def load_config(self, show_logs: bool = False) -> Dict[str, Any]:
        """Charge le fichier de configuration trading_config.yaml"""
        config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                                  "config", "trading_config.yaml")
        
        try:
            if os.path.exists(config_path):
                with open(config_path, 'r', encoding='utf-8') as f:
                    config = yaml.safe_load(f)
                if show_logs and hasattr(self, 'append_log'):
                    self.append_log(f"✅ Configuration chargée depuis {config_path}")
                return config or {}
            else:
                if show_logs and hasattr(self, 'append_log'):
                    self.append_log(f"⚠️ Fichier de configuration non trouvé: {config_path}")
                return {}
        except Exception as e:
            if show_logs and hasattr(self, 'append_log'):
                self.append_log(f"❌ Erreur lors du chargement de la configuration: {e}")
            return {}
    
    def load_sessions(self):
        """Charge les sessions disponibles dans la combo box"""
        try:
            # Effacer la combo box
            self.session_combo.clear()
            
            sessions = []
            strategies = []
            
            # Extraire les sessions depuis la configuration
            if self.config_data and 'sessions' in self.config_data:
                sessions_config = self.config_data['sessions']
                if isinstance(sessions_config, dict):
                    for session_id, session_data in sessions_config.items():
                        # Ajouter l'ID de session et le nom si disponible
                        sessions.append(session_id)
                        if isinstance(session_data, dict) and 'name' in session_data:
                            name = session_data['name'].replace(' ', '_').lower()
                            if name != session_id and name not in sessions:
                                sessions.append(name)
            
            # Extraire les stratégies depuis la configuration
            if self.config_data and 'strategies' in self.config_data:
                strategies_config = self.config_data['strategies']
                if isinstance(strategies_config, dict):
                    for strategy_id, strategy_data in strategies_config.items():
                        strategies.append(strategy_id)
                        if isinstance(strategy_data, dict) and 'name' in strategy_data:
                            name = strategy_data['name'].replace(' ', '_').lower()
                            if name != strategy_id and name not in strategies:
                                strategies.append(name)
            
            # Combiner sessions et stratégies avec préfixes
            all_items = []
            
            # Ajouter les sessions avec préfixe
            for session in sessions:
                all_items.append(f"📋 {session}")
            
            # Ajouter les stratégies avec préfixe
            for strategy in strategies:
                all_items.append(f"🎯 {strategy}")
                
            # Si aucune configuration, utiliser des valeurs par défaut
            if not all_items:
                all_items = ['🎯 ema_grid_test', '🎯 rsi_strategy', '🎯 macd_strategy', '📋 test_session']
            
            # Trier et ajouter à la combo box
            all_items.sort()
            for item in all_items:
                self.session_combo.addItem(item)
            
            # Logs informatifs
            total_sessions = len(sessions)
            total_strategies = len(strategies)
            self.append_log(f"📋 {total_sessions} sessions et 🎯 {total_strategies} stratégies chargées")
            
            if all_items:
                displayed_items = [item.split(' ', 1)[1] for item in all_items[:3]]  # Retirer les préfixes pour l'affichage
                self.append_log(f"   Exemples: {', '.join(displayed_items)}")
                if len(all_items) > 3:
                    self.append_log(f"   ... et {len(all_items) - 3} autres")
                
        except Exception as e:
            self.append_log(f"❌ Erreur lors du chargement des sessions: {e}")
            # Fallback avec sessions par défaut
            default_items = ['🎯 ema_grid_test', '🎯 rsi', '🎯 macd', '📋 test']
            for item in default_items:
                self.session_combo.addItem(item)
    
    def reload_sessions(self):
        """Recharge les sessions depuis le fichier de configuration"""
        self.append_log("🔄 Rechargement des sessions...")
        self.config_data = self.load_config(show_logs=True)
        self.load_sessions()
        
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
        
        sys_cleanup_btn = QPushButton("🧹 System Cleanup")
        sys_cleanup_btn.clicked.connect(self.system_cleanup)
        
        # Boutons sessions
        session_list_btn = QPushButton("📋 List Sessions")
        session_list_btn.clicked.connect(lambda: self.execute_quick_command("session list"))
        
        session_create_btn = QPushButton("➕ Create Session")
        session_create_btn.clicked.connect(self.show_create_session_dialog)
        
        # Layout des boutons (3x3 au lieu de 3x2)
        quick_layout.addWidget(sys_start_btn, 0, 0)
        quick_layout.addWidget(sys_status_btn, 0, 1)
        quick_layout.addWidget(sys_stop_btn, 1, 0)
        quick_layout.addWidget(sys_cleanup_btn, 1, 1)
        quick_layout.addWidget(session_list_btn, 2, 0)
        quick_layout.addWidget(session_create_btn, 2, 1)
        
        command_layout.addWidget(quick_group)
        
        # Formulaire de démarrage de session
        session_group = QGroupBox("🎯 Démarrage de session")
        session_layout = QVBoxLayout(session_group)
        
        # Combo box avec bouton reload
        combo_layout = QHBoxLayout()
        self.session_combo = QComboBox()
        self.session_combo.setEditable(True)
        self.session_combo.setPlaceholderText("ID de session (ex: ema)")
        
        reload_sessions_btn = QPushButton("🔄")
        reload_sessions_btn.setMaximumWidth(40)
        reload_sessions_btn.setToolTip("Recharger les sessions depuis config.yaml")
        reload_sessions_btn.clicked.connect(self.reload_sessions)
        
        combo_layout.addWidget(self.session_combo)
        combo_layout.addWidget(reload_sessions_btn)
        
        self.force_checkbox = QCheckBox("Force (no confirmation)")
        
        start_session_btn = QPushButton("▶️ Start Session")
        start_session_btn.clicked.connect(self.start_session)
        
        open_session_btn = QPushButton("📂 Open Session")
        open_session_btn.clicked.connect(self.open_session)
        
        stop_session_btn = QPushButton("🛑 Stop Session")
        stop_session_btn.clicked.connect(self.stop_session)
        
        session_layout.addWidget(QLabel("Session ID:"))
        session_layout.addLayout(combo_layout)
        session_layout.addWidget(self.force_checkbox)
        
        # Layout horizontal pour les boutons Start/Open/Stop
        buttons_layout = QHBoxLayout()
        buttons_layout.addWidget(start_session_btn)
        buttons_layout.addWidget(open_session_btn)
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
        
        # Tab Stratégies (dynamique selon la session)
        self.setup_strategies_tab(data_widget)
        
        parent.addWidget(data_widget)
    
    def setup_strategies_tab(self, parent_tab_widget):
        """Configure les onglets de stratégies (créés dynamiquement)"""
        # Container pour les onglets de stratégies
        self.strategies_tab_widget = QTabWidget()
        
        # Message par défaut quand aucune session n'est chargée
        default_widget = QWidget()
        default_layout = QVBoxLayout(default_widget)
        
        default_label = QLabel("🎯 Sélectionnez une session pour voir les stratégies\n\n"
                              "📂 Open Session → Charge les stratégies automatiquement\n"
                              "▶️ Start Session → Met à jour les données en temps réel")
        default_label.setAlignment(Qt.AlignCenter)
        default_label.setFont(QFont("Arial", 12))
        default_label.setStyleSheet("color: #666; padding: 40px;")
        
        default_layout.addWidget(default_label)
        self.strategies_tab_widget.addTab(default_widget, "ℹ️ Info")
        
        # Ajouter l'onglet principal
        parent_tab_widget.addTab(self.strategies_tab_widget, "🎯 Stratégies")
        
        # Registre des stratégies chargées
        self.loaded_strategies = {}
        self.strategy_dataframes = {}
        
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
        session_text = self.session_combo.currentText().strip()
        if not session_text:
            QMessageBox.warning(self, "Erreur", "Veuillez saisir un ID de session")
            return
        
        # Retirer le préfixe emoji si présent
        session_id = session_text
        if session_text.startswith(('📋 ', '🎯 ')):
            session_id = session_text.split(' ', 1)[1]
            
        command = f"session start {session_id}"
        if self.force_checkbox.isChecked():
            command += " --force"
            
        self.append_log(f"🚀 Démarrage de la session '{session_id}'...")
        self.execute_quick_command(command)
    
    def open_session(self):
        """Ouvre une session existante depuis la dernière sauvegarde"""
        session_text = self.session_combo.currentText().strip()
        if not session_text:
            QMessageBox.warning(self, "Erreur", "Veuillez saisir un ID de session")
            return
        
        # Retirer le préfixe emoji si présent
        session_id = session_text
        if session_text.startswith(('📋 ', '🎯 ')):
            session_id = session_text.split(' ', 1)[1]
            
        command = f"session open {session_id}"
        if self.force_checkbox.isChecked():
            command += " --force"
            
        self.append_log(f"📂 Ouverture de la session '{session_id}' depuis cache...")
        self.execute_quick_command(command)
        
        # Charger automatiquement les stratégies après ouverture
        QTimer.singleShot(2000, lambda: self.load_strategies_for_session(session_id))
    
    def stop_session(self):
        """Arrête une session"""
        session_text = self.session_combo.currentText().strip()
        if not session_text:
            QMessageBox.warning(self, "Erreur", "Veuillez saisir un ID de session")
            return
        
        # Retirer le préfixe emoji si présent
        session_id = session_text
        if session_text.startswith(('📋 ', '🎯 ')):
            session_id = session_text.split(' ', 1)[1]
        
        # Demander confirmation à l'utilisateur
        reply = QMessageBox.question(
            self, 
            "Confirmer l'arrêt", 
            f"Voulez-vous vraiment arrêter la session '{session_id}' ?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            # Ajouter --force automatiquement puisque l'utilisateur a déjà confirmé dans la GUI
            command = f"session stop {session_id} --force"
            self.append_log(f"🛑 Arrêt de la session '{session_id}'...")
            self.execute_quick_command(command)
    
    def system_cleanup(self):
        """Nettoie les workers orphelins et processus bloqués"""
        # Demander confirmation à l'utilisateur
        reply = QMessageBox.question(
            self, 
            "Confirmer le nettoyage", 
            "⚠️ Cette action va arrêter tous les workers orphelins et processus bloqués.\n\n"
            "Voulez-vous vraiment procéder au nettoyage du système ?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            # Utiliser --force automatiquement puisque l'utilisateur a déjà confirmé
            command = "system cleanup --force"
            self.append_log("🧹 Nettoyage du système en cours...")
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
    
    def load_strategies_for_session(self, session_id: str):
        """Charge toutes les stratégies pour une session donnée"""
        try:
            # Obtenir les stratégies depuis la configuration
            strategies = self.get_strategies_for_session(session_id)
            
            if not strategies:
                self.append_log(f"📊 Aucune stratégie trouvée pour la session {session_id}")
                return
            
            # Nettoyer les anciens onglets (sauf le premier "Info")
            while self.strategies_tab_widget.count() > 1:
                self.strategies_tab_widget.removeTab(1)
            
            # Créer un onglet pour chaque stratégie
            for strategy_id in strategies:
                self.create_strategy_tab(session_id, strategy_id)
            
            self.append_log(f"🎯 {len(strategies)} stratégies chargées: {', '.join(strategies)}")
            
        except Exception as e:
            self.append_log(f"❌ Erreur chargement stratégies: {e}")
    
    def get_strategies_for_session(self, session_id: str) -> List[str]:
        """Récupère la liste des stratégies pour une session"""
        try:
            # Chercher dans la configuration yaml
            if self.config_data and 'sessions' in self.config_data:
                sessions = self.config_data['sessions']
                
                # Chercher la session par ID ou nom
                session_config = None
                for sid, sconfig in sessions.items():
                    if sid == session_id or (isinstance(sconfig, dict) and sconfig.get('name', '').replace(' ', '_').lower() == session_id):
                        session_config = sconfig
                        break
                
                if session_config and isinstance(session_config, dict) and 'strategies' in session_config:
                    return session_config['strategies']
            
            # Fallback: chercher dans les répertoires de cache
            strategies = []
            cache_base = "cache_strategies"
            if os.path.exists(cache_base):
                for strategy_name in os.listdir(cache_base):
                    strategy_path = os.path.join(cache_base, strategy_name)
                    if os.path.isdir(strategy_path):
                        # Chercher les sous-répertoires (strategy_id)
                        for strategy_id in os.listdir(strategy_path):
                            strategy_cache_path = os.path.join(strategy_path, strategy_id)
                            if os.path.isdir(strategy_cache_path) and any(f.endswith('.feather') for f in os.listdir(strategy_cache_path)):
                                strategies.append(strategy_id)
            
            return strategies if strategies else [session_id]  # Au minimum la session elle-même
            
        except Exception as e:
            self.append_log(f"❌ Erreur get_strategies_for_session: {e}")
            return [session_id]  # Fallback
    
    def create_strategy_tab(self, session_id: str, strategy_id: str):
        """Crée un onglet pour une stratégie avec ses sous-onglets"""
        try:
            # Container principal pour la stratégie
            strategy_widget = QTabWidget()
            
            # Sous-onglet DataFrame
            dataframe_tab = self.create_dataframe_subtab(session_id, strategy_id)
            strategy_widget.addTab(dataframe_tab, "📈 DataFrame")
            
            # Sous-onglet Chart (placeholder pour l'instant)
            chart_tab = QWidget()
            chart_layout = QVBoxLayout(chart_tab)
            chart_label = QLabel(f"📊 Chart pour {strategy_id}\\n\\n(À implémenter)")
            chart_label.setAlignment(Qt.AlignCenter)
            chart_label.setStyleSheet("color: #666; padding: 40px;")
            chart_layout.addWidget(chart_label)
            strategy_widget.addTab(chart_tab, "📊 Chart")
            
            # Sous-onglet Portfolio (placeholder pour l'instant)
            portfolio_tab = QWidget()
            portfolio_layout = QVBoxLayout(portfolio_tab)
            portfolio_label = QLabel(f"💼 Portfolio pour {strategy_id}\\n\\n(À implémenter)")
            portfolio_label.setAlignment(Qt.AlignCenter)
            portfolio_label.setStyleSheet("color: #666; padding: 40px;")
            portfolio_layout.addWidget(portfolio_label)
            strategy_widget.addTab(portfolio_tab, "💼 Portfolio")
            
            # Ajouter l'onglet de stratégie au container principal
            self.strategies_tab_widget.addTab(strategy_widget, f"🎯 {strategy_id}")
            
            # Mémoriser pour références futures
            self.loaded_strategies[strategy_id] = {
                'session_id': session_id,
                'widget': strategy_widget,
                'dataframe_tab': dataframe_tab
            }
            
        except Exception as e:
            self.append_log(f"❌ Erreur création onglet stratégie {strategy_id}: {e}")
    
    def create_dataframe_subtab(self, session_id: str, strategy_id: str):
        """Crée le sous-onglet DataFrame pour une stratégie"""
        dataframe_widget = QWidget()
        dataframe_layout = QVBoxLayout(dataframe_widget)
        
        # Titre et informations
        info_layout = QHBoxLayout()
        
        info_label = QLabel(f"📈 DataFrame - {strategy_id}")
        info_label.setFont(QFont("Arial", 10, QFont.Bold))
        
        refresh_btn = QPushButton("🔄 Actualiser")
        refresh_btn.setMaximumWidth(100)
        refresh_btn.clicked.connect(lambda: self.refresh_strategy_dataframe(session_id, strategy_id))
        
        info_layout.addWidget(info_label)
        info_layout.addStretch()
        info_layout.addWidget(refresh_btn)
        
        dataframe_layout.addLayout(info_layout)
        
        # Table pour afficher la DataFrame
        dataframe_table = QTableWidget()
        dataframe_table.setAlternatingRowColors(True)
        dataframe_table.setSelectionBehavior(QTableWidget.SelectRows)
        dataframe_table.setSortingEnabled(True)
        dataframe_table.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        dataframe_table.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        
        dataframe_layout.addWidget(dataframe_table)
        
        # Sauvegarder les références
        self.strategy_dataframes[strategy_id] = {
            'table': dataframe_table,
            'info_label': info_label,
            'session_id': session_id,
            'file_path': None,
            'last_mtime': None
        }
        
        # Charger immédiatement la DataFrame si elle existe
        self.load_dataframe_for_strategy(session_id, strategy_id)
        
        return dataframe_widget
    
    def refresh_strategy_dataframe(self, session_id: str, strategy_id: str):
        """Actualise la DataFrame d'une stratégie spécifique"""
        try:
            self.load_dataframe_for_strategy(session_id, strategy_id)
        except Exception as e:
            self.append_log(f"❌ Erreur refresh DataFrame {strategy_id}: {e}")
    
    def load_dataframe_for_strategy(self, session_id: str, strategy_id: str):
        """Charge la DataFrame pour une stratégie spécifique"""
        try:
            if strategy_id not in self.strategy_dataframes:
                self.append_log(f"⚠️ Stratégie {strategy_id} non trouvée dans le registre")
                return
            
            strategy_data = self.strategy_dataframes[strategy_id]
            table = strategy_data['table']
            info_label = strategy_data['info_label']
            
            # Chercher le fichier DataFrame dans le cache
            import pandas as pd
            
            self.append_log(f"🔍 Recherche DataFrame pour {strategy_id}...")
            
            # Chercher dans tous les répertoires de cache pour trouver la stratégie
            feather_files = []
            cache_base = "cache_strategies"
            
            if not os.path.exists(cache_base):
                self.append_log(f"❌ Répertoire cache inexistant: {cache_base}")
                info_label.setText(f"📈 DataFrame - {strategy_id} | Cache inexistant")
                return
            
            self.append_log(f"📁 Scan cache dans: {cache_base}")
            
            for strategy_name in os.listdir(cache_base):
                strategy_path = os.path.join(cache_base, strategy_name, strategy_id)
                self.append_log(f"  🔍 Cherche dans: {strategy_path}")
                
                if os.path.exists(strategy_path):
                    files_in_dir = os.listdir(strategy_path)
                    feather_count = sum(1 for f in files_in_dir if f.endswith('.feather'))
                    self.append_log(f"  ✅ Trouvé répertoire avec {feather_count} fichiers .feather")
                    
                    for file in files_in_dir:
                        if file.endswith('.feather'):
                            full_path = os.path.join(strategy_path, file)
                            feather_files.append(full_path)
                            self.append_log(f"    📄 {file}")
                    break  # Trouvé la stratégie, pas besoin de continuer
                else:
                    self.append_log(f"  ❌ Pas trouvé: {strategy_path}")
            
            if not feather_files:
                info_label.setText(f"📈 DataFrame - {strategy_id} | Aucune donnée trouvée")
                table.setRowCount(0)
                table.setColumnCount(0)
                self.append_log(f"📊 Aucun cache DataFrame trouvé pour {strategy_id}")
                return
            
            # Charger le fichier .feather le plus récent
            if len(feather_files) > 1:
                # Trier par date de modification (le plus récent en premier)
                feather_files.sort(key=lambda f: os.path.getmtime(f), reverse=True)
                self.append_log(f"📊 {len(feather_files)} fichiers trouvés, sélection du plus récent")
            
            dataframe_file = feather_files[0]
            self.append_log(f"📂 Chargement: {os.path.basename(dataframe_file)}")
            
            df = pd.read_feather(dataframe_file)
            
            self.display_dataframe_for_strategy(df, dataframe_file, session_id, strategy_id)
            self.append_log(f"✅ DataFrame {strategy_id}: {len(df)} lignes, {len(df.columns)} colonnes")
            
        except Exception as e:
            if strategy_id in self.strategy_dataframes:
                self.strategy_dataframes[strategy_id]['info_label'].setText(f"❌ Erreur: {str(e)}")
            self.append_log(f"❌ Erreur chargement DataFrame {strategy_id}: {e}")
    
    def display_dataframe_for_strategy(self, df, file_path: str, session_id: str, strategy_id: str):
        """Affiche la DataFrame dans la table d'une stratégie spécifique"""
        try:
            self.append_log(f"🎨 Début affichage DataFrame pour {strategy_id}")
            self.append_log(f"📊 DataFrame shape: {df.shape}")
            self.append_log(f"📋 Colonnes: {list(df.columns)}")
            
            if strategy_id not in self.strategy_dataframes:
                self.append_log(f"❌ Strategy {strategy_id} not found in strategy_dataframes registry")
                return
                
            strategy_data = self.strategy_dataframes[strategy_id]
            table = strategy_data['table']
            info_label = strategy_data['info_label']
            
            self.append_log(f"✅ Strategy data found, table widget: {type(table)}")
            
            # Mettre à jour les informations
            file_name = os.path.basename(file_path)
            last_modified = datetime.fromtimestamp(os.path.getmtime(file_path))
            info_label.setText(
                f"📈 {strategy_id} | {file_name} | {len(df)} lignes × {len(df.columns)} colonnes | {last_modified.strftime('%H:%M:%S')}"
            )
            
            # Limiter l'affichage pour les performances
            display_df = df.tail(1000) if len(df) > 1000 else df
            self.append_log(f"📊 Display DataFrame: {len(display_df)} lignes × {len(display_df.columns)} colonnes")
            
            # Configurer la table
            table.setRowCount(len(display_df))
            table.setColumnCount(len(display_df.columns))
            table.setHorizontalHeaderLabels([str(col) for col in display_df.columns])
            self.append_log(f"🔧 Table configurée: {table.rowCount()} lignes × {table.columnCount()} colonnes")
            
            # Remplir les données
            self.append_log(f"📝 Début remplissage table...")
            for row in range(len(display_df)):
                for col in range(len(display_df.columns)):
                    value = display_df.iloc[row, col]
                    
                    # Formatage des valeurs selon le type de colonne
                    column_name = display_df.columns[col]
                    
                    if pd.isna(value):
                        display_value = "NaN"
                    elif column_name == 'timestamp':
                        # Afficher timestamp comme entier
                        display_value = str(int(value)) if isinstance(value, (int, float)) else str(value)
                    elif column_name == 'date':
                        # Date déjà formatée, afficher telle quelle
                        display_value = str(value)
                    elif column_name in ['open', 'high', 'low', 'close']:
                        # Prix avec 2 décimales
                        display_value = f"{float(value):.2f}" if isinstance(value, (int, float)) else str(value)
                    elif column_name == 'volume':
                        # Volume avec 3 décimales
                        display_value = f"{float(value):.3f}" if isinstance(value, (int, float)) else str(value)
                    elif isinstance(value, float):
                        # Autres valeurs numériques avec 6 décimales (indicateurs)
                        display_value = f"{value:.6f}"
                    else:
                        display_value = str(value)
                    
                    item = QTableWidgetItem(display_value)
                    table.setItem(row, col, item)
            
            self.append_log(f"✅ Table remplie: {len(display_df)} lignes × {len(display_df.columns)} colonnes")
            
            # Ajuster les colonnes
            table.resizeColumnsToContents()
            self.append_log(f"🔧 Colonnes ajustées")
            
            # Mémoriser le chemin et mtime
            strategy_data['file_path'] = file_path
            strategy_data['last_mtime'] = os.path.getmtime(file_path)
            
            if len(df) > 1000:
                self.append_log(f"📊 {strategy_id}: Affichage limité aux 1000 dernières lignes (total: {len(df)})")
            
            self.append_log(f"🎉 DataFrame {strategy_id} affiché avec succès!")
                
        except Exception as e:
            self.append_log(f"❌ Erreur affichage DataFrame {strategy_id}: {e}")
            import traceback
            self.append_log(f"📝 Traceback: {traceback.format_exc()}")
    
    def load_dataframe_for_session(self, session_id: str):
        """Charge la DataFrame pour une session donnée"""
        try:
            # Chercher le fichier DataFrame dans le cache
            import os
            import pandas as pd
            
            # Chemin basé sur la structure de cache existante
            strategy_name = "EMA Grid Test"  # À adapter selon la session
            cache_base = f"cache_strategies/{strategy_name}/{session_id}"
            
            # Chercher les fichiers .feather dans le répertoire de cache
            feather_files = []
            if os.path.exists(cache_base):
                for file in os.listdir(cache_base):
                    if file.endswith('.feather'):
                        feather_files.append(os.path.join(cache_base, file))
            
            if not feather_files:
                self.dataframe_info_label.setText(f"📈 Aucune DataFrame trouvée pour {session_id}")
                self.dataframe_table.setRowCount(0)
                self.dataframe_table.setColumnCount(0)
                self.append_log(f"📊 Aucun cache DataFrame trouvé pour {session_id}")
                return
            
            # Charger le premier fichier .feather trouvé
            dataframe_file = feather_files[0]
            df = pd.read_feather(dataframe_file)
            
            self.display_dataframe(df, dataframe_file, session_id)
            self.append_log(f"📈 DataFrame chargée: {len(df)} lignes, {len(df.columns)} colonnes")
            
        except Exception as e:
            self.dataframe_info_label.setText(f"❌ Erreur: {str(e)}")
            self.append_log(f"❌ Erreur chargement DataFrame: {e}")
    
    def display_dataframe(self, df, file_path: str, session_id: str):
        """Affiche la DataFrame dans la table"""
        try:
            # Mettre à jour les informations
            file_name = os.path.basename(file_path)
            last_modified = datetime.fromtimestamp(os.path.getmtime(file_path))
            self.dataframe_info_label.setText(
                f"📈 {session_id} | {file_name} | {len(df)} lignes × {len(df.columns)} colonnes | {last_modified.strftime('%H:%M:%S')}"
            )
            
            # Limiter l'affichage pour les performances (ex: 1000 lignes max)
            display_df = df.tail(1000) if len(df) > 1000 else df
            
            # Configurer la table
            self.dataframe_table.setRowCount(len(display_df))
            self.dataframe_table.setColumnCount(len(display_df.columns))
            self.dataframe_table.setHorizontalHeaderLabels([str(col) for col in display_df.columns])
            
            # Remplir les données
            for row in range(len(display_df)):
                for col in range(len(display_df.columns)):
                    value = display_df.iloc[row, col]
                    
                    # Formatage des valeurs selon le type de colonne
                    column_name = display_df.columns[col]
                    
                    if pd.isna(value):
                        display_value = "NaN"
                    elif column_name == 'timestamp':
                        # Afficher timestamp comme entier
                        display_value = str(int(value)) if isinstance(value, (int, float)) else str(value)
                    elif column_name == 'date':
                        # Date déjà formatée, afficher telle quelle
                        display_value = str(value)
                    elif column_name in ['open', 'high', 'low', 'close']:
                        # Prix avec 2 décimales
                        display_value = f"{float(value):.2f}" if isinstance(value, (int, float)) else str(value)
                    elif column_name == 'volume':
                        # Volume avec 3 décimales
                        display_value = f"{float(value):.3f}" if isinstance(value, (int, float)) else str(value)
                    elif isinstance(value, float):
                        # Autres valeurs numériques avec 6 décimales (indicateurs)
                        display_value = f"{value:.6f}"
                    else:
                        display_value = str(value)
                    
                    item = QTableWidgetItem(display_value)
                    self.dataframe_table.setItem(row, col, item)
            
            # Ajuster les colonnes
            self.dataframe_table.resizeColumnsToContents()
            
            # Mémoriser le chemin et l'heure
            self.current_dataframe_path = file_path
            self.last_dataframe_update = datetime.now()
            self.last_dataframe_mtime = os.path.getmtime(file_path)
            
            if len(df) > 1000:
                self.append_log(f"📊 Affichage limité aux 1000 dernières lignes (total: {len(df)})")
                
        except Exception as e:
            self.append_log(f"❌ Erreur affichage DataFrame: {e}")
    
    def auto_refresh_dataframe(self):
        """Rafraîchissement automatique des DataFrames de toutes les stratégies"""
        try:
            # Parcourir toutes les stratégies chargées
            for strategy_id, strategy_data in self.strategy_dataframes.items():
                file_path = strategy_data.get('file_path')
                last_mtime = strategy_data.get('last_mtime')
                
                if not file_path or not os.path.exists(file_path):
                    continue
                
                # Vérifier si le fichier a été modifié
                current_mtime = os.path.getmtime(file_path)
                if last_mtime and current_mtime == last_mtime:
                    continue  # Pas de modification
                
                # Le fichier a été modifié, recharger
                session_id = strategy_data['session_id']
                try:
                    import pandas as pd
                    df = pd.read_feather(file_path)
                    self.display_dataframe_for_strategy(df, file_path, session_id, strategy_id)
                    self.append_log(f"🔄 DataFrame {strategy_id} auto-rafraîchie: {len(df)} lignes")
                except Exception as e:
                    # Erreur silencieuse par stratégie
                    pass
                
        except Exception as e:
            # Erreur silencieuse globale
            pass
        
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