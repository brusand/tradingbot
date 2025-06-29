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
        
        update_session_btn = QPushButton("🔄 Update Session")
        update_session_btn.clicked.connect(self.update_session)
        
        restart_session_btn = QPushButton("🔁 Restart Session")
        restart_session_btn.clicked.connect(self.restart_session)
        
        open_session_btn = QPushButton("📂 Open Session")
        open_session_btn.clicked.connect(self.open_session)
        
        stop_session_btn = QPushButton("🛑 Stop Session")
        stop_session_btn.clicked.connect(self.stop_session)
        
        session_layout.addWidget(QLabel("Session ID:"))
        session_layout.addLayout(combo_layout)
        session_layout.addWidget(self.force_checkbox)
        
        # Layout horizontal pour les boutons Update/Restart/Open/Stop
        buttons_layout = QHBoxLayout()
        buttons_layout.addWidget(update_session_btn)
        buttons_layout.addWidget(restart_session_btn)
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
        """Panel de droite avec logs et données - onglets sessions au niveau principal"""
        self.main_tab_widget = QTabWidget()
        
        # Tab Logs
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setFont(QFont("Consolas", 9))
        self.main_tab_widget.addTab(self.log_text, "📋 Logs")
        
        # Tab Métriques
        metrics_widget = QWidget()
        metrics_layout = QVBoxLayout(metrics_widget)
        
        self.metrics_table = QTableWidget()
        self.metrics_table.setColumnCount(2)
        self.metrics_table.setHorizontalHeaderLabels(["Métrique", "Valeur"])
        
        metrics_layout.addWidget(self.metrics_table)
        self.main_tab_widget.addTab(metrics_widget, "📊 Métriques")
        
        # Tab Sessions (liste/gestion)
        sessions_widget = QWidget()
        sessions_layout = QVBoxLayout(sessions_widget)
        
        self.sessions_table = QTableWidget()
        self.sessions_table.setColumnCount(4)
        self.sessions_table.setHorizontalHeaderLabels(["ID", "Nom", "Status", "Mode"])
        
        sessions_layout.addWidget(self.sessions_table)
        self.main_tab_widget.addTab(sessions_widget, "🎯 Sessions")
        
        # Les onglets des sessions seront ajoutés dynamiquement au niveau principal
        
        parent.addWidget(self.main_tab_widget)
        
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
        
    def update_session(self):
        """Met à jour une session depuis la dernière sauvegarde"""
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
            
        self.append_log(f"🔄 Mise à jour de la session '{session_id}' depuis la dernière sauvegarde...")
        self.execute_quick_command(command)
    
    def restart_session(self):
        """Redémarre une session depuis le début en supprimant le cache"""
        session_text = self.session_combo.currentText().strip()
        if not session_text:
            QMessageBox.warning(self, "Erreur", "Veuillez saisir un ID de session")
            return
        
        # Retirer le préfixe emoji si présent
        session_id = session_text
        if session_text.startswith(('📋 ', '🎯 ')):
            session_id = session_text.split(' ', 1)[1]
        
        # Demander confirmation car c'est destructeur
        reply = QMessageBox.question(
            self, 
            "Confirmation Restart", 
            f"⚠️ Êtes-vous sûr de vouloir redémarrer complètement la session '{session_id}' ?\n\n"
            "Cette action va :\n"
            "• Supprimer toutes les sauvegardes existantes\n"
            "• Recalculer tous les indicateurs depuis start_date\n"
            "• Perdre l'historique de progression\n\n"
            "Cette opération est irréversible.",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )
        
        if reply != QMessageBox.Yes:
            return
            
        # Ajouter l'option --restart pour supprimer le cache
        command = f"session start {session_id} --restart"
        if self.force_checkbox.isChecked():
            command += " --force"
            
        self.append_log(f"🔁 Redémarrage complet de la session '{session_id}' depuis start_date...")
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
        """Charge une session comme onglet principal avec ses stratégies"""
        try:
            # Obtenir les stratégies depuis la configuration
            strategies = self.get_strategies_for_session(session_id)
            
            if not strategies:
                self.append_log(f"📊 Aucune stratégie trouvée pour la session {session_id}")
                return
            
            # Supprimer l'onglet session s'il existe déjà
            self.remove_session_tab(session_id)
            
            # Créer l'onglet session au niveau principal
            session_tab_widget = QTabWidget()
            
            # Créer un sous-onglet pour chaque stratégie
            for strategy_id in strategies:
                strategy_widget = self.create_strategy_subtab(session_id, strategy_id)
                session_tab_widget.addTab(strategy_widget, f"📊 {strategy_id}")
            
            # Ajouter l'onglet session au widget principal
            tab_title = f"🎯 {session_id}"
            self.main_tab_widget.addTab(session_tab_widget, tab_title)
            
            # Activer automatiquement l'onglet de la session
            session_index = self.main_tab_widget.count() - 1
            self.main_tab_widget.setCurrentIndex(session_index)
            
            self.append_log(f"🎯 Session {session_id} chargée avec {len(strategies)} stratégies: {', '.join(strategies)}")
            
        except Exception as e:
            self.append_log(f"❌ Erreur chargement session {session_id}: {e}")
    
    def remove_session_tab(self, session_id: str):
        """Supprime l'onglet d'une session s'il existe"""
        try:
            tab_title = f"🎯 {session_id}"
            for i in range(self.main_tab_widget.count()):
                if self.main_tab_widget.tabText(i) == tab_title:
                    self.main_tab_widget.removeTab(i)
                    self.append_log(f"🗑️ Onglet session {session_id} supprimé")
                    break
        except Exception as e:
            self.append_log(f"❌ Erreur suppression onglet {session_id}: {e}")
    
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
    
    def create_strategy_subtab(self, session_id: str, strategy_id: str):
        """Crée le widget pour une stratégie avec ses sous-onglets"""
        try:
            # Container pour les onglets de la stratégie
            strategy_widget = QTabWidget()
            
            # Sous-onglet DataFrame
            dataframe_tab = self.create_dataframe_subtab(session_id, strategy_id)
            strategy_widget.addTab(dataframe_tab, "📈 DataFrame")
            
            # Sous-onglet Chart (placeholder pour l'instant)
            chart_tab = QWidget()
            chart_layout = QVBoxLayout(chart_tab)
            chart_label = QLabel(f"📊 Chart pour {strategy_id}\n\n(À implémenter)")
            chart_label.setAlignment(Qt.AlignCenter)
            chart_label.setStyleSheet("color: #666; padding: 40px;")
            chart_layout.addWidget(chart_label)
            strategy_widget.addTab(chart_tab, "📊 Chart")
            
            # Sous-onglet Portfolio (placeholder pour l'instant)
            portfolio_tab = QWidget()
            portfolio_layout = QVBoxLayout(portfolio_tab)
            portfolio_label = QLabel(f"💼 Portfolio pour {strategy_id}\n\n(À implémenter)")
            portfolio_label.setAlignment(Qt.AlignCenter)
            portfolio_label.setStyleSheet("color: #666; padding: 40px;")
            portfolio_layout.addWidget(portfolio_label)
            strategy_widget.addTab(portfolio_tab, "💼 Portfolio")
            
            # Sous-onglet Config
            config_tab = self.create_config_subtab(session_id, strategy_id)
            strategy_widget.addTab(config_tab, "⚙️ Config")
            
            # Mémoriser pour références futures
            self.loaded_strategies[strategy_id] = {
                'session_id': session_id,
                'widget': strategy_widget,
                'dataframe_tab': dataframe_tab,
                'config_tab': config_tab
            }
            
            return strategy_widget
            
        except Exception as e:
            self.append_log(f"❌ Erreur création widget stratégie {strategy_id}: {e}")
            return QWidget()  # Widget vide en cas d'erreur
    
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
        
        # Contrôles de pagination
        pagination_layout = QHBoxLayout()
        
        prev_btn = QPushButton("◀ Page précédente")
        prev_btn.clicked.connect(lambda: self.goto_previous_page(strategy_id))
        
        page_label = QLabel("Page 1 / 1")
        page_label.setAlignment(Qt.AlignCenter)
        page_label.setFont(QFont("Arial", 10, QFont.Bold))
        
        next_btn = QPushButton("Page suivante ▶")
        next_btn.clicked.connect(lambda: self.goto_next_page(strategy_id))
        
        pagination_layout.addWidget(prev_btn)
        pagination_layout.addWidget(page_label)
        pagination_layout.addWidget(next_btn)
        
        dataframe_layout.addLayout(pagination_layout)
        
        # Table pour afficher la DataFrame
        dataframe_table = QTableWidget()
        dataframe_table.setAlternatingRowColors(True)
        dataframe_table.setSelectionBehavior(QTableWidget.SelectRows)
        dataframe_table.setSelectionMode(QTableWidget.SingleSelection)
        dataframe_table.setSortingEnabled(True)
        dataframe_table.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        dataframe_table.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        # Activer le scroll par pixel pour plus de fluidité
        dataframe_table.setVerticalScrollMode(QTableWidget.ScrollPerPixel)
        
        dataframe_layout.addWidget(dataframe_table)
        
        # Sauvegarder les références
        self.strategy_dataframes[strategy_id] = {
            'table': dataframe_table,
            'info_label': info_label,
            'page_label': page_label,
            'prev_btn': prev_btn,
            'next_btn': next_btn,
            'session_id': session_id,
            'file_path': None,
            'last_mtime': None,
            'current_page': 1,
            'total_pages': 1,
            'page_size': 500,
            'full_dataframe': None
        }
        
        # Charger immédiatement la DataFrame si elle existe
        self.load_dataframe_for_strategy(session_id, strategy_id)
        
        return dataframe_widget
    
    def create_config_subtab(self, session_id: str, strategy_id: str):
        """Crée le sous-onglet Config pour une stratégie"""
        config_widget = QWidget()
        config_layout = QVBoxLayout(config_widget)
        
        # Titre et boutons
        header_layout = QHBoxLayout()
        
        config_label = QLabel(f"⚙️ Configuration - {strategy_id}")
        config_label.setFont(QFont("Arial", 12, QFont.Bold))
        
        refresh_config_btn = QPushButton("🔄 Actualiser")
        refresh_config_btn.setMaximumWidth(100)
        refresh_config_btn.clicked.connect(lambda: self.refresh_strategy_config(strategy_id))
        
        edit_config_btn = QPushButton("✏️ Éditer")
        edit_config_btn.setMaximumWidth(100)
        edit_config_btn.setCheckable(True)
        edit_config_btn.clicked.connect(lambda: self.toggle_config_edit_mode(strategy_id))
        
        save_config_btn = QPushButton("💾 Sauvegarder")
        save_config_btn.setMaximumWidth(100)
        save_config_btn.setEnabled(False)  # Désactivé par défaut
        save_config_btn.clicked.connect(lambda: self.save_strategy_config(strategy_id))
        
        header_layout.addWidget(config_label)
        header_layout.addStretch()
        header_layout.addWidget(refresh_config_btn)
        header_layout.addWidget(edit_config_btn)
        header_layout.addWidget(save_config_btn)
        
        config_layout.addLayout(header_layout)
        
        # Éditeur de configuration (JSON/YAML)
        self.config_editors = getattr(self, 'config_editors', {})
        
        config_editor = QTextEdit()
        config_editor.setFont(QFont("Consolas", 10))
        config_editor.setLineWrapMode(QTextEdit.NoWrap)
        config_editor.setReadOnly(True)  # Lecture seule par défaut
        
        # Appliquer le style sombre par défaut
        config_editor.setStyleSheet("""
            QTextEdit { 
                background-color: #3a3a3a; 
                color: #c0c0c0; 
                border: 1px solid #555; 
            }
        """)
        
        # Charger la configuration de la stratégie
        self.load_strategy_config(strategy_id, config_editor)
        
        config_layout.addWidget(config_editor)
        
        # Informations de statut
        status_layout = QHBoxLayout()
        
        config_status = QLabel("📄 Configuration chargée depuis trading_config.yaml")
        config_status.setStyleSheet("color: #666; font-size: 10px;")
        
        last_modified = QLabel("Dernière modification: --")
        last_modified.setStyleSheet("color: #666; font-size: 10px;")
        
        status_layout.addWidget(config_status)
        status_layout.addStretch()
        status_layout.addWidget(last_modified)
        
        config_layout.addLayout(status_layout)
        
        # Stocker les références
        self.config_editors[strategy_id] = {
            'editor': config_editor,
            'edit_btn': edit_config_btn,
            'save_btn': save_config_btn,
            'status_label': config_status,
            'modified_label': last_modified
        }
        
        return config_widget
    
    def load_strategy_config(self, strategy_id: str, config_editor: QTextEdit):
        """Charge la configuration d'une stratégie dans l'éditeur"""
        try:
            if hasattr(self, 'config_data') and self.config_data:
                strategies = self.config_data.get('strategies', {})
                strategy_config = strategies.get(strategy_id, {})
                
                if strategy_config:
                    # Convertir en YAML pour l'affichage
                    import yaml
                    yaml_content = yaml.dump(strategy_config, default_flow_style=False, indent=2, allow_unicode=True)
                    config_editor.setPlainText(yaml_content)
                    self.append_log(f"⚙️ Configuration {strategy_id} chargée")
                else:
                    config_editor.setPlainText(f"# Configuration pour {strategy_id}\n# Aucune configuration trouvée dans trading_config.yaml")
                    self.append_log(f"⚠️ Aucune configuration trouvée pour {strategy_id}")
            else:
                config_editor.setPlainText(f"# Configuration pour {strategy_id}\n# Erreur: Impossible de charger trading_config.yaml")
                self.append_log(f"❌ Impossible de charger la configuration")
                
        except Exception as e:
            config_editor.setPlainText(f"# Erreur de chargement de la configuration\n# {str(e)}")
            self.append_log(f"❌ Erreur chargement config {strategy_id}: {e}")
    
    def refresh_strategy_config(self, strategy_id: str):
        """Actualise la configuration d'une stratégie"""
        try:
            if strategy_id in self.config_editors:
                config_editor = self.config_editors[strategy_id]['editor']
                
                # Recharger le fichier de config
                self.load_config()
                
                # Recharger dans l'éditeur
                self.load_strategy_config(strategy_id, config_editor)
                
                # Mettre à jour le timestamp
                from datetime import datetime
                self.config_editors[strategy_id]['modified_label'].setText(f"Actualisé: {datetime.now().strftime('%H:%M:%S')}")
                
                self.append_log(f"🔄 Configuration {strategy_id} actualisée")
                
        except Exception as e:
            self.append_log(f"❌ Erreur actualisation config {strategy_id}: {e}")
    
    def save_strategy_config(self, strategy_id: str):
        """Sauvegarde la configuration d'une stratégie"""
        try:
            if strategy_id in self.config_editors:
                config_editor = self.config_editors[strategy_id]['editor']
                yaml_content = config_editor.toPlainText()
                
                # Parser le YAML
                import yaml
                try:
                    parsed_config = yaml.safe_load(yaml_content)
                    
                    # Mettre à jour la configuration en mémoire
                    if hasattr(self, 'config_data') and self.config_data:
                        if 'strategies' not in self.config_data:
                            self.config_data['strategies'] = {}
                        self.config_data['strategies'][strategy_id] = parsed_config
                        
                        # Sauvegarder dans le fichier
                        config_file = "config/trading_config.yaml"
                        with open(config_file, 'w', encoding='utf-8') as f:
                            yaml.dump(self.config_data, f, default_flow_style=False, indent=2, allow_unicode=True)
                        
                        # Mettre à jour le statut
                        from datetime import datetime
                        self.config_editors[strategy_id]['status_label'].setText("💾 Configuration sauvegardée")
                        self.config_editors[strategy_id]['modified_label'].setText(f"Sauvegardé: {datetime.now().strftime('%H:%M:%S')}")
                        
                        self.append_log(f"💾 Configuration {strategy_id} sauvegardée")
                    else:
                        self.append_log(f"❌ Impossible de sauvegarder - config_data non disponible")
                        
                except yaml.YAMLError as e:
                    self.append_log(f"❌ Erreur YAML dans la configuration {strategy_id}: {e}")
                    self.config_editors[strategy_id]['status_label'].setText(f"❌ Erreur YAML: {str(e)}")
                    
        except Exception as e:
            self.append_log(f"❌ Erreur sauvegarde config {strategy_id}: {e}")
    
    def toggle_config_edit_mode(self, strategy_id: str):
        """Active/désactive le mode édition pour la configuration"""
        try:
            if strategy_id in self.config_editors:
                editor_refs = self.config_editors[strategy_id]
                config_editor = editor_refs['editor']
                edit_btn = editor_refs['edit_btn']
                save_btn = editor_refs['save_btn']
                status_label = editor_refs['status_label']
                
                if edit_btn.isChecked():
                    # Activer le mode édition - Mode sombre avec accent
                    config_editor.setReadOnly(False)
                    config_editor.setStyleSheet("""
                        QTextEdit { 
                            background-color: #2b2b2b; 
                            color: #e8e8e8; 
                            border: 2px solid #ffa500; 
                            selection-background-color: #4a4a4a;
                        }
                    """)
                    edit_btn.setText("🔒 Verrouiller")
                    save_btn.setEnabled(True)
                    status_label.setText("✏️ Mode édition activé - Modifications possibles")
                    status_label.setStyleSheet("color: #ff8c00; font-size: 10px; font-weight: bold;")
                    self.append_log(f"✏️ Mode édition activé pour {strategy_id}")
                else:
                    # Désactiver le mode édition - Mode sombre lecture seule
                    config_editor.setReadOnly(True)
                    config_editor.setStyleSheet("""
                        QTextEdit { 
                            background-color: #3a3a3a; 
                            color: #c0c0c0; 
                            border: 1px solid #555; 
                        }
                    """)
                    edit_btn.setText("✏️ Éditer")
                    save_btn.setEnabled(False)
                    status_label.setText("📄 Configuration en lecture seule")
                    status_label.setStyleSheet("color: #666; font-size: 10px;")
                    self.append_log(f"🔒 Mode lecture seule pour {strategy_id}")
                    
        except Exception as e:
            self.append_log(f"❌ Erreur toggle edit mode {strategy_id}: {e}")
    
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
    
    def filter_dataframe_by_start_date(self, df, strategy_id: str):
        """Filtre la DataFrame selon start_date de la config"""
        try:
            # Récupérer start_date de la config
            if hasattr(self, 'config_data') and self.config_data:
                strategies = self.config_data.get('strategies', {})
                strategy_config = strategies.get(strategy_id, {})
                start_date_str = strategy_config.get('start_date', '2025-01-01')
            else:
                start_date_str = '2025-01-01'  # Défaut
            
            self.append_log(f"📅 Filtrage depuis: {start_date_str}")
            
            # Convertir start_date en timestamp si la colonne date existe
            if 'date' in df.columns:
                import pandas as pd
                from datetime import datetime
                start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
                
                # Filtrer la DataFrame
                df['date_parsed'] = pd.to_datetime(df['date'])
                filtered_df = df[df['date_parsed'] >= start_date].copy()
                filtered_df = filtered_df.drop('date_parsed', axis=1)  # Nettoyer
                
                self.append_log(f"📊 Filtrage: {len(df)} → {len(filtered_df)} lignes")
                return filtered_df
            else:
                self.append_log(f"⚠️ Colonne 'date' introuvable - pas de filtrage")
                return df
                
        except Exception as e:
            self.append_log(f"❌ Erreur filtrage start_date: {e}")
            return df
    
    def populate_table_data_fast(self, table, df):
        """Version ultra-optimisée du remplissage de table"""
        try:
            self.append_log(f"⚡ Remplissage rapide: {len(df)} lignes")
            
            # Désactiver tous les updates pendant le remplissage
            table.setUpdatesEnabled(False)
            table.setSortingEnabled(False)
            
            try:
                # Remplissage par gros batch et sans formatage complexe
                batch_size = 500  # Plus gros batch
                
                for start_row in range(0, len(df), batch_size):
                    end_row = min(start_row + batch_size, len(df))
                    
                    for row in range(start_row, end_row):
                        for col in range(len(df.columns)):
                            value = df.iloc[row, col]
                            
                            # Formatage minimaliste pour la vitesse
                            if pd.isna(value):
                                display_value = ""
                            elif isinstance(value, float):
                                display_value = f"{value:.2f}"
                            else:
                                display_value = str(value)
                            
                            item = QTableWidgetItem(display_value)
                            table.setItem(row, col, item)
                    
                    # Mini pause tous les 500 lignes
                    if start_row % 1000 == 0:
                        from PySide6.QtWidgets import QApplication
                        QApplication.processEvents()
                        
            finally:
                # Réactiver
                table.setUpdatesEnabled(True)
                table.setSortingEnabled(True)
            
            self.append_log(f"✅ Remplissage terminé")
                
        except Exception as e:
            table.setUpdatesEnabled(True)
            table.setSortingEnabled(True)
            self.append_log(f"❌ Erreur remplissage rapide: {e}")
    
    def display_dataframe_for_strategy(self, df, file_path: str, session_id: str, strategy_id: str):
        """Initialise la pagination et affiche la première page"""
        try:
            self.append_log(f"🎨 Initialisation pagination pour {strategy_id}")
            self.append_log(f"📊 DataFrame brute: {df.shape}")
            
            if strategy_id not in self.strategy_dataframes:
                self.append_log(f"❌ Strategy {strategy_id} not found")
                return
                
            strategy_data = self.strategy_dataframes[strategy_id]
            
            # Filtrer selon start_date de la config
            filtered_df = self.filter_dataframe_by_start_date(df, strategy_id)
            self.append_log(f"📅 DataFrame filtrée: {filtered_df.shape} (selon start_date)")
            
            # Stocker la DataFrame complète et calculer la pagination
            strategy_data['full_dataframe'] = filtered_df
            page_size = strategy_data['page_size']
            total_pages = (len(filtered_df) + page_size - 1) // page_size  # Ceil division
            strategy_data['total_pages'] = max(1, total_pages)
            strategy_data['current_page'] = 1
            
            # Mettre à jour les informations
            file_name = os.path.basename(file_path)
            last_modified = datetime.fromtimestamp(os.path.getmtime(file_path))
            strategy_data['info_label'].setText(
                f"📈 {strategy_id} | {file_name} | {len(filtered_df)} lignes × {len(filtered_df.columns)} colonnes | {last_modified.strftime('%H:%M:%S')}"
            )
            
            # Mémoriser le chemin et mtime
            strategy_data['file_path'] = file_path
            strategy_data['last_mtime'] = os.path.getmtime(file_path)
            
            # Afficher la première page (les plus récentes)
            self.display_current_page(strategy_id)
            
            self.append_log(f"✅ Pagination initialisée: {total_pages} pages de {page_size} lignes")
            
        except Exception as e:
            self.append_log(f"❌ Erreur initialisation DataFrame {strategy_id}: {e}")
            import traceback
            self.append_log(f"📝 Traceback: {traceback.format_exc()}")
    
    def display_current_page(self, strategy_id: str):
        """Affiche la page courante"""
        try:
            if strategy_id not in self.strategy_dataframes:
                return
                
            strategy_data = self.strategy_dataframes[strategy_id]
            full_df = strategy_data['full_dataframe']
            current_page = strategy_data['current_page']
            page_size = strategy_data['page_size']
            total_pages = strategy_data['total_pages']
            table = strategy_data['table']
            
            if full_df is None:
                return
            
            # Calculer les indices pour la page courante
            # Page 1 = les 500 dernières lignes, Page 2 = les 500 d'avant, etc.
            total_rows = len(full_df)
            end_idx = total_rows - (current_page - 1) * page_size
            start_idx = max(0, end_idx - page_size)
            
            page_df = full_df.iloc[start_idx:end_idx].copy()
            
            self.append_log(f"📄 Page {current_page}/{total_pages}: lignes {start_idx}-{end_idx-1}")
            
            # Configurer la table
            table.setRowCount(len(page_df))
            table.setColumnCount(len(page_df.columns))
            table.setHorizontalHeaderLabels([str(col) for col in page_df.columns])
            
            # Remplir rapidement
            self.populate_table_data_fast(table, page_df)
            
            # Ajuster les colonnes
            table.resizeColumnsToContents()
            
            # Mettre à jour les contrôles de pagination
            strategy_data['page_label'].setText(f"Page {current_page} / {total_pages}")
            strategy_data['prev_btn'].setEnabled(current_page < total_pages)  # Peut aller vers le passé
            strategy_data['next_btn'].setEnabled(current_page > 1)  # Peut revenir vers le présent
            
            # Sélectionner la dernière ligne (plus récente) sur la page 1
            if current_page == 1 and table.rowCount() > 0:
                from PySide6.QtCore import QTimer
                def select_last_row():
                    last_row = table.rowCount() - 1
                    table.selectRow(last_row)
                    table.scrollToBottom()
                
                QTimer.singleShot(100, select_last_row)
            
        except Exception as e:
            self.append_log(f"❌ Erreur affichage page {strategy_id}: {e}")
    
    def goto_previous_page(self, strategy_id: str):
        """Aller à la page précédente (plus ancienne)"""
        try:
            if strategy_id not in self.strategy_dataframes:
                return
                
            strategy_data = self.strategy_dataframes[strategy_id]
            if strategy_data['current_page'] < strategy_data['total_pages']:
                strategy_data['current_page'] += 1
                self.display_current_page(strategy_id)
                self.append_log(f"◀ {strategy_id}: Page {strategy_data['current_page']} (vers le passé)")
                
        except Exception as e:
            self.append_log(f"❌ Erreur navigation précédente {strategy_id}: {e}")
    
    def goto_next_page(self, strategy_id: str):
        """Aller à la page suivante (plus récente)"""
        try:
            if strategy_id not in self.strategy_dataframes:
                return
                
            strategy_data = self.strategy_dataframes[strategy_id]
            if strategy_data['current_page'] > 1:
                strategy_data['current_page'] -= 1
                self.display_current_page(strategy_id)
                self.append_log(f"▶ {strategy_id}: Page {strategy_data['current_page']} (vers le présent)")
                
        except Exception as e:
            self.append_log(f"❌ Erreur navigation suivante {strategy_id}: {e}")
    
    
    def populate_table_data(self, table, display_df):
        """Remplit la table avec les données formatées (optimisé pour performance)"""
        try:
            self.append_log(f"📝 Début remplissage table optimisé...")
            
            # Désactiver les updates pendant le remplissage pour améliorer les performances
            table.setUpdatesEnabled(False)
            
            try:
                # Remplissage par batch pour éviter le blocage UI
                from PySide6.QtWidgets import QApplication
                batch_size = 100  # Traiter par batch de 100 lignes
                
                for start_row in range(0, len(display_df), batch_size):
                    end_row = min(start_row + batch_size, len(display_df))
                    
                    for row in range(start_row, end_row):
                        for col in range(len(display_df.columns)):
                            value = display_df.iloc[row, col]
                            
                            # Formatage des valeurs selon le type de colonne
                            column_name = display_df.columns[col]
                            
                            if pd.isna(value):
                                display_value = "NaN"
                            elif column_name == 'timestamp':
                                display_value = str(int(value)) if isinstance(value, (int, float)) else str(value)
                            elif column_name == 'date':
                                display_value = str(value)
                            elif column_name in ['open', 'high', 'low', 'close']:
                                display_value = f"{float(value):.2f}" if isinstance(value, (int, float)) else str(value)
                            elif column_name == 'volume':
                                display_value = f"{float(value):.3f}" if isinstance(value, (int, float)) else str(value)
                            elif isinstance(value, float):
                                display_value = f"{value:.6f}"
                            else:
                                display_value = str(value)
                            
                            item = QTableWidgetItem(display_value)
                            table.setItem(row, col, item)
                    
                    # Permettre à l'UI de se rafraîchir entre les batchs
                    QApplication.processEvents()
                    
            finally:
                # Réactiver les updates
                table.setUpdatesEnabled(True)
            
            self.append_log(f"✅ Table remplie: {len(display_df)} lignes × {len(display_df.columns)} colonnes")
                
        except Exception as e:
            table.setUpdatesEnabled(True)  # S'assurer que les updates sont réactivées
            self.append_log(f"❌ Erreur populate_table_data: {e}")
    
    
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