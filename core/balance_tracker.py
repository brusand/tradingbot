"""
Balance Tracker - Gestionnaire de balance pour les stratégies
Suit la balance courante et l'historique des P&L par chandelle
"""

import pandas as pd
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timezone
import logging

logger = logging.getLogger(__name__)


class BalanceTracker:
    """Gestionnaire de balance et P&L pour une stratégie"""
    
    def __init__(self, strategy_id: str, initial_balance: float):
        self.strategy_id = strategy_id
        self.initial_balance = initial_balance
        self.current_balance = initial_balance
        
        # Historique des balances par timestamp
        self.balance_history: List[Dict] = []
        
        # Trades ouverts
        self.open_trades: Dict[str, Dict] = {}
        
        # Métriques de performance
        self.total_trades = 0
        self.winning_trades = 0
        self.total_pnl = 0.0
        self.max_drawdown = 0.0
        self.peak_balance = initial_balance
        
        logger.info(f"BalanceTracker initialisé pour {strategy_id} avec balance {initial_balance}€")
    
    def add_balance_entry(self, timestamp: pd.Timestamp, balance: float, 
                         trade_pnl: float = 0.0, notes: str = "") -> None:
        """
        Ajoute une entrée de balance à l'historique
        
        Args:
            timestamp: Timestamp de la chandelle
            balance: Balance courante
            trade_pnl: P&L du trade fermé (si applicable)
            notes: Notes sur le changement de balance
        """
        # Mettre à jour la balance courante
        self.current_balance = balance
        
        # Calculer les métriques
        total_pnl = balance - self.initial_balance
        pnl_pct = (total_pnl / self.initial_balance * 100) if self.initial_balance > 0 else 0
        
        # Suivre le peak et drawdown
        if balance > self.peak_balance:
            self.peak_balance = balance
        
        current_drawdown = (self.peak_balance - balance) / self.peak_balance * 100
        if current_drawdown > self.max_drawdown:
            self.max_drawdown = current_drawdown
        
        # Ajouter à l'historique
        entry = {
            'timestamp': timestamp,
            'balance': balance,
            'trade_pnl': trade_pnl,
            'total_pnl': total_pnl,
            'pnl_pct': pnl_pct,
            'drawdown_pct': current_drawdown,
            'notes': notes
        }
        
        self.balance_history.append(entry)
        
        # Mise à jour des statistiques de trades
        if trade_pnl != 0.0:
            self.total_trades += 1
            self.total_pnl += trade_pnl
            if trade_pnl > 0:
                self.winning_trades += 1
        
        logger.debug(f"Balance mise à jour: {balance}€ (P&L: {total_pnl:+.2f}€)")
    
    def open_trade(self, trade_id: str, entry_price: float, quantity: float, 
                   trade_type: str, timestamp: pd.Timestamp) -> None:
        """
        Enregistre l'ouverture d'un trade
        
        Args:
            trade_id: ID unique du trade
            entry_price: Prix d'entrée
            quantity: Quantité
            trade_type: 'LONG' ou 'SHORT'
            timestamp: Timestamp d'ouverture
        """
        self.open_trades[trade_id] = {
            'entry_price': entry_price,
            'quantity': quantity,
            'type': trade_type,
            'timestamp': timestamp,
            'cost': entry_price * quantity  # Coût total du trade
        }
        
        # Réduire la balance disponible
        trade_cost = entry_price * quantity
        self.add_balance_entry(
            timestamp, 
            self.current_balance - trade_cost,
            0.0,
            f"Ouverture trade {trade_id} ({trade_type})"
        )
        
        logger.info(f"Trade ouvert: {trade_id} - {trade_type} {quantity} @ {entry_price}")
    
    def close_trade(self, trade_id: str, exit_price: float, 
                    timestamp: pd.Timestamp, fees: float = 0.0) -> float:
        """
        Ferme un trade et calcule le P&L
        
        Args:
            trade_id: ID du trade à fermer
            exit_price: Prix de sortie
            timestamp: Timestamp de fermeture
            fees: Frais de transaction
            
        Returns:
            P&L du trade fermé
        """
        if trade_id not in self.open_trades:
            logger.warning(f"Trade {trade_id} non trouvé dans les trades ouverts")
            return 0.0
        
        trade = self.open_trades[trade_id]
        entry_price = trade['entry_price']
        quantity = trade['quantity']
        trade_type = trade['type']
        
        # Calculer P&L selon le type de trade
        if trade_type == 'LONG':
            gross_pnl = (exit_price - entry_price) * quantity
        else:  # SHORT
            gross_pnl = (entry_price - exit_price) * quantity
        
        net_pnl = gross_pnl - fees
        
        # Récupérer le capital investi et ajouter le P&L
        trade_value = exit_price * quantity
        new_balance = self.current_balance + trade_value + net_pnl
        
        # Enregistrer la nouvelle balance
        self.add_balance_entry(
            timestamp,
            new_balance,
            net_pnl,
            f"Fermeture trade {trade_id} (P&L: {net_pnl:+.2f}€)"
        )
        
        # Supprimer de la liste des trades ouverts
        del self.open_trades[trade_id]
        
        logger.info(f"Trade fermé: {trade_id} - P&L: {net_pnl:+.2f}€")
        return net_pnl
    
    def get_balance_dataframe(self) -> pd.DataFrame:
        """
        Retourne l'historique des balances sous forme de DataFrame
        
        Returns:
            DataFrame avec l'historique des balances
        """
        if not self.balance_history:
            return pd.DataFrame()
        
        df = pd.DataFrame(self.balance_history)
        df.set_index('timestamp', inplace=True)
        return df
    
    def add_to_strategy_dataframe(self, strategy_df: pd.DataFrame) -> pd.DataFrame:
        """
        Ajoute les colonnes de balance au DataFrame de la stratégie
        
        Args:
            strategy_df: DataFrame principal de la stratégie
            
        Returns:
            DataFrame enrichi avec les colonnes de balance
        """
        if strategy_df.empty or not self.balance_history:
            return strategy_df
        
        # Créer DataFrame des balances
        balance_df = self.get_balance_dataframe()
        
        # Joindre avec le DataFrame de la stratégie
        result_df = strategy_df.copy()
        
        # Ajouter les colonnes de balance par timestamp
        for timestamp in strategy_df.index:
            # Trouver la balance la plus proche dans le temps
            closest_entry = None
            for entry in self.balance_history:
                if entry['timestamp'] <= timestamp:
                    closest_entry = entry
                else:
                    break
            
            if closest_entry:
                result_df.loc[timestamp, 'balance'] = closest_entry['balance']
                result_df.loc[timestamp, 'total_pnl'] = closest_entry['total_pnl']
                result_df.loc[timestamp, 'pnl_pct'] = closest_entry['pnl_pct']
                result_df.loc[timestamp, 'drawdown_pct'] = closest_entry['drawdown_pct']
            else:
                # Utiliser la balance initiale si aucune entrée trouvée
                result_df.loc[timestamp, 'balance'] = self.initial_balance
                result_df.loc[timestamp, 'total_pnl'] = 0.0
                result_df.loc[timestamp, 'pnl_pct'] = 0.0
                result_df.loc[timestamp, 'drawdown_pct'] = 0.0
        
        return result_df
    
    def get_performance_summary(self) -> Dict:
        """
        Retourne un résumé des performances
        
        Returns:
            Dictionnaire avec les métriques de performance
        """
        win_rate = (self.winning_trades / self.total_trades * 100) if self.total_trades > 0 else 0
        avg_pnl = self.total_pnl / self.total_trades if self.total_trades > 0 else 0
        
        return {
            'initial_balance': self.initial_balance,
            'current_balance': self.current_balance,
            'total_pnl': self.current_balance - self.initial_balance,
            'total_pnl_pct': ((self.current_balance - self.initial_balance) / self.initial_balance * 100) if self.initial_balance > 0 else 0,
            'total_trades': self.total_trades,
            'winning_trades': self.winning_trades,
            'win_rate': win_rate,
            'avg_pnl_per_trade': avg_pnl,
            'max_drawdown': self.max_drawdown,
            'peak_balance': self.peak_balance,
            'open_trades_count': len(self.open_trades)
        }
    
    def update_strategy_config(self, strategy_config: Dict) -> Dict:
        """
        Met à jour la configuration de stratégie avec la balance courante
        
        Args:
            strategy_config: Configuration de stratégie à mettre à jour
            
        Returns:
            Configuration mise à jour
        """
        strategy_config['current_balance'] = self.current_balance
        return strategy_config