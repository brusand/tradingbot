"""
Utilitaires pour les calculs de risk management
"""

from typing import Dict, Tuple
from data.models import RiskConfig


class RiskCalculator:
    """Calculateur pour les métriques de risk management"""
    
    @staticmethod
    def calculate_take_profit_from_rr(stop_loss_pct: float, risk_ratio: float) -> float:
        """
        Calcule le take profit optimal basé sur le stop loss et le risk ratio
        
        Args:
            stop_loss_pct: Pourcentage de stop loss (ex: 2.0 pour 2%)
            risk_ratio: Ratio risk/reward (ex: 2.0 pour 1:2)
            
        Returns:
            Pourcentage de take profit calculé
        """
        return stop_loss_pct * risk_ratio
    
    @staticmethod
    def calculate_position_size_from_risk(
        account_balance: float, 
        risk_pct: float, 
        stop_loss_pct: float
    ) -> float:
        """
        Calcule la taille de position basée sur le risque acceptable
        
        Args:
            account_balance: Balance du compte
            risk_pct: Pourcentage de risque par trade (ex: 1.0 pour 1%)
            stop_loss_pct: Pourcentage de stop loss
            
        Returns:
            Taille de position recommandée
        """
        max_loss = account_balance * (risk_pct / 100)
        position_size = max_loss / (stop_loss_pct / 100)
        return min(position_size, account_balance * 0.1)  # Max 10% du compte
    
    @staticmethod
    def validate_risk_config(risk_config: RiskConfig) -> Dict[str, str]:
        """
        Valide la cohérence d'une configuration de risque
        
        Args:
            risk_config: Configuration de risque à valider
            
        Returns:
            Dictionnaire des erreurs trouvées (vide si OK)
        """
        errors = {}
        
        # Vérifier que le take profit est cohérent avec le RR
        calculated_tp = RiskCalculator.calculate_take_profit_from_rr(
            risk_config.stop_loss_pct, 
            risk_config.risk_ratio
        )
        
        if abs(risk_config.take_profit_pct - calculated_tp) > 0.1:
            errors['take_profit'] = (
                f"Take profit ({risk_config.take_profit_pct}%) ne correspond pas "
                f"au RR 1:{risk_config.risk_ratio} (attendu: {calculated_tp:.1f}%)"
            )
        
        # Vérifier les limites logiques
        if risk_config.stop_loss_pct >= risk_config.take_profit_pct:
            errors['ratio'] = "Stop loss doit être inférieur au take profit"
        
        if risk_config.max_position_size > 1.0:
            errors['position_size'] = "Position size ne peut pas dépasser 100%"
        
        if risk_config.risk_ratio < 0.5:
            errors['risk_ratio'] = "Risk ratio trop faible (min recommandé: 0.5)"
        
        return errors
    
    @staticmethod
    def get_risk_metrics(risk_config: RiskConfig) -> Dict[str, float]:
        """
        Calcule les métriques dérivées d'une configuration de risque
        
        Args:
            risk_config: Configuration de risque
            
        Returns:
            Dictionnaire des métriques calculées
        """
        return {
            'risk_reward_ratio': risk_config.risk_ratio,
            'calculated_take_profit': RiskCalculator.calculate_take_profit_from_rr(
                risk_config.stop_loss_pct, 
                risk_config.risk_ratio
            ),
            'win_rate_needed': 1 / (1 + risk_config.risk_ratio) * 100,  # Break-even win rate
            'risk_per_trade_pct': (risk_config.stop_loss_pct * risk_config.max_position_size),
            'max_consecutive_losses': risk_config.max_daily_loss / (
                risk_config.stop_loss_pct * risk_config.max_position_size * 1000  # Estimation
            ) if risk_config.stop_loss_pct > 0 else float('inf')
        }