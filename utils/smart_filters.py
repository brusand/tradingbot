"""
Smart Filters - Système de recherche floue pour les IDs et noms
Permet d'utiliser des noms partiels dans les commandes CLI
"""

from typing import List, Dict, Optional, Tuple
import difflib
from fuzzywuzzy import fuzz
import logging
import click

# Variable globale pour vérifier la disponibilité
try:
    from fuzzywuzzy import fuzz
    SMART_FILTERS_AVAILABLE = True
except ImportError:
    SMART_FILTERS_AVAILABLE = False

logger = logging.getLogger(__name__)


class SmartFilter:
    """Filtres intelligents pour recherche floue d'IDs et noms"""
    
    @staticmethod
    def find_best_match(
        query: str, 
        items: Dict[str, Dict], 
        name_field: str = 'name',
        min_score: int = 60
    ) -> Optional[str]:
        """
        Trouve le meilleur match pour une requête
        
        Args:
            query: Terme de recherche
            items: Dictionnaire {id: config} à rechercher
            name_field: Champ contenant le nom dans la config
            min_score: Score minimum pour considérer un match
            
        Returns:
            ID du meilleur match ou None
        """
        if not query or not items:
            return None
        
        query_lower = query.lower().strip()
        
        # 1. Match exact sur ID
        if query in items:
            return query
        
        # 2. Match exact sur nom (case insensitive)
        for item_id, item_config in items.items():
            name = item_config.get(name_field, '').lower()
            if name == query_lower:
                return item_id
        
        # 3. Match partiel sur ID (commence par)
        id_matches = [item_id for item_id in items.keys() if item_id.lower().startswith(query_lower)]
        if len(id_matches) == 1:
            return id_matches[0]
        
        # 4. Match partiel sur nom (contient)
        name_matches = []
        for item_id, item_config in items.items():
            name = item_config.get(name_field, '').lower()
            if query_lower in name:
                name_matches.append(item_id)
        
        if len(name_matches) == 1:
            return name_matches[0]
        
        # 5. Recherche floue avec scoring
        best_match = None
        best_score = 0
        
        for item_id, item_config in items.items():
            name = item_config.get(name_field, '')
            
            # Score sur l'ID
            id_score = fuzz.partial_ratio(query_lower, item_id.lower())
            
            # Score sur le nom
            name_score = fuzz.partial_ratio(query_lower, name.lower())
            
            # Score combiné (privilégier le nom)
            combined_score = max(id_score * 0.7, name_score * 1.0)
            
            if combined_score > best_score and combined_score >= min_score:
                best_score = combined_score
                best_match = item_id
        
        return best_match
    
    @staticmethod
    def find_multiple_matches(
        query: str,
        items: Dict[str, Dict],
        name_field: str = 'name',
        min_score: int = 50,
        max_results: int = 5
    ) -> List[Tuple[str, int]]:
        """
        Trouve plusieurs matches possibles avec scores
        
        Returns:
            Liste de tuples (item_id, score) triés par score décroissant
        """
        if not query or not items:
            return []
        
        query_lower = query.lower().strip()
        matches = []
        
        for item_id, item_config in items.items():
            name = item_config.get(name_field, '')
            
            # Calculer les scores
            id_score = fuzz.partial_ratio(query_lower, item_id.lower())
            name_score = fuzz.partial_ratio(query_lower, name.lower())
            
            # Score combiné
            combined_score = max(id_score * 0.7, name_score * 1.0)
            
            if combined_score >= min_score:
                matches.append((item_id, int(combined_score)))
        
        # Trier par score décroissant
        matches.sort(key=lambda x: x[1], reverse=True)
        
        return matches[:max_results]
    
    @staticmethod
    def get_suggestion_message(
        query: str,
        items: Dict[str, Dict],
        item_type: str = "item",
        name_field: str = 'name'
    ) -> str:
        """
        Génère un message de suggestion avec les matches possibles
        
        Args:
            query: Requête originale
            items: Items à rechercher
            item_type: Type d'item (session, strategy, etc.)
            name_field: Champ nom
            
        Returns:
            Message formaté avec suggestions
        """
        matches = SmartFilter.find_multiple_matches(query, items, name_field)
        
        if not matches:
            return f"❌ Aucun {item_type} trouvé pour '{query}'"
        
        message = f"🔍 Plusieurs {item_type}s trouvés pour '{query}':\n"
        
        for item_id, score in matches:
            item_config = items[item_id]
            name = item_config.get(name_field, 'N/A')
            message += f"  • {item_id} ({name}) - {score}% de correspondance\n"
        
        message += f"\n💡 Utilisez un terme plus précis ou l'ID complet."
        
        return message


class SessionFilter(SmartFilter):
    """Filtres spécialisés pour les sessions"""
    
    @staticmethod
    def resolve_session_id(query: str, sessions_registry: Dict) -> Optional[str]:
        """Résout un ID de session à partir d'une requête"""
        return SmartFilter.find_best_match(query, sessions_registry, 'name')
    
    @staticmethod
    def get_session_suggestions(query: str, sessions_registry: Dict) -> str:
        """Suggestions pour sessions"""
        return SmartFilter.get_suggestion_message(
            query, sessions_registry, "session", 'name'
        )


class StrategyFilter(SmartFilter):
    """Filtres spécialisés pour les stratégies"""
    
    @staticmethod
    def resolve_strategy_id(query: str, strategies_registry: Dict) -> Optional[str]:
        """Résout un ID de stratégie à partir d'une requête"""
        return SmartFilter.find_best_match(query, strategies_registry, 'name')
    
    @staticmethod
    def get_strategy_suggestions(query: str, strategies_registry: Dict) -> str:
        """Suggestions pour stratégies"""
        return SmartFilter.get_suggestion_message(
            query, strategies_registry, "stratégie", 'name'
        )


def smart_resolve_ids(
    session_query: str,
    strategy_query: str,
    sessions_registry: Dict,
    strategies_registry: Dict
) -> Tuple[Optional[str], Optional[str], List[str]]:
    """
    Résout intelligemment les IDs de session et stratégie
    
    Returns:
        Tuple (session_id, strategy_id, warnings)
    """
    warnings = []
    
    # Résoudre session
    session_id = SessionFilter.resolve_session_id(session_query, sessions_registry)
    if not session_id:
        warnings.append(SessionFilter.get_session_suggestions(session_query, sessions_registry))
    
    # Résoudre stratégie
    strategy_id = StrategyFilter.resolve_strategy_id(strategy_query, strategies_registry)
    if not strategy_id:
        warnings.append(StrategyFilter.get_strategy_suggestions(strategy_query, strategies_registry))
    
    return session_id, strategy_id, warnings


def smart_resolve_single(
    query: str,
    items: Dict[str, Dict],
    item_type: str,
    name_field: str = 'name'
) -> Tuple[Optional[str], Optional[str]]:
    """
    Résout un seul ID avec message d'erreur/suggestion
    
    Returns:
        Tuple (resolved_id, error_message)
    """
    if not query or not items:
        return None, f"❌ Aucun {item_type} disponible"
    
    resolved_id = SmartFilter.find_best_match(query, items, name_field)
    
    if not resolved_id:
        error_msg = SmartFilter.get_suggestion_message(query, items, item_type, name_field)
        return None, error_msg
    
    return resolved_id, None


class UniversalFilter:
    """Filtre universel pour tous types d'entités"""
    
    @staticmethod
    def resolve_session(query: str, registry: Dict) -> Tuple[Optional[str], Optional[str]]:
        """Résout une session avec gestion d'erreur"""
        return smart_resolve_single(query, registry, "session", 'name')
    
    @staticmethod
    def resolve_strategy(query: str, registry: Dict) -> Tuple[Optional[str], Optional[str]]:
        """Résout une stratégie avec gestion d'erreur"""
        return smart_resolve_single(query, registry, "stratégie", 'name')
    
    @staticmethod
    def resolve_risk_profile(query: str, registry: Dict) -> Tuple[Optional[str], Optional[str]]:
        """Résout un profil de risque avec gestion d'erreur"""
        return smart_resolve_single(query, registry, "profil de risque", 'name')
    
    @staticmethod
    def resolve_indicator(query: str, registry: Dict) -> Tuple[Optional[str], Optional[str]]:
        """Résout un indicateur avec gestion d'erreur"""
        return smart_resolve_single(query, registry, "indicateur", 'description')


# Décorateur pour automatiser la résolution dans les commandes
def smart_resolve_params(**param_configs):
    """
    Décorateur pour résoudre automatiquement les paramètres avec filtres intelligents
    
    Usage:
    @smart_resolve_params(
        session_id='session',
        strategy_id='strategy'
    )
    def my_command(ctx, session_id, strategy_id):
        # session_id et strategy_id sont automatiquement résolus
    """
    def decorator(func):
        def wrapper(ctx, *args, **kwargs):
            # Vérifier si les smart filters sont disponibles
            try:
                from fuzzywuzzy import fuzz
                filters_available = True
            except ImportError:
                filters_available = False
            
            if not filters_available:
                return func(ctx, *args, **kwargs)
            
            trading_cli = ctx.obj['cli']
            resolved_params = {}
            
            # Résoudre chaque paramètre configuré
            for param_name, entity_type in param_configs.items():
                if param_name in kwargs:
                    query = kwargs[param_name]
                    
                    if entity_type == 'session':
                        resolved_id, error = UniversalFilter.resolve_session(
                            query, trading_cli.sessions_registry
                        )
                    elif entity_type == 'strategy':
                        resolved_id, error = UniversalFilter.resolve_strategy(
                            query, trading_cli.strategies_registry
                        )
                    elif entity_type == 'risk_profile':
                        resolved_id, error = UniversalFilter.resolve_risk_profile(
                            query, trading_cli.risk_profiles_registry
                        )
                    else:
                        # Pas de résolution pour ce type
                        continue
                    
                    if error:
                        click.echo(error)
                        return
                    
                    # Afficher la résolution si différente
                    if resolved_id != query:
                        entity_name = entity_type.capitalize()
                        click.echo(f"🔍 {entity_name} résolue: '{query}' → '{resolved_id}'")
                    
                    kwargs[param_name] = resolved_id
            
            return func(ctx, *args, **kwargs)
        
        return wrapper
    return decorator