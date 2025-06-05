"""
Générateur de Grid Strategies pour TradingCLI V4
Génère plusieurs instances de stratégies à partir des valeurs séparées par '-'

Utilisation:
    from grid_generator import StrategyGridGenerator

    generator = StrategyGridGenerator(trading_cli)
    generator.load_base_strategy_from_registry('ma_strategy')
    strategies = generator.generate_strategy_combinations('MA Strategy')
    generator.save_grid_strategies_to_cli(strategies)
"""

import yaml
import itertools
from datetime import datetime
from typing import Dict, List, Any, Union
import copy
import re

class StrategyGridGenerator:
    def __init__(self, trading_cli=None):
        self.base_strategy = None
        self.grid_parameters = {}
        self.trading_cli = trading_cli

    def load_base_strategy(self, strategy_file: str) -> Dict:
        """Charge la stratégie de base depuis un fichier YAML"""
        with open(strategy_file, 'r', encoding='utf-8') as file:
            strategies = yaml.safe_load(file)
            # Prend la première stratégie comme base
            strategy_name = list(strategies.keys())[0]
            self.base_strategy = strategies[strategy_name]
            return self.base_strategy

    def load_base_strategy_from_registry(self, strategy_id: str) -> Dict:
        """Charge la stratégie de base depuis le registre du CLI"""
        if not self.trading_cli or not self.trading_cli.strategies_registry:
            raise ValueError("Trading CLI non disponible ou aucune stratégie dans le registre")

        if strategy_id not in self.trading_cli.strategies_registry:
            raise ValueError(f"Stratégie '{strategy_id}' non trouvée dans le registre")

        self.base_strategy = copy.deepcopy(self.trading_cli.strategies_registry[strategy_id])
        return self.base_strategy

    def extract_grid_values(self, value: Any) -> List[Any]:
        """Extrait les valeurs de grid d'une chaîne ou retourne la valeur telle quelle"""
        if isinstance(value, str) and '-' in value:
            try:
                # Gestion des ranges numériques avec '-'
                parts = value.split('-')
                if len(parts) == 2:
                    start, end = parts

                    # Vérifie si ce sont des entiers
                    if start.isdigit() and end.isdigit():
                        start_val, end_val = int(start), int(end)
                        # Génère une séquence intelligente selon la plage
                        if end_val - start_val <= 10:
                            # Pas de 1 pour les petites plages
                            return list(range(start_val, end_val + 1))
                        else:
                            # Pas intelligent pour les grandes plages
                            step = max(1, (end_val - start_val) // 10)
                            return list(range(start_val, end_val + 1, step))

                    # Vérifie si ce sont des floats
                    else:
                        try:
                            start_f, end_f = float(start), float(end)
                            # Génère des valeurs avec un pas intelligent
                            if end_f - start_f <= 1:
                                step = 0.1
                            elif end_f - start_f <= 10:
                                step = 0.5
                            else:
                                step = 1.0

                            values = []
                            current = start_f
                            while current <= end_f:
                                values.append(round(current, 2))
                                current += step
                            return values
                        except ValueError:
                            return [value]  # Retourne la valeur originale si pas convertible
                else:
                    return [value]
            except:
                return [value]
        return [value]

    def find_grid_parameters(self, data: Dict, path: str = "") -> Dict:
        """Trouve récursivement tous les paramètres contenant des valeurs de grid"""
        grid_params = {}

        for key, value in data.items():
            current_path = f"{path}.{key}" if path else key

            if isinstance(value, dict):
                # Récursion pour les dictionnaires imbriqués
                nested_params = self.find_grid_parameters(value, current_path)
                grid_params.update(nested_params)
            else:
                # Vérifie si la valeur contient un range
                grid_values = self.extract_grid_values(value)
                if len(grid_values) > 1:
                    grid_params[current_path] = grid_values

        return grid_params

    def set_nested_value(self, data: Dict, path: str, value: Any) -> None:
        """Définit une valeur dans un dictionnaire imbriqué en utilisant un chemin pointé"""
        keys = path.split('.')
        current = data

        for key in keys[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]

        current[keys[-1]] = value

    def generate_strategy_combinations(self, base_strategy_name: str, name_pattern: str = None) -> List[Dict]:
        """Génère toutes les combinaisons possibles de stratégies"""
        if not self.base_strategy:
            raise ValueError("Aucune stratégie de base chargée")

        # Trouve tous les paramètres de grid
        grid_params = self.find_grid_parameters(self.base_strategy)

        if not grid_params:
            print("Aucun paramètre de grid trouvé")
            return [self.base_strategy]

        print(f"Paramètres de grid trouvés: {list(grid_params.keys())}")

        # Affiche les valeurs qui seront générées
        for param_path, values in grid_params.items():
            print(f"  {param_path}: {values} ({len(values)} valeurs)")

        # Génère toutes les combinaisons
        param_names = list(grid_params.keys())
        param_values = list(grid_params.values())

        combinations = list(itertools.product(*param_values))
        strategies = []

        total_combinations = len(combinations)
        print(f"Génération de {total_combinations} combinaisons...")

        for i, combination in enumerate(combinations):
            # Copie profonde de la stratégie de base
            new_strategy = copy.deepcopy(self.base_strategy)

            # Applique chaque valeur de la combinaison
            for param_name, value in zip(param_names, combination):
                self.set_nested_value(new_strategy, param_name, value)

            # Génère un nom descriptif pour la stratégie
            if name_pattern:
                strategy_name = name_pattern.format(
                    base_name=base_strategy_name,
                    index=i + 1,
                    **{param.replace('.', '_'): val for param, val in zip(param_names, combination)}
                )
            else:
                # Pattern par défaut
                param_str = "_".join([f"{param.split('.')[-1]}{val}" for param, val in zip(param_names, combination)])
                strategy_name = f"{base_strategy_name}_grid_{param_str}"

            # Met à jour les métadonnées
            new_strategy['id'] = strategy_name.lower().replace(' ', '_').replace('-', '_')
            new_strategy['name'] = strategy_name
            new_strategy['created_at'] = datetime.now().isoformat()

            # Ajoute des métadonnées sur la génération
            new_strategy['grid_info'] = {
                'base_strategy': base_strategy_name,
                'grid_index': i + 1,
                'total_combinations': total_combinations,
                'parameters': dict(zip(param_names, combination))
            }

            strategies.append(new_strategy)

        return strategies

    def save_grid_strategies(self, strategies: List[Dict], output_file: str) -> None:
        """Sauvegarde toutes les stratégies générées dans un fichier YAML"""
        output_data = {}

        for strategy in strategies:
            strategy_id = strategy['id']
            output_data[strategy_id] = strategy

        with open(output_file, 'w', encoding='utf-8') as file:
            yaml.dump(output_data, file, default_flow_style=False, allow_unicode=True, indent=2)

        print(f"{len(strategies)} stratégies sauvegardées dans {output_file}")

    def save_grid_strategies_to_cli(self, strategies: List[Dict]) -> None:
        """Sauvegarde les stratégies directement dans le registre du CLI"""
        if not self.trading_cli:
            raise ValueError("Trading CLI non disponible")

        # Initialiser le registre s'il n'existe pas
        if self.trading_cli.strategies_registry is None:
            self.trading_cli.strategies_registry = {}

        saved_count = 0
        for strategy in strategies:
            strategy_id = strategy['id']

            # Vérifier si la stratégie existe déjà
            if strategy_id in self.trading_cli.strategies_registry:
                print(f"⚠️ Stratégie '{strategy_id}' existe déjà, ignorée")
                continue

            self.trading_cli.strategies_registry[strategy_id] = strategy
            saved_count += 1

        # Sauvegarder la configuration
        self.trading_cli._save_configuration()
        print(f"✅ {saved_count} stratégies grid sauvegardées dans le registre")

        return saved_count

    def generate_grid_from_file(self, input_file: str, output_file: str = None) -> List[Dict]:
        """Méthode principale pour générer un grid à partir d'un fichier"""
        # Charge la stratégie de base
        self.load_base_strategy(input_file)

        # Extrait le nom de la stratégie
        with open(input_file, 'r', encoding='utf-8') as file:
            strategies = yaml.safe_load(file)
            strategy_name = list(strategies.keys())[0]

        # Génère les combinaisons
        grid_strategies = self.generate_strategy_combinations(strategy_name)

        # Sauvegarde si un fichier de sortie est spécifié
        if output_file:
            self.save_grid_strategies(grid_strategies, output_file)

        return grid_strategies

    def print_grid_summary(self, strategies: List[Dict]) -> None:
        """Affiche un résumé des stratégies générées"""
        print(f"\n=== RÉSUMÉ DU GRID ===")
        print(f"Nombre total de stratégies: {len(strategies)}")

        if strategies:
            print(f"\nExemple de paramètres variés:")
            first_strategy = strategies[0]

            # Trouve les paramètres qui varient
            grid_params = self.find_grid_parameters(self.base_strategy)
            for param_path in list(grid_params.keys())[:5]:  # Limite à 5 exemples
                keys = param_path.split('.')
                value = first_strategy
                for key in keys:
                    value = value.get(key, 'N/A')
                print(f"  {param_path}: {value}")


def main():
    """Fonction principale pour tester le générateur"""
    generator = StrategyGridGenerator()

    # Exemple d'utilisation
    try:
        # Génère le grid à partir du fichier
        strategies = generator.generate_grid_from_file(
            input_file='strategy_base.yaml',
            output_file='grid_strategies.yaml'
        )

        # Affiche le résumé
        generator.print_grid_summary(strategies)

        # Affiche quelques exemples
        print(f"\n=== EXEMPLES DE STRATÉGIES ===")
        for i, strategy in enumerate(strategies[:3]):  # Montre les 3 premières
            print(f"\nStratégie {i + 1}: {strategy['id']}")
            print(f"  - Period LONG: {strategy['indicators']['LONG']['parameters']['period']}")
            print(f"  - Period SHORT: {strategy['indicators']['SHORT']['parameters']['period']}")
            print(f"  - Stop Loss: {strategy['risk_management']['stop_loss_percent']}")

    except:
        print('error')
