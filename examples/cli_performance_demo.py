#!/usr/bin/env python3
"""
Démonstration des nouvelles fonctionnalités de performance dans le CLI Trading
"""

import os
import sys
import subprocess
import json
import tempfile
from datetime import datetime

# Ajouter le répertoire parent au PYTHONPATH
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def run_cli_command(command, capture_output=True):
    """Execute une commande CLI et retourne le résultat"""
    full_command = f"python -m interfaces.cli {command}"
    
    print(f"🔧 Executing: {full_command}")
    
    try:
        if capture_output:
            result = subprocess.run(
                full_command, 
                shell=True, 
                capture_output=True, 
                text=True,
                cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            )
            
            if result.returncode == 0:
                print("✅ Command successful")
                if result.stdout:
                    print("📤 Output:")
                    print(result.stdout)
            else:
                print("❌ Command failed")
                if result.stderr:
                    print("📤 Error:")
                    print(result.stderr)
            
            return result
        else:
            # Mode interactif
            subprocess.run(
                full_command,
                shell=True,
                cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            )
            
    except Exception as e:
        print(f"❌ Error executing command: {e}")
        return None


def demo_basic_usage():
    """Démonstration des fonctionnalités de base"""
    print("🚀 DÉMONSTRATION CLI TRADING - FONCTIONNALITÉS DE PERFORMANCE")
    print("=" * 80)
    
    # 1. Lister les sessions existantes
    print("\n1️⃣ Listing existing sessions:")
    run_cli_command("list_sessions")
    
    # 2. Créer une session de démonstration
    print("\n2️⃣ Creating a demo session:")
    session_name = f"demo_session_{datetime.now().strftime('%H%M%S')}"
    run_cli_command(f'create_session --name "{session_name}" --mode paper --strategy AdvancedTradingStrategy --pairs BTCUSD --timeframe 5m')
    
    # 3. Afficher le résumé de performance
    print("\n3️⃣ Performance summary of all sessions:")
    run_cli_command("performance --limit 5 --sort-by pnl")
    
    print("\n✅ Basic demo completed!")


def demo_advanced_features():
    """Démonstration des fonctionnalités avancées"""
    print("\n🎯 DÉMONSTRATION DES FONCTIONNALITÉS AVANCÉES")
    print("=" * 60)
    
    # Obtenir la liste des sessions pour les tests
    print("\n1️⃣ Getting session list for advanced demos:")
    result = run_cli_command("list_sessions")
    
    # Simuler des données de session pour la démonstration
    session_ids = ["session_001", "session_002"]  # IDs factices pour demo
    
    # 2. Comparaison de plusieurs sessions
    print("\n2️⃣ Comparing multiple sessions:")
    print("Note: Using example session IDs (actual sessions may not exist)")
    print(f"Command: compare {' '.join(session_ids)}")
    
    # 3. Affichage détaillé d'une session
    print("\n3️⃣ Detailed session view example:")
    print("Command: show_session <session_id> --detailed --trades --export report.json")
    
    # 4. Performance triée par différents critères
    print("\n4️⃣ Performance sorted by different criteria:")
    
    print("\n🏆 Top sessions by win rate:")
    run_cli_command("performance --limit 3 --sort-by win_rate")
    
    print("\n📊 Top sessions by number of trades:")
    run_cli_command("performance --limit 3 --sort-by trades")
    
    print("\n📅 Most recent sessions:")
    run_cli_command("performance --limit 3 --sort-by created")


def demo_export_functionality():
    """Démonstration de l'export de données"""
    print("\n💾 DÉMONSTRATION DES FONCTIONNALITÉS D'EXPORT")
    print("=" * 50)
    
    # Créer un fichier temporaire pour l'export
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp_file:
        temp_filename = tmp_file.name
    
    print(f"\n📁 Export file: {temp_filename}")
    
    # Simuler un export
    print("\n📤 Example export command:")
    print(f"show_session <session_id> --detailed --export {temp_filename}")
    
    # Créer un exemple de fichier d'export pour la démonstration
    example_export = {
        "session_id": "demo_session_123",
        "session_name": "Demo Trading Session",
        "mode": "paper",
        "status": "running",
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat(),
        "strategy": {
            "name": "AdvancedTradingStrategy",
            "pairs": ["BTCUSD"],
            "timeframe": "5m",
            "parameters": {
                "short_window": 10,
                "long_window": 30
            }
        },
        "risk_params": {
            "max_position_size": 0.1,
            "stop_loss_pct": 2.0,
            "take_profit_pct": 4.0,
            "max_daily_loss": 10.0,
            "max_exposure_pct": 50.0
        },
        "performance": {
            "total_pnl": 150.75,
            "total_trades": 24,
            "win_rate": 0.625
        },
        "exported_at": datetime.now().isoformat()
    }
    
    # Écrire l'exemple dans le fichier
    with open(temp_filename, 'w') as f:
        json.dump(example_export, f, indent=2)
    
    print(f"✅ Example export created at: {temp_filename}")
    
    # Afficher le contenu
    print("\n📋 Export content preview:")
    with open(temp_filename, 'r') as f:
        content = f.read()
        print(content[:500] + "..." if len(content) > 500 else content)
    
    # Nettoyer
    os.unlink(temp_filename)
    print(f"\n🗑️ Temporary file cleaned up")


def show_cli_help():
    """Afficher l'aide du CLI"""
    print("\n📚 AIDE DU CLI TRADING")
    print("=" * 30)
    
    print("\n🔧 Available commands:")
    run_cli_command("--help")
    
    print("\n🔍 Help for specific commands:")
    commands_to_show = [
        "show_session --help",
        "performance --help", 
        "compare --help",
        "list_sessions --help"
    ]
    
    for cmd in commands_to_show:
        print(f"\n📖 Help for '{cmd.split()[0]}':")
        run_cli_command(cmd)


def demo_real_world_scenarios():
    """Scénarios d'utilisation réels"""
    print("\n🌍 SCÉNARIOS D'UTILISATION RÉELS")
    print("=" * 40)
    
    scenarios = [
        {
            "title": "1️⃣ Suivi quotidien des performances",
            "description": "Vérifier le statut et les performances de toutes les sessions actives",
            "commands": [
                "status",
                "performance --limit 5",
                "show_session <best_session_id> --detailed"
            ]
        },
        {
            "title": "2️⃣ Analyse comparative de stratégies",
            "description": "Comparer différentes stratégies pour optimiser les paramètres",
            "commands": [
                "performance --sort-by win_rate",
                "compare session_A session_B session_C",
                "show_session session_best --trades --export analysis.json"
            ]
        },
        {
            "title": "3️⃣ Debugging d'une session problématique",
            "description": "Analyser en détail une session qui sous-performe",
            "commands": [
                "show_session problematic_session --detailed --trades",
                "performance --sort-by pnl",
                "compare problematic_session good_session"
            ]
        },
        {
            "title": "4️⃣ Rapport hebdomadaire automatisé",
            "description": "Générer un rapport complet pour le management",
            "commands": [
                "performance --limit 10 --sort-by pnl",
                "show_session top_session --detailed --export weekly_report.json"
            ]
        }
    ]
    
    for scenario in scenarios:
        print(f"\n{scenario['title']}")
        print(f"📋 {scenario['description']}")
        print("🔧 Commands:")
        for cmd in scenario['commands']:
            print(f"   $ python -m interfaces.cli {cmd}")
        print()


def main():
    """Fonction principale de démonstration"""
    print("🎮 CLI TRADING PERFORMANCE - INTERACTIVE DEMO")
    print("=" * 60)
    print("This demo showcases the new performance features in the Trading CLI")
    print()
    
    try:
        # Démonstrations par étapes
        demo_basic_usage()
        
        print("\n" + "="*60)
        demo_advanced_features()
        
        print("\n" + "="*60)
        demo_export_functionality()
        
        print("\n" + "="*60)
        demo_real_world_scenarios()
        
        print("\n" + "="*60)
        show_cli_help()
        
        print("\n✅ DÉMONSTRATION TERMINÉE")
        print("🎯 Nouvelles fonctionnalités disponibles:")
        print("  - 📊 show_session avec métriques avancées (--detailed, --trades, --export)")
        print("  - 🏆 performance pour comparer toutes les sessions")
        print("  - 📈 compare pour analyser plusieurs sessions en parallèle")
        print("  - 💾 Export JSON des rapports de performance")
        print("  - 🎨 Affichage enrichi avec émojis et couleurs")
        
        print("\n💡 Exemples d'usage:")
        print("  $ python -m interfaces.cli show_session <id> --detailed --trades")
        print("  $ python -m interfaces.cli performance --sort-by win_rate")
        print("  $ python -m interfaces.cli compare session1 session2 session3")
        print("  $ python -m interfaces.cli show_session <id> --export report.json")
        
    except KeyboardInterrupt:
        print("\n\n👋 Demo interrupted by user")
    except Exception as e:
        print(f"\n❌ Demo error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()