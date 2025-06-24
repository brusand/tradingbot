#!/usr/bin/env python3
"""
Script pour extraire toutes les commandes CLI du fichier trading_cli_v4.py
"""

import re

def extract_cli_commands(file_path):
    """Extrait toutes les commandes CLI avec leurs détails"""
    
    with open(file_path, 'r') as f:
        lines = f.readlines()
    
    commands = []
    
    # Rechercher tous les décorateurs @*.command
    for i, line in enumerate(lines):
        # Détecter les décorateurs de commandes
        command_match = re.search(r'@(\w+)\.command\([\'"](\w+)[\'"]\)', line.strip())
        
        if command_match:
            command_group = command_match.group(1)
            command_name = command_match.group(2)
            line_number = i + 1
            
            # Trouver la définition de fonction
            func_line = None
            func_name = None
            signature_lines = []
            
            # Chercher la fonction dans les lignes suivantes
            for j in range(i, min(i + 20, len(lines))):
                if lines[j].strip().startswith('def '):
                    func_match = re.search(r'def (\w+)\((.*?)\):', lines[j])
                    if func_match:
                        func_name = func_match.group(1)
                        func_line = j + 1
                        
                        # Extraire les premières lignes de la fonction
                        signature_lines = [lines[j].strip()]
                        
                        # Ajouter les lignes suivantes jusqu'à la docstring ou le premier code
                        for k in range(j + 1, min(j + 4, len(lines))):
                            if lines[k].strip():
                                if lines[k].strip().startswith('"""') or lines[k].strip().startswith("'''"):
                                    signature_lines.append(lines[k].strip())
                                    break
                                elif not lines[k].strip().startswith('@') and not lines[k].strip().startswith('def'):
                                    signature_lines.append(lines[k].strip())
                                    if len(signature_lines) >= 3:
                                        break
                        break
            
            if func_name:
                commands.append({
                    'line_number': line_number,
                    'command_group': command_group,
                    'command_name': command_name,
                    'function_name': func_name,
                    'function_line': func_line,
                    'signature': signature_lines
                })
    
    return commands

def main():
    file_path = "/archive_legacy/trading_cli_v4.py"
    commands = extract_cli_commands(file_path)
    
    print("📋 COMMANDES CLI DANS trading_cli_v4.py")
    print("=" * 80)
    
    # Grouper par type de commande
    groups = {}
    for cmd in commands:
        group = cmd['command_group']
        if group not in groups:
            groups[group] = []
        groups[group].append(cmd)
    
    # Trier les groupes dans un ordre logique
    group_order = ['cli', 'system', 'session', 'strategy', 'analytics', 'performance', 'test']
    
    for group_name in group_order:
        if group_name in groups:
            group_commands = groups[group_name]
            print(f"\n🔧 Groupe: {group_name.upper()}")
            print("-" * 50)
            
            for cmd in group_commands:
                print(f"✓ Ligne {cmd['line_number']:4d}: @{cmd['command_group']}.command('{cmd['command_name']}')")
                print(f"   Fonction: {cmd['function_name']}() - Ligne {cmd['function_line']}")
                
                if cmd['signature']:
                    print(f"   Signature:")
                    for i, sig_line in enumerate(cmd['signature'][:3]):
                        prefix = "      " if i > 0 else "      "
                        print(f"{prefix}{sig_line}")
                print()
    
    print(f"\n📊 RÉSUMÉ:")
    print(f"Total des commandes: {len(commands)}")
    for group_name in group_order:
        if group_name in groups:
            print(f"  {group_name}: {len(groups[group_name])} commandes")

if __name__ == "__main__":
    main()