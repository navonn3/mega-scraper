# -*- coding: utf-8 -*-
"""
JSON Helpers
============
פונקציות עזר לשמירה וטעינה של JSON במקום CSV
"""

import json
from pathlib import Path
from datetime import datetime


# ============================================
# PLAYER FILES
# ============================================

def save_player_details(player_id, details):
    """שמור פרטי שחקן לקובץ JSON"""
    player_folder = Path('data/players') / player_id
    player_folder.mkdir(parents=True, exist_ok=True)
    
    details['last_updated'] = datetime.now().isoformat()
    
    file_path = player_folder / 'details.json'
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(details, f, ensure_ascii=False, indent=2)


def save_player_history(player_id, history):
    """שמור היסטוריה של שחקן"""
    player_folder = Path('data/players') / player_id
    player_folder.mkdir(parents=True, exist_ok=True)
    
    history['last_updated'] = datetime.now().isoformat()
    
    file_path = player_folder / 'history.json'
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(history, f, ensure_ascii=False, indent=2)


def load_player_details(player_id):
    """טען פרטי שחקן"""
    file_path = Path('data/players') / player_id / 'details.json'
    
    if not file_path.exists():
        return None
    
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def load_player_history(player_id):
    """טען היסטוריה של שחקן"""
    file_path = Path('data/players') / player_id / 'history.json'
    
    if not file_path.exists():
        return None
    
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def player_exists(player_id):
    """בדוק אם שחקן קיים"""
    return (Path('data/players') / player_id / 'details.json').exists()


# ============================================
# GAME FILES
# ============================================

def save_game(game_data, season):
    """שמור משחק לקובץ JSON"""
    game_id = game_data['game_id']
    
    games_folder = Path('data/games') / season
    games_folder.mkdir(parents=True, exist_ok=True)
    
    game_data['scraped_at'] = datetime.now().isoformat()
    
    file_path = games_folder / f"{game_id}.json"
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(game_data, f, ensure_ascii=False, indent=2)


def load_game(game_id, season):
    """טען משחק"""
    file_path = Path('data/games') / season / f"{game_id}.json"
    
    if not file_path.exists():
        return None
    
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def game_exists(game_id, season):
    """בדוק אם משחק קיים"""
    return (Path('data/games') / season / f"{game_id}.json").exists()


# ============================================
# MIGRATION: CSV → JSON
# ============================================

def migrate_csv_to_json(league_code):
    """המרת CSV קיים ל-JSON"""
    import pandas as pd
    
    print(f"🔄 Migrating {league_code} from CSV to JSON...")
    
    # Migrate players
    details_csv = Path(f'data/{league_code}/{league_code}_player_details.csv')
    if details_csv.exists():
        df = pd.read_csv(details_csv, encoding='utf-8-sig')
        
        for _, row in df.iterrows():
            player_id = row['player_id']
            
            details = {
                'player_id': player_id,
                'name': row['Name'],
                'current_team_id': row.get('team_id'),
                'league_id': row['league_id'],
                'date_of_birth': row.get('Date Of Birth', ''),
                'height': row.get('Height', ''),
                'jersey_number': row.get('Number', '')
            }
            
            save_player_details(player_id, details)
        
        print(f"✅ Migrated {len(df)} players")
    
    print("✅ Migration complete!")