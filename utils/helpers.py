# -*- coding: utf-8 -*-
"""
Helper Functions - FINAL FIXED
===============================
פונקציות עזר: עם מפתח מורכב (variation, league_id)
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
from datetime import datetime
from pathlib import Path

LOG_FILE = "logs/update_log.txt"

def log_message(message, league_id=None):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if league_id:
        log_entry = f"[{timestamp}] [{league_id.upper()}] {message}"
    else:
        log_entry = f"[{timestamp}] {message}"
    print(log_entry)
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(log_entry + "\n")

def get_soup(url, timeout=10):
    try:
        response = requests.get(url, timeout=timeout)
        response.encoding = 'utf-8'
        return BeautifulSoup(response.content, 'html.parser')
    except Exception as e:
        log_message(f"❌ Error fetching {url}: {e}")
        return None

def save_to_csv(data, filepath, columns=None):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    df = pd.DataFrame(data)
    df = df.dropna(axis=1, how='all')
    for col in ['team_id', 'league_id', 'opponent_id']:
        if col in df.columns:
            df[col] = df[col].astype('Int64')
    if columns:
        existing_cols = [col for col in columns if col in df.columns]
        extra_cols = [col for col in df.columns if col not in columns]
        df = df[existing_cols + extra_cols]
    df.to_csv(filepath, index=False, encoding='utf-8-sig')

def append_to_csv(new_data, filepath, columns=None):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    df_new = pd.DataFrame(new_data)
    df_new = df_new.dropna(axis=1, how='all')
    if os.path.exists(filepath):
        try:
            df_existing = pd.read_csv(filepath, encoding='utf-8-sig')
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        except Exception as e:
            log_message(f"⚠️  Could not read existing file, creating new: {e}")
            df_combined = df_new
    else:
        df_combined = df_new
    for col in ['team_id', 'league_id', 'opponent_id']:
        if col in df_combined.columns:
            df_combined[col] = df_combined[col].astype('Int64')
    if columns:
        existing_cols = [col for col in columns if col in df_combined.columns]
        other_cols = [col for col in df_combined.columns if col not in columns]
        df_combined = df_combined[existing_cols + other_cols]
    df_combined.to_csv(filepath, index=False, encoding='utf-8-sig')

def load_csv_as_dict(filepath, key_column):
    if not os.path.exists(filepath):
        return {}
    try:
        df = pd.read_csv(filepath, encoding='utf-8-sig')
        result = {}
        for _, row in df.iterrows():
            key = row[key_column]
            result[key] = row.to_dict()
        return result
    except Exception as e:
        log_message(f"⚠️  Error loading {filepath}: {e}")
        return {}

# ============================================
# GLOBAL NORMALIZATION - WITH COMPOSITE KEY
# ============================================

def load_global_team_mapping():
    """
    טעינת מיפוי קבוצות גלובלי
    ⭐ משתמש במפתח מורכב: (variation, league_id)
    """
    teams_file = "data/teams.csv"
    
    try:
        if not os.path.exists(teams_file):
            log_message(f"⚠️  Global teams mapping file not found: {teams_file}")
            return {}
        
        df = pd.read_csv(teams_file, encoding='utf-8-sig')
        columns = df.columns.tolist()
        
        # זיהוי עמודות
        column_mapping = {
            'team_id': ['Team_ID', 'team_id', 'TeamID'],
            'league_id': ['League_ID', 'league_id', 'LeagueID'],
            'club_name': ['Team_Name', 'club_name', 'team_name', 'name'],
            'short_name': ['short_name', 'ShortName'],
            'variations': ['name_variations', 'variations', 'Variations'],
            'bg_color': ['bg_color', 'BgColor'],
            'text_color': ['text_color', 'TextColor']
        }
        
        actual_columns = {}
        for key, possible_names in column_mapping.items():
            for name in possible_names:
                if name in columns:
                    actual_columns[key] = name
                    break
        
        # יצירת מיפוי עם מפתח מורכב
        mapping = {}
        teams_count = 0
        
        for _, row in df.iterrows():
            team_id = row[actual_columns['team_id']] if 'team_id' in actual_columns else None
            league_id = row[actual_columns['league_id']] if 'league_id' in actual_columns else None
            club_name = row[actual_columns['club_name']] if 'club_name' in actual_columns else None
            
            if pd.isna(team_id) or pd.isna(league_id) or pd.isna(club_name):
                continue
            
            try:
                league_id_int = int(league_id)
                if league_id_int == 0:
                    continue
            except:
                continue
            
            teams_count += 1
            
            variations = row[actual_columns['variations']] if 'variations' in actual_columns else club_name
            
            import html
            club_name = html.unescape(str(club_name)).strip()
            variations_str = html.unescape(str(variations)).strip()
            
            variation_list = []
            for v in variations_str.split('|'):
                v_clean = html.unescape(v.strip())
                if v_clean:
                    variation_list.append(v_clean)
            
            team_info = {
                'team_id': int(team_id) if pd.notna(team_id) else None,
                'league_id': int(league_id_int),
                'club_name': club_name,
                'short_name': row[actual_columns['short_name']] if 'short_name' in actual_columns else club_name,
                'bg_color': row[actual_columns['bg_color']] if 'bg_color' in actual_columns else '#000000',
                'text_color': row[actual_columns['text_color']] if 'text_color' in actual_columns else '#FFFFFF',
                'all_variations': variation_list
            }
            
            # ⭐ מפתח מורכב: (variation, league_id)
            for variation in variation_list:
                if variation:
                    key = (variation, int(league_id_int))
                    if key not in mapping:
                        mapping[key] = team_info
        
        total_variations = len(mapping)
        log_message(f"✅ Loaded global team mapping: {teams_count} teams, {total_variations} name variations")
        
        # Debug מפורט
        league_1_keys = [k for k in mapping.keys() if k[1] == 1]
        if league_1_keys:
            unique_teams_l1 = len(set(mapping[k]['team_id'] for k in league_1_keys))
            log_message(f"   League 1: {unique_teams_l1} teams, {len(league_1_keys)} variations")
        
        return mapping
        
    except Exception as e:
        log_message(f"⚠️  Error loading global team mapping: {e}")
        import traceback
        log_message(traceback.format_exc())
        return {}


def normalize_team_name_global(team_name, league_id, team_mapping):
    """
    נרמול שם קבוצה
    ⭐ משתמש במפתח מורכב: (variation, league_id)
    """
    if not team_mapping:
        log_message(f"⚠️  No team mapping available")
        return {
            'team_id': None,
            'league_id': league_id,
            'club_name': team_name,
            'short_name': team_name,
            'bg_color': '#000000',
            'text_color': '#FFFFFF',
            'all_variations': [team_name]
        }
    
    league_id_int = int(league_id)
    
    # ⭐ חיפוש עם מפתח מורכב
    key = (team_name, league_id_int)
    if key in team_mapping:
        return team_mapping[key]
    
    # נסיון עם strip
    key_stripped = (team_name.strip(), league_id_int)
    if key_stripped in team_mapping:
        return team_mapping[key_stripped]
    
    # לא נמצא
    log_message(f"   ⚠️  No mapping found for: '{team_name}' in league {league_id}")
    
    return {
        'team_id': None,
        'league_id': league_id,
        'club_name': team_name,
        'short_name': team_name,
        'bg_color': '#000000',
        'text_color': '#FFFFFF',
        'all_variations': [team_name]
    }


# OLD FUNCTIONS - DEPRECATED
def load_team_mapping(team_mapping_file, league_id):
    log_message(f"⚠️  load_team_mapping() is deprecated", league_id)
    return load_global_team_mapping()

def normalize_team_name(team_name, team_mapping):
    if not team_mapping:
        return team_name
    # Compatibility layer
    for key, info in team_mapping.items():
        if isinstance(key, tuple):
            if key[0] == team_name:
                return info['club_name']
        else:
            if key == team_name:
                return info if isinstance(info, str) else info['club_name']
    return team_name

def ensure_directories(league_config):
    Path(league_config['data_folder']).mkdir(parents=True, exist_ok=True)
    Path(league_config['games_folder']).mkdir(parents=True, exist_ok=True)
    Path("logs").mkdir(exist_ok=True)