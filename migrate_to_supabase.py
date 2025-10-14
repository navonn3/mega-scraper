# -*- coding: utf-8 -*-
"""
Migration Script - CSV to Supabase
===================================
מעביר את כל הנתונים הקיימים מ-CSV ל-Supabase
"""

import os
import pandas as pd
from supabase_uploader import SupabaseUploader
from config import LEAGUES
from utils import log_message

class DataMigration:
    """מחלקה להעברת נתונים"""
    
    def __init__(self):
        """אתחול"""
        self.uploader = SupabaseUploader()
        self.stats = {
            'leagues': 0,
            'teams': 0,
            'players': 0,
            'history': 0,
            'games': 0,
            'quarters': 0,
            'player_stats': 0,
            'team_stats': 0,
            'player_averages': 0,
            'team_averages': 0,
            'opponent_averages': 0
        }
    
    def migrate_all(self, league_ids=None):
        """
        העברת כל הנתונים
        
        Args:
            league_ids: רשימת league_ids להעברה (None = כולם)
        """
        log_message("="*60)
        log_message("🚀 STARTING MIGRATION TO SUPABASE")
        log_message("="*60)
        
        # Test connection
        if not self.uploader.test_connection():
            log_message("❌ Cannot connect to Supabase!")
            return False
        
        # Get leagues to migrate
        if league_ids is None:
            leagues_to_migrate = {lid: cfg for lid, cfg in LEAGUES.items() if cfg.get('active', False)}
        else:
            leagues_to_migrate = {lid: cfg for lid, cfg in LEAGUES.items() if lid in league_ids}
        
        log_message(f"📊 Migrating {len(leagues_to_migrate)} leagues")
        log_message("")
        
        # Migrate global data first
        self._migrate_global_leagues()
        self._migrate_global_teams()
        self._migrate_global_players()
        
        # Migrate each league
        for league_id, config in leagues_to_migrate.items():
            log_message("="*60)
            log_message(f"[{config['code'].upper()}] Migrating: {config['name']}")
            log_message("="*60)
            
            self._migrate_league_data(league_id, config)
        
        # Print stats
        self._print_stats()
        
        log_message("="*60)
        log_message("✅ MIGRATION COMPLETED!")
        log_message("="*60)
        
        return True
    
    def _migrate_global_leagues(self):
        """העברת טבלת leagues"""
        log_message("📋 Migrating leagues...")
        
        leagues_file = 'data/leagues.csv'
        if not os.path.exists(leagues_file):
            log_message("⚠️  leagues.csv not found, creating from config")
            # Create from config
            for league_id, config in LEAGUES.items():
                if config.get('active', False):
                    league_data = {
                        'league_id': int(league_id),
                        'name': config['name'],
                        'name_en': config.get('name_en', ''),
                        'country': config.get('country', 'Israel'),
                        'season': config.get('season', ''),
                        'url': config['url'],
                        'is_active': True
                    }
                    self.uploader.upsert_league(league_data)
                    self.stats['leagues'] += 1
        else:
            df = pd.read_csv(leagues_file, encoding='utf-8-sig')
            for _, row in df.iterrows():
                league_data = {
                    'league_id': int(row['league_id']),
                    'name': row['name'],
                    'name_en': row.get('name_en', ''),
                    'country': row.get('country', 'Israel'),
                    'season': row.get('season', ''),
                    'url': row.get('url', ''),
                    'is_active': bool(row.get('is_active', False))
                }
                self.uploader.upsert_league(league_data)
                self.stats['leagues'] += 1
        
        log_message(f"✅ Migrated {self.stats['leagues']} leagues")
    
    def _migrate_global_teams(self):
        """העברת טבלת teams"""
        log_message("🏀 Migrating teams...")
        
        teams_file = 'data/teams.csv'
        if not os.path.exists(teams_file):
            log_message("⚠️  teams.csv not found, skipping")
            return
        
        df = pd.read_csv(teams_file, encoding='utf-8-sig')
        teams_data = []
        
        for _, row in df.iterrows():
            # Skip invalid teams
            if pd.isna(row['Team_ID']) or pd.isna(row['League_ID']):
                continue
            if int(row['League_ID']) == 0:
                continue
            
            teams_data.append({
                'team_id': int(row['Team_ID']),
                'league_id': int(row['League_ID']),
                'team_name': row['Team_Name'],
                'short_name': row.get('short_name', row['Team_Name']),
                'bg_color': row.get('bg_color', '#000000'),
                'text_color': row.get('text_color', '#FFFFFF'),
                'name_variations': row.get('name_variations', '')
            })
        
        if teams_data:
            self.uploader.upsert_teams(teams_data)
            self.stats['teams'] = len(teams_data)
        
        log_message(f"✅ Migrated {self.stats['teams']} teams")
    
    def _migrate_global_players(self):
        """העברת טבלת players"""
        log_message("👤 Migrating players...")
        
        players_file = 'data/players.csv'
        if not os.path.exists(players_file):
            log_message("⚠️  players.csv not found, skipping")
            return
        
        df = pd.read_csv(players_file, encoding='utf-8-sig')
        players_data = []
        
        for _, row in df.iterrows():
            if pd.isna(row['player_id']):
                continue
            
            players_data.append({
                'player_id': str(row['player_id']),
                'name': row['name'],
                'current_team_id': int(row['current_team_id']) if pd.notna(row['current_team_id']) else None,
                'league_id': int(row['league_id']),
                'date_of_birth': row['date_of_birth'] if pd.notna(row['date_of_birth']) else None,
                'height': float(row['height']) if pd.notna(row['height']) else None,
                'jersey_number': int(row['jersey_number']) if pd.notna(row['jersey_number']) else None
            })
        
        if players_data:
            self.uploader.upsert_players(players_data)
            self.stats['players'] = len(players_data)
        
        log_message(f"✅ Migrated {self.stats['players']} players")
    
    def _migrate_league_data(self, league_id, config):
        """העברת נתוני ליגה ספציפית"""
        league_code = config['code']
        data_folder = config['data_folder']
        games_folder = config['games_folder']
        
        # Player details (if not in global file)
        self._migrate_player_details(league_id, league_code, data_folder)
        
        # Player history
        self._migrate_player_history(league_id, league_code, data_folder)
        
        # Games
        self._migrate_games(league_id, games_folder)
        
        # Game quarters
        self._migrate_game_quarters(league_id, games_folder)
        
        # Game player stats
        self._migrate_game_player_stats(league_id, games_folder)
        
        # Game team stats
        self._migrate_game_team_stats(league_id, games_folder)
        
        # Averages
        self._migrate_player_averages(league_id, league_code, data_folder)
        self._migrate_team_averages(league_id, league_code, data_folder)
        self._migrate_opponent_averages(league_id, league_code, data_folder)
    
    def _migrate_player_details(self, league_id, league_code, data_folder):
        """העברת פרטי שחקנים"""
        file_path = os.path.join(data_folder, f"{league_code}_player_details.csv")
        if not os.path.exists(file_path):
            return
        
        log_message(f"  📝 Player details...")
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        
        players_data = []
        for _, row in df.iterrows():
            players_data.append({
                'player_id': str(row['player_id']),
                'name': row['Name'],
                'current_team_id': int(row['team_id']) if pd.notna(row['team_id']) else None,
                'league_id': int(league_id),
                'date_of_birth': row['Date Of Birth'] if pd.notna(row['Date Of Birth']) else None,
                'height': float(row['Height']) if pd.notna(row['Height']) else None,
                'jersey_number': int(row['Number']) if pd.notna(row['Number']) else None
            })
        
        if players_data:
            self.uploader.upsert_players(players_data)
            log_message(f"  ✅ {len(players_data)} players")
    
    def _migrate_player_history(self, league_id, league_code, data_folder):
        """העברת היסטוריית שחקנים"""
        file_path = os.path.join(data_folder, f"{league_code}_player_history.csv")
        if not os.path.exists(file_path):
            return
        
        log_message(f"  📚 Player history...")
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        
        history_data = []
        for _, row in df.iterrows():
            player_id = str(row['player_id'])
            
            # Extract seasons from columns
            for col in df.columns:
                if col not in ['player_id', 'Name', 'Current Team', 'team_id', 'league_id',
                               'Date Of Birth', 'Height', 'Number']:
                    season = col
                    if pd.notna(row[col]) and str(row[col]).strip():
                        team_league = str(row[col])
                        team_name = team_league.split('(')[0].strip() if '(' in team_league else team_league
                        league_name = team_league.split('(')[1].replace(')', '').strip() if '(' in team_league else ''
                        
                        history_data.append({
                            'player_id': player_id,
                            'season': season,
                            'team_name': team_name,
                            'league_name': league_name,
                            'league_id': int(league_id)
                        })
        
        if history_data:
            self.uploader.upsert_player_history(history_data)
            self.stats['history'] += len(history_data)
            log_message(f"  ✅ {len(history_data)} history records")
    
    def _migrate_games(self, league_id, games_folder):
        """העברת משחקים"""
        file_path = os.path.join(games_folder, 'games_schedule.csv')
        if not os.path.exists(file_path):
            return
        
        log_message(f"  🏆 Games...")
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        
        games_data = []
        for _, row in df.iterrows():
            games_data.append({
                'game_id': str(row['gameid']),
                'league_id': int(league_id),
                'code': str(row['Code']) if pd.notna(row.get('Code')) else None,
                'week_day': row.get('Week Day', ''),
                'date': row['Date'] if pd.notna(row['Date']) else None,
                'round': str(row['Round']) if pd.notna(row.get('Round')) else None,
                'time': row.get('Time', ''),
                'home_team': row['Home Team'],
                'home_team_code': row.get('Home Team Code', ''),
                'home_team_id': int(row['home_team_id']) if pd.notna(row.get('home_team_id')) else None,
                'away_team': row['Away Team'],
                'away_team_code': row.get('Away Team Code', ''),
                'away_team_id': int(row['away_team_id']) if pd.notna(row.get('away_team_id')) else None,
                'venue': row.get('Venue', ''),
                'home_score': int(row['Home Score']) if pd.notna(row.get('Home Score')) and str(row['Home Score']).replace('.','').isdigit() else None,
                'away_score': int(row['Away Score']) if pd.notna(row.get('Away Score')) and str(row['Away Score']).replace('.','').isdigit() else None,
                'arena': row.get('Arena', ''),
                'status': 'completed' if pd.notna(row.get('Home Score')) and str(row.get('Home Score')).strip() != '' else 'scheduled'
            })
        
        if games_data:
            self.uploader.upsert_games(games_data)
            self.stats['games'] += len(games_data)
            log_message(f"  ✅ {len(games_data)} games")
    
    def _migrate_game_quarters(self, league_id, games_folder):
        """העברת רבעי משחק"""
        file_path = os.path.join(games_folder, 'game_quarters.csv')
        if not os.path.exists(file_path):
            return
        
        log_message(f"  🔢 Quarters...")
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        
        quarters_data = df.to_dict('records')
        
        if quarters_data:
            self.uploader.upsert_game_quarters(quarters_data)
            self.stats['quarters'] += len(quarters_data)
            log_message(f"  ✅ {len(quarters_data)} quarters")
    
    def _migrate_game_player_stats(self, league_id, games_folder):
        """העברת סטטיסטיקות שחקן במשחק"""
        file_path = os.path.join(games_folder, 'game_player_stats.csv')
        if not os.path.exists(file_path):
            return
        
        log_message(f"  📊 Player stats...")
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        
        stats_data = df.to_dict('records')
        
        if stats_data:
            self.uploader.upsert_game_player_stats(stats_data)
            self.stats['player_stats'] += len(stats_data)
            log_message(f"  ✅ {len(stats_data)} player stats")
    
    def _migrate_game_team_stats(self, league_id, games_folder):
        """העברת סטטיסטיקות קבוצה במשחק"""
        file_path = os.path.join(games_folder, 'game_team_stats.csv')
        if not os.path.exists(file_path):
            return
        
        log_message(f"  📈 Team stats...")
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        
        stats_data = df.to_dict('records')
        
        if stats_data:
            self.uploader.upsert_game_team_stats(stats_data)
            self.stats['team_stats'] += len(stats_data)
            log_message(f"  ✅ {len(stats_data)} team stats")
    
    def _migrate_player_averages(self, league_id, league_code, data_folder):
        """העברת ממוצעי שחקנים"""
        file_path = os.path.join(data_folder, f"{league_code}_player_averages.csv")
        if not os.path.exists(file_path):
            return
        
        log_message(f"  📉 Player averages...")
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        
        avg_data = df.to_dict('records')
        
        if avg_data:
            self.uploader.upsert_player_averages(avg_data)
            self.stats['player_averages'] += len(avg_data)
            log_message(f"  ✅ {len(avg_data)} player averages")
    
    def _migrate_team_averages(self, league_id, league_code, data_folder):
        """העברת ממוצעי קבוצות"""
        file_path = os.path.join(data_folder, f"{league_code}_team_averages.csv")
        if not os.path.exists(file_path):
            return
        
        log_message(f"  📊 Team averages...")
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        
        avg_data = df.to_dict('records')
        
        if avg_data:
            self.uploader.upsert_team_averages(avg_data)
            self.stats['team_averages'] += len(avg_data)
            log_message(f"  ✅ {len(avg_data)} team averages")
    
    def _migrate_opponent_averages(self, league_id, league_code, data_folder):
        """העברת ממוצעי יריבים"""
        file_path = os.path.join(data_folder, f"{league_code}_opponent_averages.csv")
        if not os.path.exists(file_path):
            return
        
        log_message(f"  🛡️ Opponent averages...")
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        
        avg_data = df.to_dict('records')
        
        if avg_data:
            self.uploader.upsert_opponent_averages(avg_data)
            self.stats['opponent_averages'] += len(avg_data)
            log_message(f"  ✅ {len(avg_data)} opponent averages")
    
    def _print_stats(self):
        """הדפסת סטטיסטיקות"""
        log_message("")
        log_message("="*60)
        log_message("📊 MIGRATION STATISTICS")
        log_message("="*60)
        
        for key, value in self.stats.items():
            if value > 0:
                log_message(f"  {key.replace('_', ' ').title()}: {value:,}")
        
        total = sum(self.stats.values())
        log_message("")
        log_message(f"  TOTAL RECORDS: {total:,}")
        log_message("="*60)


# ============================================
# MAIN EXECUTION
# ============================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Migrate CSV data to Supabase')
    parser.add_argument('--league', type=str, help='Specific league ID to migrate')
    parser.add_argument('--all', action='store_true', help='Migrate all active leagues')
    parser.add_argument('--test', action='store_true', help='Test connection only')
    
    args = parser.parse_args()
    
    migrator = DataMigration()
    
    if args.test:
        # Test connection
        log_message("🧪 Testing Supabase connection...")
        if migrator.uploader.test_connection():
            log_message("✅ Connection successful!")
        else:
            log_message("❌ Connection failed!")
    
    elif args.all or args.league:
        # Migrate data
        league_ids = [args.league] if args.league else None
        migrator.migrate_all(league_ids)
    
    else:
        # Show help
        parser.print_help()
        print("\nExamples:")
        print("  python migrate_to_supabase.py --test              # Test connection")
        print("  python migrate_to_supabase.py --all               # Migrate all leagues")
        print("  python migrate_to_supabase.py --league 1          # Migrate league 1 only")