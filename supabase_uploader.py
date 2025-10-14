# -*- coding: utf-8 -*-
"""
Supabase Uploader
=================
מעלה נתונים מהסקריפט ישירות ל-Supabase
"""

import os
from supabase import create_client, Client
from dotenv import load_dotenv
from typing import List, Dict, Any
from utils import log_message

# Load environment variables
load_dotenv()

class SupabaseUploader:
    """מחלקה להעלאת נתונים ל-Supabase"""
    
    def __init__(self):
        """אתחול חיבור ל-Supabase"""
        supabase_url = os.getenv('SUPABASE_URL')
        supabase_key = os.getenv('SUPABASE_KEY')
        
        if not supabase_url or not supabase_key:
            raise ValueError(
                "Missing Supabase credentials!\n"
                "Please set SUPABASE_URL and SUPABASE_KEY in .env file"
            )
        
        self.client: Client = create_client(supabase_url, supabase_key)
        log_message("✅ Connected to Supabase")
    
    # ============================================
    # LEAGUES
    # ============================================
    
    def upsert_league(self, league_data: Dict[str, Any]) -> bool:
        """
        העלאה/עדכון ליגה
        
        Args:
            league_data: מילון עם נתוני ליגה
        
        Returns:
            bool: הצלחה/כישלון
        """
        try:
            response = self.client.table('leagues').upsert(
                league_data,
                on_conflict='league_id'
            ).execute()
            
            log_message(f"✅ League upserted: {league_data.get('name')}")
            return True
            
        except Exception as e:
            log_message(f"❌ Error upserting league: {e}")
            return False
    
    # ============================================
    # TEAMS
    # ============================================
    
    def upsert_teams(self, teams_data: List[Dict[str, Any]]) -> bool:
        """
        העלאה/עדכון קבוצות (batch)
        
        Args:
            teams_data: רשימת מילונים עם נתוני קבוצות
        
        Returns:
            bool: הצלחה/כישלון
        """
        if not teams_data:
            return True
        
        try:
            response = self.client.table('teams').upsert(
                teams_data,
                on_conflict='team_id,league_id'
            ).execute()
            
            log_message(f"✅ Upserted {len(teams_data)} teams")
            return True
            
        except Exception as e:
            log_message(f"❌ Error upserting teams: {e}")
            return False
    
    # ============================================
    # PLAYERS
    # ============================================
    
    def upsert_players(self, players_data: List[Dict[str, Any]]) -> bool:
        """
        העלאה/עדכון שחקנים (batch)
        
        Args:
            players_data: רשימת מילונים עם נתוני שחקנים
        
        Returns:
            bool: הצלחה/כישלון
        """
        if not players_data:
            return True
        
        try:
            # Clean data - remove None values in foreign keys
            for player in players_data:
                if player.get('current_team_id') is None:
                    player.pop('current_team_id', None)
            
            response = self.client.table('players').upsert(
                players_data,
                on_conflict='player_id'
            ).execute()
            
            log_message(f"✅ Upserted {len(players_data)} players")
            return True
            
        except Exception as e:
            log_message(f"❌ Error upserting players: {e}")
            return False
    
    # ============================================
    # PLAYER SEASON HISTORY
    # ============================================
    
    def upsert_player_history(self, history_data: List[Dict[str, Any]]) -> bool:
        """
        העלאה/עדכון היסטוריית שחקנים (batch)
        
        Args:
            history_data: רשימת מילונים עם היסטוריה
        
        Returns:
            bool: הצלחה/כישלון
        """
        if not history_data:
            return True
        
        try:
            response = self.client.table('player_season_history').upsert(
                history_data,
                on_conflict='player_id,season'
            ).execute()
            
            log_message(f"✅ Upserted {len(history_data)} history records")
            return True
            
        except Exception as e:
            log_message(f"❌ Error upserting player history: {e}")
            return False
    
    # ============================================
    # PLAYER AVERAGES
    # ============================================
    
    def upsert_player_averages(self, averages_data: List[Dict[str, Any]]) -> bool:
        """
        העלאה/עדכון ממוצעי שחקנים (batch)
        
        Args:
            averages_data: רשימת מילונים עם ממוצעים
        
        Returns:
            bool: הצלחה/כישלון
        """
        if not averages_data:
            return True
        
        try:
            response = self.client.table('player_averages').upsert(
                averages_data,
                on_conflict='player_id,league_id'
            ).execute()
            
            log_message(f"✅ Upserted {len(averages_data)} player averages")
            return True
            
        except Exception as e:
            log_message(f"❌ Error upserting player averages: {e}")
            return False
    
    # ============================================
    # TEAM AVERAGES
    # ============================================
    
    def upsert_team_averages(self, averages_data: List[Dict[str, Any]]) -> bool:
        """
        העלאה/עדכון ממוצעי קבוצות (batch)
        
        Args:
            averages_data: רשימת מילונים עם ממוצעים
        
        Returns:
            bool: הצלחה/כישלון
        """
        if not averages_data:
            return True
        
        try:
            response = self.client.table('team_averages').upsert(
                averages_data,
                on_conflict='team_id,league_id'
            ).execute()
            
            log_message(f"✅ Upserted {len(averages_data)} team averages")
            return True
            
        except Exception as e:
            log_message(f"❌ Error upserting team averages: {e}")
            return False
    
    # ============================================
    # OPPONENT AVERAGES
    # ============================================
    
    def upsert_opponent_averages(self, averages_data: List[Dict[str, Any]]) -> bool:
        """
        העלאה/עדכון ממוצעי יריבים (batch)
        
        Args:
            averages_data: רשימת מילונים עם ממוצעים
        
        Returns:
            bool: הצלחה/כישלון
        """
        if not averages_data:
            return True
        
        try:
            response = self.client.table('opponent_averages').upsert(
                averages_data,
                on_conflict='team_id,league_id'
            ).execute()
            
            log_message(f"✅ Upserted {len(averages_data)} opponent averages")
            return True
            
        except Exception as e:
            log_message(f"❌ Error upserting opponent averages: {e}")
            return False
    
    # ============================================
    # GAMES
    # ============================================
    
    def upsert_games(self, games_data: List[Dict[str, Any]]) -> bool:
        """
        העלאה/עדכון משחקים (batch)
        
        Args:
            games_data: רשימת מילונים עם משחקים
        
        Returns:
            bool: הצלחה/כישלון
        """
        if not games_data:
            return True
        
        try:
            # Convert date format if needed (DD/MM/YYYY → YYYY-MM-DD)
            for game in games_data:
                if 'date' in game and game['date']:
                    date_str = game['date']
                    if '/' in date_str:
                        parts = date_str.split('/')
                        if len(parts) == 3:
                            game['date'] = f"{parts[2]}-{parts[1].zfill(2)}-{parts[0].zfill(2)}"
            
            response = self.client.table('games').upsert(
                games_data,
                on_conflict='game_id'
            ).execute()
            
            log_message(f"✅ Upserted {len(games_data)} games")
            return True
            
        except Exception as e:
            log_message(f"❌ Error upserting games: {e}")
            return False
    
    # ============================================
    # GAME PLAYER STATS
    # ============================================
    
    def upsert_game_player_stats(self, stats_data: List[Dict[str, Any]]) -> bool:
        """
        העלאה/עדכון סטטיסטיקות שחקן במשחק (batch)
        
        Args:
            stats_data: רשימת מילונים עם סטטיסטיקות
        
        Returns:
            bool: הצלחה/כישלון
        """
        if not stats_data:
            return True
        
        try:
            # Convert date format if needed
            for stat in stats_data:
                if 'game_date' in stat and stat['game_date']:
                    date_str = stat['game_date']
                    if '/' in date_str:
                        parts = date_str.split('/')
                        if len(parts) == 3:
                            stat['game_date'] = f"{parts[2]}-{parts[1].zfill(2)}-{parts[0].zfill(2)}"
            
            # Batch upload in chunks of 500 (Supabase limit)
            chunk_size = 500
            for i in range(0, len(stats_data), chunk_size):
                chunk = stats_data[i:i + chunk_size]
                response = self.client.table('game_player_stats').upsert(
                    chunk,
                    on_conflict='game_id,player_id'
                ).execute()
            
            log_message(f"✅ Upserted {len(stats_data)} player stats")
            return True
            
        except Exception as e:
            log_message(f"❌ Error upserting player stats: {e}")
            return False
    
    # ============================================
    # GAME TEAM STATS
    # ============================================
    
    def upsert_game_team_stats(self, stats_data: List[Dict[str, Any]]) -> bool:
        """
        העלאה/עדכון סטטיסטיקות קבוצה במשחק (batch)
        
        Args:
            stats_data: רשימת מילונים עם סטטיסטיקות
        
        Returns:
            bool: הצלחה/כישלון
        """
        if not stats_data:
            return True
        
        try:
            # Convert date format if needed
            for stat in stats_data:
                if 'game_date' in stat and stat['game_date']:
                    date_str = stat['game_date']
                    if '/' in date_str:
                        parts = date_str.split('/')
                        if len(parts) == 3:
                            stat['game_date'] = f"{parts[2]}-{parts[1].zfill(2)}-{parts[0].zfill(2)}"
            
            response = self.client.table('game_team_stats').upsert(
                stats_data,
                on_conflict='game_id,team_id'
            ).execute()
            
            log_message(f"✅ Upserted {len(stats_data)} team stats")
            return True
            
        except Exception as e:
            log_message(f"❌ Error upserting team stats: {e}")
            return False
    
    # ============================================
    # GAME QUARTERS
    # ============================================
    
    def upsert_game_quarters(self, quarters_data: List[Dict[str, Any]]) -> bool:
        """
        העלאה/עדכון רבעי משחק (batch)
        
        Args:
            quarters_data: רשימת מילונים עם רבעים
        
        Returns:
            bool: הצלחה/כישלון
        """
        if not quarters_data:
            return True
        
        try:
            # Convert date format and quarter names
            for quarter in quarters_data:
                if 'game_date' in quarter and quarter['game_date']:
                    date_str = quarter['game_date']
                    if '/' in date_str:
                        parts = date_str.split('/')
                        if len(parts) == 3:
                            quarter['game_date'] = f"{parts[2]}-{parts[1].zfill(2)}-{parts[0].zfill(2)}"
                
                # Convert quarter from "Q1" to 1
                if 'quarter' in quarter and isinstance(quarter['quarter'], str):
                    quarter['quarter'] = int(quarter['quarter'].replace('Q', ''))
            
            response = self.client.table('game_quarters').upsert(
                quarters_data,
                on_conflict='game_id,team_id,quarter'
            ).execute()
            
            log_message(f"✅ Upserted {len(quarters_data)} quarters")
            return True
            
        except Exception as e:
            log_message(f"❌ Error upserting quarters: {e}")
            return False
    
    # ============================================
    # HELPER METHODS
    # ============================================
    
    def clean_numeric_fields(self, data: Dict[str, Any], numeric_fields: List[str]) -> Dict[str, Any]:
        """
        ניקוי שדות מספריים - המרה ל-None במקום NaN
        
        Args:
            data: מילון נתונים
            numeric_fields: רשימת שמות שדות מספריים
        
        Returns:
            מילון מנוקה
        """
        import pandas as pd
        
        for field in numeric_fields:
            if field in data:
                value = data[field]
                if pd.isna(value) or value == '' or value == 'nan':
                    data[field] = None
        
        return data
    
    def test_connection(self) -> bool:
        """בדיקת חיבור ל-Supabase"""
        try:
            response = self.client.table('leagues').select('league_id').limit(1).execute()
            log_message("✅ Supabase connection test successful")
            return True
        except Exception as e:
            log_message(f"❌ Supabase connection test failed: {e}")
            return False
    
    def get_existing_game_ids(self, league_id: int) -> set:
        """
        קבלת רשימת game_ids קיימים לליגה
        
        Args:
            league_id: מזהה ליגה
        
        Returns:
            set של game_ids
        """
        try:
            response = self.client.table('games')\
                .select('game_id')\
                .eq('league_id', league_id)\
                .execute()
            
            return set(game['game_id'] for game in response.data)
        except Exception as e:
            log_message(f"❌ Error getting existing games: {e}")
            return set()
    
    def get_existing_player_ids(self, league_id: int) -> set:
        """
        קבלת רשימת player_ids קיימים לליגה
        
        Args:
            league_id: מזהה ליגה
        
        Returns:
            set של player_ids
        """
        try:
            response = self.client.table('players')\
                .select('player_id')\
                .eq('league_id', league_id)\
                .execute()
            
            return set(player['player_id'] for player in response.data)
        except Exception as e:
            log_message(f"❌ Error getting existing players: {e}")
            return set()


# ============================================
# USAGE EXAMPLE
# ============================================

def example_usage():
    """דוגמה לשימוש"""
    
    # Initialize uploader
    uploader = SupabaseUploader()
    
    # Test connection
    if not uploader.test_connection():
        print("Failed to connect to Supabase!")
        return
    
    # Upload a league
    league_data = {
        'league_id': 1,
        'name': 'ליגה לאומית',
        'name_en': 'National League',
        'country': 'Israel',
        'season': '2024-25',
        'url': 'https://ibasketball.co.il/league/...',
        'is_active': True
    }
    uploader.upsert_league(league_data)
    
    # Upload teams
    teams_data = [
        {
            'team_id': 5,
            'league_id': 1,
            'team_name': 'מכבי חיפה',
            'short_name': 'מכבי חיפה',
            'bg_color': '#009900',
            'text_color': '#FFFFFF',
            'name_variations': 'מכבי חיפה גיא נתן|מכבי חיפה'
        }
    ]
    uploader.upsert_teams(teams_data)
    
    # Upload players
    players_data = [
        {
            'player_id': 'abc123',
            'name': 'שחקן דוגמה',
            'current_team_id': 5,
            'league_id': 1,
            'date_of_birth': '01/01/1995',
            'height': 1.95,
            'jersey_number': 10
        }
    ]
    uploader.upsert_players(players_data)
    
    print("✅ Example upload completed!")


if __name__ == "__main__":
    example_usage()