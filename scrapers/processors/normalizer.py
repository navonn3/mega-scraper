# -*- coding: utf-8 -*-
"""
Data Normalizer - UPDATED
==========================
נרמול נתונים: שמות קבוצות (כולל team_id), תאריכים, פורמטים
"""

from datetime import datetime
import pandas as pd

from utils import log_message, load_global_team_mapping, normalize_team_name_global


class DataNormalizer:
    """מחלקה לנרמול כל סוגי הנתונים"""
    
    def __init__(self, league_id, league_code):
        """
        Args:
            league_id: מזהה ליגה מספרי
            league_code: קוד ליגה (לlogים)
        """
        self.league_id = league_id
        self.league_code = league_code
        self.team_mapping = None
    
    def load_team_mapping(self):
        """טעינת מיפוי קבוצות גלובלי"""
        if self.team_mapping is None:
            self.team_mapping = load_global_team_mapping()
            
            if not self.team_mapping:
                log_message("⚠️  No team mapping loaded", self.league_code)
                return False
        
        return True
    
    def normalize_team_name(self, team_name_raw):
        """
        נרמול שם קבוצה
        מחזיר גם team_id מספרי מהמיפוי
        
        Args:
            team_name_raw: שם קבוצה מקורי מהאתר
        
        Returns:
            dict: {team_id, league_id, club_name, short_name, bg_color, text_color, all_variations}
        """
        if not self.team_mapping:
            self.load_team_mapping()
        
        # נרמול HTML entities (כמו &quot; → ")
        import html
        team_name_normalized = html.unescape(team_name_raw) if team_name_raw else team_name_raw
        
        return normalize_team_name_global(
            team_name_normalized,
            self.league_id,
            self.team_mapping
        )
    
    def normalize_date(self, date_str):
        """
        המרת תאריך לפורמט אחיד DD/MM/YYYY
        
        Args:
            date_str: תאריך בכל פורמט
        
        Returns:
            str: DD/MM/YYYY או None
        """
        if not date_str or pd.isna(date_str):
            return None
        
        date_str = str(date_str).strip()
        
        # אם כבר DD/MM/YYYY
        if '/' in date_str and len(date_str.split('/')) == 3:
            parts = date_str.split('/')
            if len(parts[0]) <= 2 and len(parts[2]) == 4:
                return date_str
        
        # אם עם מקפים
        if '-' in date_str:
            parts = date_str.split('-')
            if len(parts) == 3:
                # DD-MM-YYYY
                if len(parts[0]) <= 2 and len(parts[2]) == 4:
                    try:
                        dt = datetime.strptime(date_str, '%d-%m-%Y')
                        return dt.strftime('%d/%m/%Y')
                    except:
                        pass
                
                # YYYY-MM-DD
                if len(parts[0]) == 4:
                    try:
                        dt = datetime.strptime(date_str, '%Y-%m-%d')
                        return dt.strftime('%d/%m/%Y')
                    except:
                        pass
        
        # Excel serial number
        try:
            excel_date = float(date_str)
            dt = datetime(1899, 12, 30) + pd.Timedelta(days=excel_date)
            return dt.strftime('%d/%m/%Y')
        except:
            pass
        
        log_message(f"   ⚠️  Could not parse date: '{date_str}'", self.league_code)
        return None
    
    def normalize_schedule_dataframe(self, games_df):
        """
        נרמול DataFrame של לוח משחקים
        כולל שימוש ב-team_id מהמיפוי
        
        Args:
            games_df: DataFrame של משחקים
        
        Returns:
            DataFrame מנורמל
        """
        log_message("   Normalizing schedule teams...", self.league_code)
        
        # שינוי שמות עמודות לאנגלית
        column_renames = {
            'ליגה': 'League',
            'תאריך': 'Date',
            'מחזור': 'Round'
        }
        
        games_df = games_df.rename(columns={
            k: v for k, v in column_renames.items() 
            if k in games_df.columns
        })
        
        # נרמול Home Team (כולל team_id)
        if 'Home Team' in games_df.columns:
            def normalize_home(team_name):
                if pd.notna(team_name):
                    team_info = self.normalize_team_name(team_name)
                    return team_info['club_name']
                return team_name
            
            games_df['Home Team'] = games_df['Home Team'].apply(normalize_home)
        
        # נרמול Away Team (כולל team_id)
        if 'Away Team' in games_df.columns:
            def normalize_away(team_name):
                if pd.notna(team_name):
                    team_info = self.normalize_team_name(team_name)
                    return team_info['club_name']
                return team_name
            
            games_df['Away Team'] = games_df['Away Team'].apply(normalize_away)
        
        # נרמול תאריכים
        if 'Date' in games_df.columns:
            games_df['Date'] = games_df['Date'].apply(self.normalize_date)
        
        log_message(f"   ✅ Normalized {len(games_df)} games", self.league_code)
        
        return games_df
    
    def normalize_minutes(self, min_str):
        """
        המרת דקות מ-MM:SS לדקות שלמות (עיגול לפי 30 שניות)
        
        Args:
            min_str: "MM:SS" או מספר
        
        Returns:
            int: דקות
        """
        if pd.isna(min_str):
            return 0
        
        min_str = str(min_str).strip()
        
        try:
            if ':' in min_str:
                parts = min_str.split(':')
                mins = int(parts[0])
                secs = int(parts[1])
                if secs >= 30:
                    mins += 1
                return mins
            else:
                return int(min_str)
        except:
            return 0