# -*- coding: utf-8 -*-
"""
Base Scraper Class
==================
בסיס משותף לכל ה-scrapers עם פונקציונליות חוזרת
"""

from abc import ABC, abstractmethod
from pathlib import Path

from utils import log_message, ensure_directories


class BaseScraper(ABC):
    """
    מחלקת בסיס לכל ה-scrapers
    מטפלת בפונקציונליות משותפת:
    - ניהול תיקיות
    - logging
    - מצבי גזירה (full/quick)
    """
    
    def __init__(self, league_config, league_id, scrape_mode='full'):
        """
        אתחול scraper
        
        Args:
            league_config: הגדרות ליגה מ-config.py
            league_id: מזהה ליגה מספרי
            scrape_mode: 'full' או 'quick'
        """
        self.league_config = league_config
        self.league_id = league_id
        self.league_code = league_config['code']
        self.scrape_mode = scrape_mode
        
        # נתיבים
        self.data_folder = league_config['data_folder']
        self.games_folder = league_config['games_folder']
        
        # ודא תיקיות
        ensure_directories(league_config)
        
        # אתחול processors
        self._init_processors()
    
    @abstractmethod
    def _init_processors(self):
        """אתחול processors ספציפיים - יוגדר בכל scraper"""
        pass
    
    @abstractmethod
    def _update_player_details(self):
        """עדכון פרטי שחקנים - יוגדר בכל scraper"""
        pass
    
    @abstractmethod
    def _update_game_details(self):
        """עדכון משחקים - יוגדר בכל scraper"""
        pass
    
    def _calculate_averages(self):
        """חישוב ממוצעים - משותף לכולם"""
        from .processors.averages import AveragesCalculator
        
        log_message("STEP 3: CALCULATING AVERAGES", self.league_code)
        
        calculator = AveragesCalculator(
            self.league_id,
            self.league_code,
            self.data_folder,
            self.games_folder
        )
        
        return calculator.calculate_all()
    
    def run(self):
        """הרצת תהליך הגזירה"""
        try:
            self.log(f"Starting scrape in {self.scrape_mode.upper()} mode")
            
            # ✅ STEP 1: עדכון משחקים
            if not self._update_game_details():
                self.log("❌ Failed to update games")
                return False
            
            # ✅ STEP 2: עדכון שחקנים
            if not self._update_player_details():
                self.log("❌ Failed to update player details")
                return False
            
            # ✅ STEP 3: חישוב ממוצעים (אם יש)
            if hasattr(self, '_calculate_averages'):
                if not self._calculate_averages():
                    self.log("❌ Failed to calculate averages")
                    return False
            
            self.log("=" * 60)
            self.log("✅ SCRAPING COMPLETED SUCCESSFULLY")
            return True
            
        except Exception as e:
            self.log(f"❌ CRITICAL ERROR: {e}")
            import traceback
            self.log(traceback.format_exc())
            return False    
    def log(self, message, level='info'):
        """helper ל-logging"""
        log_message(message, self.league_code)
