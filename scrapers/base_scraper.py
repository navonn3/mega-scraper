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
        """הרצה ראשית - זהה לכל scrapers"""
        log_message("="*60, self.league_code)
        log_message(f"STARTING SCRAPE: {self.league_config['name']}", self.league_code)
        log_message(f"Mode: {self.scrape_mode.upper()}", self.league_code)
        log_message("="*60, self.league_code)
        
        # שלב 1: שחקנים
        if not self._update_player_details():
            return False
        
        # שלב 2: משחקים
        if not self._update_game_details():
            return False
        
        # שלב 3: ממוצעים
        if not self._calculate_averages():
            return False
        
        log_message("="*60, self.league_code)
        log_message("✅ SCRAPE COMPLETED SUCCESSFULLY", self.league_code)
        log_message("="*60, self.league_code)
        
        return True
    
    def log(self, message, level='info'):
        """helper ל-logging"""
        log_message(message, self.league_code)
