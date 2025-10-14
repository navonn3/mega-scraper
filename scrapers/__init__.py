"""
Scrapers Package
================
גזירת נתונים מאתרים שונים
"""

from .base_scraper import BaseScraper      # ← הוסף שורה זו!
from .ibasketball import IBasketballScraper

# WinnerScraper יתווסף בעתיד
try:
    from .winner import WinnerScraper
except ImportError:
    WinnerScraper = None

__all__ = ['BaseScraper', 'IBasketballScraper', 'WinnerScraper']  # ← הוסף 'BaseScraper' ברשימה