# -*- coding: utf-8 -*-
#!/usr/bin/env python
# coding: utf-8

"""
Basketball Scraper - Main Script - UPDATED
===========================================
סקריפט ראשי להרצת גזירה אוטומטית מרובת ליגות

עדכון: לא דורס את teams.csv (משתמש בקובץ המרכזי)

הרצה:
    python main.py                    # גזירה לכל הליגות הפעילות
    python main.py --league 1         # גזירה לליגה ספציפית (לפי מספר)
    python main.py --mode quick       # מצב גזירה מהיר (רק חדשים)
    python main.py --list             # רשימת ליגות זמינות
    python main.py --help             # עזרה

מצבי גזירה:
    --mode full   : גזירה מלאה + ולידציה (ברירת מחדל)
    --mode quick  : רק שחקנים חדשים + משחקים חדשים (מהיר)
"""

import sys
import argparse
import json  
from datetime import datetime
from pathlib import Path

from config import get_active_leagues, get_league_config, LEAGUES, SCRAPING_CONFIG
from scrapers import IBasketballScraper
from utils import log_message
from models import League
import pandas as pd


# ============================================
# MAIN SCRAPING LOGIC
# ============================================

def scrape_league(league_id, scrape_mode=None):
    """
    גזירה של ליגה אחת
    
    Args:
        league_id: מזהה ליגה מספרי (לדוגמה: "1", "2")
        scrape_mode: מצב גזירה ("full" או "quick"), None = מconfig
    
    Returns:
        bool: True אם הצליח
    """
    try:
        config = get_league_config(league_id)
        
        if not config.get('active', True):
            log_message(f"⚠️  League '{league_id}' ({config['name']}) is not active - skipping", config['code'])
            return False
        
        # קביעת מצב גזירה
        if scrape_mode is None:
            scrape_mode = SCRAPING_CONFIG.get('scrape_mode', 'full')
        
        # בחירת scraper לפי סוג
        scraper_type = config.get('scraper_type', 'ibasketball')
        
        if scraper_type == 'ibasketball':
            from scrapers import IBasketballScraper
            scraper = IBasketballScraper(config, league_id, scrape_mode=scrape_mode)
        elif scraper_type == 'winner':
            from scrapers import WinnerScraper
            scraper = WinnerScraper(config, league_id, scrape_mode=scrape_mode)
        else:
            log_message(f"❌ Unknown scraper type: {scraper_type}", config['code'])
            return False
        
        # הרצת גזירה
        success = scraper.run()
        
        return success
        
    except Exception as e:
        log_message(f"❌ CRITICAL ERROR in league '{league_id}': {e}")
        import traceback
        log_message(traceback.format_exc())
        return False


def scrape_all_leagues(scrape_mode=None):
    """גזירה של כל הליגות הפעילות"""
    active_leagues = get_active_leagues()
    
    if not active_leagues:
        log_message("⚠️  No active leagues found in config")
        return False
    
    log_message(f"Found {len(active_leagues)} active leagues")
    
    results = {}
    
    for league_id in active_leagues.keys():
        log_message("")
        log_message("="*80)
        log_message(f"PROCESSING LEAGUE: {league_id} - {active_leagues[league_id]['name']}")
        log_message("="*80)
        
        success = scrape_league(league_id, scrape_mode=scrape_mode)
        results[league_id] = success
    
    
    # סיכום
    log_message("")
    log_message("="*80)
    log_message("SCRAPING SUMMARY")
    log_message("="*80)
    
    successful = [lid for lid, success in results.items() if success]
    failed = [lid for lid, success in results.items() if not success]
    
    log_message(f"✅ Successful: {len(successful)} leagues")
    for lid in successful:
        log_message(f"   ✓ League {lid}: {LEAGUES[lid]['name']}")
    
    if failed:
        log_message(f"❌ Failed: {len(failed)} leagues")
        for lid in failed:
            log_message(f"   ✗ League {lid}: {LEAGUES[lid]['name']}")
    
    log_message("="*80)
    
    return len(failed) == 0


# ============================================
# CLI
# ============================================

def main():
    """פונקציה ראשית"""
    parser = argparse.ArgumentParser(
        description='Basketball Scraper - Automated data collection for multiple leagues',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py                       # Scrape all active leagues (full mode)
  python main.py --league 1            # Scrape only league #1
  python main.py --mode quick          # Quick scrape (new players/games only)
  python main.py --league 1 --mode quick  # Quick scrape for specific league
  python main.py --list                # List all available leagues
        """
    )
    
    parser.add_argument(
        '--league',
        type=str,
        help='Scrape specific league only by ID (e.g., "1", "2")'
    )
    
    parser.add_argument(
        '--mode',
        type=str,
        choices=['full', 'quick'],
        help='Scraping mode: "full" (complete + validation) or "quick" (new only)'
    )
    
    parser.add_argument(
        '--list',
        action='store_true',
        help='List all available leagues and exit'
    )
    
    args = parser.parse_args()
    
    # הצגת רשימת ליגות
    if args.list:
        print("\n" + "="*70)
        print("Available Leagues:")
        print("="*70)
        print(f"{'ID':^6} | {'Status':^10} | {'Code':^15} | {'Name':^30}")
        print("-"*70)
        for league_id, config in LEAGUES.items():
            status = "✓ ACTIVE" if config.get('active', True) else "✗ INACTIVE"
            print(f"{league_id:^6} | {status:^10} | {config['code']:^15} | {config['name']:^30}")
        print("="*70)
        print(f"\nTotal: {len(LEAGUES)} leagues")
        print(f"Active: {len(get_active_leagues())} leagues")
        
        # הצגת מצב גזירה נוכחי
        current_mode = SCRAPING_CONFIG.get('scrape_mode', 'full')
        print(f"\nCurrent scrape mode: {current_mode.upper()}")
        print("  • full  = Check all players + fix missing data (thorough)")
        print("  • quick = New players/games only (fast, daily)\n")
        return
    
    # קביעת מצב גזירה
    scrape_mode = args.mode if args.mode else SCRAPING_CONFIG.get('scrape_mode', 'full')
    
    # התחלה
    log_message("")
    log_message("="*80)
    log_message("BASKETBALL SCRAPER STARTED")
    log_message(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log_message(f"Mode: {scrape_mode.upper()}")
    log_message("="*80)
    
    # גזירה
    if args.league:
        # ליגה ספציפית
        league_id = args.league
        
        if league_id not in LEAGUES:
            log_message(f"❌ ERROR: League '{league_id}' not found in config")
            log_message(f"Available league IDs: {', '.join(LEAGUES.keys())}")
            sys.exit(1)
        
        log_message(f"Scraping single league: {league_id} - {LEAGUES[league_id]['name']}")
        success = scrape_league(league_id, scrape_mode=scrape_mode)
        
        
        exit_code = 0 if success else 1
    else:
        # כל הליגות
        all_success = scrape_all_leagues(scrape_mode=scrape_mode)
        exit_code = 0 if all_success else 1
    
    # סיום
    log_message("")
    log_message("="*80)
    log_message("BASKETBALL SCRAPER FINISHED")
    log_message(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log_message("="*80)
    
    sys.exit(exit_code)


if __name__ == "__main__":
    main()