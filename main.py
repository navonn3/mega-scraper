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
from datetime import datetime
from pathlib import Path

from config import get_active_leagues, get_league_config, LEAGUES, SCRAPING_CONFIG
from scrapers import IBasketballScraper
from utils import log_message
from models import League
import pandas as pd

# ============================================
# GLOBAL DATA MANAGEMENT
# ============================================

def update_global_leagues():
    """עדכון קובץ leagues.csv גלובלי"""
    log_message("Updating global leagues.csv...")
    
    leagues_data = []
    for league_id, config in LEAGUES.items():
        league = League(
            league_id=league_id,
            name=config['name'],
            name_en=config['name_en'],
            country=config['country'],
            season=config['season'],
            url=config['url']
        )
        leagues_data.append(league.to_dict())
    
    df = pd.DataFrame(leagues_data)
    Path("data").mkdir(exist_ok=True)
    df.to_csv("data/leagues.csv", index=False, encoding='utf-8-sig')
    log_message(f"✅ Global leagues.csv updated: {len(leagues_data)} leagues")


def update_global_teams():
    """
    ⚠️ DEPRECATED - לא דורס את teams.csv!
    
    הקובץ teams.csv מנוהל ידנית ומכיל:
    - Team_ID מספרי
    - League_ID
    - Team_Name
    - name_variations
    
    הפונקציה הזו רק מאמתת שהקובץ קיים.
    """
    log_message("Checking global teams.csv...")
    
    teams_file = "data/teams.csv"
    
    if not Path(teams_file).exists():
        log_message("⚠️  WARNING: data/teams.csv not found!")
        log_message("   Please ensure teams.csv exists with Team_ID, League_ID, Team_Name, name_variations")
        return
    
    try:
        df = pd.read_csv(teams_file, encoding='utf-8-sig')
        
        # בדיקת עמודות נדרשות
        required_cols = ['Team_ID', 'League_ID', 'Team_Name', 'name_variations']
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            log_message(f"⚠️  WARNING: Missing columns in teams.csv: {', '.join(missing_cols)}")
            return
        
        log_message(f"✅ Global teams.csv verified: {len(df)} teams")
        
    except Exception as e:
        log_message(f"❌ Error reading teams.csv: {e}")


def update_global_players():
    """עדכון קובץ players.csv גלובלי (כל השחקנים מכל הליגות)"""
    log_message("Updating global players.csv...")
    
    all_players = []
    
    for league_id, config in LEAGUES.items():
        league_code = config['code']
        player_details_file = Path(config['data_folder']) / f"{league_code}_player_details.csv"
        
        if not player_details_file.exists():
            continue
        
        try:
            df = pd.read_csv(player_details_file, encoding='utf-8-sig')
            
            for _, row in df.iterrows():
                player_data = {
                    'player_id': row['player_id'],
                    'name': row['Name'],
                    'current_team_id': row['team_id'],
                    'league_id': row['league_id'],
                    'date_of_birth': row.get('Date Of Birth', ''),
                    'height': row.get('Height', ''),
                    'jersey_number': row.get('Number', '')
                }
                all_players.append(player_data)
        
        except Exception as e:
            log_message(f"⚠️  Could not load players from league {league_id}: {e}")
    
    if all_players:
        df = pd.DataFrame(all_players)
        df.to_csv("data/players.csv", index=False, encoding='utf-8-sig')
        log_message(f"✅ Global players.csv updated: {len(all_players)} players")
    else:
        log_message("⚠️  No players found to update global file")


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
    
    # עדכון קבצים גלובליים
    log_message("")
    log_message("="*80)
    log_message("UPDATING GLOBAL FILES")
    log_message("="*80)
    
    update_global_leagues()
    update_global_teams()  # רק בדיקה, לא כתיבה!
    update_global_players()
    
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
        
        # עדכון קבצים גלובליים
        log_message("")
        log_message("="*80)
        log_message("UPDATING GLOBAL FILES")
        log_message("="*80)
        update_global_leagues()
        update_global_teams()  # רק בדיקה, לא כתיבה!
        update_global_players()
        
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