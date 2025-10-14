# -*- coding: utf-8 -*-
"""
Basketball Scraper - Configuration (Dynamic Version)
====================================================
קובץ הגדרות חכם:
- עונה מתעדכנת אוטומטית לפי השנה הנוכחית
- יצירת URLים לפי השנה הנוכחית (לכל ליגה באיגוד)
- יצירת תיקיות חסרות באופן אוטומטי
- תמיכה במצבי גזירה: full (מלא) / quick (מהיר)
"""

import os
from datetime import datetime
from pathlib import Path

# ============================================
# הגדרות גזירה גלובליות
# ============================================

SCRAPING_CONFIG = {
    "delay_between_requests": 1,
    "timeout": 10,
    "retry_attempts": 3,
    
    # מצב גזירה - בחר אחד:
    # "full"  - בדיקה מקיפה של כל השחקנים + תיקון נתונים חסרים (כמו המקור)
    # "quick" - רק שחקנים חדשים + משחקים חדשים (מהיר, יומיומי)
    "scrape_mode": "quick"  # ← ברירת מחדל: בדיקה מקיפה
}

# ============================================
# פונקציות עזר לעונה וכתובות
# ============================================

def get_current_season() -> str:
    """החזרת העונה הנוכחית בפורמט 'YYYY-YY' (למשל '2025-26')."""
    year = datetime.now().year
    return f"{year}-{str(year + 1)[-2:]}"


def make_ibasket_url(league_suffix: int) -> str:
    """בניית כתובת URL לליגה באתר האיגוד לפי השנה הנוכחית."""
    year = datetime.now().year
    return f"https://ibasketball.co.il/league/{year}-{league_suffix}/"


def ensure_folders_exist(*folders):
    """מוודא שכל התיקיות קיימות — יוצר אם חסרות."""
    for folder in folders:
        if folder and not os.path.exists(folder):
            os.makedirs(folder, exist_ok=True)

# ============================================
# הגדרות ליגות
# ============================================

LEAGUES = {
    "1": {  # ליגה לאומית
        "name": "ליגה לאומית",
        "name_en": "National League",
        "code": "leumit",
        "country": "Israel",
        "season": get_current_season(),
        "url": make_ibasket_url(2),
        "scraper_type": "ibasketball",
        "data_folder": "data/leumit",
        "games_folder": "data/leumit/leumit_games",
        "active": True
    },
    "2": {  # ליגה ארצית צפון
        "name": "ליגה ארצית צפון",
        "name_en": "National North League",
        "code": "artzit-north",
        "country": "Israel",
        "season": get_current_season(),
        "url": make_ibasket_url(3),
        "scraper_type": "ibasketball",
        "data_folder": "data/artzit_north",
        "games_folder": "data/artzit_north/artzit_north_games",
        "active": False
    },
    "3": {  # ליגה ארצית דרום
        "name": "ליגה ארצית דרום",
        "name_en": "National South League",
        "code": "artzit-south",
        "country": "Israel",
        "season": get_current_season(),
        "url": make_ibasket_url(4),
        "scraper_type": "ibasketball",
        "data_folder": "data/artzit_south",
        "games_folder": "data/artzit_south/artzit_south_games",
        "active": False
    },
    "4": {  # נוער על צפון
        "name": "נוער על צפון",
        "name_en": "U18 North",
        "code": "u18-north",
        "country": "Israel",
        "season": get_current_season(),
        "url": make_ibasket_url(101),
        "scraper_type": "ibasketball",
        "data_folder": "data/u18_north",
        "games_folder": "data/u18_north/u18_north_games",
        "active": True
    },
    "5": {  # נוער על דרום
        "name": "נוער על דרום",
        "name_en": "U18 South",
        "code": "u18-south",
        "country": "Israel",
        "season": get_current_season(),
        "url": make_ibasket_url(102),
        "scraper_type": "ibasketball",
        "data_folder": "data/u18_south",
        "games_folder": "data/u18_south/u18_south_games",
        "active": True
    },
    "6": {  # נערים א' לאומית צפון
        "name": "נערים א' לאומית צפון",
        "name_en": "U16 North",
        "code": "u16-north",
        "country": "Israel",
        "season": get_current_season(),
        "url": make_ibasket_url(152),
        "scraper_type": "ibasketball",
        "data_folder": "data/u16_north",
        "games_folder": "data/u16_north/u16_north_games",
        "active": False
    },
    "7": {  # נערים א' לאומית דרום
        "name": "נערים א' לאומית דרום",
        "name_en": "U16 South",
        "code": "u16-south",
        "country": "Israel",
        "season": get_current_season(),
        "url": make_ibasket_url(153),
        "scraper_type": "ibasketball",
        "data_folder": "data/u16_south",
        "games_folder": "data/u16_south/u16_south_games",
        "active": False
    },
    "8": {  # לאומית נשים
        "name": "ליגה לאומית נשים",
        "name_en": "Women National League",
        "code": "leumit-women",
        "country": "Israel",
        "season": get_current_season(),
        "url": make_ibasket_url(52),
        "scraper_type": "ibasketball",
        "data_folder": "data/leumit_women",
        "games_folder": "data/leumit_women/leumit_women_games",
        "active": True
    },
    "9": {  # נערות א' על
        "name": "נערות א' על",
        "name_en": "U18 Women",
        "code": "u18-women",
        "country": "Israel",
        "season": get_current_season(),
        "url": make_ibasket_url(451),
        "scraper_type": "ibasketball",
        "data_folder": "data/u18_women",
        "games_folder": "data/u18_women/u18_women_games",
        "active": True
    },
    "10": {  # ליגת העל לגברים (קבועה)
        "name": "ליגת Winner סל",
        "name_en": "Winner League",
        "code": "ibsl",
        "country": "Israel",
        "season": get_current_season(),
        "url": "https://basket.co.il/",
        "scraper_type": "winner",
        "data_folder": "data/ibsl",
        "games_folder": "data/ibsl/ibsl_games",
        "active": False
    },
    "11": {  # ליגת העל לנשים
        "name": "ליגת העל לנשים",
        "name_en": "Women Premier League",
        "code": "women-pl",
        "country": "Israel",
        "season": get_current_season(),
        "url": make_ibasket_url(51),
        "scraper_type": "ibasketball",
        "data_folder": "data/women_pl",
        "games_folder": "data/women_pl/women_pl_games",
        "active": True
    }
}

# ============================================
# הגדרות כלליות
# ============================================

BASE_URL = "https://ibasketball.co.il"
DATA_ROOT = "data"
LOGS_FOLDER = "logs"
NORMALIZATION_FOLDER = "data/normalization"

NORMALIZATION_FILES = {
    "teams": f"{NORMALIZATION_FOLDER}/teams_mapping.csv",
    "leagues": f"{NORMALIZATION_FOLDER}/leagues_mapping.csv"
}

GLOBAL_FILES = {
    "leagues": f"{DATA_ROOT}/leagues.csv",
    "teams": f"{DATA_ROOT}/teams.csv",
    "players": f"{DATA_ROOT}/players.csv"
}

# ============================================
# פונקציות עזר לניהול ליגות
# ============================================

def get_active_leagues():
    """החזר רק ליגות פעילות"""
    return {k: v for k, v in LEAGUES.items() if v.get("active", True)}

def get_league_config(league_id):
    """קבל הגדרות של ליגה ספציפית"""
    if league_id not in LEAGUES:
        raise ValueError(f"League '{league_id}' not found in config")
    return LEAGUES[league_id]

def get_all_league_ids():
    """קבל רשימת כל מזהי הליגות"""
    return list(LEAGUES.keys())

def get_league_by_code(code):
    """מצא ליגה לפי code"""
    for league_id, config in LEAGUES.items():
        if config.get("code") == code:
            return league_id, config
    return None, None

def get_scrape_mode():
    """קבל את מצב הגזירה הנוכחי"""
    return SCRAPING_CONFIG.get("scrape_mode", "full")

# ============================================
# יצירת תיקיות חסרות אוטומטית
# ============================================

for league_id, cfg in LEAGUES.items():
    ensure_folders_exist(cfg["data_folder"], cfg["games_folder"])