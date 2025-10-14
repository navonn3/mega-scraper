# -*- coding: utf-8 -*-
"""
Data Models and ID Generation - FIXED
======================================
יצירת IDs ייחודיים ומבני נתונים - עם טיפול בטיפוסים
"""

import hashlib
import pandas as pd
from dataclasses import dataclass
from typing import Optional

# ============================================
# יצירת IDs ייחודיים
# ============================================

def generate_player_id(name, date_of_birth, league_id=None):
    """
    יצירת ID ייחודי לשחקן - עם טיפול בטיפוסים שונים
    
    Args:
        name: שם השחקן
        date_of_birth: תאריך לידה (DD/MM/YYYY) - יכול להיות string/float/int/None
        league_id: מזהה ליגה (אופציונלי - לשחקנים ללא תאריך לידה)
    
    Returns:
        str: player_id ייחודי (12 תווים)
    
    Example:
        >>> generate_player_id("ג'ון דו", "01/01/1995")
        'a3f5b8c2d1e4'
    """
    # המרה ל-string ואימות
    if date_of_birth is None:
        dob_str = ""
    elif pd.isna(date_of_birth):
        dob_str = ""
    else:
        dob_str = str(date_of_birth).strip()
    
    # אם אין תאריך לידה, נשתמש בליגה
    if not dob_str or dob_str == "" or dob_str.lower() == "nan":
        unique_string = f"{name}_{league_id}"
    else:
        unique_string = f"{name}_{dob_str}"
    
    # יצירת hash MD5
    hash_object = hashlib.md5(unique_string.encode('utf-8'))
    return hash_object.hexdigest()[:12]


def generate_team_id(team_name, league_id):
    """
    יצירת ID ייחודי לקבוצה
    
    Args:
        team_name: שם הקבוצה (normalized)
        league_id: מזהה ליגה
    
    Returns:
        str: team_id ייחודי (12 תווים)
    
    Example:
        >>> generate_team_id("מכבי תל אביב", "leumit")
        'b2c4d6e8f0a1'
    """
    unique_string = f"{league_id}_{team_name}"
    hash_object = hashlib.md5(unique_string.encode('utf-8'))
    return hash_object.hexdigest()[:12]


def generate_game_id(league_id, game_code):
    """
    יצירת ID ייחודי למשחק
    
    Args:
        league_id: מזהה ליגה
        game_code: קוד המשחק מהאתר
    
    Returns:
        str: game_id ייחודי
    
    Example:
        >>> generate_game_id("leumit", "12345")
        'leumit_12345'
    """
    return f"{league_id}_{game_code}"


def generate_league_id(country, league_name, season):
    """
    יצירת ID ייחודי לליגה
    
    Args:
        country: מדינה
        league_name: שם הליגה
        season: עונה (2024-25)
    
    Returns:
        str: league_id ייחודי
    
    Example:
        >>> generate_league_id("Israel", "National League", "2024-25")
        'israel_national_league_2024_25'
    """
    normalized = f"{country}_{league_name}_{season}".lower()
    normalized = normalized.replace(" ", "_").replace("-", "_")
    return normalized


# ============================================
# מבני נתונים (Data Classes)
# ============================================

@dataclass
class League:
    """מבנה נתונים לליגה"""
    league_id: str
    name: str
    name_en: str
    country: str
    season: str
    url: str
    
    def to_dict(self):
        return {
            'league_id': self.league_id,
            'name': self.name,
            'name_en': self.name_en,
            'country': self.country,
            'season': self.season,
            'url': self.url
        }


@dataclass
class Team:
    """מבנה נתונים לקבוצה"""
    team_id: str
    league_id: str
    normalized_name: str
    short_name: str
    bg_color: Optional[str] = None
    text_color: Optional[str] = None
    
    def to_dict(self):
        return {
            'team_id': self.team_id,
            'league_id': self.league_id,
            'normalized_name': self.normalized_name,
            'short_name': self.short_name,
            'bg_color': self.bg_color,
            'text_color': self.text_color
        }


@dataclass
class Player:
    """מבנה נתונים לשחקן"""
    player_id: str
    name: str
    current_team_id: str
    date_of_birth: str
    height: str
    jersey_number: str
    
    def to_dict(self):
        return {
            'player_id': self.player_id,
            'name': self.name,
            'current_team_id': self.current_team_id,
            'date_of_birth': self.date_of_birth,
            'height': self.height,
            'jersey_number': self.jersey_number
        }


# ============================================
# פונקציות נוספות
# ============================================

def normalize_season(season_str):
    """
    המרת פורמט עונה - עם טיפול בטיפוסים
    
    Args:
        season_str: '2024-2025' או '2024-25' - יכול להיות string/float/int/None
    
    Returns:
        str: '2024-25'
    """
    if season_str is None or pd.isna(season_str):
        return ""
    
    season_str = str(season_str).strip()
    
    if not season_str or season_str.lower() == "nan":
        return ""
    
    parts = season_str.split('-')
    if len(parts) == 2:
        return f"{parts[0]}-{parts[1][-2:]}"
    return season_str


def format_date(date_str):
    """
    המרת פורמט תאריך - עם טיפול בטיפוסים
    
    Args:
        date_str: '01-02-2024' או '2024-02-01' - יכול להיות string/float/int/None
    
    Returns:
        str: 'DD/MM/YYYY' או ""
    """
    if date_str is None or pd.isna(date_str):
        return ""
    
    date_str = str(date_str).strip()
    
    if not date_str or date_str.lower() == "nan":
        return ""
    
    # אם כבר בפורמט נכון
    if '/' in date_str:
        return date_str
    
    # אם עם מקפים
    parts = date_str.split('-')
    if len(parts) == 3:
        # בדוק איזה פורמט זה
        if len(parts[0]) == 4:  # YYYY-MM-DD
            return f"{parts[2]}/{parts[1]}/{parts[0]}"
        else:  # DD-MM-YYYY
            return '/'.join(parts)
    
    return date_str


def safe_str(value):
    """
    המרה בטוחה לstring - מטפל בכל הטיפוסים
    
    Args:
        value: כל ערך
    
    Returns:
        str: string או ""
    """
    if value is None or pd.isna(value):
        return ""
    
    value_str = str(value).strip()
    
    if value_str.lower() == "nan":
        return ""
    
    return value_str


def safe_int(value, default=0):
    """
    המרה בטוחה לint
    
    Args:
        value: כל ערך
        default: ערך ברירת מחדל
    
    Returns:
        int: מספר שלם או default
    """
    if value is None or pd.isna(value):
        return default
    
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return default


def safe_float(value, default=0.0):
    """
    המרה בטוחה לfloat
    
    Args:
        value: כל ערך
        default: ערך ברירת מחדל
    
    Returns:
        float: מספר עשרוני או default
    """
    if value is None or pd.isna(value):
        return default
    
    try:
        return float(value)
    except (ValueError, TypeError):
        return default