# Basketball Scraper

מערכת לגזירת נתוני כדורסל
```
basketball_scraper/
├── config.py                    # הגדרות ליגות
├── main.py                      # סקריפט ראשי
├── models/
│   ├── __init__.py
│   └── data_models.py          # יצירת IDs ומבני נתונים
├── scrapers/
│   ├── __init__.py
│   └── ibasketball.py          # גזירה מ-ibasketball.co.il
├── utils/
│   ├── __init__.py
│   └── helpers.py              # פונקציות עזר
├── data/                        # נתונים (נוצר אוטומטית)
│   ├── normalization/          # ⭐ קבצי normalization גלובליים
│   │   ├── teams_mapping.csv   # מיפוי כל הקבוצות
│   │   └── leagues_mapping.csv # מיפוי ליגות (אופציונלי)
│   ├── leagues.csv             # כל הליגות
│   ├── teams.csv               # כל הקבוצות
│   ├── players.csv             # כל השחקנים
│   ├── leumit/                 # נתונים לליגה לאומית
│   │   ├── leumit_player_details.csv
│   │   ├── leumit_player_history.csv
│   │   ├── leumit_player_averages.csv
│   │   ├── leumit_team_averages.csv
│   │   ├── leumit_opponent_averages.csv
│   │   └── leumit_games/
│   │       ├── games_schedule.csv
│   │       ├── game_quarters.csv
│   │       ├── game_player_stats.csv
│   │       └── game_team_stats.csv
│   └── artzit/                 # נתונים לליגה ארצית
│       └── ...
└── logs/                        # קבצי לוג
    └── update_log.txt
```# 🏀 Basketball Scraper - מערכת גזירה מקצועית

מערכת מודולרית לגזירה אוטומטית של נתוני כדורסל מליגות מרובות.

---

## 📁 מבנה הפרויקט

```
basketball_scraper/
├── config.py                    # הגדרות ליגות
├── main.py                      # סקריפט ראשי
├── models/
│   ├── __init__.py
│   └── data_models.py          # יצירת IDs ומבני נתונים
├── scrapers/
│   ├── __init__.py
│   └── ibasketball.py          # גזירה מ-ibasketball.co.il
├── utils/
│   ├── __init__.py
│   └── helpers.py              # פונקציות עזר
├── data/                        # נתונים (נוצר אוטומטית)
│   ├── leagues.csv             # כל הליגות
│   ├── teams.csv               # כל הקבוצות
│   ├── players.csv             # כל השחקנים
│   ├── leumit/                 # נתונים לליגה לאומית
│   │   ├── team_names.csv
│   │   ├── leumit_player_details.csv
│   │   ├── leumit_player_history.csv
│   │   ├── leumit_player_averages.csv
│   │   ├── leumit_team_averages.csv
│   │   ├── leumit_opponent_averages.csv
│   │   └── leumit_games/
│   │       ├── games_schedule.csv
│   │       ├── game_quarters.csv
│   │       ├── game_player_stats.csv
│   │       └── game_team_stats.csv
│   └── artzit/                 # נתונים לליגה ארצית
│       └── ...
└── logs/                        # קבצי לוג
    └── update_log.txt
```

---

## 🚀 התקנה מהירה

### 1. דרישות מקדימות

וודא שיש לך Python 3.7+ מותקן:

```bash
python --version
```

### 2. התקנת ספריות

```bash
pip install requests beautifulsoup4 pandas openpyxl
```

### 3. הכנת המבנה

צור את מבנה התיקיות:

```bash
mkdir -p basketball_scraper/{models,scrapers,utils,data/normalization,logs}
cd basketball_scraper
```

העתק את כל הקבצים לתיקיות המתאימות:
- `config.py` → שורש
- `main.py` → שורש
- `models/__init__.py` ו-`models/data_models.py` → תיקיית models
- `scrapers/__init__.py` ו-`scrapers/ibasketball.py` → תיקיית scrapers
- `utils/__init__.py` ו-`utils/helpers.py` → תיקיית utils

### 4. הכנת קובץ המיפוי הגלובלי ⭐ חשוב!

צור `data/normalization/teams_mapping.csv` עם כל הקבוצות מכל הליגות:

**מבנה הקובץ:**
```csv
source_name,league_id,normalized_name,short_name,bg_color,text_color
מכבי תל אביב גיא נתן,leumit,מכבי תל אביב,מכבי ת"א,#0000FF,#FFFF00
מכבי ת"א,leumit,מכבי תל אביב,מכבי ת"א,#0000FF,#FFFF00
מכבי תל-אביב,leumit,מכבי תל אביב,מכבי ת"א,#0000FF,#FFFF00
הפועל ירושלים,leumit,הפועל ירושלים,הפועל י-ם,#FF0000,#FFFFFF
הפועל י-ם,leumit,הפועל ירושלים,הפועל י-ם,#FF0000,#FFFFFF
מכבי חיפה,artzit,מכבי חיפה,מכבי חיפה,#00AA00,#FFFFFF
```

**הסבר על העמודות:**
- `source_name` - כל וריאציה של השם כפי שמופיעה באתר (אפשר כמה שורות לאותה קבוצה!)
- `league_id` - מזהה הליגה (leumit, artzit, וכו')
- `normalized_name` - השם האחיד והסטנדרטי שתרצה להשתמש בו
- `short_name` - שם קצר לתצוגה
- `bg_color` - צבע רקע של הקבוצה (hex color)
- `text_color` - צבע טקסט (hex color)

**טיפ חשוב:** אם אתה רואה קבוצה עם שמות שונים באתר (למשל "מכבי ת"א" ו"מכבי תל אביב גיא נתן"), הוסף שורה נפרדת לכל וריאציה, אבל עם אותו `normalized_name`!

---

## 💻 שימוש

### גזירה בסיסית

**גזירה של כל הליגות הפעילות:**
```bash
python main.py
```

**גזירה של ליגה ספציפית:**
```bash
python main.py --league leumit
```

**הצגת רשימת ליגות זמינות:**
```bash
python main.py --list
```

### פלט לדוגמה

```
[2024-10-11 14:30:00] ============================================================
[2024-10-11 14:30:00] BASKETBALL SCRAPER STARTED
[2024-10-11 14:30:00] ============================================================
[2024-10-11 14:30:00] Found 2 active leagues
[2024-10-11 14:30:00] ============================================================
[2024-10-11 14:30:00] [LEUMIT] STARTING SCRAPE: ליגה לאומית
[2024-10-11 14:30:00] ============================================================
[2024-10-11 14:30:01] [LEUMIT] ✅ Loaded team mapping: 12 teams, 36 variations
[2024-10-11 14:30:02] [LEUMIT] STEP 1: UPDATING PLAYER DETAILS
[2024-10-11 14:30:02] [LEUMIT] Found 156 players
[2024-10-11 14:30:15] [LEUMIT] ✅ Player details updated
[2024-10-11 14:30:15] [LEUMIT]    Total: 156 | New: 5 | Updated: 3 | Skipped: 148
[2024-10-11 14:30:15] [LEUMIT] STEP 2: UPDATING GAME DETAILS
[2024-10-11 14:30:16] [LEUMIT] ✅ Games schedule updated: 132 games
[2024-10-11 14:30:16] [LEUMIT]    Found 45 completed games
[2024-10-11 14:30:16] [LEUMIT]    Already scraped: 42 games
[2024-10-11 14:30:16] [LEUMIT]    Scraping 3 new games
[2024-10-11 14:30:25] [LEUMIT] ✅ Game stats updated: 3 new games
[2024-10-11 14:30:25] [LEUMIT] STEP 3: CALCULATING AVERAGES
[2024-10-11 14:30:26] [LEUMIT] ✅ Player averages calculated: 145 players
[2024-10-11 14:30:26] [LEUMIT] ✅ Team averages calculated: 12 teams
[2024-10-11 14:30:26] [LEUMIT] ✅ Opponent averages calculated: 12 teams
[2024-10-11 14:30:26] ============================================================
[2024-10-11 14:30:26] ✅ ALL UPDATES COMPLETED SUCCESSFULLY
[2024-10-11 14:30:26] ============================================================
```

---

## ⚙️ הוספת ליגה חדשה

### שלב 1: עדכון `config.py`

הוסף entry חדש ל-`LEAGUES`:

```python
"super_league": {
    "name": "סופרליג",
    "name_en": "Super League",
    "country": "Israel",
    "season": "2024-25",
    "url": "https://ibasketball.co.il/league/XXXX/",  # URL אמיתי
    "data_folder": "data/super_league",
    "games_folder": "data/super_league/super_league_games",
    "active": True
}
```

### שלב 2: הוספת קבוצות ל-teams_mapping.csv

הוסף את הקבוצות של הליגה החדשה ל-`data/normalization/teams_mapping.csv`:

```csv
source_name,league_id,normalized_name,short_name,bg_color,text_color
קבוצה א,super_league,קבוצה א,קבוצה א,#FF0000,#FFFFFF
קבוצה ב,super_league,קבוצה ב,קבוצה ב,#00FF00,#000000
```

### שלב 3: הרצה

```bash
python main.py --league super_league
```

**זהו! הליגה החדשה מוכנה** ✅

**יתרון המבנה החדש:**
- קובץ אחד לכל הקבוצות מכל הליגות
- קל להוסיף ליגה חדשה - רק 2 מקומות לעדכן (config.py + teams_mapping.csv)
- אין צורך ליצור קבצים נפרדים לכל ליגה

---

## 🔄 הרצה אוטומטית יומית

### Linux/Mac (cron)

```bash
crontab -e
```

הוסף:
```
0 2 * * * cd /path/to/basketball_scraper && /usr/bin/python3 main.py
```
(ירוץ כל יום בשעה 02:00)

### Windows (Task Scheduler)

1. פתח **Task Scheduler**
2. צור **New Task**
3. **Trigger**: Daily, 02:00 AM
4. **Action**: Start a program
   - Program: `python`
   - Arguments: `main.py`
   - Start in: `C:\path\to\basketball_scraper`

---

## 📊 הנתונים שנוצרים

### קבצים גלובליים (כל הליגות ביחד)

| קובץ | תיאור |
|------|-------|
| `data/leagues.csv` | רשימת כל הליגות |
| `data/teams.csv` | כל הקבוצות מכל הליגות |
| `data/players.csv` | כל השחקנים מכל הליגות |

### קבצים לפי ליגה

| קובץ | תיאור |
|------|-------|
| `[league]_player_details.csv` | פרטי שחקנים בסיסיים |
| `[league]_player_history.csv` | היסטוריית קבוצות לפי עונות |
| `[league]_player_averages.csv` | ממוצעי שחקנים לעונה |
| `[league]_team_averages.csv` | ממוצעי קבוצות לעונה |
| `[league]_opponent_averages.csv` | ממוצעי יריבים (הגנה) |
| `games_schedule.csv` | לוח משחקים מלא |
| `game_quarters.csv` | ניקוד לפי רבעים |
| `game_player_stats.csv` | סטטיסטיקות שחקנים למשחק |
| `game_team_stats.csv` | סטטיסטיקות קבוצתיות למשחק |

---

## 🆔 מזהים ייחודיים (IDs)

המערכת יוצרת IDs ייחודיים לכל ישות:

### Player ID
```python
player_id = hash(שם_שחקן + תאריך_לידה)
# דוגמה: "a3f5b8c2d1e4"
```

### Team ID
```python
team_id = hash(league_id + שם_קבוצה_מנורמל)
# דוגמה: "b2c4d6e8f0a1"
```

### Game ID
```python
game_id = f"{league_id}_{game_code}"
# דוגמה: "leumit_12345"
```

**יתרון:** אין בעיות של שמות זהים או שינויי שמות!

---

## 🔧 פתרון בעיות

### שגיאה: "No module named 'models'"

**פתרון:** וודא שיש קובץ `__init__.py` בתיקיות:
```bash
touch models/__init__.py
touch scrapers/__init__.py
touch utils/__init__.py
```

### שגיאה: "Team mapping file not found"

**פתרון:** צור `data/[league_id]/team_names.csv` לפני הרצה.

### שגיאה: "Could not find league_id"

**פתרון:** בדוק שה-URL בconfig.py נכון ושיש קישור "export" באתר הליגה.

### לא נוצרים קבצי נתונים

**פתרון:** הרץ עם `--league [league_id]` לליגה ספציפית וצפה בשגיאות.

---

## 📝 לוגים

כל הפעולות נרשמות ב-`logs/update_log.txt`:

```bash
tail -f logs/update_log.txt  # צפייה בזמן אמת
```

---

## 🎯 מה הלאה?

### אינטגרציה עם Base44

הנתונים מוכנים להעלאה ל-Base44:
1. העלה את הקבצים מ-`data/` ל-Base44
2. צור את הטבלאות המתאימות
3. קישור הנתונים בין הטבלאות דרך ה-IDs

### מעבר ל-Supabase (עתידי)

כשתהיינה יותר ליגות:
1. הקם Supabase project
2. צור את הטבלאות
3. שנה את `save_to_csv()` ב-`helpers.py` לשמור ישירות ל-DB

---

## 💡 טיפים

- **תדירות גזירה:** הרץ פעם ביום מספיק (משחקים חדשים)
- **ביצועים:** הסקריפט חכם ודולג על נתונים קיימים
- **ליגות רבות:** הוסף/הסר ליגות בקלות דרך `config.py`
- **גיבוי:** שמור את תיקיית `data/` באופן קבוע

---

## 🤝 תמיכה

יש שאלה? בדוק את:
1. קבצי הלוג ב-`logs/update_log.txt`
2. הפלט בקונסול בזמן הרצה
3. המדריך הזה

---

**נוצר עם ❤️ לניהול נתוני כדורסל מקצועי**