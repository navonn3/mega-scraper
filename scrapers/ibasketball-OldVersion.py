# -*- coding: utf-8 -*-
"""iBasketball.co.il Scraper - FIXED VERSION
=========================
גזירת נתונים מאתר ibasketball.co.il

תיקונים:
1. פורמט תאריכים אחיד (DD/MM/YYYY)
2. נורמליזציה מלאה של שמות קבוצות
3. תיקון ניקוד משחקים
4. ולידציה של נתונים לפני שמירה
"""

import requests
import pandas as pd
import time
import os
from pathlib import Path
from datetime import datetime

from utils import (
    log_message, get_soup, save_to_csv, append_to_csv,
    load_global_team_mapping, normalize_team_name_global, ensure_directories 
)
from models import (
    generate_player_id, generate_team_id, generate_game_id,
    normalize_season
)

# ============================================
# MAIN SCRAPER CLASS
# ============================================

class IBasketballScraper:
    """גזירה מ-ibasketball.co.il"""
    
    def __init__(self, league_config, league_id, scrape_mode='full'):
        """
        אתחול הגזירה
        
        Args:
            league_config: הגדרות ליגה מ-config.py
            league_id: מזהה ליגה מספרי (לדוגמה: "1", "2")
            scrape_mode: מצב גזירה - "full" (מלא) או "quick" (מהיר)
        """
        self.league_config = league_config
        self.league_id = league_id
        self.league_code = league_config['code']
        self.scrape_mode = scrape_mode
        self.team_mapping = {}
        
        # נתיבים
        self.data_folder = league_config['data_folder']
        self.games_folder = league_config['games_folder']
        
        # וודא שתיקיות קיימות
        ensure_directories(league_config)
    
    def run(self):
        """הרצת גזירה מלאה לליגה"""
        log_message("="*60, self.league_code)
        log_message(f"STARTING SCRAPE: {self.league_config['name']}", self.league_code)
        log_message(f"Mode: {self.scrape_mode.upper()}", self.league_code)
        log_message("="*60, self.league_code)
        
        # טעינת מיפוי קבוצות גלובלי
        self.team_mapping = load_global_team_mapping()
        
        if not self.team_mapping:
            log_message("❌ No team mapping found - cannot proceed", self.league_code)
            return False
        
        # שלב 1: עדכון פרטי שחקנים
        if not self._update_player_details():
            return False
        
        # שלב 2: עדכון משחקים
        if not self._update_game_details():
            return False
        
        # שלב 3: חישוב ממוצעים
        if not self._calculate_averages():
            return False
        
        log_message("="*60, self.league_code)
        log_message("✅ SCRAPE COMPLETED SUCCESSFULLY", self.league_code)
        log_message("="*60, self.league_code)
        
        return True
    
    # ============================================
    # HELPER: DATE FORMATTING
    # ============================================
    
    def _normalize_date(self, date_str):
        """
        המרת תאריך לפורמט אחיד DD/MM/YYYY
        
        Args:
            date_str: תאריך בכל פורמט
        
        Returns:
            str: DD/MM/YYYY או None אם לא תקין
        """
        if not date_str or pd.isna(date_str):
            return None
        
        date_str = str(date_str).strip()
        
        # אם כבר בפורמט DD/MM/YYYY
        if '/' in date_str and len(date_str.split('/')) == 3:
            parts = date_str.split('/')
            if len(parts[0]) <= 2 and len(parts[2]) == 4:
                return date_str
        
        # אם בפורמט עם מקפים
        if '-' in date_str:
            parts = date_str.split('-')
            if len(parts) == 3:
                # בדוק אם זה DD-MM-YYYY
                if len(parts[0]) <= 2 and len(parts[2]) == 4:
                    try:
                        dt = datetime.strptime(date_str, '%d-%m-%Y')
                        return dt.strftime('%d/%m/%Y')
                    except:
                        pass
                
                # בדוק אם זה YYYY-MM-DD
                if len(parts[0]) == 4:
                    try:
                        dt = datetime.strptime(date_str, '%Y-%m-%d')
                        return dt.strftime('%d/%m/%Y')
                    except:
                        pass
        
        # אם מספר (Excel serial)
        try:
            excel_date = float(date_str)
            dt = datetime(1899, 12, 30) + pd.Timedelta(days=excel_date)
            return dt.strftime('%d/%m/%Y')
        except:
            pass
        
        log_message(f"   ⚠️  Could not parse date: '{date_str}'", self.league_code)
        return None
    
    # ============================================
    # PLAYER DETAILS
    # ============================================
    
    def _update_player_details(self):
        """עדכון פרטי שחקנים"""
        log_message("STEP 1: UPDATING PLAYER DETAILS", self.league_code)
        
        # גזירת רשימת שחקנים
        players = self._scrape_player_list()
        if not players:
            log_message("❌ No players found", self.league_code)
            return False
        
        log_message(f"Found {len(players)} players", self.league_code)
        
        # טעינת נתונים קיימים
        existing_details, existing_history = self._load_existing_player_data()
        
        details_data = []
        history_data = []
        all_seasons = set()
        
        new_players = 0
        updated_players = 0
        skipped_players = 0
        
        for i, player in enumerate(players, 1):
            player_name = player['Name']
            current_team_raw = player['Team']
            
            # נרמול שם קבוצה
            team_info = normalize_team_name_global(current_team_raw, self.league_id, self.team_mapping)
            current_team = team_info['club_name']
            
            # בדיקה האם צריך לגזור
            should_scrape, reason = self._needs_scraping(
                player_name, existing_details, existing_history
            )
            
            if should_scrape:
                log_message(f"[{i}/{len(players)}] Scraping: {player_name} ({reason})", self.league_code)
                
                details = self._scrape_player_details(player['URL'])
                history = self._scrape_player_history(player['URL'])
                
                all_seasons.update(history.keys())
                
                if player_name not in existing_details:
                    new_players += 1
                else:
                    updated_players += 1
                
                time.sleep(1)
            else:
                # שימוש בנתונים קיימים
                details = {
                    "Date Of Birth": existing_details[player_name].get("Date Of Birth", ""),
                    "Height": existing_details[player_name].get("Height", ""),
                    "Number": existing_details[player_name].get("Number", "")
                }
                
                if player_name in existing_history:
                    history = {k: v for k, v in existing_history[player_name].items() 
                              if k not in ["Name", "Current Team", "Date Of Birth", "Height", "Number", "player_id", "team_id", "league_id"]}
                    all_seasons.update(history.keys())
                else:
                    history = {}
                
                skipped_players += 1
            
            # יצירת IDs
            player_id = generate_player_id(player_name, details["Date Of Birth"], self.league_id)
            team_id = generate_team_id(current_team, self.league_id)
            
            # פרטים בסיסיים
            new_details = {
                "player_id": player_id,
                "Name": player_name,
                "Team": current_team,
                "team_id": team_id,
                "league_id": self.league_id,
                "Date Of Birth": details["Date Of Birth"],
                "Height": details["Height"],
                "Number": details["Number"]
            }
            
            # היסטוריה
            new_history = {
                "player_id": player_id,
                "Name": player_name,
                "Current Team": current_team,
                "team_id": team_id,
                "league_id": self.league_id,
                "Date Of Birth": details["Date Of Birth"],
                "Height": details["Height"],
                "Number": details["Number"]
            }
            new_history.update(history)
            
            details_data.append(new_details)
            history_data.append(new_history)
        
        # שמירה
        sorted_seasons = sorted(list(all_seasons), reverse=True)
        
        details_file = os.path.join(self.data_folder, f"{self.league_code}_player_details.csv")
        history_file = os.path.join(self.data_folder, f"{self.league_code}_player_history.csv")
        
        details_columns = ["player_id", "Name", "Team", "team_id", "league_id", "Date Of Birth", "Height", "Number"]
        history_columns = ["player_id", "Name", "Current Team", "team_id", "league_id", "Date Of Birth", "Height", "Number"] + sorted_seasons
        
        save_to_csv(details_data, details_file, details_columns)
        save_to_csv(history_data, history_file, history_columns)
        
        log_message(f"✅ Player details updated", self.league_code)
        log_message(f"   Total: {len(players)} | New: {new_players} | Updated: {updated_players} | Skipped: {skipped_players}", self.league_code)
        
        return True
    
    def _scrape_player_list(self):
        """גזירת רשימת שחקנים"""
        soup = get_soup(self.league_config['url'])
        if not soup:
            return []
        
        players = []
        for player_tag in soup.select(".player-gallery a.player"):
            name = player_tag.get_text("|", strip=True).split('|')[0]
            team = player_tag.find("span").get_text(strip=True)
            player_url = player_tag["href"]
            players.append({"Name": name, "Team": team, "URL": player_url})
        
        return players
    
    def _scrape_player_details(self, player_url):
        """גזירת פרטי שחקן בודד"""
        soup = get_soup(player_url)
        if not soup:
            return {"Date Of Birth": "", "Height": "", "Number": ""}
        
        dob = soup.find("div", class_="data-birthdate")
        height = soup.find("div", class_="data-other", attrs={"data-metric": "גובה"})
        
        # מספר שחקן
        number = ""
        general_ul = soup.find("ul", class_="general")
        if general_ul:
            for li in general_ul.find_all("li"):
                label = li.find("span", class_="label")
                if label and "מספר" in label.text:
                    data_span = li.find("span", class_="data-number")
                    if data_span:
                        number = data_span.get_text(strip=True)
                        break
        
        dob_text = dob.get_text("|", strip=True).split("|")[-1] if dob else ""
        dob_formatted = "/".join(dob_text.split("-")[::-1]) if dob_text else ""
        
        return {
            "Date Of Birth": dob_formatted,
            "Height": height.get_text("|", strip=True).split("|")[-1] if height else "",
            "Number": number
        }
    
    def _scrape_player_history(self, player_url):
        """גזירת היסטוריית קבוצות של שחקן"""
        soup = get_soup(player_url)
        if not soup:
            return {}
        
        history_tag = soup.find("div", class_="data-teams")
        history = {}
        youth_count = 0
        
        if history_tag:
            br_tags = history_tag.find_all('br')
            
            for br in br_tags:
                season_span = br.find_next_sibling('span', title=True)
                
                if season_span:
                    season_raw = season_span.get_text(strip=True)
                    season = normalize_season(season_raw)
                    
                    team_link = season_span.find_next_sibling('a')
                    if team_link:
                        team = team_link.get_text(strip=True)
                        league_link = team_link.find_next_sibling('a')
                        
                        if league_link:
                            league = league_link.get_text(strip=True)
                            
                            if "נוער" in league:
                                youth_count += 1
                                if youth_count > 1:
                                    break
                            
                            if season in history:
                                history[season] += f", {team} ({league})"
                            else:
                                history[season] = f"{team} ({league})"
        
        return history
    
    def _load_existing_player_data(self):
        """טעינת נתוני שחקנים קיימים"""
        existing_details = {}
        existing_history = {}
        
        details_file = os.path.join(self.data_folder, f"{self.league_code}_player_details.csv")
        history_file = os.path.join(self.data_folder, f"{self.league_code}_player_history.csv")
        
        if os.path.exists(details_file):
            df = pd.read_csv(details_file, encoding='utf-8-sig')
            for _, row in df.iterrows():
                existing_details[row['Name']] = row.to_dict()
        
        if os.path.exists(history_file):
            df = pd.read_csv(history_file, encoding='utf-8-sig')
            for _, row in df.iterrows():
                existing_history[row['Name']] = row.to_dict()
        
        return existing_details, existing_history
    
    def _needs_scraping(self, player_name, existing_details, existing_history):
        """בדיקה האם צריך לגזור שחקן"""
        # שחקן חדש - תמיד גוזרים
        if player_name not in existing_details:
            return True, "New player"
        
        # במצב QUICK - אם השחקן קיים, דלג
        if self.scrape_mode == 'quick':
            return False, "Existing player (quick mode)"
        
        # במצב FULL - בדיקה מקיפה
        player = existing_details[player_name]
        dob = player.get("Date Of Birth", "")
        height = player.get("Height", "")
        number = player.get("Number", "")
        
        if pd.isna(dob) or str(dob).strip() == "":
            return True, "Missing DOB"
        if pd.isna(height) or str(height).strip() == "":
            return True, "Missing Height"
        if pd.isna(number) or str(number).strip() == "":
            return True, "Missing Number"
        
        # בדיקת היסטוריה
        if player_name not in existing_history:
            return True, "No history"
        
        player_hist = existing_history[player_name]
        has_history = False
        for key, value in player_hist.items():
            if key not in ["Name", "Current Team", "Date Of Birth", "Height", "Number", "player_id", "team_id", "league_id"]:
                if not pd.isna(value) and str(value).strip() != "":
                    has_history = True
                    break
        
        if not has_history:
            return True, "No history data"
        
        return False, "Complete data"
    
    # ============================================
    # GAME DETAILS - FIXED VERSION
    # ============================================
    
    def _update_game_details(self):
        """עדכון משחקים וסטטיסטיקות - גרסה מתוקנת"""
        log_message("STEP 2: UPDATING GAME DETAILS", self.league_code)
        
        # הורדת לוח משחקים
        games_df = self._download_games_schedule()
        if games_df is None:
            log_message("❌ Failed to download games schedule", self.league_code)
            return False
        
        # תיקון שמות קבוצות בלוח המשחקים
        games_df = self._normalize_schedule_teams(games_df)
        
        # שמירת לוח מתוקן
        csv_path = os.path.join(self.games_folder, 'games_schedule.csv')
        games_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        log_message(f"✅ Games schedule normalized and saved", self.league_code)
        
        # גזירת סטטיסטיקות משחקים
        return self._scrape_all_games(games_df)
    
    def _normalize_schedule_teams(self, games_df):
        """
        נרמול שמות קבוצות בלוח המשחקים
        
        Args:
            games_df: DataFrame של לוח משחקים
        
        Returns:
            DataFrame: לוח משחקים מנורמל
        """
        log_message("   Normalizing team names in schedule...", self.league_code)
        
        # שינוי שמות עמודות לאנגלית
        column_renames = {
            'ליגה': 'League',
            'תאריך': 'Date',
            'מחזור': 'Round'
        }
        
        # שנה שמות שקיימים
        games_df = games_df.rename(columns={k: v for k, v in column_renames.items() if k in games_df.columns})
        
        # נרמול Home Team
        if 'Home Team' in games_df.columns:
            games_df['Home Team'] = games_df['Home Team'].apply(
                lambda x: normalize_team_name_global(x, self.league_id, self.team_mapping)['club_name']
                if pd.notna(x) else x
            )
        
        # נרמול Away Team
        if 'Away Team' in games_df.columns:
            games_df['Away Team'] = games_df['Away Team'].apply(
                lambda x: normalize_team_name_global(x, self.league_id, self.team_mapping)['club_name']
                if pd.notna(x) else x
            )
        
        # תיקון פורמט תאריך (עכשיו העמודה כבר נקראת Date)
        if 'Date' in games_df.columns:
            games_df['Date'] = games_df['Date'].apply(self._normalize_date)
        
        log_message(f"   ✅ Normalized {len(games_df)} games", self.league_code)
        
        return games_df
    
    def _download_games_schedule(self):
        """הורדת לוח משחקים מהאתר"""
        league_id_num = self._extract_league_id()
        if not league_id_num:
            log_message("❌ Could not find league_id", self.league_code)
            return None
        
        base_url = self.league_config['url'].rstrip('/')
        excel_url = f"{base_url}/?feed=xlsx&league_id={league_id_num}"
        
        try:
            response = requests.get(excel_url, timeout=15)
            response.raise_for_status()
            
            temp_excel = os.path.join(self.games_folder, 'temp_games.xlsx')
            with open(temp_excel, 'wb') as f:
                f.write(response.content)
            
            df = pd.read_excel(temp_excel)
            os.remove(temp_excel)
            
            log_message(f"✅ Downloaded schedule: {len(df)} games", self.league_code)
            return df
            
        except Exception as e:
            log_message(f"❌ Error downloading games: {e}", self.league_code)
            return None
    
    def _extract_league_id(self):
        """חילוץ league_id מהאתר"""
        soup = get_soup(self.league_config['url'])
        if not soup:
            return None
        
        export_link = soup.find('a', class_='export')
        if export_link and 'href' in export_link.attrs:
            href = export_link['href']
            if 'league_id=' in href:
                league_id = href.split('league_id=')[1].split('&')[0]
                return league_id
        
        return None
    
    def _scrape_all_games(self, games_df):
        """גזירת כל המשחקים שהסתיימו"""
        if 'Home Score' not in games_df.columns:
            log_message("❌ 'Home Score' column not found", self.league_code)
            return False
        
        completed_games = games_df[games_df['Home Score'].notna() & (games_df['Home Score'] != '')]
        log_message(f"   Found {len(completed_games)} completed games", self.league_code)
        
        # טעינת משחקים שכבר נגזרו
        existing_game_ids = self._load_existing_game_ids()
        if existing_game_ids:
            log_message(f"   Already scraped: {len(existing_game_ids)} games", self.league_code)
        
        # מציאת משחקים חדשים
        games_to_scrape = []
        for idx, row in completed_games.iterrows():
            game_code = row.get('Code')
            if pd.isna(game_code):
                continue
            
            game_id = generate_game_id(self.league_id, str(int(game_code)) if isinstance(game_code, float) else str(game_code))
            
            if game_id not in existing_game_ids:
                games_to_scrape.append((idx, row, game_id))
        
        if not games_to_scrape:
            log_message("   ✅ All games already scraped", self.league_code)
            return True
        
        log_message(f"   Scraping {len(games_to_scrape)} new games", self.league_code)
        
        # גזירה
        all_quarters = []
        all_player_stats = []
        all_team_stats = []
        
        for count, (idx, row, game_id) in enumerate(games_to_scrape, 1):
            home_team = row.get('Home Team', '')
            away_team = row.get('Away Team', '')
            game_date = self._normalize_date(row.get('Date', ''))
            
            log_message(f"   [{count}/{len(games_to_scrape)}] Game {game_id}: {home_team} vs {away_team}", self.league_code)
            
            try:
                quarters, players, teams = self._scrape_game_details(game_id, game_date)
                
                if quarters:
                    all_quarters.extend(quarters)
                if players:
                    all_player_stats.extend(players)
                if teams:
                    all_team_stats.extend(teams)
                
                if not quarters and not players and not teams:
                    log_message(f"   ⚠️  No stats found for game {game_id}", self.league_code)
                
            except Exception as e:
                log_message(f"   ❌ Error scraping game {game_id}: {e}", self.league_code)
                continue
            
            time.sleep(1)
        
        # שמירה
        if all_quarters:
            quarters_path = os.path.join(self.games_folder, 'game_quarters.csv')
            append_to_csv(all_quarters, quarters_path, 
                         columns=['game_id', 'league_id', 'team', 'team_id', 'opponent', 'opponent_id', 'game_date', 'quarter', 'score', 'score_against'])
            log_message(f"   ✅ Saved quarters: {len(all_quarters)} records", self.league_code)
        
        if all_player_stats:
            stats_path = os.path.join(self.games_folder, 'game_player_stats.csv')
            player_columns = ['game_id', 'league_id', 'player_id', 'player_name', 'team', 'team_id', 'game_date',
                             'number', 'starter', 'min', 'pts',
                             '2ptm', '2pta', '2pt_pct',
                             '3ptm', '3pta', '3pt_pct',
                             'fgm', 'fga', 'fg_pct',
                             'ftm', 'fta', 'ft_pct',
                             'def', 'off', 'reb', 'pf', 'pfa',
                             'stl', 'to', 'ast', 'blk', 'blka', 'rate']
            append_to_csv(all_player_stats, stats_path, columns=player_columns)
            log_message(f"   ✅ Saved player stats: {len(all_player_stats)} records", self.league_code)
        
        if all_team_stats:
            team_stats_path = os.path.join(self.games_folder, 'game_team_stats.csv')
            team_columns = ['game_id', 'league_id', 'team', 'team_id', 'opponent', 'opponent_id', 'game_date', 'pts',
                           '2ptm', '2pta', '2pt_pct',
                           '3ptm', '3pta', '3pt_pct',
                           'fgm', 'fga', 'fg_pct',
                           'ftm', 'fta', 'ft_pct',
                           'def', 'off', 'reb', 'pf', 'pfa',
                           'stl', 'to', 'ast', 'blk', 'blka', 'rate',
                           'second_chance_pts', 'bench_pts', 'fast_break_pts',
                           'points_in_paint', 'pts_off_turnovers']
            append_to_csv(all_team_stats, team_stats_path, columns=team_columns)
            log_message(f"   ✅ Saved team stats: {len(all_team_stats)} records", self.league_code)
        
        log_message(f"✅ Game stats updated: {len(games_to_scrape)} new games", self.league_code)
        return True
    
    def _load_existing_game_ids(self):
        """טעינת משחקים שכבר נגזרו"""
        existing_ids = set()
        
        quarters_path = os.path.join(self.games_folder, 'game_quarters.csv')
        if os.path.exists(quarters_path):
            try:
                df = pd.read_csv(quarters_path, encoding='utf-8-sig')
                if 'game_id' in df.columns:
                    existing_ids.update(df['game_id'].astype(str).unique())
            except Exception as e:
                log_message(f"   ⚠️  Could not read existing quarters: {e}", self.league_code)
        
        return existing_ids
    
    def _scrape_game_details(self, game_id, game_date):
        """גזירת פרטי משחק בודד"""
        # חילוץ קוד המשחק המקורי
        original_code = game_id.split('_')[-1]
        game_url = f"https://ibasketball.co.il/match/{original_code}/"
        
        soup = get_soup(game_url)
        if not soup:
            return None, None, None
        
        quarters = self._scrape_quarter_scores(soup, game_id, game_date)
        players = self._scrape_player_stats(soup, game_id, game_date)
        teams = self._scrape_team_stats(soup, game_id, game_date)
        
        return quarters, players, teams
    
    def _scrape_quarter_scores(self, soup, game_id, game_date):
        """גזירת ניקוד לפי רבעים"""
        quarters_data = []
        
        try:
            results_table = soup.find('table', class_='sp-event-results')
            if not results_table:
                return quarters_data
            
            rows = results_table.find('tbody').find_all('tr')
            
            teams = []
            for row in rows:
                team_cell = row.find('td', class_='data-name')
                if team_cell:
                    team_link = team_cell.find('a')
                    team_name_raw = team_link.text.strip() if team_link else team_cell.text.strip()
                    team_info = normalize_team_name_global(team_name_raw, self.league_id, self.team_mapping)
                    teams.append(team_info['club_name'])
            
            if len(teams) != 2:
                return quarters_data
            
            for idx, row in enumerate(rows):
                team_name = teams[idx]
                opponent_name = teams[1 - idx]
                team_id = generate_team_id(team_name, self.league_id)
                opponent_id = generate_team_id(opponent_name, self.league_id)
                
                q1 = row.find('td', class_='data-one')
                q2 = row.find('td', class_='data-two')
                q3 = row.find('td', class_='data-three')
                q4 = row.find('td', class_='data-four')
                
                quarters = [
                    ('Q1', q1.text.strip() if q1 else '0'),
                    ('Q2', q2.text.strip() if q2 else '0'),
                    ('Q3', q3.text.strip() if q3 else '0'),
                    ('Q4', q4.text.strip() if q4 else '0')
                ]
                
                opponent_row = rows[1 - idx]
                opp_q1 = opponent_row.find('td', class_='data-one')
                opp_q2 = opponent_row.find('td', class_='data-two')
                opp_q3 = opponent_row.find('td', class_='data-three')
                opp_q4 = opponent_row.find('td', class_='data-four')
                
                opponent_quarters = [
                    opp_q1.text.strip() if opp_q1 else '0',
                    opp_q2.text.strip() if opp_q2 else '0',
                    opp_q3.text.strip() if opp_q3 else '0',
                    opp_q4.text.strip() if opp_q4 else '0'
                ]
                
                for (quarter, score), opp_score in zip(quarters, opponent_quarters):
                    quarters_data.append({
                        'game_id': game_id,
                        'league_id': self.league_id,
                        'team': team_name,
                        'team_id': team_id,
                        'opponent': opponent_name,
                        'opponent_id': opponent_id,
                        'game_date': game_date,
                        'quarter': quarter,
                        'score': int(score) if score.isdigit() else 0,
                        'score_against': int(opp_score) if opp_score.isdigit() else 0
                    })
            
            return quarters_data
            
        except Exception as e:
            log_message(f"   ❌ Error parsing quarters: {e}", self.league_code)
            return quarters_data
    
    def _scrape_player_stats(self, soup, game_id, game_date):
        """גזירת סטטיסטיקות שחקנים"""
        player_stats = []
        
        try:
            performance_sections = soup.find_all('div', class_='sp-template-event-performance-values')
            
            # טעינת פרטי שחקנים לקבלת IDs
            player_details = self._load_existing_player_data()[0]
            
            for section in performance_sections:
                team_header = section.find('h4', class_='sp-table-caption')
                if not team_header:
                    continue
                
                team_name_raw = team_header.text.strip()
                team_info = normalize_team_name_global(team_name_raw, self.league_id, self.team_mapping)
                team_name = team_info['club_name']
                team_id = generate_team_id(team_name, self.league_id)
                
                table = section.find('table', class_='sp-event-performance')
                if not table:
                    continue
                
                headers = []
                thead = table.find('thead')
                if thead:
                    header_row = thead.find('tr')
                    for th in header_row.find_all('th'):
                        headers.append(th.text.strip())
                
                tbody = table.find('tbody')
                if not tbody:
                    continue
                
                for row in tbody.find_all('tr'):
                    if 'sp-total-row' in row.get('class', []):
                        continue
                    
                    player_data = {
                        'game_id': game_id,
                        'league_id': self.league_id,
                        'team': team_name,
                        'team_id': team_id,
                        'game_date': game_date
                    }
                    
                    row_classes = row.get('class', [])
                    player_data['starter'] = 1 if 'lineup' in row_classes else 0
                    
                    cells = row.find_all('td')
                    
                    for idx, cell in enumerate(cells):
                        if idx < len(headers):
                            header = headers[idx]
                            
                            if header == 'שחקן' or 'data-name' in cell.get('class', []):
                                player_link = cell.find('a')
                                if player_link:
                                    player_data['player_name'] = player_link.text.strip()
                                else:
                                    player_data['player_name'] = cell.text.strip()
                            else:
                                data_key = cell.get('data-key', header)
                                player_data[data_key] = cell.text.strip()
                    
                    if 'player_name' in player_data and player_data['player_name']:
                        minutes = player_data.get('min', '00:00')
                        if minutes != '00:00' and minutes != '0:00':
                            # המרת דקות
                            if 'min' in player_data:
                                min_str = player_data['min']
                                try:
                                    if ':' in min_str:
                                        parts = min_str.split(':')
                                        mins = int(parts[0])
                                        secs = int(parts[1])
                                        if secs >= 30:
                                            mins += 1
                                        player_data['min'] = mins
                                    else:
                                        player_data['min'] = int(min_str)
                                except:
                                    player_data['min'] = 0
                            
                            # מספר חולצה
                            if '#' in player_data:
                                player_data['number'] = player_data.pop('#')
                            
                            # יצירת player_id
                            player_name = player_data['player_name']
                            if player_name in player_details:
                                dob = player_details[player_name].get('Date Of Birth', '')
                                player_data['player_id'] = generate_player_id(player_name, dob, self.league_id)
                            else:
                                player_data['player_id'] = generate_player_id(player_name, '', self.league_id)
                            
                            # עיבוד סטטיסטיקות זריקה
                            player_data.pop('pm', None)
                            player_data = self._split_shooting_stats(player_data)
                            player_stats.append(player_data)
            
            return player_stats
            
        except Exception as e:
            log_message(f"   ❌ Error parsing player stats: {e}", self.league_code)
            return player_stats
    
    def _scrape_team_stats(self, soup, game_id, game_date):
        """גזירת סטטיסטיקות קבוצתיות"""
        team_stats = []
        
        try:
            performance_sections = soup.find_all('div', class_='sp-template-event-performance-values')
            
            for section in performance_sections:
                team_header = section.find('h4', class_='sp-table-caption')
                if not team_header:
                    continue
                
                team_name_raw = team_header.text.strip()
                team_info = normalize_team_name_global(team_name_raw, self.league_id, self.team_mapping)
                team_name = team_info['club_name']
                team_id = generate_team_id(team_name, self.league_id)
                
                table = section.find('table', class_='sp-event-performance')
                if not table:
                    continue
                
                thead = table.find('thead')
                header_keys = []
                if thead:
                    header_row = thead.find('tr')
                    for th in header_row.find_all('th'):
                        data_key = None
                        th_classes = th.get('class', [])
                        for cls in th_classes:
                            if cls.startswith('data-'):
                                data_key = cls.replace('data-', '')
                                break
                        header_keys.append(data_key)
                
                # מציאת שורת סיכום
                total_row = None
                tfoot = table.find('tfoot')
                if tfoot:
                    total_row = tfoot.find('tr', class_='sp-total-row')
                
                if not total_row:
                    tbody = table.find('tbody')
                    if tbody:
                        all_rows = tbody.find_all('tr')
                        for row in reversed(all_rows):
                            name_cell = row.find('td', class_='data-name')
                            if name_cell and 'סך הכל' in name_cell.text:
                                total_row = row
                                break
                
                if total_row:
                    # מציאת קבוצה יריבה
                    opponent_name = None
                    for other_section in performance_sections:
                        other_header = other_section.find('h4', class_='sp-table-caption')
                        if other_header:
                            other_team_raw = other_header.text.strip()
                            other_team_info = normalize_team_name_global(other_team_raw, self.league_id, self.team_mapping)
                            other_team = other_team_info['club_name']
                            if other_team != team_name:
                                opponent_name = other_team
                                break
                    
                    opponent_id = generate_team_id(opponent_name, self.league_id) if opponent_name else None
                    
                    stats_dict = {
                        'game_id': game_id,
                        'league_id': self.league_id,
                        'team': team_name,
                        'team_id': team_id,
                        'opponent': opponent_name,
                        'opponent_id': opponent_id,
                        'game_date': game_date
                    }
                    
                    cells = total_row.find_all('td')
                    
                    for idx, cell in enumerate(cells):
                        cell_classes = cell.get('class', [])
                        if 'data-name' in cell_classes:
                            continue
                        
                        data_key = None
                        for cls in cell_classes:
                            if cls.startswith('data-'):
                                data_key = cls.replace('data-', '')
                                break
                        
                        if not data_key and idx < len(header_keys):
                            data_key = header_keys[idx]
                        
                        if data_key:
                            value = cell.text.strip()
                            stats_dict[data_key] = value
                    
                    # עיבוד סטטיסטיקות זריקה
                    stats_dict = self._split_shooting_stats(stats_dict)
                    
                    # הסרת שדות מיותרים
                    stats_dict.pop('min', None)
                    stats_dict.pop('pm', None)
                    stats_dict.pop('#', None)
                    stats_dict.pop('number', None)
                    
                    # סטטיסטיקות נוספות
                    team_stats_div = section.find('div', class_='team-stats')
                    
                    if team_stats_div:
                        labels = team_stats_div.find_all('label')
                        for label in labels:
                            stat_text = label.contents[0].strip() if label.contents else ''
                            stat_value_span = label.find('span')
                            
                            if stat_value_span:
                                stat_value = stat_value_span.text.strip()
                                
                                stat_mapping = {
                                    'נקודות מהזדמנויות שניות:': 'second_chance_pts',
                                    'נקודות ספסל:': 'bench_pts',
                                    'נקודות ממתפרצת:': 'fast_break_pts',
                                    'נקודות בצבע:': 'points_in_paint',
                                    'נקודות מאובדנים:': 'pts_off_turnovers'
                                }
                                
                                stat_key = stat_mapping.get(stat_text, stat_text)
                                stats_dict[stat_key] = int(stat_value) if stat_value.isdigit() else stat_value
                    
                    team_stats.append(stats_dict)
            
            return team_stats
            
        except Exception as e:
            log_message(f"   ❌ Error parsing team stats: {e}", self.league_code)
            return team_stats
    
    def _split_shooting_stats(self, stats_dict):
        """פיצול סטטיסטיקות זריקה (X-Y) לעמודות נפרדות"""
        
        # 2-point shots
        if 'fgs' in stats_dict:
            if '-' in str(stats_dict['fgs']):
                parts = stats_dict['fgs'].split('-')
                stats_dict['2ptm'] = int(parts[0].strip()) if parts[0].strip().isdigit() else 0
                stats_dict['2pta'] = int(parts[1].strip()) if parts[1].strip().isdigit() else 0
                del stats_dict['fgs']
        
        # 3-point shots
        if 'threeps' in stats_dict:
            if '-' in str(stats_dict['threeps']):
                parts = stats_dict['threeps'].split('-')
                stats_dict['3ptm'] = int(parts[0].strip()) if parts[0].strip().isdigit() else 0
                stats_dict['3pta'] = int(parts[1].strip()) if parts[1].strip().isdigit() else 0
                del stats_dict['threeps']
        
        # Free throws
        if 'fts' in stats_dict:
            if '-' in str(stats_dict['fts']):
                parts = stats_dict['fts'].split('-')
                stats_dict['ftm'] = int(parts[0].strip()) if parts[0].strip().isdigit() else 0
                stats_dict['fta'] = int(parts[1].strip()) if parts[1].strip().isdigit() else 0
                del stats_dict['fts']
        
        # ויידוא שכל הערכים מספריים
        for key in ['2ptm', '2pta', '3ptm', '3pta', 'ftm', 'fta']:
            if key in stats_dict:
                if isinstance(stats_dict[key], str):
                    val = stats_dict[key].strip()
                    stats_dict[key] = int(val) if val.isdigit() else 0
                elif not isinstance(stats_dict[key], int):
                    try:
                        stats_dict[key] = int(stats_dict[key])
                    except:
                        stats_dict[key] = 0
        
        # חישוב field goals (2pt + 3pt)
        two_ptm = stats_dict.get('2ptm', 0)
        two_pta = stats_dict.get('2pta', 0)
        three_ptm = stats_dict.get('3ptm', 0)
        three_pta = stats_dict.get('3pta', 0)
        
        stats_dict['fgm'] = two_ptm + three_ptm
        stats_dict['fga'] = two_pta + three_pta
        
        # חישוב אחוזים
        if two_pta > 0:
            stats_dict['2pt_pct'] = round((two_ptm / two_pta) * 100, 1)
        else:
            stats_dict['2pt_pct'] = 0.0
        
        if three_pta > 0:
            stats_dict['3pt_pct'] = round((three_ptm / three_pta) * 100, 1)
        else:
            stats_dict['3pt_pct'] = 0.0
        
        if stats_dict['fga'] > 0:
            stats_dict['fg_pct'] = round((stats_dict['fgm'] / stats_dict['fga']) * 100, 1)
        else:
            stats_dict['fg_pct'] = 0.0
        
        ftm = stats_dict.get('ftm', 0)
        fta = stats_dict.get('fta', 0)
        if fta > 0:
            stats_dict['ft_pct'] = round((ftm / fta) * 100, 1)
        else:
            stats_dict['ft_pct'] = 0.0
        
        # הסרת עמודות ישנות
        for key in ['fgpercent', 'threeppercent', 'ftpercent']:
            if key in stats_dict:
                del stats_dict[key]
        
        return stats_dict
    
    # ============================================
    # CALCULATE AVERAGES
    # ============================================
    
    def _calculate_averages(self):
        """חישוב ממוצעי שחקנים, קבוצות ויריבים"""
        log_message("STEP 3: CALCULATING AVERAGES", self.league_code)
        
        player_stats_file = os.path.join(self.games_folder, "game_player_stats.csv")
        team_stats_file = os.path.join(self.games_folder, "game_team_stats.csv")
        
        if not os.path.exists(player_stats_file):
            log_message(f"❌ No player stats found", self.league_code)
            return False
        
        if not os.path.exists(team_stats_file):
            log_message(f"❌ No team stats found", self.league_code)
            return False
        
        try:
            player_df = pd.read_csv(player_stats_file, encoding='utf-8-sig')
            team_df = pd.read_csv(team_stats_file, encoding='utf-8-sig')
        except Exception as e:
            log_message(f"❌ Error reading stats files: {e}", self.league_code)
            return False
        
        # חישוב ממוצעי שחקנים
        player_avg = self._calculate_player_averages(player_df)
        if player_avg is not None:
            player_averages_file = os.path.join(self.data_folder, f"{self.league_code}_player_averages.csv")
            player_avg.to_csv(player_averages_file, index=False, encoding='utf-8-sig')
            log_message(f"✅ Player averages calculated: {len(player_avg)} players", self.league_code)
        
        # חישוב ממוצעי קבוצות
        team_avg = self._calculate_team_averages(team_df)
        if team_avg is not None:
            # חישוב ממוצעי יריבים
            opponent_avg = self._calculate_opponent_averages(team_df)
            
            # הוספת נקודות מול (pts_allowed)
            if opponent_avg is not None and 'opp_pts' in opponent_avg.columns:
                opp_pts = opponent_avg[['team', 'opp_pts', 'opp_pts_rank']].copy()
                opp_pts.rename(columns={
                    'opp_pts': 'pts_allowed',
                    'opp_pts_rank': 'pts_allowed_rank'
                }, inplace=True)
                
                team_avg = pd.merge(team_avg, opp_pts, on='team', how='left')
                
                # סידור עמודות
                cols = team_avg.columns.tolist()
                if 'pts' in cols and 'pts_allowed' in cols:
                    pts_idx = cols.index('pts_rank') if 'pts_rank' in cols else cols.index('pts')
                    cols.remove('pts_allowed')
                    cols.remove('pts_allowed_rank')
                    cols.insert(pts_idx + 1, 'pts_allowed')
                    cols.insert(pts_idx + 2, 'pts_allowed_rank')
                    team_avg = team_avg[cols]
            
            team_averages_file = os.path.join(self.data_folder, f"{self.league_code}_team_averages.csv")
            team_avg.to_csv(team_averages_file, index=False, encoding='utf-8-sig')
            log_message(f"✅ Team averages calculated: {len(team_avg)} teams", self.league_code)
            
            # שמירת ממוצעי יריבים
            if opponent_avg is not None:
                opponent_averages_file = os.path.join(self.data_folder, f"{self.league_code}_opponent_averages.csv")
                opponent_avg.to_csv(opponent_averages_file, index=False, encoding='utf-8-sig')
                log_message(f"✅ Opponent averages calculated: {len(opponent_avg)} teams", self.league_code)
        
        return True
    
    def _calculate_player_averages(self, player_df):
        """חישוב ממוצעי שחקנים"""
        numeric_cols = [
            'pts', '2ptm', '2pta', '3ptm', '3pta', 'fgm', 'fga',
            'ftm', 'fta', 'def', 'off', 'reb', 'pf', 'pfa',
            'stl', 'to', 'ast', 'blk', 'blka', 'rate', 'min'
        ]
        
        for col in numeric_cols:
            if col in player_df.columns:
                player_df[col] = pd.to_numeric(player_df[col], errors='coerce')
        
        if 'starter' in player_df.columns:
            player_df['starter'] = pd.to_numeric(player_df['starter'], errors='coerce')
        
        agg_dict = {col: 'mean' for col in numeric_cols if col in player_df.columns}
        agg_dict['game_id'] = 'count'
        
        if 'starter' in player_df.columns:
            agg_dict['starter'] = 'sum'
        
        if 'player_id' not in player_df.columns:
            log_message("⚠️  player_id not found in player stats", self.league_code)
            return None
        
        player_avg = player_df.groupby(['player_id', 'player_name', 'team', 'team_id']).agg(agg_dict).reset_index()
        player_avg.rename(columns={'game_id': 'games_played'}, inplace=True)
        
        if 'starter' in player_avg.columns:
            player_avg.rename(columns={'starter': 'games_started'}, inplace=True)
        
        # חישוב אחוזים
        if '2ptm' in player_avg.columns and '2pta' in player_avg.columns:
            player_avg['2pt_pct'] = (player_avg['2ptm'] / player_avg['2pta']).fillna(0) * 100
        
        if '3ptm' in player_avg.columns and '3pta' in player_avg.columns:
            player_avg['3pt_pct'] = (player_avg['3ptm'] / player_avg['3pta']).fillna(0) * 100
        
        if 'fgm' in player_avg.columns and 'fga' in player_avg.columns:
            player_avg['fg_pct'] = (player_avg['fgm'] / player_avg['fga']).fillna(0) * 100
        
        if 'ftm' in player_avg.columns and 'fta' in player_avg.columns:
            player_avg['ft_pct'] = (player_avg['ftm'] / player_avg['fta']).fillna(0) * 100
        
        player_avg = player_avg.round(1)
        
        # הוספת league_id
        player_avg['league_id'] = self.league_id
        
        # סידור עמודות
        desired_order = [
            'player_id', 'player_name', 'team', 'team_id', 'league_id',
            'games_played', 'games_started', 'min', 'pts',
            'fgm', 'fga', 'fg_pct',
            '2ptm', '2pta', '2pt_pct',
            '3ptm', '3pta', '3pt_pct',
            'ftm', 'fta', 'ft_pct',
            'def', 'off', 'reb',
            'ast', 'stl', 'to', 'pf', 'pfa',
            'blk', 'blka', 'rate'
        ]
        
        player_avg = player_avg[[col for col in desired_order if col in player_avg.columns]]
        
        return player_avg
    
    def _calculate_team_averages(self, team_df):
        """חישוב ממוצעי קבוצות"""
        numeric_team_cols = [
            'pts', '2ptm', '2pta', '3ptm', '3pta', 'fgm', 'fga',
            'ftm', 'fta', 'def', 'off', 'reb', 'pf', 'pfa',
            'stl', 'to', 'ast', 'blk', 'blka', 'rate',
            'second_chance_pts', 'bench_pts', 'fast_break_pts',
            'points_in_paint', 'pts_off_turnovers'
        ]
        
        for col in numeric_team_cols:
            if col in team_df.columns:
                team_df[col] = pd.to_numeric(team_df[col], errors='coerce')
        
        if 'team_id' not in team_df.columns:
            log_message("⚠️  team_id not found in team stats", self.league_code)
            return None
        
        team_avg = team_df.groupby(['team', 'team_id']).agg({
            **{col: 'mean' for col in numeric_team_cols if col in team_df.columns},
            'game_id': 'count'
        }).reset_index()
        
        team_avg.rename(columns={'game_id': 'games_played'}, inplace=True)
        
        # חישוב possessions
        if all(col in team_avg.columns for col in ['fga', 'fta', 'off', 'to']):
            team_avg['possessions'] = (
                team_avg['fga'] +
                (0.44 * team_avg['fta']) -
                team_avg['off'] +
                team_avg['to']
            ).round(2)
        
        # חישוב אחוזים
        if '2ptm' in team_avg.columns and '2pta' in team_avg.columns:
            team_avg['2pt_pct'] = (team_avg['2ptm'] / team_avg['2pta']).fillna(0) * 100
        
        if '3ptm' in team_avg.columns and '3pta' in team_avg.columns:
            team_avg['3pt_pct'] = (team_avg['3ptm'] / team_avg['3pta']).fillna(0) * 100
        
        if 'fgm' in team_avg.columns and 'fga' in team_avg.columns:
            team_avg['fg_pct'] = (team_avg['fgm'] / team_avg['fga']).fillna(0) * 100
        
        if 'ftm' in team_avg.columns and 'fta' in team_avg.columns:
            team_avg['ft_pct'] = (team_avg['ftm'] / team_avg['fta']).fillna(0) * 100
        
        team_avg = team_avg.round(1)
        
        # הוספת דירוגים
        higher_better_cols = [
            'pts', 'fgm', 'fga', 'fg_pct', '2ptm', '2pta', '2pt_pct',
            '3ptm', '3pta', '3pt_pct', 'ftm', 'fta', 'ft_pct',
            'def', 'off', 'reb', 'ast', 'stl', 'blk', 'pfa', 'rate',
            'second_chance_pts', 'bench_pts', 'fast_break_pts',
            'points_in_paint', 'pts_off_turnovers', 'possessions'
        ]
        
        lower_better_cols = ['to', 'pf', 'blka']
        
        for col in higher_better_cols:
            if col in team_avg.columns:
                rank_col = f"{col}_rank"
                team_avg[rank_col] = team_avg[col].rank(ascending=False, method='min').astype(int)
        
        for col in lower_better_cols:
            if col in team_avg.columns:
                rank_col = f"{col}_rank"
                team_avg[rank_col] = team_avg[col].rank(ascending=True, method='min').astype(int)
        
        # הוספת league_id
        team_avg['league_id'] = self.league_id
        
        # סידור עמודות
        final_cols = ['team', 'team_id', 'league_id', 'games_played']
        for col in team_avg.columns:
            if col not in final_cols and not col.endswith('_rank'):
                final_cols.append(col)
                rank_col = f"{col}_rank"
                if rank_col in team_avg.columns:
                    final_cols.append(rank_col)
        
        team_avg = team_avg[final_cols]
        
        return team_avg
    
    def _calculate_opponent_averages(self, team_df):
        """חישוב ממוצעי יריבים"""
        numeric_team_cols = [
            'pts', '2ptm', '2pta', '3ptm', '3pta', 'fgm', 'fga',
            'ftm', 'fta', 'def', 'off', 'reb', 'pf', 'pfa',
            'stl', 'to', 'ast', 'blk', 'blka', 'rate',
            'second_chance_pts', 'fast_break_pts',
            'points_in_paint', 'pts_off_turnovers'
        ]
        
        opponent_stats = []
        
        for game_id in team_df['game_id'].unique():
            game_teams = team_df[team_df['game_id'] == game_id]
            if len(game_teams) == 2:
                team1 = game_teams.iloc[0]
                team2 = game_teams.iloc[1]
                
                opp1 = {'team': team1['team'], 'team_id': team1['team_id'], 'game_id': game_id}
                opp2 = {'team': team2['team'], 'team_id': team2['team_id'], 'game_id': game_id}
                
                for col in numeric_team_cols:
                    if col in team2:
                        opp1[f"opp_{col}"] = team2[col]
                    if col in team1:
                        opp2[f"opp_{col}"] = team1[col]
                
                opponent_stats.append(opp1)
                opponent_stats.append(opp2)
        
        if not opponent_stats:
            return None
        
        opp_df = pd.DataFrame(opponent_stats)
        
        opp_cols = [col for col in opp_df.columns if col.startswith('opp_')]
        opponent_avg = opp_df.groupby(['team', 'team_id']).agg({
            **{col: 'mean' for col in opp_cols},
            'game_id': 'count'
        }).reset_index()
        
        opponent_avg.rename(columns={'game_id': 'games_played'}, inplace=True)
        
        # חישוב אחוזים
        if 'opp_2ptm' in opponent_avg.columns and 'opp_2pta' in opponent_avg.columns:
            opponent_avg['opp_2pt_pct'] = (opponent_avg['opp_2ptm'] / opponent_avg['opp_2pta']).fillna(0) * 100
        
        if 'opp_3ptm' in opponent_avg.columns and 'opp_3pta' in opponent_avg.columns:
            opponent_avg['opp_3pt_pct'] = (opponent_avg['opp_3ptm'] / opponent_avg['opp_3pta']).fillna(0) * 100
        
        if 'opp_fgm' in opponent_avg.columns and 'opp_fga' in opponent_avg.columns:
            opponent_avg['opp_fg_pct'] = (opponent_avg['opp_fgm'] / opponent_avg['opp_fga']).fillna(0) * 100
        
        if 'opp_ftm' in opponent_avg.columns and 'opp_fta' in opponent_avg.columns:
            opponent_avg['opp_ft_pct'] = (opponent_avg['opp_ftm'] / opponent_avg['opp_fta']).fillna(0) * 100
        
        if all(col in opponent_avg.columns for col in ['opp_fga', 'opp_fta', 'opp_off', 'opp_to']):
            opponent_avg['opp_possessions'] = (
                opponent_avg['opp_fga'] +
                (0.44 * opponent_avg['opp_fta']) -
                opponent_avg['opp_off'] +
                opponent_avg['opp_to']
            ).round(2)
        
        opponent_avg = opponent_avg.round(1)
        
        # הסרת עמודות מיותרות
        cols_to_drop = ['opp_bench_pts', 'opp_pfa']
        for col in cols_to_drop:
            if col in opponent_avg.columns:
                opponent_avg = opponent_avg.drop(col, axis=1)
        
        # דירוגים
        opp_stat_cols = [col for col in opponent_avg.columns if col.startswith('opp_') and col != 'opp_to']
        
        for col in opp_stat_cols:
            rank_col = f"{col}_rank"
            opponent_avg[rank_col] = opponent_avg[col].rank(ascending=True, method='min').astype(int)
        
        if 'opp_to' in opponent_avg.columns:
            opponent_avg['opp_to_rank'] = opponent_avg['opp_to'].rank(ascending=False, method='min').astype(int)
        
        # הוספת league_id
        opponent_avg['league_id'] = self.league_id
        
        # סידור עמודות
        final_opp_cols = ['team', 'team_id', 'league_id', 'games_played']
        for col in opponent_avg.columns:
            if col not in final_opp_cols and not col.endswith('_rank'):
                final_opp_cols.append(col)
                rank_col = f"{col}_rank"
                if rank_col in opponent_avg.columns:
                    final_opp_cols.append(rank_col)
        
        opponent_avg = opponent_avg[final_opp_cols]
        
        return opponent_avg