# -*- coding: utf-8 -*-
"""
scrapers/winner.py
==================
Scraper לליגת Winner (basket.co.il) - עם נורמליזציה של קבוצות

גרסה מעודכנת: משתמש ב-DataNormalizer כמו IBasketballScraper
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import re, unicodedata
import time
from pathlib import Path

from utils import log_message, save_to_csv, get_soup
from .base_scraper import BaseScraper
from .processors import DataNormalizer
from models import generate_player_id
from datetime import datetime


class WinnerScraper(BaseScraper):
    """
    Scraper לליגת Winner מ-basket.co.il
    יורש מ-BaseScraper ומשתמש ב-DataNormalizer לשמות קבוצות
    """
    
    def _init_processors(self):
        """אתחול ייחודי לליגת Winner"""
        self.base_url = "https://basket.co.il"
        self.board_ids = self.league_config.get('board_ids', [])
        
        # 🆕 שימוש ב-DataNormalizer במקום team_id_map
        self.normalizer = DataNormalizer(self.league_id, self.league_code)
        
        # טעינת מיפוי קבוצות
        if not self.normalizer.load_team_mapping():
            raise ValueError("Failed to load team mapping")
        
        self.log(f"Initialized WinnerScraper with {len(self.board_ids)} boards")
    
    # ============================================
    # PLAYER DETAILS
    # ============================================

    def _update_player_details(self):
        """עדכון פרטי שחקנים ללא מחיקת נתונים קיימים"""
        self.log("STEP 1: UPDATING PLAYER DETAILS")
    
        all_details = []
        all_history = []
    
        # 🟩 שלב 1: טעינת נתונים קיימים
        details_file = Path(self.data_folder) / f"{self.league_code}_player_details.csv"
        history_file = Path(self.data_folder) / f"{self.league_code}_player_history.csv"
    
        existing_details = {}
        existing_history = {}
    
        if details_file.exists():
            df = pd.read_csv(details_file, encoding='utf-8-sig')
            if "Name" in df.columns:
                existing_details = {row["Name"]: row.to_dict() for _, row in df.iterrows()}
    
        if history_file.exists():
            df = pd.read_csv(history_file, encoding='utf-8-sig')
            if "Name" in df.columns:
                existing_history = {row["Name"]: row.to_dict() for _, row in df.iterrows()}
    
        self.log(f"Loaded {len(existing_details)} existing players")
    
        # 🟩 שלב 2: נירמול שמות להשוואה
        import re, unicodedata
    
        def normalize_name(name: str) -> str:
            if not name:
                return ""
            name = unicodedata.normalize("NFKC", name)
            name = (name.replace("’", "'")
                        .replace("`", "'")
                        .replace("׳", "'")
                        .replace("״", "'")
                        .replace('"', "'"))
            name = re.sub(r"[\u200f\u200e\u202b\u202c\u00A0]", "", name)
            name = re.sub(r"\s+", "", name.strip())
            name = name.replace("-", "").replace("–", "").replace("—", "")
            return name
    
        normalized_existing = {normalize_name(k): v for k, v in existing_details.items()}
    
        total_players = 0
        new_players = 0
        updated_players = 0
        skipped_players = 0
    
        # 🟩 שלב 3: קבלת קבוצות
        web_team_ids = self._get_web_team_ids()
        if not web_team_ids:
            self.log("❌ No teams found in mapping")
            return False
    
        # 🟩 שלב 4: מעבר על קבוצות ושחקנים
        for web_team_id in web_team_ids:
            self.log(f"Processing team web_id: {web_team_id}")
    
            players = self._get_team_players(web_team_id)
            self.log(f"  Found {len(players)} players")
    
            for player in players:
                total_players += 1
                player_name = player["Name"]
                normalized_name = normalize_name(player_name)
    
                if normalized_name in normalized_existing:
                    skipped_players += 1
                    orig_data = normalized_existing[normalized_name]
                    all_details.append(orig_data)
                    all_history.append(existing_history.get(orig_data.get("Name"), {}))
                    continue

    
                self.log(f"  ↳ Scraping {player_name}")
                details, history = self._scrape_player_details(player)
    
                if details:
                    all_details.append(details)
                    all_history.append(history)
                    if normalized_name in normalized_existing:
                        updated_players += 1
                    else:
                        new_players += 1
    
                time.sleep(1)
    
        # 🟩 שלב 5: שמירה משולבת
        new_details_df = pd.DataFrame(all_details)
        new_history_df = pd.DataFrame(all_history)
    
        # טען קבצים קיימים מחדש (במקרה של quick mode)
        existing_df = pd.read_csv(details_file, encoding='utf-8-sig') if details_file.exists() else pd.DataFrame()
        history_df = pd.read_csv(history_file, encoding='utf-8-sig') if history_file.exists() else pd.DataFrame()
    
        # 🟦 מיזוג ללא מחיקה של עמודות
        if not existing_df.empty and "Name" in existing_df.columns and "Name" in new_details_df.columns:
            merged_df = pd.concat([existing_df, new_details_df], ignore_index=True)
            merged_df = merged_df.drop_duplicates(subset=["Name"], keep="last")
        else:
            merged_df = new_details_df
    
        if not history_df.empty and "Name" in history_df.columns and "Name" in new_history_df.columns:
            merged_history = pd.concat([history_df, new_history_df], ignore_index=True)
            merged_history = merged_history.drop_duplicates(subset=["Name"], keep="last")
        else:
            merged_history = new_history_df
    
        # 🟩 שלב 6: שמירה סופית — כולל עמודת Name תמיד
        if "Name" not in merged_df.columns and not new_details_df.empty:
            merged_df.insert(0, "Name", new_details_df["Name"])
        if "Name" not in merged_history.columns and not new_history_df.empty:
            merged_history.insert(0, "Name", new_history_df["Name"])
    
        save_to_csv(merged_df, details_file)
        save_to_csv(merged_history, history_file)
    
        self.log("✅ Player details updated successfully")
        self.log(f"   Total: {total_players} | New: {new_players} | Updated: {updated_players} | Skipped: {skipped_players}")
    
        return True

    
    
    
    def _get_web_team_ids(self):
        """
        🆕 מחלץ את רשימת web_team_ids מה-team_id_map בconfig
        או מגלה אותם באופן אוטומטי מהאתר
        """
        # אם יש team_id_map בconfig - השתמש בו
        team_id_map = self.league_config.get('team_id_map', {})
        if team_id_map:
            return list(team_id_map.keys())
        
        # אחרת - תצטרך להוסיף לוגיקה לגילוי אוטומטי
        self.log("⚠️ No team_id_map in config - using default list")
        return ["1096", "1093", "1095", "1092", "1099", "1108", "1097", 
                "1104", "1103", "1098", "1105", "1094", "1100", "1102"]
    
    def _get_team_players(self, web_team_id):
        """גזירת רשימת שחקנים מקבוצה"""
        url = f"{self.base_url}/team.asp?TeamId={web_team_id}"
        soup = get_soup(url)
        
        if not soup:
            return []
                
        # קבל team_id ושם ישירות מה-team_id_map
        team_id_map = self.league_config.get('team_id_map', {})
        team_id_official = team_id_map.get(web_team_id)
        
        # קבל שם מנורמל מ-data/teams.csv
        if team_id_official:
            # טען את השם מ-teams.csv לפי team_id
            import pandas as pd
            teams_df = pd.read_csv('data/teams.csv', encoding='utf-8-sig')
            team_row = teams_df[teams_df['Team_ID'] == team_id_official]
            team_name_normalized = team_row['Team_Name'].values[0] if not team_row.empty else f"Team_{web_team_id}"
        else:
            self.log(f"  ⚠️ No team_id mapping for web_id: {web_team_id}")
            team_name_normalized = f"Team_{web_team_id}"        
        players = []
        
        for player_link in soup.select('.roster_players a[href*="PlayerId="]'):
            player_url = self.base_url + "/" + player_link["href"]
            player_id = player_link["href"].split("PlayerId=")[-1]
            
            name_elem = player_link.select_one(".box_role_data.role_name.he")
            player_name = name_elem.get_text(strip=True) if name_elem else player_link.get_text(strip=True)
            player_name = player_name.replace("\xa0", " ")
            
            players.append({
                "player_id": player_id,
                "Name": player_name,
                "Team": team_name_normalized,  # 🆕 שם מנורמל
                "team_id": team_id_official,   # 🆕 מזהה מהמיפוי
                "league_id": self.league_id,
                "url": player_url
            })
        
        return players
    
    def _scrape_player_details(self, player):
        """גזירת פרטים של שחקן בודד"""

        
        soup = get_soup(player["url"])
        if not soup:
            return None, None
        
        # שם מלא
        first = soup.select_one(".p_first_name.he")
        last = soup.select_one(".p_last_name.he")
        
        if first or last:
            first_name = first.get_text(strip=True) if first else ""
            last_name = last.get_text(strip=True) if last else ""
            full_name = f"{first_name} {last_name}".strip()
        else:
            full_name = player["Name"]
        
        # פרטים בסיסיים
        details = {
            "player_id": "",
            "Name": full_name,
            "Team": player["Team"],
            "team_id": player["team_id"],
            "league_id": player["league_id"],
            "Date Of Birth": "",
            "Height": "",
            "Number": ""
        }
        
        # מספר חולצה
        num_div = soup.find("div", class_="p_num")
        if num_div:
            details["Number"] = num_div.get_text(strip=True)
        
        # גובה ותאריך לידה
        info_div = soup.find("div", class_="p_info he")
        if info_div:
            for span in info_div.find_all("span", class_="p_info_title"):
                label = span.get_text(strip=True)
                value = ""
                
                for sib in span.next_siblings:
                    if getattr(sib, "name", None) == "br":
                        break
                    if hasattr(sib, "get_text"):
                        value += sib.get_text(" ", strip=True)
                    else:
                        value += str(sib).strip()
                
                value = self._clean_text(value)
                
                if "גובה" in label:
                    details["Height"] = value
                elif "תאריך לידה" in label:
                    # המר DD/MM/YYYY → YYYY-MM-DD
                    if value and '/' in value:
                        try:
                            parts = value.split('/')
                            if len(parts) == 3:
                                day, month, year = parts
                                details["Date Of Birth"] = f"{year}/{month.zfill(2)}/{day.zfill(2)}"
                            else:
                                details["Date Of Birth"] = value
                        except:
                            details["Date Of Birth"] = value
                    else:
                        details["Date Of Birth"] = value
        
        # יצירת player_id אחרי שיש תאריך לידה
        player_id = generate_player_id(full_name, details["Date Of Birth"], self.league_id)
        details["player_id"] = player_id
        
        # היסטוריה
        history = self._scrape_player_history(soup)
        history_row = {**details, **history}
        
        return details, history_row
    
    def _scrape_player_history(self, soup):
        """גזירת היסטוריית עונות"""
        seasons = {}
        for year in range(2013, 2025):
            seasons[f"{year}-{str(year+1)[-2:]}"] = ""
        
        about = soup.find("div", class_="page_content he")
        if not about:
            return seasons
        
        html = about.decode()
        
        # קולג'
        college_match = re.search(r"קולג.?[:：]?\s*(.*?)\((\d{4})[-–](\d{4})\)", html)
        if college_match:
            name, start, end = college_match.groups()
            name = self._clean_text(name)
            for y in range(int(start), min(int(end), 2025)):
                key = f"{y}-{str(y+1)[-2:]}"
                if key in seasons:
                    seasons[key] = f"{name} (קולג')"
        
        # עונות
        lines = re.findall(r"<strong>(.*?)</strong>(.*?)(?:<br|</div>|</p|$)", html, re.DOTALL)
        
        for year_str, team_str in lines:
            year_str = re.sub(r"[^\d\-]", "", year_str)
            team_str = self._clean_text(team_str)
            
            if not team_str or not re.match(r"\d{4}-\d{4}", year_str):
                continue
            
            match = re.match(r"(\d{4})-(\d{4})", year_str)
            if not match:
                continue
            
            start, end = match.groups()
            teams = [t.strip() for t in re.split(r",|;", team_str) if t.strip()]
            
            for y in range(int(start), min(int(end), 2025)):
                key = f"{y}-{str(y+1)[-2:]}"
                if key in seasons and seasons[key] == "":
                    formatted = ", ".join(f"{t} (ליגת העל)" for t in teams)
                    seasons[key] = formatted
        
        return seasons


    
    # ============================================
    # GAME DETAILS
    # ============================================
    
    def _update_game_details(self):
        """עדכון משחקים"""
        self.log("STEP 2: UPDATING GAME DETAILS")
        
        # 1. לוח משחקים
        self.log("Scraping games schedule...")
        games = self._scrape_games_schedule()
        
        if games:
            # 🟩 שמירה בתוך תיקיית games (כמו ב-ibasketball)
            games_folder = Path(self.data_folder) / f"{self.league_code}_games"
            games_folder.mkdir(parents=True, exist_ok=True)

            schedule_file = games_folder / f"games_schedule.csv"
            save_to_csv(pd.DataFrame(games), schedule_file)

            self.log(f"✅ Games schedule updated: {len(games)} games")
        
        # 2. סטטיסטיקות משחקים שהסתיימו
        completed_games = [g for g in games if g.get('completed', False)]
        self.log(f"   Found {len(completed_games)} completed games")
        
        # בדיקה אילו כבר נגזרו
        games_folder = Path(self.games_folder)
        existing_games = set()
        for f in games_folder.glob("*.csv"):
            existing_games.add(f.stem.replace("_stats", ""))
        
        to_scrape = [g for g in completed_games if g['game_id'] not in existing_games]
        
        if not to_scrape:
            self.log("   Already scraped: all games")
            return True
        
        self.log(f"   Scraping {len(to_scrape)} new games")
        
        for i, game in enumerate(to_scrape, 1):
            game_id = game['game_id']
            self.log(f"   [{i}/{len(to_scrape)}] Game {game_id}")
            
            stats = self._scrape_game_stats(game_id)
            
            if stats:
                game_file = games_folder / f"{game_id}_stats.csv"
                save_to_csv(pd.DataFrame(stats), game_file)
            
            time.sleep(1)
        
        self.log(f"✅ Game stats updated: {len(to_scrape)} new games")
        return True
    
    def _scrape_games_schedule(self):
        """גזירת לוח משחקים"""
        all_games = {}
        current_round = None
        
        for board_id in self.board_ids:
            url = f"{self.base_url}/results.asp?Board={board_id}&RoundNumber=0&TeamId=0&cYear=2026"
            soup = get_soup(url)
            
            if not soup:
                continue
            
            results_div = soup.find('div', id='MY-RESULTS')
            if not results_div:
                continue
            
            rows = results_div.find_all('tr')
            
            for row in rows:
                # מחזור
                round_break = row.find('td', class_='round_break')
                if round_break:
                    current_round = round_break.text.strip()
                    continue
                
                if 'row' not in row.get('class', []):
                    continue
                
                # תאריך
                date_cell = row.find('td', class_='da_ltr_center')
                if not date_cell:
                    continue
                
                date_match = re.search(r'(\d{2}/\d{2}/\d{4})', date_cell.text)
                time_match = re.search(r'(\d{2}:\d{2})', date_cell.text)
                
                if not date_match:
                    continue
                
                date_str = date_match.group(1)
                time_str = time_match.group(1) if time_match else "00:00"
                
                # משחק
                game_links = row.find_all('a', href=lambda x: x and "game-zone.asp?GameId=" in x)
                if not game_links:
                    continue
                
                game_id_match = re.search(r"GameId=(\d+)", game_links[0]['href'])
                if not game_id_match:
                    continue
                
                game_id = game_id_match.group(1)
                
                # תוצאה
                score = game_links[-1].get_text(strip=True)
                has_score = bool(re.match(r'\d+-\d+', score))
                
                home_score = None
                away_score = None
                overtimes = 0
                
                if has_score:
                    ot_sup = game_links[-1].find('sup')
                    if ot_sup:
                        ot_text = ot_sup.get_text(strip=True)
                        ot_match = re.search(r'\((\d+)\)', ot_text)
                        if ot_match:
                            overtimes = int(ot_match.group(1))
                    
                    score_clean = re.match(r'(\d+)-(\d+)', score)
                    if score_clean:
                        try:
                            away_score = int(score_clean.group(1))
                            home_score = int(score_clean.group(2))
                        except ValueError:
                            pass
                
                # קבוצות - 🆕 עם נורמליזציה
                team_cells = row.find_all('td', class_='da_rtl_right')
                team_cells = [cell for cell in team_cells if cell.find('a', href=lambda x: x and "team.asp?TeamId=" in x)]
                
                if len(team_cells) < 2:
                    continue
                
                team_ids = []
                team_names = []
                
                for team_cell in team_cells[:2]:
                    team_link = team_cell.find('a', href=lambda x: x and "team.asp?TeamId=" in x)
                    if team_link:
                        team_name_elem = team_link.find('div', class_='game_item mid deskOnly')
                        if team_name_elem:
                            team_name_raw = team_name_elem.text.strip()
                            
                            # 🆕 נרמול שם הקבוצה
                            team_info = self.normalizer.normalize_team_name(team_name_raw)
                            team_id = team_info.get('team_id')
                            team_name = team_info.get('club_name', team_name_raw)
                            
                            team_ids.append(team_id)
                            team_names.append(team_name)
                
                if len(team_ids) != 2 or len(team_names) != 2:
                    continue
                
                # אולם
                arena = ""
                arena_cell = row.find('td', class_='da_rtl_right space deskOnly')
                if arena_cell:
                    arena = arena_cell.text.strip()
                
                # מנצח/מפסיד
                winner = None
                loser = None
                close_game = False
                
                if has_score and home_score is not None and away_score is not None:
                    if abs(home_score - away_score) <= 5 or overtimes > 0:
                        close_game = True
                    
                    if home_score > away_score:
                        winner = team_names[0]
                        loser = team_names[1]
                    elif away_score > home_score:
                        winner = team_names[1]
                        loser = team_names[0]
                
                all_games[game_id] = {
                    'game_id': game_id,
                    'round': current_round,
                    'date': date_str,
                    'hour': time_str,
                    'arena': arena,
                    'home_team_id': team_ids[0],
                    'away_team_id': team_ids[1],
                    'home_team_name': team_names[0],
                    'away_team_name': team_names[1],
                    'match': f"{team_names[0]} vs {team_names[1]}",
                    'completed': has_score,
                    'home_score': home_score,
                    'away_score': away_score,
                    'winner': winner,
                    'loser': loser,
                    'close_game': close_game,
                    'overtimes': overtimes
                }
        
        return list(all_games.values())
    
    def _scrape_game_stats(self, game_id):
        """גזירת סטטיסטיקות משחק"""
        url = f"{self.base_url}/game-zone.asp?GameId={game_id}"
        soup = get_soup(url)
        
        if not soup:
            return None
        
        tables = soup.select("table.stats_tbl")
        if not tables:
            return None
        
        players_stats = []
        
        for table in tables:
            team_row = table.select_one("td.round_break.he")
            if not team_row or not team_row.select_one("a"):
                continue
            
            team_name_raw = team_row.select_one("a").text.strip()
            
            # 🆕 נרמול שם הקבוצה
            team_info = self.normalizer.normalize_team_name(team_name_raw)
            team_name = team_info.get('club_name', team_name_raw)
            
            rows = table.select("tr.row.odd, tr.row.even")
            
            for row in rows:
                cols = row.select("td")
                if len(cols) < 23:
                    continue
                
                player_name = cols[1].text.strip()

                # 🧹 דילוג על שורה של "קבוצתי"
                if player_name == "קבוצתי":
                    continue

                
                def parse_fraction(stat):
                    if '/' in stat:
                        made, attempted = map(int, stat.split('/'))
                        return made, attempted
                    return 0, 0
                
                made_2, attempted_2 = parse_fraction(cols[5].text.strip())
                made_3, attempted_3 = parse_fraction(cols[7].text.strip())
                made_ft, attempted_ft = parse_fraction(cols[9].text.strip())
                
                made_fg = made_2 + made_3
                attempted_fg = attempted_2 + attempted_3
                fg_pct = int(round((made_fg / attempted_fg) * 100)) if attempted_fg > 0 else 0
                
                player_data = {
                    "game_id": game_id,
                    "team": team_name,  # 🆕 שם מנורמל
                    "player": player_name,
                    "minutes": cols[3].text.strip(),
                    "points": cols[4].text.strip(),
                    "fg_made": made_fg,
                    "fg_attempted": attempted_fg,
                    "fg_pct": fg_pct,
                    "fg2_made": made_2,
                    "fg2_attempted": attempted_2,
                    "fg2_pct": cols[6].text.strip(),
                    "fg3_made": made_3,
                    "fg3_attempted": attempted_3,
                    "fg3_pct": cols[8].text.strip(),
                    "ft_made": made_ft,
                    "ft_attempted": attempted_ft,
                    "ft_pct": cols[10].text.strip(),
                    "def_rebounds": cols[11].text.strip(),
                    "off_rebounds": cols[12].text.strip(),
                    "total_rebounds": cols[13].text.strip(),
                    "assists": cols[18].text.strip(),
                    "steals": cols[16].text.strip(),
                    "turnovers": cols[17].text.strip(),
                    "fouls": cols[14].text.strip(),
                    "fouls_drawn": cols[15].text.strip(),
                    "blocks": cols[19].text.strip(),
                    "blocked": cols[20].text.strip(),
                    "efficiency": cols[21].text.strip(),
                    "plus_minus": cols[22].text.strip()
                }
                
                players_stats.append(player_data)
        
        return players_stats
    
    # ============================================
    # HELPER METHODS
    # ============================================
    
    def _clean_text(self, text):
        """ניקוי טקסט"""
        if not text:
            return ""
        text = re.sub(r"<.*?>", "", str(text))
        text = re.sub(r"&nbsp;|[\r\n\t]", " ", text)
        text = re.sub(r"</?strong>|[:：•\-–]+", " ", text)
        text = re.sub(r'\s+', ' ', text)
        return text.strip()
    
    def log(self, message):
        """logging wrapper"""
        log_message(message, self.league_code)