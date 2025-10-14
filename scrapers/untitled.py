# -*- coding: utf-8 -*-
"""
scrapers/winner.py
==================
Scraper לליגת Winner (basket.co.il)

שמור קובץ זה ב: scrapers/winner.py
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
from datetime import datetime
from pathlib import Path

from utils import log_message, save_to_csv


class WinnerScraper:
    """
    Scraper לליגת Winner מ-basket.co.il
    תואם למבנה הקיים של הפרויקט
    """
    
    def __init__(self, config, league_id, scrape_mode='full'):
        self.config = config
        self.league_id = league_id
        self.league_code = config['code']
        self.scrape_mode = scrape_mode
        
        self.base_url = "https://basket.co.il"
        self.board_ids = config.get('board_ids', [])
        self.team_id_map = config.get('team_id_map', {})
        
        # תיקיות
        self.data_folder = Path(config['data_folder'])
        self.games_folder = Path(config['games_folder'])
        
        self.data_folder.mkdir(parents=True, exist_ok=True)
        self.games_folder.mkdir(exist_ok=True)
        
        log_message(f"Initialized WinnerScraper for {config['name']}", self.league_code)
    
    # ============================================
    # MAIN RUN METHOD
    # ============================================
    
    def run(self):
        """הרצת גזירה מלאה"""
        try:
            log_message("="*60, self.league_code)
            log_message(f"Starting scrape: {self.config['name']}", self.league_code)
            log_message(f"Mode: {self.scrape_mode.upper()}", self.league_code)
            log_message("="*60, self.league_code)
            
            # שלב 1: שחקנים
            log_message("STEP 1: UPDATING PLAYER DETAILS", self.league_code)
            self.scrape_players()
            
            # שלב 2: משחקים
            log_message("STEP 2: UPDATING GAME DETAILS", self.league_code)
            self.scrape_games()
            
            log_message("="*60, self.league_code)
            log_message("✅ SCRAPE COMPLETED SUCCESSFULLY", self.league_code)
            log_message("="*60, self.league_code)
            
            return True
            
        except Exception as e:
            log_message(f"❌ ERROR in WinnerScraper: {e}", self.league_code)
            import traceback
            log_message(traceback.format_exc(), self.league_code)
            return False
    
    # ============================================
    # PLAYER SCRAPING
    # ============================================
    
    def scrape_players(self):
        """גזירת כל השחקנים"""
        all_details = []
        all_history = []
        
        # טען נתונים קיימים אם במצב עדכון
        existing_details = {}
        existing_history = {}
        
        if self.scrape_mode == 'quick':
            existing_details, existing_history = self._load_existing_players()
        
        total_players = 0
        new_players = 0
        updated_players = 0
        skipped_players = 0
        
        # עבור על כל הקבוצות
        for web_team_id, official_team_id in self.team_id_map.items():
            log_message(f"Processing team: {web_team_id}", self.league_code)
            
            players = self._get_team_players(web_team_id, official_team_id)
            log_message(f"  Found {len(players)} players", self.league_code)
            
            for player in players:
                total_players += 1
                player_name = player['Name']
                
                # בדיקה אם צריך לגזור
                if self.scrape_mode == 'quick':
                    if player_name in existing_details:
                        skipped_players += 1
                        all_details.append(existing_details[player_name])
                        all_history.append(existing_history.get(player_name, {}))
                        continue
                
                # גזירת פרטי שחקן
                log_message(f"  ↳ {player_name}", self.league_code)
                
                details, history = self._scrape_player_details(player)
                
                if details:
                    all_details.append(details)
                    all_history.append(history)
                    
                    if player_name in existing_details:
                        updated_players += 1
                    else:
                        new_players += 1
                
                time.sleep(1)  # Rate limiting
        
        # שמירה
        if all_details:
            details_file = self.data_folder / f"{self.league_code}_player_details.csv"
            save_to_csv(pd.DataFrame(all_details), details_file)
        
        if all_history:
            history_file = self.data_folder / f"{self.league_code}_player_history.csv"
            save_to_csv(pd.DataFrame(all_history), history_file)
        
        log_message("✅ Player details updated", self.league_code)
        log_message(f"   Total: {total_players} | New: {new_players} | Updated: {updated_players} | Skipped: {skipped_players}", self.league_code)
    
    def _get_team_players(self, web_team_id, official_team_id):
        """גזירת רשימת שחקנים מקבוצה"""
        url = f"{self.base_url}/team.asp?TeamId={web_team_id}"
        soup = self._get_soup(url)
        
        if not soup:
            return []
        
        # שם קבוצה
        team_header = soup.select_one(".team_header h1")
        team_name = team_header.get_text(strip=True) if team_header else ""
        
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
                "Team": team_name,
                "team_id": official_team_id,
                "league_id": self.league_id,
                "url": player_url
            })
        
        return players
    
    def _scrape_player_details(self, player):
        """גזירת פרטים של שחקן בודד"""
        soup = self._get_soup(player["url"])
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
            "player_id": player["player_id"],
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
                    details["Date Of Birth"] = value
        
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
    
    def _load_existing_players(self):
        """טעינת שחקנים קיימים"""
        details_file = self.data_folder / f"{self.league_code}_player_details.csv"
        history_file = self.data_folder / f"{self.league_code}_player_history.csv"
        
        existing_details = {}
        existing_history = {}
        
        if details_file.exists():
            df = pd.read_csv(details_file, encoding='utf-8-sig')
            existing_details = df.set_index('Name').to_dict('index')
        
        if history_file.exists():
            df = pd.read_csv(history_file, encoding='utf-8-sig')
            existing_history = df.set_index('Name').to_dict('index')
        
        return existing_details, existing_history
    
    # ============================================
    # GAME SCRAPING
    # ============================================
    
    def scrape_games(self):
        """גזירת משחקים"""
        # 1. לוח משחקים
        log_message("Scraping games schedule...", self.league_code)
        games = self._scrape_games_schedule()
        
        if games:
            schedule_file = self.data_folder / f"{self.league_code}_games_schedule.csv"
            save_to_csv(pd.DataFrame(games), schedule_file)
            log_message(f"✅ Games schedule updated: {len(games)} games", self.league_code)
        
        # 2. סטטיסטיקות משחקים שהסתיימו
        completed_games = [g for g in games if g.get('completed', False)]
        log_message(f"   Found {len(completed_games)} completed games", self.league_code)
        
        # בדיקה אילו כבר נגזרו
        existing_games = set()
        for f in self.games_folder.glob("*.csv"):
            existing_games.add(f.stem.replace("_stats", ""))
        
        to_scrape = [g for g in completed_games if g['game_id'] not in existing_games]
        
        if not to_scrape:
            log_message("   All games already scraped", self.league_code)
            return
        
        log_message(f"   Scraping {len(to_scrape)} new games", self.league_code)
        
        for i, game in enumerate(to_scrape, 1):
            game_id = game['game_id']
            log_message(f"   [{i}/{len(to_scrape)}] Game {game_id}", self.league_code)
            
            stats = self._scrape_game_stats(game_id)
            
            if stats:
                game_file = self.games_folder / f"{game_id}_stats.csv"
                save_to_csv(pd.DataFrame(stats), game_file)
            
            time.sleep(1)
        
        log_message(f"✅ Game stats updated: {len(to_scrape)} new games", self.league_code)
    
    def _scrape_games_schedule(self):
        """גזירת לוח משחקים"""
        all_games = {}
        current_round = None
        
        for board_id in self.board_ids:
            url = f"{self.base_url}/results.asp?Board={board_id}&RoundNumber=0&TeamId=0&cYear=2025"
            soup = self._get_soup(url)
            
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
                
                # קבוצות
                team_cells = row.find_all('td', class_='da_rtl_right')
                team_cells = [cell for cell in team_cells if cell.find('a', href=lambda x: x and "team.asp?TeamId=" in x)]
                
                if len(team_cells) < 2:
                    continue
                
                team_ids = []
                team_names = []
                
                for team_cell in team_cells[:2]:
                    team_link = team_cell.find('a', href=lambda x: x and "team.asp?TeamId=" in x)
                    if team_link:
                        web_id = team_link['href'].split('=')[-1]
                        official_id = self.team_id_map.get(web_id, web_id)
                        team_ids.append(official_id)
                        
                        team_name_elem = team_link.find('div', class_='game_item mid deskOnly')
                        if team_name_elem:
                            team_names.append(team_name_elem.text.strip())
                
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
        soup = self._get_soup(url)
        
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
            
            team_name = team_row.select_one("a").text.strip()
            
            rows = table.select("tr.row.odd, tr.row.even")
            
            for row in rows:
                cols = row.select("td")
                if len(cols) < 23:
                    continue
                
                player_name = cols[1].text.strip()
                
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
                    "team": team_name,
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
    
    def _get_soup(self, url):
        """שליפת HTML"""
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            response = requests.get(url, headers=headers, timeout=30)
            response.encoding = 'utf-8'
            return BeautifulSoup(response.content, 'html.parser')
        except Exception as e:
            log_message(f"❌ Error fetching {url}: {e}", self.league_code)
            return None
    
    def _clean_text(self, text):
        """ניקוי טקסט"""
        if not text:
            return ""
        text = re.sub(r"<.*?>", "", str(text))
        text = re.sub(r"&nbsp;|[\r\n\t]", " ", text)
        text = re.sub(r"</?strong>|[:：•\-–]+", " ", text)
        text = re.sub(r'\s+', ' ', text)
        return text.strip()