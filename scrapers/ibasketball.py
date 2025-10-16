# -*- coding: utf-8 -*-
"""
iBasketball JSON Scraper
========================
גזירה מ-ibasketball.co.il - שמירה ב-JSON עם קובץ נפרד לכל אובייקט
"""

import requests
import json
import time
import os
from pathlib import Path
from datetime import datetime

from utils import log_message, get_soup
from models import generate_player_id, generate_game_id, normalize_season
from .base_scraper import BaseScraper
from .processors import DataNormalizer, StatsCalculator


class IBasketballScraper(BaseScraper):
    """גזירה מ-ibasketball.co.il עם שמירה ב-JSON"""
    
    def _init_processors(self):
        """אתחול processors"""
        self.normalizer = DataNormalizer(self.league_id, self.league_code)
        self.stats_calc = StatsCalculator()
        
        # טעינת מיפוי קבוצות
        if not self.normalizer.load_team_mapping():
            raise ValueError("Failed to load team mapping")
        
        # יצירת מבנה תיקיות JSON
        self._create_json_structure()
    
    def _create_json_structure(self):
        """יצירת מבנה תיקיות למבנה JSON"""
        base = Path('data')
        
        # ✅ מבנה: data/players/{league}/
        self.players_folder = base / 'players' / self.league_code
        # ✅ מבנה: data/games/{league}/{season}/
        self.games_folder = base / 'games' / self.league_code / self.league_config['season']
        
        self.players_folder.mkdir(parents=True, exist_ok=True)
        self.games_folder.mkdir(parents=True, exist_ok=True)
        
        self.log(f"✅ JSON structure ready")
        self.log(f"   Players: {self.players_folder}")
        self.log(f"   Games: {self.games_folder}")
    
    # ============================================
    # PLAYER FILE MANAGEMENT
    # ============================================
    
    def _save_player_details(self, player_id, folder_name, details):
        """שמור פרטי שחקן לקובץ JSON"""
        player_folder = self.players_folder / folder_name
        player_folder.mkdir(exist_ok=True)
        
        details['last_updated'] = datetime.now().isoformat()
        
        file_path = player_folder / f'{player_id}_details.json'
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(details, f, ensure_ascii=False, indent=2)

    def _save_player_history(self, player_id, folder_name, history_raw):
        """שמור היסטוריה של שחקן - פורמט שטוח לטבלה"""
        player_folder = self.players_folder / folder_name
        player_folder.mkdir(exist_ok=True)
        
        
        # ✅ בדיקת תקינות
        if not isinstance(history_raw, dict):
            self.log(f"   ⚠️  Invalid history data type")
            file_path = player_folder / f'{player_id}_history.json'
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump([], f, ensure_ascii=False, indent=2)
            return
        
        # ✅ המרה לפורמט שטוח + סינון קט סל
        history_rows = []
        for season, entries in history_raw.items():
            if not isinstance(entries, list):
                continue
            
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                
                league_name = entry.get('league', '')
                
                # ✅ דלג על קט סל
                if any(x in league_name for x in ['קט סל', 'קט-סל',  'ילדות','ילדים', 'קטסל']):
                    continue
                
                history_rows.append({
                    'player_id': player_id,
                    'season': season,
                    'team_name': entry.get('team', ''),
                    'league_name': self._clean_league_name(league_name),
                    'league_id': self.league_id
                })
        
        file_path = player_folder / f'{player_id}_history.json'
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(history_rows, f, ensure_ascii=False, indent=2)


    def _load_player_details(self, folder_name):
        """טען פרטי שחקן מקובץ"""
        player_folder = self.players_folder / folder_name
        
        # מצא את הקובץ שמתחיל ב-player_id
        details_files = list(player_folder.glob('*_details.json'))
        
        if not details_files:
            return None
        
        with open(details_files[0], 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def _load_player_history(self, folder_name):
        """טען היסטוריה של שחקן"""
        player_folder = self.players_folder / folder_name
        
        # מצא את הקובץ שמתחיל ב-player_id
        history_files = list(player_folder.glob('*_history.json'))
        
        if not history_files:
            return None
        
        with open(history_files[0], 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def _player_exists(self, folder_name):
        """בדוק אם שחקן קיים"""
        player_folder = self.players_folder / folder_name
        return player_folder.exists() and any(player_folder.glob('*_details.json'))
    
    def _needs_player_update(self, folder_name):
        """בדוק אם שחקן צריך עדכון"""
        player_folder = self.players_folder / folder_name
        
        # בדוק אם התיקייה קיימת
        if not player_folder.exists():
            return True, "New player"
        
        # חפש קבצים
        details_files = list(player_folder.glob('*_details.json'))
        history_files = list(player_folder.glob('*_history.json'))
        
        # ✅ אם יש גם details וגם history - השחקן קיים ומלא
        if details_files and history_files:
            # במצב QUICK - דלג
            if self.scrape_mode == 'quick':
                return False, "Complete (quick mode)"
            
      # במצב FULL - בדוק אם יש נתונים חסרים
        try:
            with open(details_files[0], 'r', encoding='utf-8') as f:
                details = json.load(f)
            
            # בדוק שדות חובה
            if not details.get('date_of_birth') or details['date_of_birth'] == '':
                return True, "Missing DOB"
            if not details.get('height') or details['height'] == '':
                return True, "Missing height"
            if not details.get('jersey_number') or details['jersey_number'] == '':
                return True, "Missing number"
            
            return False, "Complete data"
        except:
            return True, "Corrupted file"
    
        # אם חסר אחד מהקבצים
        if not details_files:
            return True, "Missing details"
        if not history_files:
            return True, "Missing history"
        
        return True, "Unknown"
        # ============================================
    # GAME FILE MANAGEMENT
    # ============================================
    
    def _save_game(self, game_data):
        """שמור משחק לקובץ JSON"""
        game_id = game_data['game_id']
        game_data['scraped_at'] = datetime.now().isoformat()
        
        file_path = self.games_folder / f"{game_id}.json"
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(game_data, f, ensure_ascii=False, indent=2)
    
    def _load_game(self, game_id):
        """טען משחק מקובץ"""
        file_path = self.games_folder / f"{game_id}.json"
        
        if not file_path.exists():
            return None
        
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def _game_exists(self, game_id):
        """בדוק אם משחק קיים"""
        return (self.games_folder / f"{game_id}.json").exists()
    
    # ============================================
    # PLAYER SCRAPING
    # ============================================
    
    def _update_player_details(self):
        """עדכון פרטי שחקנים"""
        from models import generate_player_folder_name  # ✅ import
        
        self.log("STEP 1: UPDATING PLAYER DETAILS")
        
        players = self._scrape_player_list()
        if not players:
            self.log("❌ No players found")
            return False
        
        self.log(f"Found {len(players)} players")
        
        new_players = 0
        updated_players = 0
        skipped_players = 0
        
        for i, player in enumerate(players, 1):
            player_name = player['Name']
            current_team_raw = player['Team']
            player_url = player['URL']
            
            # נרמול שם קבוצה
            team_info = self.normalizer.normalize_team_name(current_team_raw)
            current_team = team_info['club_name']
            team_id = team_info['team_id']
            
            # ✅ יצירת folder_name מהיר (ללא תאריך לידה)
            folder_name = generate_player_folder_name(player_name, current_team)
            
            self.log(f"[{i}/{len(players)}] Checking: {player_name}")
            self.log(f"   📁 Folder: {folder_name}")
            
            # בדוק אם צריך לגזור
            needs_update, reason = self._needs_player_update(folder_name)
            
            if needs_update:
                self.log(f"   ⚙️ Updating: {reason}")
                
                try:
                    # ✅ עכשיו גזור פרטים (כולל תאריך לידה)
                    details_raw = self._scrape_player_details(player_url)
                    
                    # ✅ צור player_id עם תאריך לידה
                    player_id = generate_player_id(player_name, details_raw['Date Of Birth'], self.league_id)
                    
                    self.log(f"   🆔 Player ID: {player_id}")
                    
                    # גזור היסטוריה
                    history_raw = self._scrape_player_history(player_url)

                    # ✅ שמירה ישירות - הפונקציה תטפל בהמרה
                    self._save_player_history(player_id, folder_name, history_raw)

                                        
                    # הכנת פרטים
                    details = {
                        'player_id': player_id,
                        'name': player_name,
                        'current_team_id': team_id,
                        'league_id': self.league_id,
                        'date_of_birth': details_raw['Date Of Birth'],
                        'height': details_raw['Height'],
                        'jersey_number': details_raw['Number']
                    }

                    self._save_player_details(player_id, folder_name, details)
                   
                    
                    if not (self.players_folder / folder_name).exists():
                        new_players += 1
                    else:
                        updated_players += 1
                    
                    self.log(f"   ✅ Saved")
                    
                except Exception as e:
                    self.log(f"   ❌ Error: {e}")
            else:
                skipped_players += 1
                self.log(f"   ⏭️  Skipped: {reason}")
            
            time.sleep(1)
        
        self.log(f"✅ Total: {len(players)} | New: {new_players} | Updated: {updated_players} | Skipped: {skipped_players}")
        
        return True
        
    def _scrape_player_list(self):
        """גזירת רשימת שחקנים מדף הליגה"""
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
        
        # תאריך לידה
        dob = soup.find("div", class_="data-birthdate")
        dob_text = dob.get_text("|", strip=True).split("|")[-1] if dob else ""
        dob_formatted = "/".join(dob_text.split("-")[::-1]) if dob_text else ""
        
        # גובה
        height = soup.find("div", class_="data-other", attrs={"data-metric": "גובה"})
        height_text = height.get_text("|", strip=True).split("|")[-1] if height else ""
        
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
        
        return {
            "Date Of Birth": dob_formatted,
            "Height": height_text,
            "Number": number
        }
    
    def _scrape_player_history(self, player_url):
        """גזירת היסטוריית קבוצות"""
        soup = get_soup(player_url)
        if not soup:
            return {}
        
        history_tag = soup.find("div", class_="data-teams")
        history = {}
        youth_count = 0
        
        # ✅ מעקב אחרי צירופים שכבר נוספו
        seen_combinations = set()
        
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
                            
                            # ✅ בדוק אם הצירוף כבר קיים
                            combination = (season, team)
                            
                            if combination in seen_combinations:
                                continue  # ✅ דלג רק על השורה הזאת, המשך ללולאה הבאה
                            
                            # ✅ סמן שראינו את הצירוף הזה
                            seen_combinations.add(combination)
                            
                            # עצור אחרי 2 ליגות נוער
                            if "נוער" in league:
                                youth_count += 1
                                if youth_count > 1:
                                    break  # ✅ רק פה עוצרים את הכל
                            
                            # שמור בפורמט
                            if season not in history:
                                history[season] = []
                            
                            history[season].append({
                                'team': team,
                                'league': league
                            })
        
        return history

    def _clean_league_name(self, league_name):
        """ניקוי שם ליגה ממילים מיותרות"""
        words_to_remove = ['נשים', 'ג', 'גמרסל', 'עליון', 'צפון', 'דרום']
        
        cleaned = league_name
        for word in words_to_remove:
            cleaned = cleaned.replace(f' {word}', '')
            cleaned = cleaned.replace(f'{word} ', '')
            if cleaned == word:
                cleaned = ''
        
        cleaned = ' '.join(cleaned.split())
        return cleaned.strip()



    # ============================================
    # GAME SCRAPING
    # ============================================
    
    def _update_game_details(self):
        """עדכון משחקים"""
        self.log("STEP 1: UPDATING GAME DETAILS")
        
        # הורדת לוח משחקים
        games_df = self._download_games_schedule()
        if games_df is None:
            self.log("❌ Failed to download games schedule")
            return False
        
        # נרמול שמות קבוצות
        games_df = self._normalize_schedule_teams(games_df)
        
        self.log(f"Found {len(games_df)} games in schedule")
        
        # ✅ שמירת לו"ז מלא כ-JSON
        self._save_full_schedule(games_df)
        
        # גזירת סטטיסטיקות רק למשחקים עם תוצאה
        return self._scrape_all_games(games_df)
    
    def _save_full_schedule(self, games_df):
        """שמור לו"ז מלא של הליגה"""
        import pandas as pd
        
        schedule_data = []
        
        for idx, row in games_df.iterrows():
            game_code = str(row.get('Code', ''))
            
            schedule_data.append({
                'game_id': f"{self.league_id}_{game_code}",
                'league_id': self.league_id,
                'season': self.league_config['season'],
                'code': game_code,
                'date': row.get('תאריך', ''),
                'time': row.get('Time', ''),
                'round': row.get('מחזור', ''),
                'home_team': row.get('Home Team', ''),
                'away_team': row.get('Away Team', ''),
                'home_score': int(row['Home Score']) if pd.notna(row.get('Home Score')) else None,
                'away_score': int(row['Away Score']) if pd.notna(row.get('Away Score')) else None,
                'venue': row.get('Venue', ''),
                'status': 'completed' if pd.notna(row.get('Home Score')) else 'scheduled'
            })
        
        schedule_path = self.games_folder / 'schedule.json'
        with open(schedule_path, 'w', encoding='utf-8') as f:
            json.dump(schedule_data, f, ensure_ascii=False, indent=2)
        
        self.log(f"✅ Full schedule saved: {len(schedule_data)} games")
    
    def _scrape_all_games(self, games_df):
        """גזירת משחקים - רק עם תוצאה"""
        import pandas as pd
        
        games_scraped = 0
        games_skipped = 0
        
        for idx, row in games_df.iterrows():
            # ✅ דלג אם אין תוצאה
            if pd.isna(row.get('Home Score')) or pd.isna(row.get('Away Score')):
                continue
            
            # ✅ בדוק אם יש Code
            if pd.isna(row.get('Code')) or row['Code'] == '':
                continue
            
            game_code = str(row['Code'])
            game_url = f"https://ibasketball.co.il/event/{game_code}/"
            game_id = f"{self.league_id}_{game_code}"
            
            # בדוק אם המשחק קיים
            if self._game_exists(game_id):
                games_skipped += 1
                continue
            
            self.log(f"   [{idx+1}/{len(games_df)}] Scraping game: {game_id}")
            
            # גזור את המשחק
            game_data = self._scrape_single_game(game_id, game_url, row)
            
            if game_data:
                self._save_game(game_data)
                games_scraped += 1
            
            time.sleep(1)
        
        self.log(f"✅ Games updated: {games_scraped} scraped, {games_skipped} skipped")
        return True
    
    
    def _download_games_schedule(self):
        """הורדת לוח משחקים מהאתר"""
        league_url = self.league_config['url']
        soup = get_soup(league_url)
        if not soup:
            return None
        
        export_link = soup.find('a', href=lambda x: x and ('export' in x or 'feed=xlsx' in x))
        if not export_link:
            self.log("❌ Export link not found")
            import pandas as pd
            return pd.DataFrame()
        
        export_url = export_link['href']
        
        if not export_url.startswith('http'):
            if export_url.startswith('?'):
                export_url = f"{league_url}{export_url}"
            else:
                export_url = f"https://ibasketball.co.il{export_url}"
        
        self.log(f"   Downloading from: {export_url}")
        
        try:
            import pandas as pd
            import os
            from pathlib import Path
            
            response = requests.get(export_url, timeout=30)
            response.raise_for_status()
            
            # ✅ שמור קובץ זמני
            temp_excel = self.games_folder / 'temp_games.xlsx'
            with open(temp_excel, 'wb') as f:
                f.write(response.content)
            
            # ✅ קרא מהקובץ
            df = pd.read_excel(temp_excel, engine='openpyxl')
            os.remove(temp_excel)
            
            self.log(f"   ✅ Downloaded {len(df)} games")
            return df
            
        except Exception as e:
            self.log(f"❌ Error downloading schedule: {e}")
            return None


    def _normalize_schedule_teams(self, games_df):
        """נרמול שמות קבוצות בלוח משחקים"""
        import pandas as pd
        
        # שינוי שמות עמודות לאנגלית
        column_renames = {
            'ליגה': 'League',
            'מועד': 'Round',
            'תאריך': 'Date',
            'שעה': 'Time',
            'בית': 'Home Team',
            'אורח': 'Away Team',
            'ת. בית': 'Home Score',
            'ת. אורח': 'Away Score',
            'היכל': 'Arena',
            'קישור': 'Link'
        }
        
        games_df = games_df.rename(columns=column_renames)
        
        # נרמול בית
        if 'Home Team' in games_df.columns:
            games_df['Home Team'] = games_df['Home Team'].apply(
                lambda x: self.normalizer.normalize_team_name(x)['club_name'] if pd.notna(x) else x
            )
        
        # נרמול אורח
        if 'Away Team' in games_df.columns:
            games_df['Away Team'] = games_df['Away Team'].apply(
                lambda x: self.normalizer.normalize_team_name(x)['club_name'] if pd.notna(x) else x
            )
        
        return games_df
    
    
        
    def _scrape_single_game(self, game_id, game_url, schedule_row):
        """גזירת משחק בודד"""
        import pandas as pd
        
        soup = get_soup(game_url)
        if not soup:
            return None
        
        # פרטי משחק בסיסיים
        home_team = schedule_row.get('Home Team', '')
        away_team = schedule_row.get('Away Team', '')
        
        home_team_info = self.normalizer.normalize_team_name(home_team)
        away_team_info = self.normalizer.normalize_team_name(away_team)
        
        game_data = {
            'game_id': game_id,
            'league_id': self.league_id,
            'season': self.league_config['season'],
            'date': schedule_row.get('Date', ''),
            'time': schedule_row.get('Time', ''),
            'round': schedule_row.get('Round', ''),
            'home_team': home_team_info['club_name'],
            'home_team_id': home_team_info['team_id'],
            'away_team': away_team_info['club_name'],
            'away_team_id': away_team_info['team_id'],
            'home_score': int(schedule_row['Home Score']) if pd.notna(schedule_row.get('Home Score')) else None,
            'away_score': int(schedule_row['Away Score']) if pd.notna(schedule_row.get('Away Score')) else None,
            'arena': schedule_row.get('Arena', ''),
            'status': 'completed' if pd.notna(schedule_row.get('Home Score')) else 'scheduled'
        }
        
        # גזירת רבעים
        quarters = self._scrape_quarters(soup, game_id)
        if quarters:
            game_data['quarters'] = quarters
        
        # גזירת סטטיסטיקות שחקנים
        player_stats = self._scrape_player_stats(soup, game_id)
        if player_stats:
            game_data['player_stats'] = player_stats
        
        # גזירת סטטיסטיקות קבוצתיות
        team_stats = self._scrape_team_stats(soup, game_id)
        if team_stats:
            game_data['team_stats'] = team_stats
        
        return game_data
    
    def _scrape_quarters(self, soup, game_id):
        """גזירת ניקוד לפי רבעים"""
        quarters = []
        
        quarters_section = soup.find('div', class_='sp-template-event-blocks')
        if not quarters_section:
            return quarters
        
        table = quarters_section.find('table')
        if not table:
            return quarters
        
        tbody = table.find('tbody')
        if not tbody:
            return quarters
        
        rows = tbody.find_all('tr')
        for row in rows[:-1]:  # לא כולל שורת סיכום
            cells = row.find_all('td')
            if len(cells) >= 5:
                try:
                    quarter_num = len(quarters) + 1
                    home_score = int(cells[-2].get_text(strip=True))
                    away_score = int(cells[-1].get_text(strip=True))
                    
                    quarters.append({
                        'quarter': quarter_num,
                        'home_score': home_score,
                        'away_score': away_score
                    })
                except:
                    continue
        
        return quarters
    
    def _scrape_player_stats(self, soup, game_id):
        """גזירת סטטיסטיקות שחקנים"""
        player_stats = []
        
        # מציאת כל הטבלאות של שחקנים
        performance_sections = soup.find_all('div', class_='sp-template-event-performance')
        
        for section in performance_sections:
            team_header = section.find('h4', class_='sp-table-caption')
            if not team_header:
                continue
            
            team_name_raw = team_header.text.strip()
            team_info = self.normalizer.normalize_team_name(team_name_raw)
            team_id = team_info['team_id']
            
            table = section.find('table', class_='sp-event-performance')
            if not table:
                continue
            
            tbody = table.find('tbody')
            if not tbody:
                continue
            
            for row in tbody.find_all('tr'):
                if 'sp-total-row' in row.get('class', []):
                    continue
                
                cells = row.find_all('td')
                if len(cells) < 2:
                    continue
                
                # שם שחקן
                player_cell = cells[0]
                player_link = player_cell.find('a')
                if not player_link:
                    continue
                
                player_name = player_link.get_text(strip=True)
                
                # סטטיסטיקות
                stats = {
                    'game_id': game_id,
                    'player_name': player_name,
                    'team_id': team_id
                }
                
                # קריאת הסטטיסטיקות מהתאים
                for i, cell in enumerate(cells[1:], 1):
                    stat_value = cell.get_text(strip=True)
                    # כאן תצטרך להוסיף לוגיקה לזיהוי איזה stat זה
                    # לפי המבנה של האתר
                
                player_stats.append(stats)
        
        return player_stats
    
    def _scrape_team_stats(self, soup, game_id):
        """גזירת סטטיסטיקות קבוצתיות"""
        team_stats = []
        
        # כאן תוסיף את הלוגיקה לגזירת סטטיסטיקות קבוצתיות
        # בהתאם למבנה של האתר
        
        return team_stats