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
        from models import generate_player_folder_name
        
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
            
            # יצירת folder_name מהיר (ללא תאריך לידה)
            folder_name = generate_player_folder_name(player_name, current_team)
            
            self.log(f"[{i}/{len(players)}] Checking: {player_name}")
            self.log(f"   📁 Folder: {folder_name}")
            
            # בדוק אם צריך לגזור
            needs_update, reason = self._needs_player_update(folder_name)
            
            if needs_update:
                self.log(f"   ⚙️ Updating: {reason}")
                
                try:
                    # גזור פרטים (כולל תאריך לידה)
                    details_raw = self._scrape_player_details(player_url)
                    
                    # צור player_id עם תאריך לידה
                    player_id = generate_player_id(player_name, details_raw['Date Of Birth'], self.league_id)
                    
                    self.log(f"   🆔 Player ID: {player_id}")
                    
                    # גזור היסטוריה
                    history_raw = self._scrape_player_history(player_url)
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
        
        # ✅ יצירת קובץ אינדקס
        self._create_player_index()
        
        return True
    
    
    def _create_player_index(self):
        """יצירת קובץ אינדקס לכל שחקני הליגה"""
        self.log("Creating player index...")
        
        index = {}
        
        if not self.players_folder.exists():
            self.log("   ⚠️  Players folder doesn't exist")
            return
        
        player_count = 0
        
        for player_folder in self.players_folder.iterdir():
            if not player_folder.is_dir():
                continue
            
            # טען פרטים
            details_files = list(player_folder.glob('*_details.json'))
            if not details_files:
                continue
            
            try:
                with open(details_files[0], 'r', encoding='utf-8') as f:
                    details = json.load(f)
                
                player_name = details.get('name', '')
                if not player_name:
                    continue
                
                # הוסף לאינדקס
                index[player_name] = {
                    'player_id': details.get('player_id', ''),
                    'folder_name': player_folder.name,
                    'current_team_id': details.get('current_team_id', ''),
                    'date_of_birth': details.get('date_of_birth', ''),
                    'jersey_number': details.get('jersey_number', ''),
                    'height': details.get('height', '')
                }
                
                player_count += 1
                
            except Exception as e:
                self.log(f"   ⚠️  Error reading {player_folder.name}: {e}")
                continue
        
        # שמירת אינדקס
        index_path = self.players_folder / 'index.json'
        with open(index_path, 'w', encoding='utf-8') as f:
            json.dump(index, f, ensure_ascii=False, indent=2)
        
        self.log(f"✅ Player index created: {player_count} players")
        self.log(f"   📄 {index_path}")
    
    
    def _load_player_index(self):
        """טען קובץ אינדקס של שחקנים"""
        index_path = self.players_folder / 'index.json'
        
        if not index_path.exists():
            self.log("   ⚠️  Player index not found, creating...")
            self._create_player_index()
        
        try:
            with open(index_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            self.log(f"   ❌ Error loading player index: {e}")
            return {}
    
        
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
                'date': row.get('Date', ''),
                'time': row.get('Time', ''),
                'round': row.get('Round', ''),
                'home_team': row.get('Home Team', ''),
                'away_team': row.get('Away Team', ''),
                'home_score': int(row['Home Score']) if pd.notna(row.get('Home Score')) else None,
                'away_score': int(row['Away Score']) if pd.notna(row.get('Away Score')) else None,
                'venue': row.get('Arena', ''),  # ✅ שינוי ל-venue
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
        corrected_scores = []  # ✅ רשימת משחקים שהתוצאה תוקנה
        
        for idx, row in games_df.iterrows():
            # דלג אם אין תוצאה
            if pd.isna(row.get('Home Score')) or pd.isna(row.get('Away Score')):
                continue
            
            # בדוק אם יש Code
            if pd.isna(row.get('Code')) or row['Code'] == '':
                continue
            
            game_code = str(row['Code'])
            game_url = f"https://ibasketball.co.il/match/{game_code}/"
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
                
                # ✅ בדוק אם התוצאה שונה מה-XLS
                xls_home = int(row['Home Score']) if pd.notna(row.get('Home Score')) else None
                xls_away = int(row['Away Score']) if pd.notna(row.get('Away Score')) else None
                real_home = game_data.get('home_score')
                real_away = game_data.get('away_score')
                
                if (xls_home != real_home or xls_away != real_away):
                    corrected_scores.append({
                        'game_id': game_id,
                        'xls_score': f"{xls_home}-{xls_away}",
                        'real_score': f"{real_home}-{real_away}"
                    })
                    
                    # ✅ עדכן את ה-DataFrame
                    games_df.at[idx, 'Home Score'] = real_home
                    games_df.at[idx, 'Away Score'] = real_away
            
            time.sleep(1)
        
        # ✅ שמור schedule מעודכן
        self._save_full_schedule(games_df)
        
        # ✅ הצג תיקונים
        if corrected_scores:
            self.log(f"⚠️  Corrected {len(corrected_scores)} scores from XLS:")
            for correction in corrected_scores:
                self.log(f"   {correction['game_id']}: {correction['xls_score']} → {correction['real_score']}")
        
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
        
        # ✅ הדפסת עמודות לבדיקה
        self.log(f"   XLS Columns: {list(games_df.columns)}")
        
        # שינוי שמות עמודות לאנגלית
        column_renames = {
            'ליגה': 'League',
            'מועד': 'Round',
            'מחזור': 'Round',  # ✅ הוספה
            'תאריך': 'Date',
            'שעה': 'Time',
            'בית': 'Home Team',
            'אורח': 'Away Team',
            'ת. בית': 'Home Score',
            'ת. אורח': 'Away Score',
            'היכל': 'Arena',
            'Venue': 'Arena',  # ✅ הוספה
            'קישור': 'Link'
        }
        
        games_df = games_df.rename(columns=column_renames)
        
        # ✅ הדפסת עמודות אחרי שינוי שם
        self.log(f"   After rename: {list(games_df.columns)}")
        
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
        from datetime import datetime
        
        # תקן את ה-URL
        game_code = game_id.split('_')[-1]
        game_url = f"https://ibasketball.co.il/match/{game_code}/"
        
        soup = get_soup(game_url)
        if not soup:
            return None
        
        # פרטי משחק בסיסיים
        home_team = schedule_row.get('Home Team', '')
        away_team = schedule_row.get('Away Team', '')
        game_date = schedule_row.get('Date', '')
        game_round = schedule_row.get('Round', '')
        game_venue = schedule_row.get('Arena', '')
        
        home_team_info = self.normalizer.normalize_team_name(home_team)
        away_team_info = self.normalizer.normalize_team_name(away_team)
        
        game_data = {
            'game_id': game_id,
            'league_id': self.league_id,
            'season': self.league_config['season'],
            'date': game_date,
            'time': schedule_row.get('Time', ''),
            'round': game_round,
            'home_team': home_team_info['club_name'],
            'home_team_id': home_team_info['team_id'],
            'away_team': away_team_info['club_name'],
            'away_team_id': away_team_info['team_id'],
            'venue': game_venue,  # ✅ venue בכל מקום
            'status': 'scheduled'
        }
        
        # גזירת רבעים + תוצאה אמיתית מעמוד המשחק
        quarters_list, final_scores = self._scrape_quarter_scores(soup, game_id, game_date)
        
        # אם יש תוצאה סופית מעמוד המשחק - השתמש בה
        if final_scores and 'home_score' in final_scores and 'away_score' in final_scores:
            home_score = final_scores['home_score']
            away_score = final_scores['away_score']
            game_data['status'] = 'completed'
            
            self.log(f"      ✅ Real scores from page: {home_score} - {away_score}")
        else:
            # אם אין תוצאה מהעמוד - נסה מה-XLS (fallback)
            home_score = int(schedule_row['Home Score']) if pd.notna(schedule_row.get('Home Score')) else None
            away_score = int(schedule_row['Away Score']) if pd.notna(schedule_row.get('Away Score')) else None
            
            if home_score is not None and away_score is not None:
                game_data['status'] = 'completed'
                self.log(f"      ⚠️  Using XLS scores: {home_score} - {away_score}")
        
        # ✅ בדיקה קריטית: אם יש תוצאה אבל אין סטטיסטיקות
        performance_sections = soup.find_all('div', class_='sp-template-event-performance-values')
        
        if (home_score is not None and away_score is not None and 
            home_score + away_score > 0 and len(performance_sections) == 0):
            
            # ✅ בדוק אם המשחק היום (LIVE) או בעבר (CANCELLED)
            try:
                # פורמט: DD-MM-YYYY
                game_datetime = datetime.strptime(game_date, '%d-%m-%Y')
                today = datetime.now().date()
                
                if game_datetime.date() == today:
                    self.log(f"      🔴 Game is LIVE - has score but stats not ready yet")
                    game_data['status'] = 'live'
                else:
                    self.log(f"      🚫 Game cancelled - has score but no stats table")
                    home_score = 0
                    away_score = 0
                    game_data['status'] = 'cancelled'
            except Exception as e:
                # אם יש בעיה בפרסור תאריך - נניח שבוטל
                self.log(f"      🚫 Game cancelled - date parse error: {e}")
                home_score = 0
                away_score = 0
                game_data['status'] = 'cancelled'
        
        game_data['home_score'] = home_score
        game_data['away_score'] = away_score
        
        # המרת רבעים למבנה מקונן
        if quarters_list:
            quarters_by_team = {}
            for q in quarters_list:
                team_id = q['team_id']  # ✅ שימוש ב-team_id
                if team_id not in quarters_by_team:
                    quarters_by_team[team_id] = []
                quarters_by_team[team_id].append({
                    'quarter': q['quarter'],
                    'score': q['score'],
                    'score_against': q['score_against']
                })
                
            game_data['quarters'] = quarters_by_team
            
            # ספירת הארכות
            overtimes = 0
            for team_quarters in quarters_by_team.values():
                num_quarters = len(team_quarters)
                if num_quarters > 4:
                    overtimes = num_quarters - 4
                    break
        else:
            overtimes = 0
        
        # חישוב winner/loser/close_game (רק אם המשחק הסתיים)
        if (game_data['status'] == 'completed' and 
            home_score is not None and away_score is not None and 
            home_score + away_score > 0):
            
            point_diff = abs(home_score - away_score)
            close_game = point_diff <= 5 or overtimes > 0
            
            if home_score > away_score:
                winner = home_team_info['club_name']
                loser = away_team_info['club_name']
            elif away_score > home_score:
                winner = away_team_info['club_name']
                loser = home_team_info['club_name']
            else:
                winner = None
                loser = None
            
            game_data.update({
                'winner': winner,
                'loser': loser,
                'close_game': close_game,
                'overtimes': overtimes
            })
        
        # גזירת סטטיסטיקות שחקנים (רק אם יש)
        if len(performance_sections) > 0:
            player_stats = self._scrape_player_stats(soup, game_id, game_date)
            if player_stats:
                game_data['player_stats'] = player_stats
            
            # גזירת סטטיסטיקות קבוצתיות
            team_stats = self._scrape_team_stats(soup, game_id, game_date)
            if team_stats:
                game_data['team_stats'] = team_stats
        
        return game_data

    
    def _scrape_quarter_scores(self, soup, game_id, game_date):
        """גזירת ניקוד לפי רבעים + תוצאה סופית"""
        quarters_data = []
        final_scores = {}
        
        try:
            results_table = soup.find('table', class_='sp-event-results')
            if not results_table:
                return quarters_data, final_scores
            
            tbody = results_table.find('tbody')
            if not tbody:
                return quarters_data, final_scores
                
            rows = tbody.find_all('tr')
            
            teams = []
            team_ids = []
            team_totals = {}
            
            for row in rows:
                team_cell = row.find('td', class_='data-name')
                if team_cell:
                    team_link = team_cell.find('a')
                    team_name_raw = team_link.text.strip() if team_link else team_cell.text.strip()
                    team_info = self.normalizer.normalize_team_name(team_name_raw)
                    teams.append(team_info['club_name'])
                    team_ids.append(team_info['team_id'])
            
            if len(teams) != 2:
                return quarters_data, final_scores
            
            for idx, row in enumerate(rows):
                team_name = teams[idx]
                team_id = team_ids[idx]
                opponent_name = teams[1 - idx]
                opponent_id = team_ids[1 - idx]
                
                # קריאת רבעים + סה"כ
                q1 = row.find('td', class_='data-one')
                q2 = row.find('td', class_='data-two')
                q3 = row.find('td', class_='data-three')
                q4 = row.find('td', class_='data-four')
                total = row.find('td', class_='data-points')
                
                # המרה למספרים
                q1_score = int(q1.text.strip()) if q1 and q1.text.strip().isdigit() else 0
                q2_score = int(q2.text.strip()) if q2 and q2.text.strip().isdigit() else 0
                q3_score = int(q3.text.strip()) if q3 and q3.text.strip().isdigit() else 0
                q4_score = int(q4.text.strip()) if q4 and q4.text.strip().isdigit() else 0
                total_score = int(total.text.strip()) if total and total.text.strip().isdigit() else 0
                
                # שמירת תוצאה סופית
                team_totals[team_name] = total_score
                
                quarters = [
                    ('Q1', q1_score),
                    ('Q2', q2_score),
                    ('Q3', q3_score),
                    ('Q4', q4_score)
                ]
                
                # ✅ בדיקת הארכות
                ot_cells = row.find_all('td', class_=lambda x: x and 'data-ot' in x)
                if ot_cells:
                    for ot_idx, ot_cell in enumerate(ot_cells, 1):
                        ot_score = int(ot_cell.text.strip()) if ot_cell.text.strip().isdigit() else 0
                        quarters.append((f'OT{ot_idx}', ot_score))
                
                opponent_row = rows[1 - idx]
                opp_q1 = opponent_row.find('td', class_='data-one')
                opp_q2 = opponent_row.find('td', class_='data-two')
                opp_q3 = opponent_row.find('td', class_='data-three')
                opp_q4 = opponent_row.find('td', class_='data-four')
                
                opponent_quarters = [
                    int(opp_q1.text.strip()) if opp_q1 and opp_q1.text.strip().isdigit() else 0,
                    int(opp_q2.text.strip()) if opp_q2 and opp_q2.text.strip().isdigit() else 0,
                    int(opp_q3.text.strip()) if opp_q3 and opp_q3.text.strip().isdigit() else 0,
                    int(opp_q4.text.strip()) if opp_q4 and opp_q4.text.strip().isdigit() else 0
                ]
                
                # ✅ הארכות יריבה
                opp_ot_cells = opponent_row.find_all('td', class_=lambda x: x and 'data-ot' in x)
                if opp_ot_cells:
                    for ot_cell in opp_ot_cells:
                        ot_score = int(ot_cell.text.strip()) if ot_cell.text.strip().isdigit() else 0
                        opponent_quarters.append(ot_score)
                
                for (quarter, score), opp_score in zip(quarters, opponent_quarters):
                    quarters_data.append({
                        'team': team_name,
                        'team_id': team_id,
                        'opponent': opponent_name,
                        'opponent_id': opponent_id,
                        'quarter': quarter,
                        'score': score,
                        'score_against': opp_score
                    })
            
            # יצירת מילון תוצאות סופיות
            if len(teams) == 2 and all(team in team_totals for team in teams):
                final_scores = {
                    'home_team': teams[0],
                    'away_team': teams[1],
                    'home_score': team_totals[teams[0]],
                    'away_score': team_totals[teams[1]]
                }
                
                self.log(f"      Final Score: {teams[0]} {team_totals[teams[0]]} - {team_totals[teams[1]]} {teams[1]}")
            
            return quarters_data, final_scores
            
        except Exception as e:
            self.log(f"   ❌ Error parsing quarters: {e}")
            import traceback
            self.log(traceback.format_exc())
            return quarters_data, final_scoresדןנ
            
    def _scrape_player_stats(self, soup, game_id, game_date):
        """גזירת סטטיסטיקות שחקנים"""
        player_stats = []
        
        try:
            # טען אינדקס
            player_index = self._load_player_index()
            
            performance_sections = soup.find_all('div', class_='sp-template-event-performance-values')
            
            for section in performance_sections:
                team_header = section.find('h4', class_='sp-table-caption')
                if not team_header:
                    continue
                
                team_name_raw = team_header.text.strip()
                team_info = self.normalizer.normalize_team_name(team_name_raw)
                team_name = team_info['club_name']
                team_id = team_info['team_id']
                
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
                        'team': team_name,
                        'team_id': team_id
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
                            # ✅ המרת דקות לשניות
                            if 'min' in player_data:
                                player_data['min'] = self.normalizer.normalize_minutes(player_data['min'])
                            
                            # מספר חולצה
                            if '#' in player_data:
                                player_data['number'] = player_data.pop('#')
                            
                            # יצירת player_id מהאינדקס
                            player_name = player_data['player_name']
                            if player_name in player_index:
                                player_data['player_id'] = player_index[player_name]['player_id']
                            else:
                                player_data['player_id'] = generate_player_id(player_name, '', self.league_id)
                                self.log(f"      ⚠️  Player not in index: {player_name}")
                            
                            # עיבוד סטטיסטיקות זריקה
                            player_data.pop('pm', None)
                            player_data = self.stats_calc.split_shooting_stats(player_data)
                            
                            # ✅ המרת כל הערכים למספרים
                            numeric_fields = ['pts', 'def', 'off', 'reb', 'pf', 'pfa', 'stl', 'to', 
                                            'ast', 'blk', 'blka', 'rate', 'number',
                                            'fgm', 'fga', 'fg_pct', '2pm', '2pa', '2p_pct',
                                            '3pm', '3pa', '3p_pct', 'ftm', 'fta', 'ft_pct']
                            
                            for field in numeric_fields:
                                if field in player_data:
                                    try:
                                        # אם זה אחוזים עם % - הסר אותו
                                        val = str(player_data[field]).replace('%', '').strip()
                                        if val and val != '-':
                                            if field.endswith('_pct'):
                                                player_data[field] = float(val)
                                            else:
                                                player_data[field] = int(val)
                                        else:
                                            player_data[field] = 0
                                    except:
                                        player_data[field] = 0
                            
                            player_stats.append(player_data)
            
            return player_stats
            
        except Exception as e:
            self.log(f"   ❌ Error parsing player stats: {e}")
            import traceback
            self.log(traceback.format_exc())
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
                team_info = self.normalizer.normalize_team_name(team_name_raw)
                team_name = team_info['club_name']
                team_id = team_info['team_id']
                
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
                    opponent_id = None
                    for other_section in performance_sections:
                        other_header = other_section.find('h4', class_='sp-table-caption')
                        if other_header:
                            other_team_raw = other_header.text.strip()
                            other_team_info = self.normalizer.normalize_team_name(other_team_raw)
                            other_team = other_team_info['club_name']
                            if other_team != team_name:
                                opponent_name = other_team
                                opponent_id = other_team_info['team_id']
                                break
                    
                    stats_dict = {
                        'team': team_name,
                        'team_id': team_id,
                        'opponent': opponent_name,
                        'opponent_id': opponent_id
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
                    stats_dict = self.stats_calc.split_shooting_stats(stats_dict)
                    
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
                                    'נקודות מהזדמנות שנייה:': '2nd_chance_pts',  # ✅ שינוי
                                    'נקודות ספסל:': 'bench_pts',
                                    'נקודות ממתפרצת:': 'fast_break_pts',
                                    'נקודות בצבע:': 'points_in_paint',
                                    'נקודות מאיבודים:': 'pts_from_tov'  # ✅ שינוי
                                }
                                
                                stat_key = stat_mapping.get(stat_text, stat_text)
                                stats_dict[stat_key] = int(stat_value) if stat_value.isdigit() else 0
                    
                    # ✅ המרת כל הערכים למספרים
                    numeric_fields = ['pts', 'def', 'off', 'reb', 'pf', 'pfa', 'stl', 'to', 
                                    'ast', 'blk', 'blka', 'rate',
                                    'fgm', 'fga', 'fg_pct', '2pm', '2pa', '2p_pct',
                                    '3pm', '3pa', '3p_pct', 'ftm', 'fta', 'ft_pct']
                    
                    for field in numeric_fields:
                        if field in stats_dict:
                            try:
                                val = str(stats_dict[field]).replace('%', '').strip()
                                if val and val != '-':
                                    if field.endswith('_pct'):
                                        stats_dict[field] = float(val)
                                    else:
                                        stats_dict[field] = int(val)
                                else:
                                    stats_dict[field] = 0
                            except:
                                stats_dict[field] = 0
                    
                    # ✅ חישוב starters_pts
                    total_pts = stats_dict.get('pts', 0)
                    bench_pts = stats_dict.get('bench_pts', 0)
                    stats_dict['starters_pts'] = total_pts - bench_pts
                    
                    team_stats.append(stats_dict)
            
            return team_stats
            
        except Exception as e:
            self.log(f"   ❌ Error parsing team stats: {e}")
            import traceback
            self.log(traceback.format_exc())
            return team_stats