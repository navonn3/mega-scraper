# -*- coding: utf-8 -*-
"""
iBasketball.co.il Scraper - UPDATED
====================================
גזירה מ-ibasketball.co.il - משתמש ב-team_id מהמיפוי
"""

import requests
import pandas as pd
import time
import os

from utils import log_message, get_soup, save_to_csv, append_to_csv
from models import generate_player_id, generate_game_id, normalize_season
# הוסר generate_team_id - נשתמש ב-team_id מהמיפוי!
from .base_scraper import BaseScraper
from .processors import DataNormalizer, StatsCalculator


class IBasketballScraper(BaseScraper):
    """גזירה מ-ibasketball.co.il"""
    
    def _init_processors(self):
        """אתחול processors"""
        self.normalizer = DataNormalizer(self.league_id, self.league_code)
        self.stats_calc = StatsCalculator()
        
        # טעינת מיפוי קבוצות
        if not self.normalizer.load_team_mapping():
            raise ValueError("Failed to load team mapping")
    
    # ============================================
    # PLAYER DETAILS
    # ============================================
    
    def _update_player_details(self):
        """עדכון פרטי שחקנים"""
        self.log("STEP 1: UPDATING PLAYER DETAILS")
        
        # גזירת רשימת שחקנים
        players = self._scrape_player_list()
        if not players:
            self.log("❌ No players found")
            return False
        
        self.log(f"Found {len(players)} players")
        
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
            
            # נרמול שם קבוצה - מחזיר גם team_id!
            team_info = self.normalizer.normalize_team_name(current_team_raw)
            current_team = team_info['club_name']
            team_id = team_info['team_id']  # ⭐ team_id מהמיפוי
            
            # בדיקה האם צריך לגזור
            should_scrape, reason = self._needs_scraping(
                player_name, existing_details, existing_history
            )
            
            if should_scrape:
                self.log(f"[{i}/{len(players)}] Scraping: {player_name} ({reason})")
                
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
            
            # יצירת player_id
            player_id = generate_player_id(player_name, details["Date Of Birth"], self.league_id)
            
            # פרטים בסיסיים
            new_details = {
                "player_id": player_id,
                "Name": player_name,
                "Team": current_team,
                "team_id": team_id,  # ⭐ team_id מהמיפוי
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
                "team_id": team_id,  # ⭐ team_id מהמיפוי
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
        
        self.log(f"✅ Player details updated")
        self.log(f"   Total: {len(players)} | New: {new_players} | Updated: {updated_players} | Skipped: {skipped_players}")
        
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
        """גזירת היסטוריית קבוצות"""
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
        # שחקן חדש
        if player_name not in existing_details:
            return True, "New player"
        
        # במצב QUICK - דלג על קיימים
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
    # GAME DETAILS
    # ============================================

    def _update_game_details(self):
        """עדכון משחקים"""
        self.log("STEP 2: UPDATING GAME DETAILS")
        
        # הורדת לוח משחקים
        games_df = self._download_games_schedule()
        if games_df is None:
            self.log("❌ Failed to download games schedule")
            return False
        
        # ⭐ הוספת league_id כעמודה ראשונה
        games_df.insert(0, 'league_id', self.league_id)
        # ⭐ הוספת gameid בעמודה השנייה
        games_df.insert(1, 'gameid', games_df['Code'].apply(lambda code: f"{self.league_id}_{code}"))

        
        # נרמול שמות קבוצות
        games_df = self.normalizer.normalize_schedule_dataframe(games_df)
        
        # שמירת לוח מנורמל
        csv_path = os.path.join(self.games_folder, 'games_schedule.csv')
        games_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        self.log(f"✅ Games schedule normalized and saved")
        
        # גזירת סטטיסטיקות
        return self._scrape_all_games(games_df)
    
    def _download_games_schedule(self):
        """הורדת לוח משחקים"""
        league_id_num = self._extract_league_id()
        if not league_id_num:
            self.log("❌ Could not find league_id")
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
            
            self.log(f"✅ Downloaded schedule: {len(df)} games")
            return df
            
        except Exception as e:
            self.log(f"❌ Error downloading games: {e}")
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
        """גזירת כל המשחקים + עדכון תוצאות"""
        if 'Home Score' not in games_df.columns:
            self.log("❌ 'Home Score' column not found")
            return False
        
        completed_games = games_df[games_df['Home Score'].notna() & (games_df['Home Score'] != '')]
        self.log(f"   Found {len(completed_games)} completed games")
        
        # טעינת משחקים קיימים
        existing_game_ids = self._load_existing_game_ids()
        if existing_game_ids:
            self.log(f"   Already scraped: {len(existing_game_ids)} games")
        
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
            self.log("   ✅ All games already scraped")
            return True
        
        self.log(f"   Scraping {len(games_to_scrape)} new games")
        
        # גזירה
        all_quarters = []
        all_player_stats = []
        all_team_stats = []
        final_scores_updates = []  # ⭐ חדש
        
        for count, (idx, row, game_id) in enumerate(games_to_scrape, 1):
            home_team = row.get('Home Team', '')
            away_team = row.get('Away Team', '')
            game_date = self.normalizer.normalize_date(row.get('Date', ''))
            
            self.log(f"   [{count}/{len(games_to_scrape)}] Game {game_id}: {home_team} vs {away_team}")
            
            try:
                quarters, players, teams, final_scores = self._scrape_game_details(game_id, game_date)  # ⭐ שונה - 4 ערכים
                self.log(f"      DEBUG: final_scores = {final_scores}")

                
                if quarters:
                    all_quarters.extend(quarters)
                if players:
                    all_player_stats.extend(players)
                if teams:
                    all_team_stats.extend(teams)
                
                # ⭐ שמירת תוצאה סופית לעדכון
                if final_scores:
                    final_scores_updates.append({
                        'game_id': game_id,
                        'idx': idx,
                        **final_scores
                    })
                
                if not quarters and not players and not teams:
                    self.log(f"   ⚠️  No stats found for game {game_id}")
                
            except Exception as e:
                self.log(f"   ❌ Error scraping game {game_id}: {e}")
                continue
            
            time.sleep(1)
        
        # שמירה
        if all_quarters:
            quarters_path = os.path.join(self.games_folder, 'game_quarters.csv')
            append_to_csv(all_quarters, quarters_path, 
                         columns=['game_id', 'league_id', 'team', 'team_id', 'opponent', 'opponent_id', 'game_date', 'quarter', 'score', 'score_against'])
            self.log(f"   ✅ Saved quarters: {len(all_quarters)} records")
        
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
            self.log(f"   ✅ Saved player stats: {len(all_player_stats)} records")
        
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
            self.log(f"   ✅ Saved team stats: {len(all_team_stats)} records")
        

        # ⭐ עדכון תוצאות בלוח המשחקים
        if final_scores_updates:
            self.log(f"   DEBUG: About to update {len(final_scores_updates)} scores")
            self._update_schedule_scores(games_df, final_scores_updates)
        else:
            self.log(f"   DEBUG: final_scores_updates is empty!")
        
        self.log(f"✅ Game stats updated: {len(games_to_scrape)} new games")
        return True

    def _update_schedule_scores(self, games_df, final_scores_updates):
        """
        עדכון תוצאות בלוח המשחקים
        
        Args:
            games_df: DataFrame של לוח המשחקים
            final_scores_updates: רשימת תוצאות שנגזרו מדפי המשחקים
        """
        self.log("   Updating game scores in schedule...")
        
        for score_update in final_scores_updates:
            idx = score_update['idx']
            
            # עדכון התוצאות
            games_df.at[idx, 'Home Score'] = score_update['home_score']
            games_df.at[idx, 'Away Score'] = score_update['away_score']
            
            self.log(f"      Updated: {score_update['home_team']} {score_update['home_score']}-{score_update['away_score']} {score_update['away_team']}")
        
        # שמירה מחדש של הלוח המעודכן
        csv_path = os.path.join(self.games_folder, 'games_schedule.csv')
        games_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        
        self.log(f"   ✅ Updated {len(final_scores_updates)} game scores in schedule")
    
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
                self.log(f"   ⚠️  Could not read existing quarters: {e}")
        
        return existing_ids
    

    def _scrape_game_details(self, game_id, game_date):
        """גזירת פרטי משחק בודד"""
        original_code = game_id.split('_')[-1]
        game_url = f"https://ibasketball.co.il/match/{original_code}/"
        
        soup = get_soup(game_url)
        if not soup:
            return None, None, None, None  # ⭐ שונה - 4 ערכים
        
        quarters, final_scores = self._scrape_quarter_scores(soup, game_id, game_date)  # ⭐ שונה - 2 ערכים
        players = self._scrape_player_stats(soup, game_id, game_date)
        teams = self._scrape_team_stats(soup, game_id, game_date)
        
        return quarters, players, teams, final_scores  # ⭐ שונה - 4 ערכים
        
    def _scrape_quarter_scores(self, soup, game_id, game_date):
        """גזירת ניקוד לפי רבעים + תוצאה סופית"""
        quarters_data = []
        final_scores = {}
        
        try:
            results_table = soup.find('table', class_='sp-event-results')
            if not results_table:
                return quarters_data, final_scores
            
            rows = results_table.find('tbody').find_all('tr')
            
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
            return quarters_data, final_scores
        
    
    def _scrape_player_stats(self, soup, game_id, game_date):
        """גזירת סטטיסטיקות שחקנים"""
        player_stats = []
        
        try:
            performance_sections = soup.find_all('div', class_='sp-template-event-performance-values')
            player_details = self._load_existing_player_data()[0]
            
            for section in performance_sections:
                team_header = section.find('h4', class_='sp-table-caption')
                if not team_header:
                    continue
                
                team_name_raw = team_header.text.strip()
                team_info = self.normalizer.normalize_team_name(team_name_raw)
                team_name = team_info['club_name']
                team_id = team_info['team_id']  # ⭐ team_id מהמיפוי
                
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
                        'team_id': team_id,  # ⭐ team_id מהמיפוי
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
                                player_data['min'] = self.normalizer.normalize_minutes(player_data['min'])
                            
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
                            player_data = self.stats_calc.split_shooting_stats(player_data)
                            player_stats.append(player_data)
            
            return player_stats
            
        except Exception as e:
            self.log(f"   ❌ Error parsing player stats: {e}")
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
                team_id = team_info['team_id']  # ⭐ team_id מהמיפוי
                
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
                                opponent_id = other_team_info['team_id']  # ⭐ team_id מהמיפוי
                                break
                    
                    stats_dict = {
                        'game_id': game_id,
                        'league_id': self.league_id,
                        'team': team_name,
                        'team_id': team_id,  # ⭐ team_id מהמיפוי
                        'opponent': opponent_name,
                        'opponent_id': opponent_id,  # ⭐ team_id מהמיפוי
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
            self.log(f"   ❌ Error parsing team stats: {e}")
            return team_stats