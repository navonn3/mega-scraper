# -*- coding: utf-8 -*-
"""
Averages Calculator - FIXED
============================
חישוב ממוצעי שחקנים, קבוצות ויריבים
תוקן: משתמש ב-team_id מהמיפוי (לא מהנתונים הקיימים)
"""

import os
import pandas as pd

from utils import log_message, load_global_team_mapping, normalize_team_name_global
from .stats_calculator import StatsCalculator


class AveragesCalculator:
    """מחלקה לחישוב ממוצעים"""
    
    def __init__(self, league_id, league_code, data_folder, games_folder):
        """
        Args:
            league_id: מזהה ליגה מספרי
            league_code: קוד ליגה
            data_folder: תיקיית נתונים ראשית
            games_folder: תיקיית משחקים
        """
        self.league_id = league_id
        self.league_code = league_code
        self.data_folder = data_folder
        self.games_folder = games_folder
        
        # טעינת מיפוי קבוצות
        self.team_mapping = load_global_team_mapping()
    
    def _get_team_id(self, team_name):
        """
        קבלת team_id נכון מהמיפוי
        ⭐ מחפש רק בליגה הרלוונטית!
        
        Args:
            team_name: שם הקבוצה המנורמל
        
        Returns:
            int: team_id מהמיפוי (רק מהליגה הנכונה)
        """
        if not self.team_mapping:
            log_message("⚠️  No team mapping available", self.league_code)
            return None
        
        league_id_int = int(self.league_id)
        
        # ⭐ חיפוש עם מפתח מורכב: (team_name, league_id)
        key = (team_name, league_id_int)
        if key in self.team_mapping:
            return self.team_mapping[key]['team_id']
        
        # נסיון עם strip
        key_stripped = (team_name.strip(), league_id_int)
        if key_stripped in self.team_mapping:
            return self.team_mapping[key_stripped]['team_id']
        
        log_message(f"   ⚠️  No team_id found for: {team_name} in league {self.league_id}", self.league_code)
        return None
    
    def calculate_all(self):
        """חישוב כל הממוצעים"""
        
        # טעינת קבצי stats
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
        player_avg = self.calculate_player_averages(player_df)
        if player_avg is not None:
            player_averages_file = os.path.join(self.data_folder, f"{self.league_code}_player_averages.csv")
            player_avg.to_csv(player_averages_file, index=False, encoding='utf-8-sig')
            log_message(f"✅ Player averages: {len(player_avg)} players", self.league_code)
        
        # חישוב ממוצעי קבוצות
        team_avg = self.calculate_team_averages(team_df)
        if team_avg is not None:
            # חישוב ממוצעי יריבים
            opponent_avg = self.calculate_opponent_averages(team_df)
            
            # הוספת pts_allowed
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
            log_message(f"✅ Team averages: {len(team_avg)} teams", self.league_code)
            
            # שמירת ממוצעי יריבים
            if opponent_avg is not None:
                opponent_averages_file = os.path.join(self.data_folder, f"{self.league_code}_opponent_averages.csv")
                opponent_avg.to_csv(opponent_averages_file, index=False, encoding='utf-8-sig')
                log_message(f"✅ Opponent averages: {len(opponent_avg)} teams", self.league_code)
        
        return True
    
    def calculate_player_averages(self, player_df):
        """חישוב ממוצעי שחקנים"""
        
        numeric_cols = [
            'pts', '2ptm', '2pta', '3ptm', '3pta', 'fgm', 'fga',
            'ftm', 'fta', 'def', 'off', 'reb', 'pf', 'pfa',
            'stl', 'to', 'ast', 'blk', 'blka', 'rate', 'min'
        ]
        
        # המרה למספרים
        for col in numeric_cols:
            if col in player_df.columns:
                player_df[col] = pd.to_numeric(player_df[col], errors='coerce')
        
        if 'starter' in player_df.columns:
            player_df['starter'] = pd.to_numeric(player_df['starter'], errors='coerce')
        
        # הגדרת aggregation
        agg_dict = {col: 'mean' for col in numeric_cols if col in player_df.columns}
        agg_dict['game_id'] = 'count'
        
        if 'starter' in player_df.columns:
            agg_dict['starter'] = 'sum'
        
        # קיבוץ
        if 'player_id' not in player_df.columns:
            log_message("⚠️  player_id not found", self.league_code)
            return None
        
        player_avg = player_df.groupby(['player_id', 'player_name', 'team']).agg(agg_dict).reset_index()
        player_avg.rename(columns={'game_id': 'games_played'}, inplace=True)
        
        if 'starter' in player_avg.columns:
            player_avg.rename(columns={'starter': 'games_started'}, inplace=True)
        
        # ⭐ תיקון team_id - נקבל מהמיפוי!
        player_avg['team_id'] = player_avg['team'].apply(self._get_team_id)
        
        # חישוב אחוזים
        player_avg = self._add_percentages(player_avg)
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
    
    def calculate_team_averages(self, team_df):
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
        
        if 'team' not in team_df.columns:
            log_message("⚠️  team column not found", self.league_code)
            return None
        
        team_avg = team_df.groupby(['team']).agg({
            **{col: 'mean' for col in numeric_team_cols if col in team_df.columns},
            'game_id': 'count'
        }).reset_index()
        
        team_avg.rename(columns={'game_id': 'games_played'}, inplace=True)
        
        # ⭐ תיקון team_id - נקבל מהמיפוי!
        team_avg['team_id'] = team_avg['team'].apply(self._get_team_id)
        
        # חישוב possessions
        if all(col in team_avg.columns for col in ['fga', 'fta', 'off', 'to']):
            team_avg['possessions'] = team_avg.apply(
                lambda row: StatsCalculator.calculate_possessions(row.to_dict()),
                axis=1
            )
        
        # חישוב אחוזים
        team_avg = self._add_percentages(team_avg)
        team_avg = team_avg.round(1)
        
        # דירוגים
        team_avg = self._add_rankings(team_avg)
        
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
    
    def calculate_opponent_averages(self, team_df):
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
                
                opp1 = {'team': team1['team'], 'game_id': game_id}
                opp2 = {'team': team2['team'], 'game_id': game_id}
                
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
        opponent_avg = opp_df.groupby(['team']).agg({
            **{col: 'mean' for col in opp_cols},
            'game_id': 'count'
        }).reset_index()
        
        opponent_avg.rename(columns={'game_id': 'games_played'}, inplace=True)
        
        # ⭐ תיקון team_id - נקבל מהמיפוי!
        opponent_avg['team_id'] = opponent_avg['team'].apply(self._get_team_id)
        
        # חישוב אחוזים
        if 'opp_2ptm' in opponent_avg.columns and 'opp_2pta' in opponent_avg.columns:
            opponent_avg['opp_2pt_pct'] = (opponent_avg['opp_2ptm'] / opponent_avg['opp_2pta']).fillna(0) * 100
        
        if 'opp_3ptm' in opponent_avg.columns and 'opp_3pta' in opponent_avg.columns:
            opponent_avg['opp_3pt_pct'] = (opponent_avg['opp_3ptm'] / opponent_avg['opp_3pta']).fillna(0) * 100
        
        if 'opp_fgm' in opponent_avg.columns and 'opp_fga' in opponent_avg.columns:
            opponent_avg['opp_fg_pct'] = (opponent_avg['opp_fgm'] / opponent_avg['opp_fga']).fillna(0) * 100
        
        if 'opp_ftm' in opponent_avg.columns and 'opp_fta' in opponent_avg.columns:
            opponent_avg['opp_ft_pct'] = (opponent_avg['opp_ftm'] / opponent_avg['opp_fta']).fillna(0) * 100
        
        # possessions
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
        
        # דירוגים (גבוה = גרוע בהגנה, חוץ מ-TO)
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
    
    def _add_percentages(self, df):
        """הוספת אחוזים ל-DataFrame"""
        
        if '2ptm' in df.columns and '2pta' in df.columns:
            df['2pt_pct'] = (df['2ptm'] / df['2pta']).fillna(0) * 100
        
        if '3ptm' in df.columns and '3pta' in df.columns:
            df['3pt_pct'] = (df['3ptm'] / df['3pta']).fillna(0) * 100
        
        if 'fgm' in df.columns and 'fga' in df.columns:
            df['fg_pct'] = (df['fgm'] / df['fga']).fillna(0) * 100
        
        if 'ftm' in df.columns and 'fta' in df.columns:
            df['ft_pct'] = (df['ftm'] / df['fta']).fillna(0) * 100
        
        return df
    
    def _add_rankings(self, team_avg):
        """הוספת דירוגים לקבוצות"""
        
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
        
        return team_avg