# utils/supabase_uploader.py
from supabase import create_client
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# === פונקציות עזר ===

def convert_date(date_str):
    """ממיר תאריכים לפורמט SQL"""
    if not date_str:
        return None
    try:
        if '/' in date_str:
            return datetime.strptime(date_str, "%Y/%m/%d").strftime("%Y-%m-%d")
        elif '-' in date_str:
            parts = date_str.split('-')
            if len(parts[0]) == 4:
                return date_str
            return datetime.strptime(date_str, "%d-%m-%Y").strftime("%Y-%m-%d")
    except:
        return None

# === שחקנים ===

def upsert_player(player_data):
    """מעלה/מעדכן שחקן"""
    data = {
        'player_id': player_data['player_id'],
        'name': player_data['name'],
        'current_team_id': player_data.get('current_team_id'),
        'league_id': int(player_data['league_id']),
        'date_of_birth': convert_date(player_data.get('date_of_birth')),
        'height': float(player_data['height']) if player_data.get('height') else None,
        'jersey_number': player_data.get('jersey_number'),
        'ibba_url': f"https://ibba.co.il/Players/Profile/{player_data['player_id']}",
        'updated_at': datetime.now().isoformat()
    }
    
    try:
        supabase.table('players').upsert(data).execute()
        print(f"✅ Player: {data['name']}")
        return True
    except Exception as e:
        print(f"❌ Error player {data['name']}: {e}")
        return False

def upsert_player_history(history_data):
    """מעלה היסטוריית עונות"""
    success_count = 0
    for record in history_data:
        data = {
            'player_id': record['player_id'],
            'season': record['season'],
            'team_name': record['team_name'],
            'league_name': record['league_name'],
            'league_id': int(record['league_id']) if record.get('league_id') else None,
            'updated_at': datetime.now().isoformat()
        }
        try:
            supabase.table('player_season_history').upsert(data).execute()
            success_count += 1
        except Exception as e:
            print(f"❌ Error season {record['season']}: {e}")
    
    print(f"✅ History: {success_count}/{len(history_data)}")
    return success_count

# === משחקים ===

def upsert_game(game_data):
    """מעלה משחק"""
    data = {
        'game_id': game_data['game_id'],
        'league_id': int(game_data['league_id']),
        'season': game_data.get('season'),
        'code': game_data.get('code'),
        'date': convert_date(game_data['date']),
        'time': game_data.get('time'),
        'round': int(game_data['round']) if game_data.get('round') else None,
        'home_team': game_data['home_team'],
        'home_team_id': game_data.get('home_team_id'),
        'away_team': game_data['away_team'],
        'away_team_id': game_data.get('away_team_id'),
        'venue': game_data.get('venue'),
        'home_score': game_data.get('home_score'),
        'away_score': game_data.get('away_score'),
        'status': game_data.get('status', 'scheduled'),
        'overtimes': game_data.get('overtimes', 0),
        'close_game': game_data.get('close_game', False),
        'winner': game_data.get('winner'),
        'loser': game_data.get('loser'),
        'updated_at': datetime.now().isoformat()
    }
    
    try:
        supabase.table('games').upsert(data).execute()
        print(f"✅ Game: {data['game_id']}")
        return True
    except Exception as e:
        print(f"❌ Error game {data['game_id']}: {e}")
        return False

def upsert_game_quarters(game_id, league_id, quarters_data):
    """מעלה רבעים"""
    success_count = 0
    for team_id, quarters in quarters_data.items():
        for i, quarter in enumerate(quarters, 1):
            data = {
                'game_id': game_id,
                'team_id': int(team_id),
                'league_id': int(league_id),
                'quarter': i,
                'score': quarter['score'],
                'score_against': quarter['score_against'],
                'updated_at': datetime.now().isoformat()
            }
            try:
                supabase.table('game_quarters').upsert(data).execute()
                success_count += 1
            except Exception as e:
                print(f"❌ Error quarter {i}: {e}")
    
    print(f"✅ Quarters: {success_count}")
    return success_count

def upsert_player_stats(game_id, league_id, player_stats):
    """מעלה סטטיסטיקות שחקנים"""
    success_count = 0
    for stat in player_stats:
        data = {
            'game_id': game_id,
            'player_id': stat['player_id'],
            'league_id': int(league_id),
            'player_name': stat.get('player_name'),
            'team': stat.get('team'),
            'team_id': stat.get('team_id'),
            'min': stat.get('min'),
            'pts': stat.get('pts'),
            'fgm': stat.get('fgm'),
            'fga': stat.get('fga'),
            'fg_pct': stat.get('fg_pct'),
            '2ptm': stat.get('2ptm'),
            '2pta': stat.get('2pta'),
            '2pt_pct': stat.get('2pt_pct'),
            '3ptm': stat.get('3ptm'),
            '3pta': stat.get('3pta'),
            '3pt_pct': stat.get('3pt_pct'),
            'ftm': stat.get('ftm'),
            'fta': stat.get('fta'),
            'ft_pct': stat.get('ft_pct'),
            'def': stat.get('def'),
            'off': stat.get('off'),
            'reb': stat.get('reb'),
            'ast': stat.get('ast'),
            'stl': stat.get('stl'),
            'to': stat.get('to'),
            'pf': stat.get('pf'),
            'pfa': stat.get('pfa'),
            'blk': stat.get('blk'),
            'blka': stat.get('blka'),
            'rate': stat.get('rate'),
            'starter': stat.get('starter', 0),
            'number': stat.get('number'),
            'updated_at': datetime.now().isoformat()
        }
        try:
            supabase.table('game_player_stats').upsert(data).execute()
            success_count += 1
        except Exception as e:
            print(f"❌ Error player stat {stat.get('player_name')}: {e}")
    
    print(f"✅ Player stats: {success_count}/{len(player_stats)}")
    return success_count

def upsert_team_stats(game_id, league_id, team_stats):
    """מעלה סטטיסטיקות קבוצות"""
    success_count = 0
    for stat in team_stats:
        data = {
            'game_id': game_id,
            'team_id': stat['team_id'],
            'league_id': int(league_id),
            'team': stat.get('team'),
            'opponent': stat.get('opponent'),
            'opponent_id': stat.get('opponent_id'),
            'pts': stat.get('pts'),
            'fgm': stat.get('fgm'),
            'fga': stat.get('fga'),
            'fg_pct': stat.get('fg_pct'),
            '2ptm': stat.get('2ptm'),
            '2pta': stat.get('2pta'),
            '2pt_pct': stat.get('2pt_pct'),
            '3ptm': stat.get('3ptm'),
            '3pta': stat.get('3pta'),
            '3pt_pct': stat.get('3pt_pct'),
            'ftm': stat.get('ftm'),
            'fta': stat.get('fta'),
            'ft_pct': stat.get('ft_pct'),
            'def': stat.get('def'),
            'off': stat.get('off'),
            'reb': stat.get('reb'),
            'ast': stat.get('ast'),
            'stl': stat.get('stl'),
            'to': stat.get('to'),
            'pf': stat.get('pf'),
            'pfa': stat.get('pfa'),
            'blk': stat.get('blk'),
            'blka': stat.get('blka'),
            'rate': stat.get('rate'),
            'second_chance_pts': stat.get('2nd_chance_pts'),
            'bench_pts': stat.get('bench_pts'),
            'fast_break_pts': stat.get('fast_break_pts'),
            'points_in_paint': stat.get('points_in_paint'),
            'pts_off_turnovers': stat.get('pts_from_tov'),
            'starters_pts': stat.get('starters_pts'),
            'updated_at': datetime.now().isoformat()
        }
        try:
            supabase.table('game_team_stats').upsert(data).execute()
            success_count += 1
        except Exception as e:
            print(f"❌ Error team stat {stat.get('team')}: {e}")
    
    print(f"✅ Team stats: {success_count}/{len(team_stats)}")
    return success_count

# === פונקציות מורכבות ===

def upload_full_game(game_data):
    """מעלה משחק מלא עם כל הנתונים"""
    print(f"\n{'='*50}")
    print(f"📤 Uploading game: {game_data['game_id']}")
    print(f"{'='*50}")
    
    # משחק
    if not upsert_game(game_data):
        return False
    
    league_id = game_data['league_id']
    game_id = game_data['game_id']
    
    # רבעים
    if 'quarters' in game_data:
        upsert_game_quarters(game_id, league_id, game_data['quarters'])
    
    # סטטיסטיקות שחקנים
    if 'player_stats' in game_data:
        upsert_player_stats(game_id, league_id, game_data['player_stats'])
    
    # סטטיסטיקות קבוצות
    if 'team_stats' in game_data:
        upsert_team_stats(game_id, league_id, game_data['team_stats'])
    
    print(f"✅ Game {game_id} uploaded successfully!\n")
    return True

def upload_player_full(player_details, player_history):
    """מעלה שחקן + היסטוריה"""
    print(f"\n{'='*50}")
    print(f"📤 Uploading player: {player_details['name']}")
    print(f"{'='*50}")
    
    if not upsert_player(player_details):
        return False
    
    if player_history:
        upsert_player_history(player_history)
    
    print(f"✅ Player uploaded successfully!\n")
    return True