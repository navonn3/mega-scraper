# -*- coding: utf-8 -*-
"""
Statistics Calculator
====================
חישובי סטטיסטיקה: זריקות, אחוזים, מדדים
"""


class StatsCalculator:
    """מחלקה לחישובי סטטיסטיקה"""
    
    @staticmethod
    def split_shooting_stats(stats_dict):
        """
        פיצול סטטיסטיקות זריקה (X-Y) לעמודות נפרדות
        
        Args:
            stats_dict: מילון עם סטטיסטיקות
        
        Returns:
            dict: מילון מעודכן עם פיצול
        """
        
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
        stats_dict = StatsCalculator._calculate_percentages(stats_dict)
        
        # הסרת עמודות ישנות
        for key in ['fgpercent', 'threeppercent', 'ftpercent']:
            if key in stats_dict:
                del stats_dict[key]
        
        return stats_dict
    
    @staticmethod
    def _calculate_percentages(stats_dict):
        """חישוב אחוזי הצלחה"""
        
        # 2-point %
        two_ptm = stats_dict.get('2ptm', 0)
        two_pta = stats_dict.get('2pta', 0)
        if two_pta > 0:
            stats_dict['2pt_pct'] = round((two_ptm / two_pta) * 100, 1)
        else:
            stats_dict['2pt_pct'] = 0.0
        
        # 3-point %
        three_ptm = stats_dict.get('3ptm', 0)
        three_pta = stats_dict.get('3pta', 0)
        if three_pta > 0:
            stats_dict['3pt_pct'] = round((three_ptm / three_pta) * 100, 1)
        else:
            stats_dict['3pt_pct'] = 0.0
        
        # Field Goal %
        fgm = stats_dict.get('fgm', 0)
        fga = stats_dict.get('fga', 0)
        if fga > 0:
            stats_dict['fg_pct'] = round((fgm / fga) * 100, 1)
        else:
            stats_dict['fg_pct'] = 0.0
        
        # Free Throw %
        ftm = stats_dict.get('ftm', 0)
        fta = stats_dict.get('fta', 0)
        if fta > 0:
            stats_dict['ft_pct'] = round((ftm / fta) * 100, 1)
        else:
            stats_dict['ft_pct'] = 0.0
        
        return stats_dict
    
    @staticmethod
    def calculate_possessions(stats_dict):
        """
        חישוב Possessions לפי הנוסחה:
        Possessions ≈ FGA + 0.44 × FTA - OFF + TO
        
        Args:
            stats_dict: מילון עם סטטיסטיקות
        
        Returns:
            float: מספר possessions
        """
        fga = stats_dict.get('fga', 0)
        fta = stats_dict.get('fta', 0)
        off = stats_dict.get('off', 0)
        to = stats_dict.get('to', 0)
        
        possessions = fga + (0.44 * fta) - off + to
        
        return round(possessions, 2)
