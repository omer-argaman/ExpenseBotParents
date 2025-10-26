"""
Gamification module for the Expense Tracker Bot
Implements streaks, XP, levels, achievements, and challenges
"""

import json
import os
import random
from datetime import datetime, timedelta
import logging

# Set up logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# XP constants
XP_EXPENSE_LOGGED = 10
XP_CATEGORY_USED = 5
XP_REPORT_VIEWED = 15
XP_STREAK_DAY = 20
XP_CHALLENGE_COMPLETED = 50
XP_UNDER_BUDGET = 25
XP_MONTHLY_COMPLETE = 100

# Streak constants
STREAK_FREEZE_COST = 100  # XP cost to purchase a streak freeze

# Level thresholds - XP needed for each level
LEVEL_THRESHOLDS = [
    0,      # Level 1
    100,    # Level 2
    250,    # Level 3
    500,    # Level 4
    1000,   # Level 5
    2000,   # Level 6
    3500,   # Level 7
    5000,   # Level 8
    7500,   # Level 9
    10000,  # Level 10
    15000,  # Level 11
    20000,  # Level 12
    30000,  # Level 13
    50000,  # Level 14
    75000,  # Level 15
]

# Achievement definitions
ACHIEVEMENTS = {
    'expense_logger': {
        'name': 'Expense Logger',
        'levels': [
            {'count': 10, 'xp': 20, 'description': 'Log 10 expenses', 'emoji': '📝'},
            {'count': 50, 'xp': 50, 'description': 'Log 50 expenses', 'emoji': '📝'},
            {'count': 100, 'xp': 100, 'description': 'Log 100 expenses', 'emoji': '📝'},
            {'count': 500, 'xp': 200, 'description': 'Log 500 expenses', 'emoji': '📝'},
            {'count': 1000, 'xp': 500, 'description': 'Log 1000 expenses', 'emoji': '📝'},
        ],
        'metric': 'total_expenses_logged'
    },
    'streak_master': {
        'name': 'Streak Master',
        'levels': [
            {'count': 3, 'xp': 30, 'description': '3-day logging streak', 'emoji': '🔥'},
            {'count': 7, 'xp': 70, 'description': '7-day logging streak', 'emoji': '🔥'},
            {'count': 14, 'xp': 140, 'description': '14-day logging streak', 'emoji': '🔥'},
            {'count': 30, 'xp': 300, 'description': '30-day logging streak', 'emoji': '🔥'},
            {'count': 100, 'xp': 1000, 'description': '100-day logging streak', 'emoji': '🔥'},
        ],
        'metric': 'current_streak'
    },
    'budget_hero': {
        'name': 'Budget Hero',
        'levels': [
            {'count': 1, 'xp': 50, 'description': 'Stay under budget for 1 month', 'emoji': '🦸'},
            {'count': 3, 'xp': 150, 'description': 'Stay under budget for 3 months', 'emoji': '🦸'},
            {'count': 6, 'xp': 300, 'description': 'Stay under budget for 6 months', 'emoji': '🦸'},
            {'count': 12, 'xp': 1000, 'description': 'Stay under budget for 12 months', 'emoji': '🦸'},
        ],
        'metric': 'months_under_budget'
    },
    'category_explorer': {
        'name': 'Category Explorer',
        'levels': [
            {'count': 5, 'xp': 25, 'description': 'Use 5 different categories', 'emoji': '🧭'},
            {'count': 10, 'xp': 75, 'description': 'Use 10 different categories', 'emoji': '🧭'},
            {'count': 20, 'xp': 150, 'description': 'Use 20 different categories', 'emoji': '🧭'},
        ],
        'metric': 'unique_categories_used'
    },
    'data_analyst': {
        'name': 'Data Analyst',
        'levels': [
            {'count': 5, 'xp': 25, 'description': 'View 5 reports', 'emoji': '📊'},
            {'count': 25, 'xp': 75, 'description': 'View 25 reports', 'emoji': '📊'},
            {'count': 100, 'xp': 150, 'description': 'View 100 reports', 'emoji': '📊'},
        ],
        'metric': 'reports_viewed'
    }
}

# Weekly challenges - rotated weekly
WEEKLY_CHALLENGES = [
    {
        'id': 'reduce_dining',
        'description': 'Reduce dining expenses by 15% from last week',
        'type': 'category_reduction',
        'category': 'Restaurant',
        'target_percentage': 15,
        'xp_reward': 75,
        'emoji': '🍽️'
    },
    {
        'id': 'no_coffee_week',
        'description': 'Skip coffee shops for a week',
        'type': 'category_avoid',
        'category': 'Coffee',
        'xp_reward': 50,
        'emoji': '☕'
    },
    {
        'id': 'grocery_budget',
        'description': 'Stay under your grocery budget',
        'type': 'category_under_budget',
        'category': 'Groceries',
        'xp_reward': 60,
        'emoji': '🛒'
    },
    {
        'id': 'log_streak',
        'description': 'Log expenses for 5 days in a row',
        'type': 'streak',
        'days_required': 5,
        'xp_reward': 80,
        'emoji': '📆'
    },
    {
        'id': 'use_all_reports',
        'description': 'Check all report types in one week',
        'type': 'use_features',
        'features': ['monthly', 'category', 'chart', 'balance'],
        'xp_reward': 65,
        'emoji': '📈'
    }
]

class GamificationSystem:
    """Handles all gamification mechanics for the expense tracker bot"""
    
    def __init__(self, user_id):
        """Initialize the gamification system for a user"""
        self.user_id = str(user_id)
        self.user_data = self._load_user_data()
        self._ensure_default_values()
        
    def _load_user_data(self):
        """Load user's gamification data from file"""
        filename = f"user_gamification_{self.user_id}.json"
        
        if not os.path.exists(filename):
            # Create a new file with default values
            default_data = self._get_default_data()
            with open(filename, 'w') as f:
                json.dump(default_data, f)
            return default_data
        
        try:
            with open(filename, 'r') as f:
                data = json.load(f)
                
                # Convert lists back to sets where needed
                if "unique_categories_used" in data and isinstance(data["unique_categories_used"], list):
                    data["unique_categories_used"] = set(data["unique_categories_used"])
                
                return data
        except Exception as e:
            logger.error(f"Error loading gamification data: {str(e)}")
            return self._get_default_data()
    
    def _save_user_data(self):
        """Save user's gamification data to file"""
        filename = f"user_gamification_{self.user_id}.json"
        try:
            # Convert sets to lists for JSON serialization before saving
            save_data = self.user_data.copy()
            
            # Handle the unique_categories_used set
            if "unique_categories_used" in save_data:
                if isinstance(save_data["unique_categories_used"], set):
                    save_data["unique_categories_used"] = list(save_data["unique_categories_used"])
            
            with open(filename, 'w') as f:
                json.dump(save_data, f)
            return True
        except Exception as e:
            logger.error(f"Error saving gamification data: {str(e)}")
            return False
    
    def _get_default_data(self):
        """Create default gamification data for a new user"""
        today = datetime.now().strftime("%Y-%m-%d")
        
        # Select a random weekly challenge
        selected_challenge = random.choice(WEEKLY_CHALLENGES)
        
        # Calculate challenge end date (next Sunday)
        now = datetime.now()
        days_until_sunday = (6 - now.weekday()) % 7  # 6 = Sunday
        if days_until_sunday == 0:  # If today is Sunday, go to next Sunday
            days_until_sunday = 7
        challenge_end = (now + timedelta(days=days_until_sunday)).strftime("%Y-%m-%d")
        
        return {
            "xp": 0,
            "level": 1,
            "total_expenses_logged": 0,
            "unique_categories_used": [],  # Use a list instead of a set for JSON serialization
            "reports_viewed": 0,
            "months_under_budget": 0,
            "last_activity_date": today,
            "current_streak": 0,
            "longest_streak": 0,
            "streak_freezes": 0,
            "achievements_unlocked": {},
            "current_challenge": {
                "id": selected_challenge["id"],
                "description": selected_challenge["description"],
                "end_date": challenge_end,
                "progress": 0,
                "completed": False,
                "data": selected_challenge
            },
            "completed_challenges": []
        }
    
    def _ensure_default_values(self):
        """Make sure all expected keys exist in user data"""
        default_data = self._get_default_data()
        
        # Convert sets to lists for JSON serialization
        if "unique_categories_used" in self.user_data and isinstance(self.user_data["unique_categories_used"], list):
            self.user_data["unique_categories_used"] = set(self.user_data["unique_categories_used"])
        
        # Add any missing keys with default values
        for key, value in default_data.items():
            if key not in self.user_data:
                self.user_data[key] = value
        
        # Handle unique_categories_used as a special case (set vs list)
        if "unique_categories_used" not in self.user_data:
            self.user_data["unique_categories_used"] = set()
    
    def _check_streak(self):
        """Check and update the user's streak"""
        today = datetime.now().strftime("%Y-%m-%d")
        last_date = self.user_data.get("last_activity_date", None)
        
        if not last_date:
            # First activity
            self.user_data["current_streak"] = 1
            self.user_data["last_activity_date"] = today
            return
        
        # Convert dates to datetime objects
        today_dt = datetime.strptime(today, "%Y-%m-%d")
        last_dt = datetime.strptime(last_date, "%Y-%m-%d")
        
        # Calculate the difference in days
        diff = (today_dt - last_dt).days
        
        if diff == 0:
            # Already logged today
            return
        elif diff == 1:
            # Consecutive day
            self.user_data["current_streak"] += 1
            self.user_data["last_activity_date"] = today
            
            # Check if this is a new longest streak
            if self.user_data["current_streak"] > self.user_data.get("longest_streak", 0):
                self.user_data["longest_streak"] = self.user_data["current_streak"]
                
            # Award XP for continuing the streak
            self.add_xp(XP_STREAK_DAY, "Continuing your streak")
            
        elif diff == 2 and self.user_data.get("streak_freezes", 0) > 0:
            # Use a streak freeze
            self.user_data["streak_freezes"] -= 1
            self.user_data["last_activity_date"] = today
            logger.info(f"Used a streak freeze for user {self.user_id}")
            
        else:
            # Streak broken
            logger.info(f"Streak broken for user {self.user_id}. Previous streak: {self.user_data['current_streak']}")
            self.user_data["current_streak"] = 1
            self.user_data["last_activity_date"] = today
    
    def add_xp(self, amount, reason):
        """Add XP to the user and check for level up"""
        previous_level = self.get_level()
        self.user_data["xp"] += amount
        current_level = self.get_level()
        
        # Check if the user leveled up
        if current_level > previous_level:
            logger.info(f"User {self.user_id} leveled up to {current_level}")
            self._save_user_data()
            return True, current_level
        
        self._save_user_data()
        return False, current_level
    
    def get_level(self):
        """Get the user's current level based on XP"""
        xp = self.user_data.get("xp", 0)
        
        # Find the highest level threshold that the user's XP exceeds
        level = 1
        for i, threshold in enumerate(LEVEL_THRESHOLDS):
            if xp >= threshold:
                level = i + 1
            else:
                break
                
        return level
    
    def log_expense(self, category, amount, date=None):
        """Record an expense for gamification purposes"""
        # Update stats
        self.user_data["total_expenses_logged"] += 1
        
        # Add category to unique categories
        self.user_data["unique_categories_used"].add(category)
        
        # Check and update streak
        self._check_streak()
        
        # Award XP
        self.add_xp(XP_EXPENSE_LOGGED, "Logging an expense")
        
        # Check for achievements
        achievement_results = self._check_achievements()
        
        # Update challenge progress
        challenge_progress = self._update_challenge_progress(category, amount)
        
        # Convert sets to lists for JSON serialization
        self.user_data["unique_categories_used"] = list(self.user_data["unique_categories_used"])
        
        # Save changes
        self._save_user_data()
        
        # Return any achievements/level ups to notify the user
        return {
            "achievements": achievement_results,
            "challenge_progress": challenge_progress
        }
    
    def log_report_view(self, report_type):
        """Record that the user viewed a report"""
        self.user_data["reports_viewed"] += 1
        
        # Award XP
        self.add_xp(XP_REPORT_VIEWED, f"Viewing {report_type} report")
        
        # Check and update streak
        self._check_streak()
        
        # Check for achievements
        achievement_results = self._check_achievements()
        
        # Update challenge progress if challenge involves using reports
        challenge_progress = self._update_challenge_feature_used(report_type)
        
        # Save changes
        self._save_user_data()
        
        # Return any achievements/level ups to notify the user
        return {
            "achievements": achievement_results,
            "challenge_progress": challenge_progress
        }
    
    def _check_achievements(self):
        """Check for unlocked achievements and award XP"""
        unlocked_achievements = []
        
        for achievement_id, achievement in ACHIEVEMENTS.items():
            metric_name = achievement['metric']
            
            # Special handling for unique_categories_used which is a set
            metric_value = self.user_data.get(metric_name, 0)
            if metric_name == "unique_categories_used" and isinstance(metric_value, set):
                metric_value = len(metric_value)
            
            # Get currently unlocked levels
            unlocked_levels = self.user_data.get("achievements_unlocked", {}).get(achievement_id, [])
            
            # Check each level of the achievement
            for level_idx, level in enumerate(achievement['levels']):
                level_id = level_idx + 1
                
                # Skip if already unlocked
                if level_id in unlocked_levels:
                    continue
                
                # Check if requirement is met
                if metric_value >= level['count']:
                    # Unlock this level
                    if achievement_id not in self.user_data.get("achievements_unlocked", {}):
                        self.user_data.setdefault("achievements_unlocked", {})[achievement_id] = []
                    
                    self.user_data["achievements_unlocked"][achievement_id].append(level_id)
                    
                    # Award XP
                    xp_awarded = level['xp']
                    old_level = self.get_level()
                    self.add_xp(xp_awarded, f"Achievement: {achievement['name']} - {level['description']}")
                    new_level = self.get_level()
                    
                    # Add to the return list
                    unlocked_achievements.append({
                        "achievement_id": achievement_id,
                        "name": achievement['name'],
                        "level": level_id,
                        "description": level['description'],
                        "emoji": level['emoji'],
                        "xp_awarded": xp_awarded,
                        "leveled_up": old_level != new_level,
                        "new_level": new_level
                    })
        
        return unlocked_achievements
    
    def _update_challenge_progress(self, category, amount):
        """Update progress for category-based challenges"""
        if not self.user_data.get("current_challenge"):
            return None
        
        challenge = self.user_data["current_challenge"]
        if challenge.get("completed", False):
            return None
            
        challenge_data = challenge.get("data", {})
        challenge_type = challenge_data.get("type", "")
        
        # Skip if not a category challenge or wrong category
        if "category" not in challenge_data:
            return None
            
        target_category = challenge_data["category"]
        if category.lower() != target_category.lower():
            return None
            
        # Handle different challenge types
        result = None
        
        if challenge_type == "category_avoid":
            # Failed - spent money in category that should be avoided
            challenge["completed"] = True
            challenge["success"] = False
            result = {
                "completed": True,
                "success": False,
                "description": challenge["description"]
            }
                
        elif challenge_type == "category_under_budget":
            # Track spending for this category
            if "current_spending" not in challenge:
                challenge["current_spending"] = 0
                
            challenge["current_spending"] = challenge["current_spending"] + amount
                
            # Get budget for this category - simplified implementation
            # In a real app, we would get this from a budget table
            sample_budgets = {
                "groceries": 1000,
                "dining": 500,
                "entertainment": 300,
                "transportation": 400,
                "coffee": 100
            }
            
            budget = sample_budgets.get(target_category.lower(), 500)  # default budget
            
            # Check if over budget
            if challenge["current_spending"] > budget:
                challenge["completed"] = True
                challenge["success"] = False
                result = {
                    "completed": True,
                    "success": False,
                    "description": challenge["description"]
                }
        
        # If challenge was completed
        if result and result["completed"] and result["success"]:
            # Award XP
            xp_reward = challenge_data.get("xp_reward", XP_CHALLENGE_COMPLETED)
            self.add_xp(xp_reward, f"Challenge completed: {challenge['description']}")
            
            # Add to completed challenges
            self.user_data.setdefault("completed_challenges", []).append({
                "id": challenge["id"],
                "description": challenge["description"],
                "completion_date": datetime.now().strftime("%Y-%m-%d"),
                "xp_awarded": xp_reward
            })
            
        self._save_user_data()
        return result
    
    def _update_challenge_feature_used(self, feature):
        """Update progress for feature usage challenges"""
        if not self.user_data.get("current_challenge"):
            return None
        
        challenge = self.user_data["current_challenge"]
        if challenge.get("completed", False):
            return None
            
        challenge_data = challenge.get("data", {})
        
        # Skip if not a feature usage challenge
        if challenge_data.get("type") != "use_features" or "features" not in challenge_data:
            return None
            
        target_features = challenge_data["features"]
        
        # Skip if this feature not in target list
        if feature not in target_features:
            return None
            
        # Initialize features_used if needed
        if "features_used" not in challenge:
            challenge["features_used"] = []
            
        # Skip if already used
        if feature in challenge["features_used"]:
            return None
            
        # Add to used features
        challenge["features_used"].append(feature)
        
        # Check if all features used
        used_features = set(challenge["features_used"])
        required_features = set(target_features)
        
        result = None
        if used_features >= required_features:  # Now comparing set to set, not set to int
            # Challenge completed
            challenge["completed"] = True
            challenge["success"] = True
            
            # Award XP
            xp_reward = challenge_data.get("xp_reward", XP_CHALLENGE_COMPLETED)
            self.add_xp(xp_reward, f"Challenge completed: {challenge['description']}")
            
            # Add to completed challenges
            self.user_data.setdefault("completed_challenges", []).append({
                "id": challenge["id"],
                "description": challenge["description"],
                "completion_date": datetime.now().strftime("%Y-%m-%d"),
                "xp_awarded": xp_reward
            })
            
            result = {
                "completed": True,
                "success": True,
                "description": challenge["description"],
                "data": challenge_data
            }
            
        self._save_user_data()
        return result
    
    def check_month_under_budget(self, is_under_budget):
        """Record whether the user stayed under budget for the month"""
        if is_under_budget:
            self.user_data["months_under_budget"] += 1
            self.add_xp(XP_UNDER_BUDGET, "Staying under budget this month")
            
            # Check for achievements
            achievement_results = self._check_achievements()
            self._save_user_data()
            
            return achievement_results
        return []
    
    def _assign_new_challenge(self):
        """Assign a new weekly challenge to the user"""
        # Add current challenge to completed list if it exists
        if "current_challenge" in self.user_data:
            current = self.user_data["current_challenge"]
            if "completed_challenges" not in self.user_data:
                self.user_data["completed_challenges"] = []
            self.user_data["completed_challenges"].append(current)
        
        # Select a random challenge
        # Try to avoid recently completed challenges
        recent_ids = []
        if "completed_challenges" in self.user_data:
            recent = self.user_data["completed_challenges"][-3:]  # Last 3 challenges
            recent_ids = [c["id"] for c in recent]
        
        available_challenges = [c for c in WEEKLY_CHALLENGES if c["id"] not in recent_ids]
        if not available_challenges:  # If all recent, just use all challenges
            available_challenges = WEEKLY_CHALLENGES
            
        selected_challenge = random.choice(available_challenges)
        
        # Calculate end date (next Sunday)
        now = datetime.now()
        days_until_sunday = (6 - now.weekday()) % 7  # 6 = Sunday
        if days_until_sunday == 0:  # If today is Sunday, go to next Sunday
            days_until_sunday = 7
        challenge_end = (now + timedelta(days=days_until_sunday)).strftime("%Y-%m-%d")
        
        # Create the new challenge
        self.user_data["current_challenge"] = {
            "id": selected_challenge["id"],
            "description": selected_challenge["description"],
            "end_date": challenge_end,
            "progress": 0,
            "completed": False,
            "data": selected_challenge
        }
        
        self._save_user_data()
        return self.user_data["current_challenge"]
    
    def buy_streak_freeze(self):
        """Attempt to purchase a streak freeze with XP"""
        if self.user_data["xp"] >= STREAK_FREEZE_COST:
            self.user_data["xp"] -= STREAK_FREEZE_COST
            self.user_data["streak_freezes"] += 1
            self._save_user_data()
            return True, self.user_data["streak_freezes"]
        return False, self.user_data["streak_freezes"]
    
    def get_user_stats(self):
        """Get user stats including level, XP, streak, and achievements"""
        # Calculate level and next level thresholds
        xp = self.user_data.get("xp", 0)
        level = self.get_level()
        
        # Determine XP to next level
        next_level_xp = 0
        for threshold in LEVEL_THRESHOLDS:
            if threshold > xp:
                next_level_xp = threshold
                break
        
        # If at max level
        if next_level_xp == 0 and level == len(LEVEL_THRESHOLDS):
            next_level_xp = LEVEL_THRESHOLDS[-1]
        
        # Calculate progress percentage to next level
        if level == 1:
            current_level_xp = 0
        else:
            current_level_xp = LEVEL_THRESHOLDS[level-2]
        
        level_progress = xp - current_level_xp
        level_total = next_level_xp - current_level_xp
        level_progress_percent = int((level_progress / max(1, level_total)) * 100)
        
        # Handle unique_categories safely
        unique_categories = self.user_data.get("unique_categories_used", set())
        if isinstance(unique_categories, set):
            unique_categories_count = len(unique_categories)
        else:
            unique_categories_count = 0
        
        # Count unlocked achievements
        achievement_count = 0
        for achievement_id, levels in self.user_data.get("achievements_unlocked", {}).items():
            achievement_count += len(levels)
        
        # Return comprehensive stats dictionary
        stats = {
            "xp": xp,
            "level": level,
            "xp_to_next_level": next_level_xp - xp,
            "level_progress_percent": level_progress_percent,
            "current_streak": self.user_data.get("current_streak", 0),
            "longest_streak": self.user_data.get("longest_streak", 0),
            "streak_freezes": self.user_data.get("streak_freezes", 0),
            "total_expenses_logged": self.user_data.get("total_expenses_logged", 0),
            "unique_categories": unique_categories_count,
            "reports_viewed": self.user_data.get("reports_viewed", 0),
            "months_under_budget": self.user_data.get("months_under_budget", 0),
            "achievements_unlocked": achievement_count
        }
        
        # Add current challenge if exists
        if "current_challenge" in self.user_data and self.user_data["current_challenge"]:
            stats["current_challenge"] = self.user_data["current_challenge"]
        
        return stats
    
    def get_achievements(self):
        """Get all achievements with unlock status"""
        achievements_list = []
        
        # Get user's unlocked achievements
        unlocked = self.user_data.get("achievements_unlocked", {})
        
        # Process each achievement
        for achievement_id, achievement_data in ACHIEVEMENTS.items():
            # Get unlocked levels for this achievement
            unlocked_levels = unlocked.get(achievement_id, [])
            
            # Process achievement levels
            levels = []
            for level_idx, level_data in enumerate(achievement_data['levels']):
                level_id = level_idx + 1
                levels.append({
                    "level": level_id,
                    "description": level_data['description'],
                    "emoji": level_data['emoji'],
                    "unlocked": level_id in unlocked_levels
                })
            
            # Get current metric value
            metric_name = achievement_data['metric']
            metric_value = self.user_data.get(metric_name, 0)
            
            # Handle special case for sets
            if metric_name == "unique_categories_used" and isinstance(metric_value, set):
                metric_value = len(metric_value)
            
            # Add achievement to list
            achievements_list.append({
                "id": achievement_id,
                "name": achievement_data['name'],
                "levels": levels,
                "current_value": metric_value
            })
        
        return achievements_list 