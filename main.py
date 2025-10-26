from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from google.oauth2 import service_account
from datetime import datetime, timedelta
import re
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler
import os
import json
from flask import Flask
import asyncio
from hypercorn.config import Config
from hypercorn.asyncio import serve
import logging
from fuzzywuzzy import process
import matplotlib.pyplot as plt
import matplotlib
import io
from tempfile import NamedTemporaryFile
from datetime import datetime, timedelta
import importlib.util
import copy
from typing import List, Optional, Dict, Tuple
import time
import calendar
import psutil
import sys
import telegram
import uuid
import requests

# Set up logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("expense_tracker")

# Import the gamification module
try:
    import gamification
    from gamification import GamificationSystem
    logger.info("Gamification module loaded successfully")
except ImportError:
    logger.warning("Gamification module not found, gamification features will be disabled")
    gamification = None

# Configure matplotlib to use a non-interactive backend
matplotlib.use('Agg')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # Output to console
    ])
logger = logging.getLogger("expense_tracker")

# Global variables
expense_history = []  # Store recent expenses in memory to avoid excessive API calls
MAX_HISTORY_SIZE = 50  # Maximum number of expenses to keep in history
sheet_cache = {}
CACHE_EXPIRY = 60  # Cache expiry in seconds
GAMIFICATION_ENABLED = True  # Whether gamification features are enabled

# Initialize Google Sheets API credentials
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID = os.getenv('SPREADSHEET_ID', '1yrl2-2OsIMLkMTT0-7tT1bfeEDjkuGKk2SwenY5zKVk')

# Global variable for the Google Sheets service
service = None

# Use service account credentials if available
try:
    creds_info = json.loads(os.getenv('GOOGLE_CREDENTIALS', '{}'))
    if not creds_info:
        logger.error("GOOGLE_CREDENTIALS environment variable is empty or invalid")
        raise ValueError("Missing Google credentials")
        
    creds = service_account.Credentials.from_service_account_info(creds_info, scopes=SCOPES)
    service = build('sheets', 'v4', credentials=creds)
    logger.info("Google Sheets API initialized with service account")
except Exception as e:
    logger.error(f"Error initializing Google Sheets API: {e}", exc_info=True)
    logger.critical("Google Sheets service is not initialized! The bot will have limited functionality.")
    
    # Try fallback method to initialize sheets (this will only work for read-only public sheets)
    try:
        from googleapiclient.discovery import build
        from google.oauth2.service_account import Credentials
        
        # Use public access - only works for read-only sheets that are published to the web
        service = build('sheets', 'v4')
        logger.info("Successfully initialized Google Sheets API with fallback method")
    except Exception as fallback_error:
        logger.error(f"Fallback Google Sheets initialization also failed: {fallback_error}", exc_info=True)
        service = None

# Verify service initialization
if service is None:
    logger.critical("Google Sheets service is not initialized! The bot will have limited functionality.")
    
    # Attempt to initialize with fallback method
    try:
        # Parse the credentials from environment variable
        credentials_json = os.getenv("GOOGLE_CREDENTIALS_JSON", "")
        if credentials_json:
            credentials_data = json.loads(credentials_json)
            if "private_key" in credentials_data:
                credentials_data["private_key"] = credentials_data["private_key"].replace("\\n", "\n").strip()
            
            creds = Credentials.from_service_account_info(credentials_data, scopes=SCOPES)
            service = build("sheets", "v4", credentials=creds)
            logger.info("Successfully initialized Google Sheets API with fallback method")
        else:
            logger.error("Both GOOGLE_CREDENTIALS and GOOGLE_CREDENTIALS_JSON variables are missing or invalid")
    except Exception as fallback_error:
        logger.error(f"Failed to initialize Google Sheets API with fallback method: {fallback_error}", exc_info=True)

# Category mapping
category_map = {
    # Transportation
    "Public Transportation": [
        "train", "taxi", "bus", "metro", "tram", "cab", "subway", "ride",
        "public", "transport"
    ],
    "Fuel Toyota": ["Fuel Toyota", "fuel toyota", "diesel"],
    "Fuel MG": ["Fuel MG", "petrol", "fuel mg"],
    "Parking": ["parking", "garage", "space", "charging", "park"],
    "Other (Trans)": [
        "transportation other", "other transportation", "Other (Trans)",
        "Other Trans", "Other Transportation"
    ],

    # Home Expenses
    "Rent": ["rent", "lease", "apartment fee", "monthly payment", "mortgage"],
    "Electricity": ["electricity", "electric", "power", "energy", "bill", "utility"],
    "Gas": ["gas", "oil", "heating",],
    "Water":
    ["water", "sewer", "trash", "garbage", "utilities", "waste"],
    "Property Tax": ["Property Tax", "Tax", "Arnona"],
    "Internet": ["internet", "wifi", "broadband", "net", "phone"],
    "House Committee":
    ["committee", "house committee", "hoa", "maintenance fee"],
    "Maintenance":
    ["maintenance", "improvement", "repair", "fix", "home", "flowers", "flower", "upgrade", "repairs"],

    # Daily Living
    "Groceries":
    ["groceries", "supermarket", "market", "super", "store", "shopping", "food", "dani" , "danny"],
    "Coffee": [
        "coffee", "latte", "espresso", "cappuccino", "brew", "americano",
        "mocha", "cafe"
    ],
    "Dining Out": [
        "dining", "restaurant", "meal", "food", "breakfast", "lunch", "dinner", "eat",
        "takeout", "delivery"
    ],
    "Beer / Wine": [
        "beer", "wine", "alcohol", "bar", "cocktail", "drink", "vodka",
        "whiskey", "liquor"
    ],
    "Cloths": [ "cloths", "shirt", "pants", "dress", "clothes", "cloth", "tshirt", "t-shirt", "t shirt"
    ],
    "Pharm": ["Pharm", "pharm", "superpharm" "pharm", "super pharm", "super-pharm", "pharmacy"
    ],
    "Other (Daily)": [
        "daily living other", "other daily living", "haircut",
        "miscellaneous living", "Other (Daily)", "Other Daily", "other", "cosmetics", "laser",
        "personal", "gym", "personal care", "present", "presents"
    ],

    # Entertainment and Recreation
    "Entertainment": [
        "entertainment", "movie", "theater", "show", "concert", "game",
        "festival", "fun", "games"
    ],
    "Vacation": [
        "vacation", "holiday", "trip", "travel", "hotel", "flight", "beach",
        "resort"
    ],

    # Education and Healthcare
    "Education": ["books", "courses", "education", "school"],
    "Health": [
        "health", "doctor", "medicine", "hospital", "clinic", "checkup",
        "insurance",
        "medical", "healthcare"
    ],

    # Savings and Insurance
    "Life Insurance": ["life insurance", "policy", "premium", "coverage", "car insurance", "health insurance"],
    "Emergency Fund": ["emergency fund", "savings", "rainy day"],
    
    # Personal Categories
    "Omer": ["omer"],
    "Gil": ["gil"],
}

# Broad Category Definitions mapping main categories to their subcategories
# IMPORTANT: Ensure these subcategory names exactly match Column A in your sheet
BROAD_CATEGORIES = {
    "Home": [
        "Rent", "Mortgage", # Add/Remove Mortgage if needed based on your sheet
        "Electricity", "Gas", "Water",
        "Property Tax", "Internet", "House Committee", "Maintenance/Improvements"
    ],
    "Transportation": [
        "Public Transportation", "Fuel", "Parking", "Other (Trans)"
    ],
    "Daily Living": [
        "Groceries", "Coffee", "Dining Out", "Beer / Wine", "Other (Daily)"
    ],
    "Other": [ # Based on your description for the summary - VERIFY THESE ARE CORRECT
        "Entertainment", "Vacation", "Health", "Life Insurance"
        # Add others like "Education", "Emergency Fund" if they sum into your 'Other' total
    ]
}

# Rows where the total BALANCE for each broad category is stored (Column D)
# IMPORTANT: Double-check these row numbers against your actual Google Sheet template
BROAD_CATEGORY_TOTAL_ROWS = {
    "Home": 25,           # Assuming Balance for Home is in D35
    "Transportation": 42, # Assuming Balance for Transportation is in D42
    "Daily Living": 65,   # Assuming Balance for Daily Living is in D50
    "Other": 57           # Assuming Balance for Other is in D57
}

# Function to log expenses and update notes
async def log_expense_to_google_sheets_with_notes(subcategory, amount, original_text, user_id=None):
    """
    Log an expense to Google Sheets, update the amount, and append notes safely.
    Handles cases where notes don't exist and provides better error logging.
    """
    try:
        # Normalize category and get current sheet name
        normalized_subcategory = subcategory.lower().strip()
        now = datetime.now()
        sheet_name = now.strftime("%m%y")
        logger.info(f"Logging: '{original_text}' (₪{amount}) to '{subcategory}' in sheet '{sheet_name}' for user '{user_id}'")

        # Get spreadsheet service and check sheet existence
        sheet = service.spreadsheets()
        try:
            sheet_metadata = sheet.get(spreadsheetId=SPREADSHEET_ID).execute()
            available_sheets = [s.get("properties", {}).get("title") for s in sheet_metadata.get("sheets", [])]
            if sheet_name not in available_sheets:
                logger.error(f"Sheet '{sheet_name}' not found.")
                return f"Error: Sheet for {now.strftime('%B %Y')} ({sheet_name}) does not exist. Please create it first."
        except Exception as meta_err:
            logger.error(f"Error fetching spreadsheet metadata: {meta_err}", exc_info=True)
            return "Error connecting to the spreadsheet. Could not verify sheet existence."

        # Find the row number for the subcategory
        range_to_search = f"{sheet_name}!A1:A100" # Adjust range if you have more categories
        try:
            result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=range_to_search).execute()
            values = result.get('values', [])
            row_number = None
            for i, row in enumerate(values):
                # Check if row is not empty, has a first element, and matches the category
                if row and row[0] and row[0].strip().lower() == normalized_subcategory:
                    row_number = i + 1  # Sheets API is 1-indexed
                    break
        except Exception as find_row_err:
             logger.error(f"Error searching for category '{subcategory}' in {range_to_search}: {find_row_err}", exc_info=True)
             return f"Error searching for category '{subcategory}' in the sheet."


        if not row_number:
            logger.error(f"Could not find category '{subcategory}' (normalized: '{normalized_subcategory}') in range {range_to_search}.")
            # Suggest similar categories or list them
            similar_cat, _, _ = find_similar_category(subcategory)
            suggestion = f" Maybe you meant '{similar_cat}'?" if similar_cat else ""
            all_cats = ", ".join(list(category_map.keys())[:5]) + "..."
            return f"Error: Category '{subcategory}' not found in sheet.{suggestion}\nAvailable start with: {all_cats}"

        logger.info(f"Found category '{subcategory}' at row {row_number}")

        # --- Update Amount (Column C) ---
        amount_cell = f"{sheet_name}!C{row_number}"
        current_amount = 0.0
        try:
            result_amount = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=amount_cell).execute()
            cell_values = result_amount.get('values', [['0']])
            # Safely access the value, default to '0'
            current_amount_str = cell_values[0][0] if (cell_values and cell_values[0]) else '0'
            # Clean and convert to float
            current_amount = float(current_amount_str.replace('₪', '').replace(',', '').strip() or 0)
        except (ValueError, IndexError, TypeError) as e:
            logger.warning(f"Could not parse current amount from {amount_cell}. Value potentially problematic. Error: {e}. Assuming 0.", exc_info=True)
            current_amount = 0.0 # Default to 0 if parsing fails

        new_amount = current_amount + amount
        logger.info(f"Updating amount in {amount_cell} from {current_amount:.2f} to {new_amount:.2f}")
        try:
            sheet.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=amount_cell,
                valueInputOption="USER_ENTERED", # Preserves formulas if any, otherwise acts like RAW for numbers
                body={"values": [[new_amount]]}
            ).execute()
            logger.info("Amount updated successfully.")
        except Exception as update_err:
             logger.error(f"FAILED to update amount in {amount_cell}: {update_err}", exc_info=True)
             return f"Error updating amount for '{subcategory}': {update_err}. Please check spreadsheet permissions or connection." # Stop if amount fails


        # --- Add Note (Also to Column C cell) ---
        timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
        new_note_line = f"{timestamp}: {original_text} (₪{amount:.2f})"
        existing_note = ""

        # --- Safer fetching of existing note ---
        note_cell_range = f"{sheet_name}!C{row_number}" # Note is on the same cell as the amount (Column C)
        try:
            logger.debug(f"Fetching existing note from {note_cell_range}")
            # Request only the 'note' field for efficiency
            note_result = sheet.get(
                spreadsheetId=SPREADSHEET_ID,
                ranges=[note_cell_range], # ranges should be a list
                fields="sheets(data(rowData(values(note))))" # Ensure correct field mask
            ).execute()
            # Log structure for debugging if needed: logger.debug(f"Note result structure: {json.dumps(note_result, indent=2)}")

            # Safely navigate the dictionary structure using .get()
            sheets_data = note_result.get("sheets")
            if sheets_data and isinstance(sheets_data, list) and len(sheets_data) > 0:
                data_list = sheets_data[0].get("data")
                if data_list and isinstance(data_list, list) and len(data_list) > 0:
                    row_data_list = data_list[0].get("rowData")
                    # rowData might be missing/empty if row has formatting but no data/note
                    if row_data_list and isinstance(row_data_list, list) and len(row_data_list) > 0:
                        values_list = row_data_list[0].get("values")
                        if values_list and isinstance(values_list, list) and len(values_list) > 0:
                            note_value = values_list[0].get("note")
                            if note_value: # Check if note_value is not None and not empty
                                existing_note = note_value + "\n" # Add newline separator ONLY if note exists
                                logger.debug("Existing note found and retrieved.")
                            else:
                                 logger.debug("Cell exists but 'note' field is missing, empty, or null.")
                        else:
                             logger.debug(f"Row {row_number} exists but 'values' list is missing or empty.")
                    else:
                         logger.debug(f"'rowData' is missing or empty for row {row_number} (possibly formatted but empty row).")
                else:
                     logger.debug("sheets[0] does not contain 'data' or it's empty.")
            else:
                 logger.debug("API response does not contain 'sheets' or it's empty.")

        except Exception as get_note_err:
            # Log error but continue, assuming note is empty
            logger.warning(f"Error fetching existing note from {note_cell_range}: {get_note_err}. Proceeding as if note is empty.", exc_info=True)
            existing_note = "" # Ensure it's reset on error

        # Append new entry
        full_new_note = existing_note + new_note_line
        logger.debug(f"Constructed full new note (length {len(full_new_note)}).")

        # --- Update the note using batchUpdate ---
        try:
            # Get sheet ID - needed for batchUpdate range
            target_sheet_id = get_sheet_id(sheet_metadata, sheet_name)
            if target_sheet_id is None:
                 # This is a critical error for batchUpdate
                 logger.error(f"Could not find sheetId for sheet name '{sheet_name}'. Cannot update note.")
                 return f"Amount updated, but could not find internal ID for sheet '{sheet_name}' to update the note."

            update_note_request = {
                "updateCells": {
                    "rows": [ # Correct key: 'rows'
                        {
                            "values": [ # Corresponds to the cell(s) in the range
                                {
                                    "note": full_new_note # Set the note content
                                }
                                # If updating multiple cells' notes in the range, add more dicts here
                            ]
                        }
                    ],
                    "range": { # Define the specific cell for the update
                        "sheetId": target_sheet_id,
                        "startRowIndex": row_number - 1, # 0-indexed row
                        "endRowIndex": row_number,       # End row is exclusive
                        "startColumnIndex": 2,           # Column C = index 2
                        "endColumnIndex": 3              # End column is exclusive (just Col C)
                    },
                    "fields": "note" # IMPORTANT: Crucial to only update the 'note' field
                }
            }
            request_body = {"requests": [update_note_request]}
            # logger.debug(f"Executing batchUpdate request body: {json.dumps(request_body, indent=2)}") # Uncomment for deep debug

            sheet.batchUpdate(
                spreadsheetId=SPREADSHEET_ID,
                body=request_body
            ).execute()
            logger.info(f"Note updated successfully for {note_cell_range}.")

        except Exception as update_note_err:
            # Specific logging for note update failure
            logger.error(f"FAILED to update note for {note_cell_range}: {update_note_err}", exc_info=True)
            # Return specific error about note failure, but mention amount success
            return f"Amount updated successfully, but failed to add the note for '{subcategory}': {update_note_err}"


        # --- Post-logging actions (History, Notifications, Gamification) ---

        # Add to expense history
        expense_entry = {
            "timestamp": timestamp,
            "category": subcategory,
            "amount": amount,
            "text": original_text,
            "row": row_number,
            "sheet_name": sheet_name,
            "sheet_id": get_sheet_id(sheet_metadata, sheet_name) # Get sheet_id again or pass from above
        }
        if len(expense_history) >= MAX_HISTORY_SIZE:
            expense_history.pop(0)
        expense_history.append(expense_entry)
        logger.debug(f"Added expense to history. History size: {len(expense_history)}")

        # Check budget thresholds
        threshold_notification = None
        if user_id: # Only check thresholds if we have a user context
             threshold_notification = check_budget_thresholds(subcategory, sheet_name, user_id)
        else:
             logger.warning("No user_id provided, skipping budget threshold check.")


        # Prepare success message
        emoji = get_category_emoji(subcategory)
        success_message = f"{emoji} Added ₪{amount:.2f} to *{subcategory}*. New total: *₪{new_amount:,.2f}*" # Added formatting

        # Handle Gamification if available and user_id present
        gamification_message = ""
        if gamification and user_id:
            try:
                gs = GamificationSystem(user_id)
                gam_result = gs.log_expense(subcategory, amount) # Log the expense

                # Process achievements
                if gam_result.get("achievements"):
                    achievement_texts = []
                    for a in gam_result["achievements"]:
                        leveled_up_msg = f"🆙 *LEVEL UP!* You're now Level {a['new_level']}!" if a.get("leveled_up") else ""
                        achievement_texts.append(f"{a.get('emoji','🏆')} Achievement: {a.get('description','Unknown')} (+{a.get('xp_awarded',0)} XP) {leveled_up_msg}")
                    if achievement_texts:
                         gamification_message += "\n\n---\nAchievements:\n" + "\n".join(achievement_texts)

                # Process challenge progress
                if gam_result.get("challenge_progress"):
                     challenge = gam_result["challenge_progress"]
                     if challenge.get("completed"):
                          if challenge.get("success"):
                              chall_emoji = challenge.get("data", {}).get("emoji", "🏅")
                              chall_xp = challenge.get("data", {}).get("xp_reward", 0)
                              gamification_message += f"\n\n{chall_emoji} Challenge Complete: {challenge.get('description','Unknown')} (+{chall_xp} XP)"
                          else:
                              gamification_message += f"\n\n❌ Challenge Failed: {challenge.get('description','Unknown')}"

                # Add streak info
                stats = gs.get_user_stats() # Get fresh stats
                if stats.get("current_streak", 0) > 1:
                    gamification_message += f"\n\n🔥 Day {stats['current_streak']} of your tracking streak!"
                elif stats.get("current_streak", 0) == 1 and amount > 0: # Only show for first log of the day
                     gamification_message += f"\n\n🔥 New tracking streak started!"

            except Exception as e:
                logger.error(f"Error in gamification processing: {str(e)}", exc_info=True)
                gamification_message += "\n\n(Could not update gamification stats)"


        # Combine messages for final output
        final_message = success_message
        if threshold_notification:
            final_message += f"\n\n---\n{threshold_notification}" # Add separator
        if gamification_message:
            # Add separator only if notifications weren't already added or if message isn't just streak
            if not threshold_notification and len(gamification_message) > 60: # Avoid separator for just streak message
                 final_message += "\n\n---"
            final_message += gamification_message

        # Use Markdown for the final reply
        # Note: Ensure the bot instance/context used to send the reply supports parse_mode='Markdown'
        # Example (within a handler): await update.message.reply_text(final_message, parse_mode='Markdown')
        # Since this function *returns* the message, the calling handler needs to set parse_mode.
        return final_message

    except Exception as e:
        # Catch-all for unexpected errors during the entire process
        logger.error(f"CRITICAL error in log_expense_to_google_sheets_with_notes for '{subcategory}': {str(e)}", exc_info=True)
        # Return the specific error message encountered
        return f"Error logging expense: {str(e)}"
def get_sheet_id(sheet_metadata, sheet_name):
    """Get the sheet ID from sheet metadata by sheet name"""
    for sheet in sheet_metadata.get('sheets', []):
        if sheet.get('properties', {}).get('title') == sheet_name:
            return sheet.get('properties', {}).get('sheetId')
    return None

def get_current_user_id_from_context():
    """
    Get the current user's ID for the context.
    In a real implementation, this would use a global context or a thread local.
    """
    # For simplicity, this is now a dummy function that returns a default user ID
    # In the real handler function, we store the user ID in context.user_data
    return "default_user"

def get_category_emoji(category):
    """Get an appropriate emoji for a category"""
    category = category.lower()
    
    emoji_map = {
        "groceries": "🛒",
        "restaurant": "🍽️",
        "coffee": "☕",
        "fast food": "🍔",
        "lunch": "🥪",
        "breakfast": "🥐",
        "dinner": "🍲",
        "transportation": "🚗",
        "fuel": "⛽",
        "taxi": "🚕",
        "public transportation": "🚇",
        "rent": "🏠",
        "utilities": "💡",
        "internet": "📶",
        "phone": "📱",
        "household": "🏡",
        "entertainment": "🎬",
        "streaming": "📺",
        "shopping": "🛍️",
        "clothing": "👕",
        "electronics": "💻",
        "health": "💊",
        "fitness": "🏋️",
        "education": "📚",
        "travel": "✈️",
        "gifts": "🎁",
        "charity": "❤️",
        "subscriptions": "📱",
        "insurance": "🔒",
        "taxes": "📝",
        "savings": "💰",
        "investment": "📈",
        "loan payment": "💳",
        "pet": "🐾",
        "home maintenance": "🔧",
        "personal care": "💇",
        "children": "👶",
    }
    
    # Try to find a direct match
    for key, emoji in emoji_map.items():
        if key in category.lower():
            return emoji
    
    # Default emoji if no match
    return "💸"

async def reset_notifications(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin command to reset budget notification thresholds"""
    try:
        user_id = update.effective_user.id
        if not context.args:
            # Reset all notifications
            now = datetime.now()
            sheet_name = now.strftime("%m%y")
            user_key = f"notified_thresholds_{sheet_name}"
            
            save_user_data(user_id, user_key, {})
            await update.message.reply_text("All budget notifications have been reset.")
        else:
            # Reset notifications for specific category
            category = " ".join(context.args).strip()
            now = datetime.now()
            sheet_name = now.strftime("%m%y")
            user_key = f"notified_thresholds_{sheet_name}"
            
            notification_history = get_user_data(user_id, user_key, {})
            if category in notification_history:
                notification_history[category] = []
                save_user_data(user_id, user_key, notification_history)
                await update.message.reply_text(f"Budget notifications for {category} have been reset.")
            else:
                await update.message.reply_text(f"No notifications found for category {category}.")
    except Exception as e:
        logger.error(f"Error resetting notifications: {str(e)}", exc_info=True)
        await update.message.reply_text(f"Error resetting notifications: {str(e)}")

# Function to start the bot
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        'Hello! I am your Expense Tracker Bot. Send me your expenses in the format "Item Amount" (e.g., Coffee 13).'
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show help for the bot"""
    try:
        logger.info("Help command triggered")

        help_text = """
🤖 *Expense Bot Commands* 🤖

*Expense Tracking:*
• Simply type expenses naturally: `spent 25 on lunch`, `groceries 120.50`
• The bot understands amounts and matches keywords to categories

*Key Commands:*
• */monthly* - Budget summary for current/specified month
• */overview* `[category]` - Overall summary or category breakdown
• */category* `<subcategory>` - Details for specific subcategory
• */delete* - Delete your last expense entry
• */budget* - Shows current month's Income vs Expenses
• */chart* `[month]` - Generates expense pie chart

*Gamification:*
• */stats* - View Level, XP, Streaks, and game stats
• */achievements* - Display achievement badges
• */challenge* `[new]` - View/get weekly challenge
• */buyfreeze* - Purchase streak freeze using XP

*Utilities:*
• */help* - This help message
• */categories* - List all expense categories
• */balance* `<subcategory>` - Check remaining balance
"""

        # Create buttons like in overview
        keyboard = [
            [
                InlineKeyboardButton("📊 Overview", callback_data="cmd:overview"),
                InlineKeyboardButton("🗑️ Delete", callback_data="cmd:delete")
            ],
            [
                InlineKeyboardButton("📋 Categories", callback_data="cmd:categories"),
                InlineKeyboardButton("📅 Monthly", callback_data="cmd:monthly")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        logger.info("Sending help message with buttons")
        await update.message.reply_text(help_text, parse_mode='Markdown', reply_markup=reply_markup)
        logger.info("Help message sent successfully")

    except Exception as e:
        logger.error(f"Error in help_command: {str(e)}", exc_info=True)
        await update.message.reply_text("Error displaying help. Please try again later.")

async def categories_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    categories_text = "📋 Available Expense Categories:\n\n"
    for category, keywords in category_map.items():
        categories_text += f"• {category}:\n  Keywords: {', '.join(keywords[:3])}{'...' if len(keywords) > 3 else ''}\n\n"
    await update.message.reply_text(categories_text)


async def delete_last_entry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # TODO: Implement delete functionality
    await update.message.reply_text("⚠️ Delete functionality coming soon!")


# Function to find similar categories or keywords using fuzzy matching
def find_similar_category(input_text, threshold=60):
    """Find similar categories or keywords using fuzzy matching"""
    try:
        logger.info(f"Finding similar category for: '{input_text}'")
        
        # First, check if it matches any category directly
        categories = list(category_map.keys())
        logger.info(f"Available categories: {categories}")
        
        category_match, category_score = process.extractOne(input_text.lower(), categories)
        logger.info(f"Category match: '{category_match}' with score {category_score}")
        
        # Then, check if it matches any keyword
        all_keywords = []
        keyword_to_category = {}
        for category, keywords in category_map.items():
            for keyword in keywords:
                all_keywords.append(keyword.lower())
                keyword_to_category[keyword.lower()] = category
        
        logger.info(f"Checking against {len(all_keywords)} keywords")
        keyword_match, keyword_score = process.extractOne(input_text.lower(), all_keywords)
        logger.info(f"Keyword match: '{keyword_match}' with score {keyword_score}")
        
        # Return the best match
        if category_score >= threshold and category_score >= keyword_score:
            logger.info(f"Selected category match: '{category_match}'")
            return category_match, "category", category_score
        elif keyword_score >= threshold:
            matched_category = keyword_to_category[keyword_match]
            logger.info(f"Selected keyword match: '{keyword_match}' → '{matched_category}'")
            return matched_category, "keyword", keyword_score
        
        logger.info(f"No good match found for '{input_text}'")
        return None, None, 0
    
    except Exception as e:
        logger.error(f"Error in find_similar_category: {str(e)}", exc_info=True)
        return None, None, 0
        
def get_safe_float(values, row_index_1_based, col_index_0_based):
    """Safely extracts and converts a value from the sheet data."""
    row_index_0_based = row_index_1_based - 1
    try:
        # Check if row and column exist
        if len(values) > row_index_0_based and len(values[row_index_0_based]) > col_index_0_based:
            cell_value = values[row_index_0_based][col_index_0_based]
            if cell_value: # Check if the cell is not empty
                # Convert to string first for reliable replacement
                cleaned_value = str(cell_value).replace('₪', '').replace(',', '').strip()
                # Return float if cleaned_value is not empty, otherwise 0.0
                return float(cleaned_value) if cleaned_value else 0.0
        # Return 0.0 if row/col index out of bounds or cell is empty
        return 0.0
    except (ValueError, TypeError) as e:
        logger.warning(f"Could not parse float at row {row_index_1_based}, col {col_index_0_based+1}. Value: '{values[row_index_0_based][col_index_0_based]}'. Error: {e}")
        return 0.0 # Return 0.0 on conversion error
        
# Add a new function to handle category suggestions
async def handle_category_suggestion(update: Update, context: ContextTypes.DEFAULT_TYPE, query: str):
    """Handle category suggestions when a user enters an unknown category"""
    try:
        logger.info(f"Handling category suggestion for: '{query}'")
        
        # Find a similar category
        similar_category, match_type, score = find_similar_category(query)
        logger.info(f"Got result: category='{similar_category}', type='{match_type}', score={score}")
        
        if similar_category:
            # Create a confirmation keyboard
            keyboard = [
                [
                    InlineKeyboardButton("Yes, use this category", callback_data=f"use_category:{similar_category}"),
                    InlineKeyboardButton("No, cancel", callback_data="cancel_expense")
                ]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            # Store the current expense details in user_data for later use
            if not hasattr(context, 'user_data'):
                context.user_data = {}
            
            context.user_data['pending_expense'] = {
                'amount': context.user_data.get('amount', 0),
                'note': context.user_data.get('note', ''),
                'original_text': context.user_data.get('original_text', '')
            }
            
            logger.info(f"Stored pending expense: {context.user_data.get('pending_expense')}")
            
            # Send suggestion
            if match_type == "category":
                message = f"I couldn't find '{query}' but found a similar category: '{similar_category}'. Would you like to use this category?"
            else:
                message = f"I couldn't find '{query}' but '{similar_category}' might be appropriate based on similar keywords. Would you like to use this category?"
            
            logger.info(f"Sending suggestion message: '{message}'")
            await update.message.reply_text(message, reply_markup=reply_markup)
            return True
        else:
            # No similar category found
            categories_text = "I couldn't find a matching category. Available categories:\n\n"
            for category in sorted(category_map.keys()):
                categories_text += f"• {category}\n"
            categories_text += "\nYou can also use /categories to see all available categories."
            
            logger.info(f"No similar category found, sending categories list")
            await update.message.reply_text(categories_text)
            return False
    
    except Exception as e:
        logger.error(f"Error in handle_category_suggestion: {str(e)}", exc_info=True)
        await update.message.reply_text("Sorry, I encountered an error trying to suggest a category.")
        return False

# Add a callback query handler for category suggestions
async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle button callbacks from inline keyboards for category suggestions and overviews."""
    query = update.callback_query
    # Always acknowledge the button press immediately to stop the loading icon
    await query.answer()

    data = query.data
    user_id = query.from_user.id # Get user ID from the query object
    logger.info(f"Received callback query from user {user_id} with data: '{data}'")

    try:
        if data.startswith("use_category:"):
            # --- Handle Category Suggestion Confirmation ---
            category = data.split(":", 1)[1]
            logger.info(f"Processing category suggestion confirmation for: '{category}'")

            # Retrieve pending expense details safely from context
            pending_expense_data = context.user_data.get('pending_expense')
            if pending_expense_data:
                amount = pending_expense_data.get('amount', 0)
                # Note might not always be present, default to empty string
                note = pending_expense_data.get('note', '')
                original_text = pending_expense_data.get('original_text', 'Unknown expense text') # Fallback text
                # Ensure user_id from context matches query user if needed, or just use query user ID
                expense_user_id = pending_expense_data.get('user_id', user_id)

                logger.info(f"Retrieved pending expense: amount={amount}, note='{note}', text='{original_text}' for user {expense_user_id}")

                # Log the expense
                result = await log_expense_to_google_sheets_with_notes(category, amount, original_text, expense_user_id)
                logger.info(f"Expense logged result: '{result}'")
                # Edit the original message where the button was
                await query.edit_message_text(f"✅ Expense logged under '{category}'.\n\nDetails:\n{result}")

                # Clear the pending expense data from context
                context.user_data.pop('pending_expense', None)
                # Also clear related temporary keys if they exist
                context.user_data.pop('amount', None)
                context.user_data.pop('note', None)
                context.user_data.pop('original_text', None)
            else:
                logger.warning("No 'pending_expense' data found in context.user_data for use_category callback.")
                await query.edit_message_text("😕 Sorry, the details for this expense seem to have expired. Please try adding the expense again.")

        elif data == "cancel_expense":
            # --- Handle Category Suggestion Cancellation ---
            logger.info("User cancelled the expense suggestion.")
            # Edit the original message where the button was
            await query.edit_message_text("❌ Expense entry cancelled.")
            # Clear pending expense data from context if it exists
            context.user_data.pop('pending_expense', None)
            context.user_data.pop('amount', None)
            context.user_data.pop('note', None)
            context.user_data.pop('original_text', None)

        elif data.startswith("overview:"):
            # --- Handle Overview Button Click ---
            category_name = data.split(":", 1)[1]
            logger.info(f"Processing overview button press for: '{category_name}'")

            # Get the summary text using the helper function
            summary_text = await get_overview_summary_text(category_name)

            # Create back button
            keyboard = [[InlineKeyboardButton("« Back to Overview", callback_data="cmd:overview")]]
            reply_markup = InlineKeyboardMarkup(keyboard)

            # Send the overview summary as a NEW message in the chat
            if len(summary_text) > 4096:
                 # Truncate if too long
                 await query.message.reply_text(summary_text[:4050] + "\n\n... (Summary too long, truncated)", 
                                               parse_mode='Markdown', 
                                               reply_markup=reply_markup)
            else:
                 await query.message.reply_text(summary_text, 
                                               parse_mode='Markdown',
                                               reply_markup=reply_markup)

        elif data.startswith("cmd:"):
            # --- Handle Command Button Click ---
            command = data.split(":", 1)[1]
            logger.info(f"Processing command button press for: '{command}'")
            
            if command == "overview":
                try:
                    # Create a proper update object with the callback_query
                    update_obj = Update(update_id=query.id, callback_query=query)
                    
                    # Create a context copy with empty args for overview command
                    new_context = copy.copy(context)
                    new_context.args = []
                    
                    # Call the overview command
                    await overview_command(update_obj, new_context)
                except Exception as overview_error:
                    logger.error(f"Error handling overview command from button: {overview_error}", exc_info=True)
                    await query.message.reply_text("❌ Error processing overview request. Please try using the /overview command directly.")
                
            elif command == "delete":
                # Call delete command with the query object
                await query.message.reply_text("Processing delete request...")
                try:
                    # Create a proper update object with the callback_query
                    update_obj = Update(update_id=query.id, callback_query=query)
                    await delete_command(update_obj, context)
                except Exception as delete_error:
                    logger.error(f"Error handling delete command from button: {delete_error}", exc_info=True)
                    await query.message.reply_text("❌ Error processing delete request. Please try using the /delete command directly.")
                
            elif command == "categories":
                # Call categories command
                categories_text = "📋 Available Expense Categories:\n\n"
                for category, keywords in category_map.items():
                    categories_text += f"• {category}:\n  Keywords: {', '.join(keywords[:3])}{'...' if len(keywords) > 3 else ''}\n\n"
                
                # Add back button
                keyboard = [[InlineKeyboardButton("« Back to Help", callback_data="cmd:help")]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                
                await query.message.reply_text(categories_text, reply_markup=reply_markup)
                
            elif command == "monthly":
                try:
                    # Create a proper update object with the callback_query
                    update_obj = Update(update_id=query.id, callback_query=query)
                    await send_monthly_summary_with_buttons(update_obj, context, None)
                except Exception as monthly_error:
                    logger.error(f"Error handling monthly command from button: {monthly_error}", exc_info=True)
                    await query.message.reply_text("❌ Error processing monthly request. Please try using the /monthly command directly.")
            elif command == "help":
                # Show help command
                help_text = """
🤖 *Expense Bot Commands* 🤖

*Expense Tracking:*
• Simply type expenses naturally: `spent 25 on lunch`, `groceries 120.50`
• The bot understands amounts and matches keywords to categories

*Key Commands:*
• */monthly* - Budget summary for current/specified month
• */overview* `[category]` - Overall summary or category breakdown
• */category* `<subcategory>` - Details for specific subcategory
• */delete* - Delete your last expense entry
• */budget* - Shows current month's Income vs Expenses
• */chart* `[month]` - Generates expense pie chart

*Gamification:*
• */stats* - View Level, XP, Streaks, and game stats
• */achievements* - Display achievement badges
• */challenge* `[new]` - View/get weekly challenge
• */buyfreeze* - Purchase streak freeze using XP

*Utilities:*
• */help* - This help message
• */categories* - List all expense categories
• */balance* `<subcategory>` - Check remaining balance
"""

                # Create buttons
                keyboard = [
                    [
                        InlineKeyboardButton("📊 Overview", callback_data="cmd:overview"),
                        InlineKeyboardButton("🗑️ Delete", callback_data="cmd:delete")
                    ],
                    [
                        InlineKeyboardButton("📋 Categories", callback_data="cmd:categories"),
                        InlineKeyboardButton("📅 Monthly", callback_data="cmd:monthly")
                    ]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)

                await query.message.reply_text(help_text, parse_mode='Markdown', reply_markup=reply_markup)
                
            elif command == "reset_notifications":
                # Call reset_notifications command
                await reset_notifications(query, context)
            else:
                logger.warning(f"Unknown command button: {command}")
                await query.message.reply_text(f"Sorry, the command '{command}' is not recognized.")

        elif data.startswith("month:"):
            # --- Handle Month Navigation ---
            month = data.split(":", 1)[1]
            logger.info(f"Processing month navigation for: '{month}'")
            
            # Use the monthly summary with buttons function to show the selected month
            update_obj = Update(update_id=query.id, callback_query=query)
            await send_monthly_summary_with_buttons(update_obj, context, month)

        elif data.startswith("broad_category:"):
            # --- Handle Broad Category Button ---
            broad_category = data.split(":", 1)[1]
            logger.info(f"Processing broad category button: '{broad_category}'")
            
            try:
                # Get the subcategories for this broad category
                if broad_category in BROAD_CATEGORIES:
                    subcategories = BROAD_CATEGORIES[broad_category]
                    
                    # Create a message showing all subcategories in this broad category
                    category_emojis = {
                        "Home": "🏠",
                        "Transportation": "🚗",
                        "Daily Living": "🍽️",
                        "Other": "📦"
                    }
                    emoji = category_emojis.get(broad_category, "📦")
                    
                    message_text = f"{emoji} *{broad_category} Categories:*\n\n"
                    
                    # Get current sheet information to fetch balances
                    now = datetime.now()
                    sheet_name = get_sheet_name_for_current_month()
                    
                    try:
                        # Fetch current sheet data
                        sheet = service.spreadsheets()
                        result = sheet.values().get(
                            spreadsheetId=SPREADSHEET_ID,
                            range=f"{sheet_name}!A:D"
                        ).execute()
                        
                        values = result.get("values", [])
                        
                        if values:
                            # Process each subcategory
                            for subcategory in subcategories:
                                subcategory_row = None
                                
                                # Find the row for this subcategory
                                for i, row in enumerate(values):
                                    if row and len(row) > 0 and row[0].strip() == subcategory:
                                        subcategory_row = row
                                        break
                                
                                if subcategory_row and len(subcategory_row) >= 4:
                                    # Extract data
                                    try:
                                        budget = float(subcategory_row[1].replace('₪', '').replace(',', '').strip() or 0)
                                        actual = float(subcategory_row[2].replace('₪', '').replace(',', '').strip() or 0)
                                        balance = float(subcategory_row[3].replace('₪', '').replace(',', '').strip() or 0)
                                        
                                        # Format with status emoji
                                        status = "✅" if balance >= 0 else "⚠️"
                                        message_text += f"{status} *{subcategory}*: ₪{balance:,.2f} (Spent: ₪{actual:,.2f})\n"
                                    except (ValueError, IndexError):
                                        message_text += f"• *{subcategory}*: Data unavailable\n"
                                else:
                                    message_text += f"• *{subcategory}*: Not found in current sheet\n"
                        else:
                            message_text += "No data available for the current month."
                    except Exception as sheet_error:
                        logger.error(f"Error fetching sheet data: {sheet_error}")
                        message_text += "Error fetching detailed category data.\n"
                    
                    # Add back button
                    keyboard = [[InlineKeyboardButton("« Back to Overview", callback_data="cmd:overview")]]
                    reply_markup = InlineKeyboardMarkup(keyboard)
                    
                    # Send the subcategories message
                    await query.message.reply_text(message_text, parse_mode='Markdown', reply_markup=reply_markup)
                    
                    # Log for gamification
                    user_id = query.from_user.id
                    if gamification and user_id:
                        try:
                            gs = GamificationSystem(user_id)
                            reason = f"Viewing {broad_category} categories"
                            gs.add_xp(gamification.XP_REPORT_VIEWED / 2, reason)  # Half XP for subcategory view
                            gs.log_report_view(f"broad_category:{broad_category}")
                        except Exception as g_err:
                            logger.error(f"Error in gamification for broad category view: {g_err}")
                else:
                    await query.message.reply_text(f"Category '{broad_category}' not found.")
            except Exception as e:
                logger.error(f"Error processing broad category button: {e}", exc_info=True)
                await query.message.reply_text("Error showing category details. Please try again.")

        elif data.startswith("overview_month:"):
            # --- Handle Overview Month Navigation ---
            month = data.split(":", 1)[1]
            logger.info(f"Processing overview month navigation for: '{month}'")
            
            try:
                # Parse month to get formatted display name
                month_date = datetime.strptime(month, "%Y-%m")
                month_name = month_date.strftime("%B %Y")
                
                # Create a summary message that includes all broad categories for the selected month
                summary_parts = []
                total_balance = 0
                category_balances = {}
                
                # Collect data for each broad category
                for broad_category in BROAD_CATEGORIES.keys():
                    try:
                        # Get sheet name for the selected month
                        month_number = month_date.strftime('%m')
                        year_short = month_date.strftime('%y')
                        sheet_name = f"{month_number}{year_short}"  # Format: MMYY (e.g., 0423)
                        
                        # Attempt to get data for this category from the sheet
                        sheet = service.spreadsheets()
                        result = sheet.values().get(
                            spreadsheetId=SPREADSHEET_ID,
                            range=f"{sheet_name}!A:D"
                        ).execute()
                        
                        values = result.get("values", [])
                        
                        # Calculate category balance based on subcategories
                        category_balance = 0
                        if values:
                            subcategories = BROAD_CATEGORIES[broad_category]
                            for row in values:
                                if len(row) > 0 and row[0].strip() in subcategories and len(row) >= 4:
                                    try:
                                        # Add to the category balance
                                        balance = float(row[3].replace('₪', '').replace(',', '').strip() or 0)
                                        category_balance += balance
                                    except (ValueError, IndexError):
                                        pass
                        
                        category_balances[broad_category] = category_balance
                        total_balance += category_balance
                        
                    except Exception as cat_error:
                        logger.error(f"Error getting data for {broad_category} in {month}: {cat_error}")
                        category_balances[broad_category] = 0
                
                # Determine overall status emoji based on total balance
                status_emoji = "😐"  # Default neutral
                status_text = "On budget"
                
                if total_balance > 1500:
                    status_emoji = "🤑"
                    status_text = "Amazing month!"
                elif total_balance > 750:
                    status_emoji = "😄"
                    status_text = "Great month!"
                elif total_balance > 250:
                    status_emoji = "🙂"
                    status_text = "Good month!"
                elif total_balance > -250:
                    status_emoji = "😐"
                    status_text = "On budget"
                elif total_balance > -750:
                    status_emoji = "😕"
                    status_text = "Tight month"
                elif total_balance > -1500:
                    status_emoji = "😟"
                    status_text = "Difficult month"
                else:
                    status_emoji = "😰"
                    status_text = "Challenging month"
                
                # Format header
                summary_text = f"{status_emoji} {status_text} Budget Summary for {month_name}:\n\n"
                
                # Format each category line with appropriate emoji
                category_emojis = {
                    "Home": "🏠",
                    "Transportation": "🚗",
                    "Daily Living": "🍽️",
                    "Other": "📦"
                }
                
                # Add each category with emoji
                for category, balance in category_balances.items():
                    emoji = category_emojis.get(category, "📦")
                    summary_text += f"{emoji} {category}: ₪{balance:,.2f}\n"
                
                # Add total with status indicator
                status_indicator = "✅ (Within Budget)" if total_balance >= 0 else "⚠️ (Over Budget)"
                summary_text += f"\n💰 Total Balance: ₪{total_balance:,.2f} {status_indicator}"
                
                # Create buttons for each broad category
                keyboard = []
                row = []
                for i, category in enumerate(BROAD_CATEGORIES.keys()):
                    emoji = category_emojis.get(category, "📦")
                    row.append(InlineKeyboardButton(f"{emoji} {category}", 
                                                    callback_data=f"broad_category:{category}"))
                    # Two buttons per row
                    if i % 2 == 1 or i == len(BROAD_CATEGORIES.keys()) - 1:
                        keyboard.append(row)
                        row = []
                
                # Add previous/next month navigation buttons
                prev_month = get_previous_month(month)
                next_month = get_next_month(month)
                
                keyboard.append([
                    InlineKeyboardButton("⬅️ Previous Month", callback_data=f"overview_month:{prev_month}"),
                    InlineKeyboardButton("Next Month ➡️", callback_data=f"overview_month:{next_month}")
                ])
                
                reply_markup = InlineKeyboardMarkup(keyboard)
                
                # Edit the current message
                await query.edit_message_text(
                    text=summary_text,
                    reply_markup=reply_markup,
                    parse_mode='Markdown'
                )
                
                # Log for gamification
                user_id = query.from_user.id
                if gamification and user_id:
                    try:
                        gs = GamificationSystem(user_id)
                        reason = f"Viewing overview for {month_name}"
                        gs.add_xp(gamification.XP_REPORT_VIEWED / 2, reason) # Half XP for month navigation
                        gs.log_report_view(f"overview_month:{month}")
                    except Exception as g_err:
                        logger.error(f"Error in gamification for overview month: {g_err}")
                        
            except Exception as e:
                logger.error(f"Error processing overview month navigation: {e}", exc_info=True)
                await query.answer("Error showing overview for this month", show_alert=True)

        else:
             # --- Handle Unknown Callback Data ---
             logger.warning(f"Received unknown callback data from user {user_id}: {data}")
             # Optionally inform the user, but silently failing might be better UX
             # await query.answer("This button seems outdated.", show_alert=True) # show_alert makes it a popup

    except Exception as e:
        logger.error(f"Critical error in button_callback processing data '{data}' for user {user_id}: {e}", exc_info=True)
        try:
            # Try to inform the user via a new message if editing failed or wasn't appropriate
            await query.message.reply_text("⚠️ Sorry, an error occurred while processing your request. Please try again.")
        except Exception as follow_up_error:
             # Log if even the error message fails
             logger.error(f"Failed to send error notification message in button_callback: {follow_up_error}")
def parse_natural_language(text):
    """
    Parse natural language expense entries using pattern recognition.
    
    Examples:
    - "Spent 50 on lunch"
    - "Paid 120 for electricity bill"
    - "Taxi to airport cost me 85 shekels"
    - "I bought groceries for 200"
    - "I just had coffee" (will detect coffee as expense but needs amount)
    """
    try:
        logger.info(f"Trying to parse natural language: '{text}'")
        text = text.lower()
        
        # Extract amount - look for numbers possibly followed by currency symbols
        amount_match = re.search(r'\b(\d+(?:\.\d+)?)\b', text)
        amount = None
        if amount_match:
            amount = float(amount_match.group(1))
            logger.info(f"Found amount: {amount}")
        
        # Find all category keywords in the text
        potential_categories = []
        
        # Create flattened list of all keywords for easier searching
        all_keywords = {}
        for category, keywords in category_map.items():
            for keyword in keywords:
                all_keywords[keyword.lower()] = category
        
        # Expense-related verbs that suggest a transaction
        expense_verbs = ["spent", "paid", "bought", "purchased", "ordered", "got", "had", "ate", "drank", "took"]
        has_expense_verb = any(verb in text for verb in expense_verbs)
        
        # Search for each keyword in the text
        for keyword, category in all_keywords.items():
            if keyword in text:
                # Count occurrence and position of keyword
                score = text.count(keyword) + (1.0 / (text.find(keyword) + 1))
                # Boost score if there's an expense verb
                if has_expense_verb:
                    score += 0.5
                potential_categories.append((category, keyword, score))
        
        # If no keywords found, try fuzzy matching
        if not potential_categories:
            # Extract potential category words (nouns)
            words = re.findall(r'\b[a-z]{3,}\b', text)
            
            # Skip common stopwords
            stopwords = ['the', 'and', 'for', 'that', 'with', 'this', 'was', 'are', 'not', 'have', 'from']
            words = [w for w in words if w not in stopwords]
            
            for word in words:
                # Try fuzzy matching on each word
                for keyword, category in all_keywords.items():
                    if process.fuzz.ratio(word, keyword) > 75:
                        potential_categories.append((category, keyword, 0.5))
        
        if not potential_categories:
            logger.info("No category keywords found in text")
            return None
            
        # Sort by score (higher is better)
        potential_categories.sort(key=lambda x: x[2], reverse=True)
        
        # Get the best category
        best_category, matched_keyword, _ = potential_categories[0]
        logger.info(f"Best category match: {best_category} (matched '{matched_keyword}')")
        
        # Create a result dictionary
        result = {
            'category': best_category,
            'amount': amount,
            'needs_amount': amount is None,
            'original_text': text,
            'matched_keyword': matched_keyword
        }
        
        logger.info(f"Natural language parsing result: {result}")
        return result
        
    except Exception as e:
        logger.error(f"Error parsing natural language: {str(e)}", exc_info=True)
        return None

async def category_summary(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show summary for a specific subcategory, including all notes with chunking."""
    try:
        if not context.args:
            # Suggest categories if none provided
            available_categories_text = "Please specify a subcategory. Example: /category Groceries\n\nCommon Subcategories:\n"
            common_cats = list(category_map.keys())[:10]
            available_categories_text += "\n".join(f"- {cat}" for cat in common_cats)
            if len(category_map) > 10: available_categories_text += "\n..."
            available_categories_text += "\nUse /categories for the full list."
            await update.message.reply_text(available_categories_text)
            return

        subcategory_query = " ".join(context.args).strip().lower()
        logger.info(f"Processing category summary for query: '{subcategory_query}'")

        # Find the exact matching subcategory name
        matched_category_name = None
        found_by_keyword = False
        for cat_name in category_map.keys():
            if subcategory_query == cat_name.lower():
                matched_category_name = cat_name
                break
        if not matched_category_name:
            for cat_name, keywords in category_map.items():
                if subcategory_query in [kw.lower() for kw in keywords]:
                    matched_category_name = cat_name
                    found_by_keyword = True
                    break

        if not matched_category_name:
            logger.warning(f"No matching subcategory found for '{subcategory_query}'")
            similar_cat, _, _ = find_similar_category(subcategory_query, threshold=70)
            suggestion = f" Did you mean '{similar_cat}'?" if similar_cat else ""
            await update.message.reply_text(f"Subcategory '{subcategory_query}' not found.{suggestion}\nUse /categories for the full list.")
            return

        logger.info(f"Found matching canonical subcategory: '{matched_category_name}' (found by keyword: {found_by_keyword})")

        now = datetime.now()
        sheet_name = now.strftime("%m%y")
        logger.info(f"Using sheet: {sheet_name}")

        # Get sheet values
        try:
            result = service.spreadsheets().values().get(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{sheet_name}!A:D"
            ).execute()
            values = result.get("values", [])
            if not values:
                 await update.message.reply_text(f"No data found for {now.strftime('%B %Y')}.")
                 return
        except Exception as sheet_error:
            logger.error(f"Error fetching sheet data for {sheet_name} for category summary: {sheet_error}", exc_info=True)
            await update.message.reply_text("Error retrieving sheet data. Please try again later.")
            return

        # Find row
        match_row = None
        for i, row in enumerate(values):
            if row and len(row) > 0 and row[0] and row[0].strip().lower() == matched_category_name.lower():
                match_row = i + 1
                break

        if not match_row:
            logger.error(f"Subcategory '{matched_category_name}' found in map but not in sheet '{sheet_name}'.")
            await update.message.reply_text(f"Subcategory '{matched_category_name}' wasn't found in the sheet for {now.strftime('%B %Y')}.")
            return

        logger.info(f"Found subcategory '{matched_category_name}' at row: {match_row}")

        # Get budget/actual/balance using safe helper if defined, otherwise direct access
        # NOTE: Ensure get_safe_float is defined *before* this function if you use it here
        # Example using direct access (less safe but avoids dependency if needed):
        try:
             row_data = values[match_row - 1]
             budget = float(row_data[1].replace('₪', '').replace(',', '').strip() or 0) if len(row_data) > 1 else 0.0
             actual = float(row_data[2].replace('₪', '').replace(',', '').strip() or 0) if len(row_data) > 2 else 0.0
             balance = float(row_data[3].replace('₪', '').replace(',', '').strip() or 0) if len(row_data) > 3 else 0.0
        except (ValueError, IndexError, TypeError):
             logger.error(f"Error parsing B/A/B for {matched_category_name}, row {match_row}")
             budget, actual, balance = 0.0, 0.0, 0.0 # Default on error


        # Get notes
        notes = ""
        try:
            note_cell_range = f"{sheet_name}!C{match_row}"
            notes_result = service.spreadsheets().get(
                spreadsheetId=SPREADSHEET_ID,
                ranges=[note_cell_range],
                fields="sheets(data(rowData(values(note))))",
            ).execute()
            sheets_data = notes_result.get("sheets")
            # ... (add the safe note extraction logic from log_expense function here) ...
            if (sheets_data and isinstance(sheets_data, list) and len(sheets_data) > 0 and
                sheets_data[0].get("data") and isinstance(sheets_data[0]["data"], list) and len(sheets_data[0]["data"]) > 0 and
                sheets_data[0]["data"][0].get("rowData") and isinstance(sheets_data[0]["data"][0]["rowData"], list) and len(sheets_data[0]["data"][0]["rowData"]) > 0 and
                sheets_data[0]["data"][0]["rowData"][0].get("values") and isinstance(sheets_data[0]["data"][0]["rowData"][0]["values"], list) and len(sheets_data[0]["data"][0]["rowData"][0]["values"]) > 0):
                 note_data = sheets_data[0]["data"][0]["rowData"][0]["values"][0]
                 notes = note_data.get("note", "")
            logger.info(f"Fetched notes for {matched_category_name}: {len(notes)} characters")
        except Exception as e:
            logger.error(f"Error getting notes for {matched_category_name}: {str(e)}", exc_info=True)
            notes = "(Could not retrieve notes)"

        # Message Chunking for Notes
        MAX_MSG_LENGTH = 4096
        initial_summary = (
            f"📊 *{matched_category_name}* Summary ({now.strftime('%B %Y')}):\n\n"
            f"Budget: ₪{budget:,.2f}\n"
            f"Actual: ₪{actual:,.2f}\n"
            f"Balance: ₪{balance:,.2f}\n\n"
            "--- All Entries ---\n"
        )

        current_message = initial_summary
        all_entries = notes.split('\n') if notes and notes != "(Could not retrieve notes)" else []
        entry_count = len(all_entries)

        if not all_entries:
             current_message += "(No entries recorded in notes)" if notes != "(Could not retrieve notes)" else notes

        # Send initial part
        if len(current_message) > MAX_MSG_LENGTH:
             await update.message.reply_text(current_message[:MAX_MSG_LENGTH - 20] + "\n... (summary truncated)")
             return
        else:
            if all_entries:
                await update.message.reply_text(current_message, parse_mode='Markdown')
                current_message = ""
            else:
                 await update.message.reply_text(current_message, parse_mode='Markdown')
                 # Log gamification here if needed (no notes part)
                 # ...
                 return # Exit if no notes

        # Chunk and send notes
        for entry in all_entries:
            if not entry.strip(): continue
            entry_line = entry + "\n"
            if len(current_message) + len(entry_line) > MAX_MSG_LENGTH:
                if current_message: await update.message.reply_text(current_message)
                current_message = entry_line
            else:
                current_message += entry_line
        if current_message: await update.message.reply_text(current_message)

        # Gamification Logging (after success)
        user_id = update.effective_user.id
        if gamification and user_id:
             try:
                 gs = GamificationSystem(user_id)
                 reason = f"Viewing category summary: {matched_category_name}"
                 leveled_up, new_level = gs.add_xp(gamification.XP_REPORT_VIEWED, reason)
                 gs.log_report_view(f"category: {matched_category_name}")
                 if leveled_up: await update.message.reply_text(f"🎉 LEVEL UP! You reached Level {new_level}!")
             except Exception as gamify_error: logger.error(f"Error logging gamification: {gamify_error}")

    except Exception as e:
        logger.error(f"Error in category_summary for '{subcategory_query}': {str(e)}", exc_info=True)
        await update.message.reply_text("An error occurred while retrieving the category summary.")
                    
# Function to handle messages
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle expense entries"""
    try:
        # Set the global user_id for the current request context
        # This will be used by get_current_user_id_from_context()
        # Store user ID in context for potential budget notifications
        user_id = update.effective_user.id
        context.user_data['current_user_id'] = user_id

        # Get the full text of the message
        message_text = update.message.text.strip()
        logger.info(f"Received message: '{message_text}'")
        
        # Check if we're waiting for an amount for a previously detected expense
        if hasattr(context, 'user_data') and 'pending_expense_needs_amount' in context.user_data:
            try:
                # Try to parse this message as an amount
                amount_match = re.search(r'\b(\d+(?:\.\d+)?)\b', message_text)
                if amount_match:
                    amount = float(amount_match.group(1))
                    category = context.user_data['pending_expense_category']
                    original_text = context.user_data['pending_expense_text']
                    
                    # Log the expense with the now-complete data
                    result = await log_expense_to_google_sheets_with_notes(category, amount, original_text, user_id)
                    
                    # Provide a more informative response
                    await update.message.reply_text(
                        f"Thanks! Logged {amount} for {category}\n{result}"
                    )
                    
                    # Clear the pending state
                    del context.user_data['pending_expense_needs_amount']
                    del context.user_data['pending_expense_category']
                    del context.user_data['pending_expense_text']
                    return
                else:
                    # User didn't provide a number, ask again
                    await update.message.reply_text(
                        f"I need a number for the amount. How much did the {context.user_data['pending_expense_matched_keyword']} cost?"
                    )
                    return
            except Exception as e:
                logger.error(f"Error processing amount response: {str(e)}", exc_info=True)
                # If there's an error, continue with normal message processing
        
        # Check if this message has already been processed
        if hasattr(context, 'processed_messages') and message_text in context.processed_messages:
            logger.info("Message already processed, skipping")
            return
            
        # Initialize processed_messages if it doesn't exist
        if not hasattr(context, 'processed_messages'):
            context.processed_messages = set()
            
        # Add this message to processed set
        context.processed_messages.add(message_text)
        
        # Split into individual expense entries
        expense_entries = [entry.strip() for entry in message_text.split(',')]
        logger.info(f"Processing {len(expense_entries)} expense entries")
        
        # Process each expense entry
        for entry in expense_entries:
            logger.info(f"Processing entry: '{entry}'")
            
            # First, try traditional parsing
            traditional_parsing_succeeded = False
            
            # Split into parts (e.g., "super 25.50 - groceries" -> ["super", "25.50", "-", "groceries"])
            parts = entry.split()
            
            if len(parts) >= 2:
                # Extract category and amount
                category_input = parts[0].lower()
                logger.info(f"Category input: '{category_input}'")
                
                try:
                    amount = float(parts[1])
                    logger.info(f"Amount: {amount}")
                    
                    # Extract note if present
                    note = " ".join(parts[2:]) if len(parts) > 2 else ""
                    logger.info(f"Note: '{note}'")
                    
                    # Find matching category - check all keywords in lowercase
                    matching_category = None
                    for main_category, keywords in category_map.items():
                        if category_input in [kw.lower() for kw in keywords]:
                            matching_category = main_category
                            break
                    
                    if matching_category:
                        logger.info(f"Found matching category: '{matching_category}'")
                        traditional_parsing_succeeded = True
                        
                        # Log the expense and get the response
                        try:
                            logger.info(f"Logging expense: category='{matching_category}', amount={amount}, text='{entry}'")
                            result = await log_expense_to_google_sheets_with_notes(matching_category, amount, entry, user_id)
                            logger.info(f"Expense logged with result: '{result}'")
                            await update.message.reply_text(result)
                        except Exception as e:
                            logger.error(f"Error logging expense: {str(e)}", exc_info=True)
                            await update.message.reply_text(f"Error logging expense: {str(e)}")
                    else:
                        logger.info(f"No matching category found for '{category_input}', trying fuzzy matching")
                        
                        try:
                            # Store expense details for later use if suggestion is accepted
                            if not hasattr(context, 'user_data'):
                                context.user_data = {}
                            
                            context.user_data['amount'] = amount
                            context.user_data['note'] = note
                            context.user_data['original_text'] = entry
                            context.user_data['user_id'] = user_id
                            
                            # Try to suggest similar categories
                            suggestion_result = await handle_category_suggestion(update, context, category_input)
                            logger.info(f"Category suggestion result: {suggestion_result}")
                            traditional_parsing_succeeded = True  # We're handling this with fuzzy matching
                        except Exception as e:
                            logger.error(f"Error suggesting category: {str(e)}", exc_info=True)
                            traditional_parsing_succeeded = False
                except ValueError:
                    logger.warning(f"Invalid amount in entry: '{entry}'")
                    traditional_parsing_succeeded = False
            else:
                logger.warning(f"Not enough parts in entry: '{entry}'")
                traditional_parsing_succeeded = False
            
            # If traditional parsing failed, try natural language parsing
            if not traditional_parsing_succeeded:
                logger.info("Traditional parsing failed, trying natural language parsing")
                parsed = parse_natural_language(entry)
                
                if parsed:
                    # Check if we need to ask for an amount
                    if parsed.get('needs_amount', False):
                        # Store the information to continue later
                        if not hasattr(context, 'user_data'):
                            context.user_data = {}
                        
                        context.user_data['pending_expense_needs_amount'] = True
                        context.user_data['pending_expense_category'] = parsed['category']
                        context.user_data['pending_expense_text'] = entry
                        context.user_data['pending_expense_matched_keyword'] = parsed['matched_keyword']
                        
                        # Ask for the amount
                        await update.message.reply_text(
                            f"How much did the {parsed['matched_keyword']} cost?"
                        )
                        return
                    elif parsed['amount'] is not None:
                        try:
                            category = parsed['category']
                            amount = parsed['amount']
                            
                            # Log the expense with the extracted data
                            result = await log_expense_to_google_sheets_with_notes(category, amount, entry, user_id)
                            
                            # Provide a more informative response
                            await update.message.reply_text(
                                f"I understood that as: {amount} for {category}\n{result}"
                            )
                        except Exception as e:
                            logger.error(f"Error logging natural language expense: {str(e)}", exc_info=True)
                            await update.message.reply_text(f"Error logging expense: {str(e)}")
                else:
                    # Both traditional and NLP parsing failed
                    await update.message.reply_text(
                        "I couldn't understand that format. Try 'category amount' or '/help' for examples."
                    )
            
    except Exception as e:
        logger.error(f"Error handling message: {str(e)}", exc_info=True)
        await update.message.reply_text("Error processing expense. Please try again later.")


# Flask app for HTTP server
app = Flask(__name__)


@app.route("/")
def index():
    return "Telegram Expense Tracker Bot is running!"


async def run_web_server():
    port = int(os.environ.get("PORT", 5000))
    config = Config()
    config.bind = [f"0.0.0.0:{port}"]
    logger.info(f"Starting web server on port {port}")
    await serve(app, config)

async def get_expenses_from_sheets() -> List[Dict]:
    """Get expenses from Google Sheets, returning a list of dicts with date, category, amount, note, and row"""
    global expense_history, sheet_cache

    try:
        # If expense history exists, return it instead of making API calls
        if expense_history:
            logger.info(f"Using in-memory expense history ({len(expense_history)} items)")
            return expense_history

        logger.info("No expense history available, fetching from Google Sheets")
        
        # Check if service is None
        if service is None:
            logger.error("Google Sheets service is not initialized. Cannot fetch expenses.")
            return []  # Return empty list to prevent further errors

        # Get sheets metadata to check available sheets
        try:
            sheet_metadata = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
            sheets = sheet_metadata.get('sheets', [])
            available_sheets = [sheet.get('properties', {}).get('title') for sheet in sheets]
            logger.info(f"Available sheets: {available_sheets}")
        except Exception as e:
            logger.error(f"Error fetching sheet metadata: {e}", exc_info=True)
            return []

        # Try both formats: 'MMYY' and 'Month YYYY'
        current_date = datetime.now()
        sheet_name_mmyy = current_date.strftime('%m%y')  # Format: 0423 for April 2023
        sheet_name_month_year = current_date.strftime('%B %Y')  # Format: April 2023
        
        # First check cache for either format
        for cache_key in [sheet_name_mmyy, sheet_name_month_year]:
            if cache_key in sheet_cache and (datetime.now() - sheet_cache[cache_key]['timestamp']).seconds < CACHE_EXPIRY:
                logger.info(f"Using cached data for {cache_key} (cached {(datetime.now() - sheet_cache[cache_key]['timestamp']).seconds}s ago)")
                expenses = sheet_cache[cache_key]['data']
                expense_history = expenses[:MAX_HISTORY_SIZE]  # Store in memory for future use
                return expenses
        
        # No cache, so check which sheet format exists
        sheet_name = None
        if sheet_name_mmyy in available_sheets:
            sheet_name = sheet_name_mmyy
            logger.info(f"Using sheet format 'MMYY': {sheet_name}")
        elif sheet_name_month_year in available_sheets:
            sheet_name = sheet_name_month_year
            logger.info(f"Using sheet format 'Month YYYY': {sheet_name}")
        else:
            # Try looking for other month patterns to determine format used
            for sheet_title in available_sheets:
                # Check if any existing sheet matches MMYY format (e.g., 0123, 0223, etc.)
                if re.match(r'^(0[1-9]|1[0-2])\d{2}$', sheet_title):
                    # If found, use current month in same format
                    sheet_name = sheet_name_mmyy
                    logger.info(f"Found MMYY pattern, using: {sheet_name}")
                    break
                # Check if any existing sheet matches Month YYYY format (e.g., January 2023)
                elif re.match(r'^[A-Z][a-z]+ \d{4}$', sheet_title):
                    # If found, use current month in same format
                    sheet_name = sheet_name_month_year
                    logger.info(f"Found Month YYYY pattern, using: {sheet_name}")
                    break
            
            if not sheet_name:
                logger.warning(f"No sheet found for the current month in any format. Available sheets: {available_sheets}")
                return []
        
        # Now let's check if the chosen sheet name exists
        if sheet_name not in available_sheets:
            logger.warning(f"Sheet '{sheet_name}' not found in available sheets")
            return []
            
        # Get data from the chosen sheet
        range_name = f"'{sheet_name}'!A2:F"
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=range_name
        ).execute()
        
        values = result.get('values', [])
        if not values:
            logger.info(f"No data found in sheet {sheet_name}")
            return []
            
        # Process the data
        expenses = []
        for row_idx, row in enumerate(values, start=2):  # Start from row 2 (1-indexed)
            if len(row) >= 3:  # Ensure we have date, category, and amount
                date_str = row[0] if len(row) > 0 else ""
                category = row[1] if len(row) > 1 else ""
                amount_str = row[2] if len(row) > 2 else "0"
                subcategory = row[3] if len(row) > 3 else ""
                note = row[4] if len(row) > 4 else ""
                
                # Convert amount to float
                try:
                    amount = float(amount_str.replace(',', '').replace('₪', '').strip())
                except (ValueError, AttributeError):
                    logger.warning(f"Invalid amount format in row {row_idx}: {amount_str}")
                    amount = 0.0
                    
                expenses.append({
                    'date': date_str,
                    'category': category,
                    'subcategory': subcategory,
                    'amount': amount,
                    'note': note,
                    'row': row_idx,
                    'sheet_name': sheet_name,
                    'sheet_id': None  # Will fill this if needed
                })
        
        # Find sheet ID for the processed sheet
        for sheet in sheets:
            props = sheet.get('properties', {})
            if props.get('title') == sheet_name:
                sheet_id = props.get('sheetId')
                # Add sheet ID to each expense record
                for expense in expenses:
                    expense['sheet_id'] = sheet_id
                break
                
        # Cache the results
        sheet_cache[sheet_name] = {
            'data': expenses,
            'timestamp': datetime.now()
        }
        
        # Store in memory for future use (limit to recent expenses)
        expense_history = expenses[:MAX_HISTORY_SIZE]
        logger.info(f"Fetched {len(expenses)} expenses from Google Sheets, cached {len(expense_history)} in memory")
        
        return expenses
    except Exception as e:
        logger.error(f"Error fetching expenses from sheets: {e}", exc_info=True)
        return []

async def delete_expense_from_sheets(row_number: int, sheet_name: Optional[str] = None) -> bool:
    """Deletes an expense from Google Sheets by row number.
    
    Args:
        row_number: The 1-indexed row number to delete
        sheet_name: The sheet name to delete from (defaults to current month)
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Use current month if sheet_name not provided
        if not sheet_name:
            sheet_name = datetime.now().strftime("%m%y")
            
        logger.info(f"Deleting expense from row {row_number} in sheet {sheet_name}")
        
        # Clear the row values instead of deleting it to maintain sheet structure
        range_name = f"{sheet_name}!E{row_number}:H{row_number}"
        body = {
            "values": [["", "", "", ""]]  # Empty values for each column
        }
        
        result = service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=range_name,
            valueInputOption="USER_ENTERED",
            body=body
        ).execute()
        
        logger.info(f"Successfully cleared expense row {row_number} in sheet {sheet_name}")
        return True
        
    except Exception as e:
        logger.error(f"Error deleting expense from sheet: {str(e)}", exc_info=True)
        return False

def get_previous_month(month_str: str) -> str:
    """Get the previous month in YYYY-MM format.
    
    Args:
        month_str: Current month in YYYY-MM format
        
    Returns:
        Previous month in YYYY-MM format
    """
    try:
        # Parse the input month
        current_month = datetime.strptime(month_str, "%Y-%m")
        
        # Calculate previous month by subtracting one month
        previous_month = current_month.replace(day=1) - timedelta(days=1)
        
        # Format the result in YYYY-MM format
        return previous_month.strftime("%Y-%m")
    except Exception as e:
        logger.error(f"Error in get_previous_month: {str(e)}")
        # Return current month if there's an error
        return datetime.now().strftime("%Y-%m")

def get_next_month(month_str: str) -> str:
    """Get the next month in YYYY-MM format.
    
    Args:
        month_str: Current month in YYYY-MM format
        
    Returns:
        Next month in YYYY-MM format
    """
    try:
        # Parse the input month
        current_month = datetime.strptime(month_str, "%Y-%m")
        
        # Calculate the first day of the current month
        first_day = current_month.replace(day=1)
        
        # Calculate the first day of the next month
        if first_day.month == 12:
            # If December, go to January of next year
            next_month = first_day.replace(year=first_day.year + 1, month=1)
        else:
            # Otherwise, just increment the month
            next_month = first_day.replace(month=first_day.month + 1)
        
        # Format the result in YYYY-MM format
        return next_month.strftime("%Y-%m")
    except Exception as e:
        logger.error(f"Error in get_next_month: {str(e)}")
        # Return current month if there's an error
        return datetime.now().strftime("%Y-%m")

async def get_monthly_summary(month: str = None, category: str = None) -> Tuple[str, Dict[str, float]]:
    """
    Get a summary of expenses for a given month, optionally filtered by category.
    
    Args:
        month: Month in the format "YYYY-MM" (default: current month)
        category: Optional category to filter by
        
    Returns:
        Tuple containing the formatted summary text and a dictionary of category totals
    """
    try:
        if service is None:
            logger.error("Google Sheets service is not initialized. Cannot generate monthly summary.")
            return "Error: Unable to connect to Google Sheets. Please try again later.", {}
            
        # Default to current month if no month specified
        if not month:
            month = datetime.now().strftime('%Y-%m')
            
        # Parse the year and month
        try:
            year, month_num = month.split('-')
            year = int(year)
            month_num = int(month_num)
            month_name = calendar.month_name[month_num]
            date_range = f"{month_name} {year}"
        except (ValueError, IndexError):
            logger.error(f"Invalid month format: {month}")
            return f"Error: Invalid month format '{month}'. Please use YYYY-MM format.", {}
            
        # Get all expenses
        expenses = await get_expenses_from_sheets()
        if not expenses:
            return f"No expenses found for {date_range}.", {}
            
        # Filter by month and category if specified
        filtered_expenses = []
        for expense in expenses:
            expense_date = expense.get('date', '')
            
            # Skip if date is invalid
            try:
                if expense_date:
                    expense_date_obj = datetime.strptime(expense_date, '%Y-%m-%d')
                    if expense_date_obj.year == year and expense_date_obj.month == month_num:
                        if not category or expense.get('category', '').lower() == category.lower():
                            filtered_expenses.append(expense)
            except (ValueError, TypeError) as e:
                logger.warning(f"Invalid date format in expense: {expense_date} - {e}")
                continue
                
        # If no expenses found after filtering
        if not filtered_expenses:
            if category:
                return f"No expenses found for {category} in {date_range}.", {}
            else:
                return f"No expenses found for {date_range}.", {}
                
        # Calculate totals by category
        category_totals = {}
        total_amount = 0
        
        for expense in filtered_expenses:
            expense_category = expense.get('category', 'Unknown')
            amount = expense.get('amount', 0)
            
            # Convert amount to float if it's a string
            if isinstance(amount, str):
                try:
                    amount = float(amount.replace(',', '').replace('₪', '').strip())
                except (ValueError, AttributeError):
                    logger.warning(f"Invalid amount format: {amount}")
                    amount = 0
                    
            if expense_category not in category_totals:
                category_totals[expense_category] = 0
            category_totals[expense_category] += amount
            total_amount += amount
            
        # Sort categories by amount (highest first)
        sorted_categories = sorted(category_totals.items(), key=lambda x: x[1], reverse=True)
        
        # Format the summary text
        if category:
            summary = f"📊 *{category} Expenses for {date_range}*\n\n"
            summary += f"Total: ₪{total_amount:.2f}\n\n"
            
            # Add individual expenses for the category
            summary += "*Individual Expenses:*\n"
            for expense in sorted(filtered_expenses, key=lambda x: x.get('date', ''), reverse=True):
                date = expense.get('date', 'Unknown date')
                note = expense.get('note', '')
                subcategory = expense.get('subcategory', '')
                amount = expense.get('amount', 0)
                
                expense_detail = f"• {date}: ₪{amount:.2f}"
                if subcategory:
                    expense_detail += f" - {subcategory}"
                if note:
                    expense_detail += f" ({note})"
                    
                summary += f"{expense_detail}\n"
        else:
            summary = f"📊 *Monthly Summary for {date_range}*\n\n"
            summary += f"*Total Expenses: ₪{total_amount:.2f}*\n\n"
            
            # Add category breakdown
            summary += "*Categories:*\n"
            for cat, amount in sorted_categories:
                percentage = (amount / total_amount) * 100 if total_amount > 0 else 0
                summary += f"• {cat}: ₪{amount:.2f} ({percentage:.1f}%)\n"
                
        return summary, dict(sorted_categories)
        
    except Exception as e:
        logger.error(f"Error generating monthly summary: {e}", exc_info=True)
        return "Error generating monthly summary. Please try again later.", {}

async def send_monthly_summary_with_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE, month: Optional[str] = None) -> None:
    """Send monthly summary with navigation buttons."""
    try:
        # Get the current month if none specified
        if not month:
            month = datetime.now().strftime("%Y-%m")
            
        logger.info(f"Generating monthly summary for month: {month}")
        
        # Get the summary text and category totals
        summary_text, category_totals = await get_monthly_summary(month)
        
        # Create navigation buttons
        keyboard = [
            [
                InlineKeyboardButton("⬅️ Previous Month", callback_data=f"month:{get_previous_month(month)}"),
                InlineKeyboardButton("Next Month ➡️", callback_data=f"month:{get_next_month(month)}")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Check if we're responding to a callback query or a direct command
        if update.callback_query:
            query = update.callback_query
            try:
                await query.edit_message_text(
                    text=summary_text,
                    reply_markup=reply_markup,
                    parse_mode='Markdown'
                )
                logger.info(f"Edited message with monthly summary for {month}")
            except Exception as e:
                logger.error(f"Error editing message: {e}")
                await query.answer("Could not update the message. Please try again.")
        else:
            try:
                await update.message.reply_text(
                    text=summary_text,
                    reply_markup=reply_markup,
                    parse_mode='Markdown'
                )
                logger.info(f"Sent new message with monthly summary for {month}")
            except Exception as e:
                logger.error(f"Error sending message: {e}")
                await update.message.reply_text("❌ An error occurred. Please try again.")
                
        # Log for gamification if successful
        if gamification and "No expenses found" not in summary_text:
            try:
                # Get the user ID from either callback query or message
                user_id = None
                if update.callback_query:
                    user_id = update.callback_query.from_user.id
                elif update.effective_user:
                    user_id = update.effective_user.id
                
                if user_id:
                    gs = GamificationSystem(user_id)
                    date_str = datetime.strptime(month, "%Y-%m").strftime("%B %Y")
                    reason = f"Viewing monthly summary: {date_str}"
                    leveled_up, new_level = gs.add_xp(gamification.XP_REPORT_VIEWED, reason)
                    gs.log_report_view(f"monthly:{month}")
                    
                    if leveled_up:
                        if update.callback_query:
                            await update.callback_query.answer(f"🎉 LEVEL UP! You reached Level {new_level}!", show_alert=True)
                        elif update.message:
                            await update.message.reply_text(f"🎉 LEVEL UP! You reached Level {new_level} by viewing the monthly summary!")
            except Exception as g_err:
                logger.error(f"Error in gamification for monthly summary: {g_err}")
            
    except Exception as e:
        logger.error(f"Error in send_monthly_summary_with_buttons: {e}", exc_info=True)
        # Determine if we're responding to a callback query or a direct command
        if update.callback_query:
            await update.callback_query.answer("❌ An error occurred. Please try again.")
        else:
            await update.message.reply_text("❌ An error occurred while generating the monthly summary. Please try again.")

async def monthly_summary(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /monthly command by calling the summary helper function."""
    # Call the helper, passing the user's arguments (if any) for month selection
    month = None
    if context.args and len(context.args) > 0:
        month = context.args[0]
        # Ensure proper format (YYYY-MM)
        if month and not re.match(r'^\d{4}-\d{2}$', month):
            current_year = datetime.now().year
            if re.match(r'^\d{1,2}$', month):  # Single or double digit month
                month = f"{current_year}-{int(month):02d}"
            else:
                await update.message.reply_text(
                    "Invalid month format. Please use YYYY-MM (e.g., 2023-04) or just the month number (e.g., 4 for April)."
                )
                return
    
    logger.info(f"Running monthly summary for month: {month}")
    await send_monthly_summary_with_buttons(update, context, month)
    
async def get_overview_summary_text(broad_category_name: str) -> str:
    """Fetches and formats the overview summary text for a given broad category."""
    try:
        # Validate the requested broad category
        if broad_category_name not in BROAD_CATEGORIES:
            valid_broad_categories = ", ".join(BROAD_CATEGORIES.keys())
            return f"Invalid broad category '{broad_category_name}'. Available: {valid_broad_categories}"

        target_subcategories = BROAD_CATEGORIES[broad_category_name]
        target_subcategories_lower = {sub.lower() for sub in target_subcategories}

        now = datetime.now()
        
        # Get sheets metadata to check available sheets
        try:
            sheet_metadata = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
            sheets = sheet_metadata.get('sheets', [])
            available_sheets = [sheet.get('properties', {}).get('title') for sheet in sheets]
            logger.info(f"Available sheets: {available_sheets}")
        except Exception as e:
            logger.error(f"Error fetching sheet metadata: {e}", exc_info=True)
            return "Error retrieving available sheets."

        # Try both formats: 'MMYY' and 'Month YYYY'
        sheet_name_mmyy = now.strftime('%m%y')  # Format: 0423 for April 2023
        sheet_name_month_year = now.strftime('%B %Y')  # Format: April 2023
        
        # Determine which sheet name to use
        sheet_name = None
        if sheet_name_mmyy in available_sheets:
            sheet_name = sheet_name_mmyy
            logger.info(f"Using sheet format 'MMYY': {sheet_name}")
        elif sheet_name_month_year in available_sheets:
            sheet_name = sheet_name_month_year
            logger.info(f"Using sheet format 'Month YYYY': {sheet_name}")
        else:
            # Try looking for other month patterns to determine format used
            for sheet_title in available_sheets:
                # Check if any existing sheet matches MMYY format (e.g., 0123, 0223, etc.)
                if re.match(r'^(0[1-9]|1[0-2])\d{2}$', sheet_title):
                    # If found, use current month in same format
                    sheet_name = sheet_name_mmyy
                    logger.info(f"Found MMYY pattern, using: {sheet_name}")
                    break
                # Check if any existing sheet matches Month YYYY format (e.g., January 2023)
                elif re.match(r'^[A-Z][a-z]+ \d{4}$', sheet_title):
                    # If found, use current month in same format
                    sheet_name = sheet_name_month_year
                    logger.info(f"Found Month YYYY pattern, using: {sheet_name}")
                    break
            
            if not sheet_name:
                logger.warning(f"No sheet found for the current month in any format. Available sheets: {available_sheets}")
                return f"No sheet found for {now.strftime('%B %Y')} in any format."

        logger.info(f"Getting overview for '{broad_category_name}' from sheet '{sheet_name}'")

        # Get sheet data
        try:
            result = service.spreadsheets().values().get(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{sheet_name}!A:D"
            ).execute()
            values = result.get("values", [])
            if not values:
                 return f"No data found for {now.strftime('%B %Y')}."
        except Exception as sheet_error:
            logger.error(f"Error fetching sheet data for {sheet_name}: {sheet_error}", exc_info=True)
            return "Error retrieving sheet data."

        # Find Data for Each Subcategory
        subcategory_details = {}
        for i, row in enumerate(values):
            if row and len(row) > 0 and row[0]:
                current_subcategory_name = row[0].strip()
                if current_subcategory_name.lower() in target_subcategories_lower:
                    try:
                        budget_str = row[1] if len(row) > 1 else '0'
                        actual_str = row[2] if len(row) > 2 else '0'
                        balance_str = row[3] if len(row) > 3 else '0'
                        budget = float(budget_str.replace('₪', '').replace(',', '').strip() or 0)
                        actual = float(actual_str.replace('₪', '').replace(',', '').strip() or 0)
                        balance = float(balance_str.replace('₪', '').replace(',', '').strip() or 0)
                        subcategory_details[current_subcategory_name] = {'budget': budget, 'actual': actual, 'balance': balance}
                    except (ValueError, IndexError) as parse_error:
                        logger.warning(f"Could not parse data for '{current_subcategory_name}' in row {i+1}: {parse_error}")
                        subcategory_details[current_subcategory_name] = {'error': 'Data format issue'}

        # Format the Summary Message
        summary_lines = [f"📊 Overview: *{broad_category_name}* ({now.strftime('%B %Y')})\n"]
        for subcat_name in target_subcategories:
            if subcat_name in subcategory_details:
                details = subcategory_details[subcat_name]
                if 'error' in details:
                     summary_lines.append(f"  - {subcat_name}: Error reading data!")
                else:
                     emoji = "✅" if details['balance'] >= 0 else "⚠️" # Simplified emoji
                     summary_lines.append(
                         f"  {emoji} *{subcat_name}:* Bal: ₪{details['balance']:,.2f} (Act: ₪{details['actual']:,.2f} / Bud: ₪{details['budget']:,.2f})"
                     )
            else:
                summary_lines.append(f"  - {subcat_name}: (Not found)") # Simplified 'not found'

        # Fetch and Add Overall Balance
        total_balance = None
        total_row_index = BROAD_CATEGORY_TOTAL_ROWS.get(broad_category_name)
        if total_row_index and total_row_index <= len(values):
             try:
                 total_balance_str = values[total_row_index - 1][3] if len(values[total_row_index - 1]) > 3 else '0'
                 total_balance = float(total_balance_str.replace('₪', '').replace(',', '').strip() or 0)
             except (ValueError, IndexError): pass # Ignore error if total balance parsing fails

        if total_balance is not None:
             summary_lines.append(f"\n💰 *Total Balance for {broad_category_name}: ₪{total_balance:,.2f}*")

        return "\n".join(summary_lines)

    except Exception as e:
        logger.error(f"Error in get_overview_summary_text for {broad_category_name}: {str(e)}", exc_info=True)
        return f"An error occurred while generating the overview for {broad_category_name}."

async def overview_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Handles the /overview command.
    Shows specific broad category overview if args provided.
    Shows summary of all broad categories if no args provided.
    """
    if not context.args:
        # --- No arguments: Show an overview of all broad categories ---
        logger.info("/overview command called with no arguments, showing all broad categories.")
        try:
            # Get the current month
            now = datetime.now()
            month_name = now.strftime("%B %Y")
            
            # Create a summary message that includes all broad categories
            summary_parts = []
            total_balance = 0
            category_balances = {}
            
            # Collect data for each broad category
            for broad_category in BROAD_CATEGORIES.keys():
                category_summary = await get_overview_summary_text(broad_category)
                # Extract just the total line if it exists
                total_line = None
                for line in category_summary.split('\n'):
                    if "Total Balance for" in line:
                        total_line = line.strip()
                        break
                
                if total_line:
                    # Extract the balance amount from the total line
                    try:
                        balance_text = total_line.split('*')[-2].strip()
                        # Remove currency symbol and commas
                        balance_amount = float(balance_text.replace('₪', '').replace(',', '').strip())
                        category_balances[broad_category] = balance_amount
                        total_balance += balance_amount
                    except (ValueError, IndexError):
                        category_balances[broad_category] = 0
                else:
                    category_balances[broad_category] = 0
            
            # Determine overall status emoji based on total balance
            status_emoji = "😐"  # Default neutral
            status_text = "On budget"
            
            if total_balance > 1500:
                status_emoji = "🤑"
                status_text = "Amazing month!"
            elif total_balance > 750:
                status_emoji = "😄"
                status_text = "Great month!"
            elif total_balance > 250:
                status_emoji = "🙂"
                status_text = "Good month!"
            elif total_balance > -250:
                status_emoji = "😐"
                status_text = "On budget"
            elif total_balance > -750:
                status_emoji = "😕"
                status_text = "Tight month"
            elif total_balance > -1500:
                status_emoji = "😟"
                status_text = "Difficult month"
            else:
                status_emoji = "😰"
                status_text = "Challenging month"
            
            # Format header
            summary_text = f"{status_emoji} {status_text} Budget Summary for {month_name}:\n\n"
            
            # Format each category line with appropriate emoji
            category_emojis = {
                "Home": "🏠",
                "Transportation": "🚗",
                "Daily Living": "🍽️",
                "Other": "📦"
            }
            
            # Add each category with emoji
            for category, balance in category_balances.items():
                emoji = category_emojis.get(category, "📦")
                summary_text += f"{emoji} {category}: ₪{balance:,.2f}\n"
            
            # Add total with status indicator
            status_indicator = "✅ (Within Budget)" if total_balance >= 0 else "⚠️ (Over Budget)"
            summary_text += f"\n💰 Total Balance: ₪{total_balance:,.2f} {status_indicator}"
            
            # Create buttons for each broad category
            keyboard = []
            row = []
            for i, category in enumerate(BROAD_CATEGORIES.keys()):
                emoji = category_emojis.get(category, "📦")
                row.append(InlineKeyboardButton(f"{emoji} {category}", callback_data=f"broad_category:{category}"))
                # Two buttons per row
                if i % 2 == 1 or i == len(BROAD_CATEGORIES.keys()) - 1:
                    keyboard.append(row)
                    row = []
            
            # Add previous/next month navigation buttons
            current_month_str = now.strftime("%Y-%m")
            prev_month = get_previous_month(current_month_str)
            next_month = get_next_month(current_month_str)
            
            keyboard.append([
                InlineKeyboardButton("⬅️ Previous Month", callback_data=f"overview_month:{prev_month}"),
                InlineKeyboardButton("Next Month ➡️", callback_data=f"overview_month:{next_month}")
            ])
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            # Send the overview
            await update.message.reply_text(summary_text, parse_mode='Markdown', reply_markup=reply_markup)
            
            # Log for gamification
            if gamification and update.effective_user:
                try:
                    user_id = update.effective_user.id
                    gs = GamificationSystem(user_id)
                    reason = "Viewing all categories overview"
                    leveled_up, new_level = gs.add_xp(gamification.XP_REPORT_VIEWED, reason)
                    gs.log_report_view("overview: all")
                    
                    if leveled_up:
                        await update.message.reply_text(f"🎉 LEVEL UP! You reached Level {new_level} by viewing the overview!")
                except Exception as g_err:
                    logger.error(f"Error in gamification for overview command: {g_err}")
                    
        except Exception as e:
            logger.error(f"Error generating overview of all categories: {e}", exc_info=True)
            # Check if we're responding to a callback query or a direct command
            if hasattr(update, 'callback_query') and update.callback_query:
                await update.callback_query.answer("Error generating overview", show_alert=True)
            elif hasattr(update, 'message') and update.message:
                await update.message.reply_text("Error generating overview. Please try again later.")
            else:
                logger.error("Unable to send error response - neither callback_query nor message available")
    else:
        # --- Arguments provided: Show specific category overview ---
        try:
            # Normalize input
            broad_category_query = " ".join(context.args).strip()
            normalized_broad_category = broad_category_query.capitalize()
            logger.info(f"/overview command for specific category: '{normalized_broad_category}'")

            # Get the summary text using the specific overview helper function
            summary_text = await get_overview_summary_text(normalized_broad_category)

            # Send the message (handle potential length issues)
            if len(summary_text) > 4096:
                 await update.message.reply_text(summary_text[:4050] + "\n\n... (Summary too long, truncated)", parse_mode='Markdown')
            else:
                await update.message.reply_text(summary_text, parse_mode='Markdown')

            # Log gamification only if successful and args were provided
            if "Invalid broad category" not in summary_text and "Error" not in summary_text:
                user_id = update.effective_user.id
                if gamification and user_id:
                    try:
                        gs = GamificationSystem(user_id)
                        reason = f"Viewing overview: {normalized_broad_category}"
                        leveled_up, new_level = gs.add_xp(gamification.XP_REPORT_VIEWED, reason)
                        gs.log_report_view(f"overview: {normalized_broad_category}")
                        logger.info(f"Logged gamification report view for overview '{normalized_broad_category}'")
                        if leveled_up:
                           await update.message.reply_text(f"🎉 LEVEL UP! You reached Level {new_level} by viewing the overview!")
                    except Exception as gamify_error:
                        logger.error(f"Error logging gamification for overview command: {gamify_error}", exc_info=True)

        except Exception as e:
            logger.error(f"Error processing /overview command with args: {e}", exc_info=True)
            await update.message.reply_text("An error occurred while processing the overview command.")

async def budget_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    now = datetime.now()
    
    try:
        sheet = service.spreadsheets()
        
        # Get sheets metadata to check available sheets
        try:
            sheet_metadata = sheet.get(spreadsheetId=SPREADSHEET_ID).execute()
            available_sheets = [s.get('properties', {}).get('title') for s in sheet_metadata.get('sheets', [])]
            logger.info(f"Available sheets for budget status: {available_sheets}")
        except Exception as e:
            logger.error(f"Error fetching sheet metadata: {e}", exc_info=True)
            await update.message.reply_text("Error retrieving available sheets.")
            return

        # Try both formats: 'MMYY' and 'Month YYYY'
        sheet_name_mmyy = now.strftime('%m%y')  # Format: 0423 for April 2023
        sheet_name_month_year = now.strftime('%B %Y')  # Format: April 2023
        
        # Determine which sheet name to use
        sheet_name = None
        if sheet_name_mmyy in available_sheets:
            sheet_name = sheet_name_mmyy
            logger.info(f"Using sheet format 'MMYY': {sheet_name}")
        elif sheet_name_month_year in available_sheets:
            sheet_name = sheet_name_month_year
            logger.info(f"Using sheet format 'Month YYYY': {sheet_name}")
        else:
            # Try looking for other month patterns to determine format used
            for sheet_title in available_sheets:
                # Check if any existing sheet matches MMYY format (e.g., 0123, 0223, etc.)
                if re.match(r'^(0[1-9]|1[0-2])\d{2}$', sheet_title):
                    # If found, use current month in same format
                    sheet_name = sheet_name_mmyy
                    logger.info(f"Found MMYY pattern, using: {sheet_name}")
                    break
                # Check if any existing sheet matches Month YYYY format (e.g., January 2023)
                elif re.match(r'^[A-Z][a-z]+ \d{4}$', sheet_title):
                    # If found, use current month in same format
                    sheet_name = sheet_name_month_year
                    logger.info(f"Found Month YYYY pattern, using: {sheet_name}")
                    break
            
            if not sheet_name:
                logger.warning(f"No sheet found for the current month in any format. Available sheets: {available_sheets}")
                await update.message.reply_text(f"No sheet found for {now.strftime('%B %Y')} in any format.")
                return
        
        result = sheet.values().get(spreadsheetId=SPREADSHEET_ID,
                                  range=f"{sheet_name}!A:D").execute()
        values = result.get("values", [])
        
        if not values:
            await update.message.reply_text("No data found in the sheet.")
            return
            
        summary_text = f"💰 Budget Status ({now.strftime('%B %Y')}):\n\n"
        
        # Find the important rows
        total_income = None
        total_expenses = None
        budget = None
        
        for row in values:
            if len(row) >= 3:
                category = row[0].strip()
                if category == "Total Income":
                    total_income = float(row[2]) if row[2] else 0
                elif category == "Total Expenses":
                    total_expenses = float(row[2]) if row[2] else 0
                elif category == "Budget":
                    budget = float(row[2]) if row[2] else 0
        
        # Add summary information
        if total_income is not None:
            summary_text += f"💰 Total Income: {total_income:.2f}\n"
        if total_expenses is not None:
            summary_text += f"💸 Total Expenses: {total_expenses:.2f}\n"
        if budget is not None:
            summary_text += f"📈 Budget: {budget:.2f}\n"
        
        # Calculate and add balance
        if total_income is not None and total_expenses is not None:
            balance = total_income - total_expenses
            summary_text += f"\n💵 Balance: {balance:.2f}"
            
            # Add status indicators
            if balance >= 0:
                summary_text += " ✅ (Within Budget)"
            else:
                summary_text += " ⚠️ (Over Budget)"
        
        await update.message.reply_text(summary_text)
        
    except Exception as e:
        logger.error(f"Error getting budget status: {e}", exc_info=True)
        await update.message.reply_text("Error retrieving budget status.")

async def check_sheet_structure(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Check and display the structure of the Google Sheet."""
    now = datetime.now()
    sheet_name = now.strftime("%m%y")
    
    try:
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=SPREADSHEET_ID,
                                  range=f"{sheet_name}!A:D").execute()
        values = result.get("values", [])
        
        if not values:
            await update.message.reply_text("No data found in the sheet.")
            return
            
        # Get the header row
        header_row = values[0] if values else []
        
        # Get a sample of the data
        sample_data = values[1:6] if len(values) > 6 else values[1:]
        
        structure_text = "📊 Sheet Structure:\n\n"
        structure_text += f"Sheet Name: {sheet_name}\n"
        structure_text += f"Columns: {', '.join(header_row)}\n\n"
        structure_text += "Sample Data:\n"
        
        for row in sample_data:
            if len(row) >= 2:  # Make sure we have at least category and budget
                category = row[0] if row[0] else "N/A"
                budget = row[1] if len(row) > 1 and row[1] else "N/A"
                actual = row[2] if len(row) > 2 and row[2] else "N/A"
                structure_text += f"• {category}: Budget={budget}, Actual={actual}\n"
        
        await update.message.reply_text(structure_text)
        
    except Exception as e:
        logger.error(f"Error checking sheet structure: {e}", exc_info=True)
        await update.message.reply_text("Error checking sheet structure.")

async def category_balance(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Check the available balance for a specific category."""
    if not context.args:
        await update.message.reply_text("Please specify a category. Example: /balance groceries")
        return
        
    # Join all arguments to handle categories with spaces
    category_query = " ".join(context.args).strip().lower()
    
    # Find the matching category using the category_map
    matched_category = None
    for category, keywords in category_map.items():
        if category_query in [kw.lower() for kw in keywords]:
            matched_category = category
            break
    
    if not matched_category:
        await update.message.reply_text(f"Category not found. Try /categories to see available categories.")
        return
    
    now = datetime.now()
    sheet_name = now.strftime("%m%y")
    
    try:
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=SPREADSHEET_ID,
                                  range=f"{sheet_name}!A:D").execute()
        values = result.get("values", [])
        
        # Find the row with the matching category
        category_row = None
        for i, row in enumerate(values):
            if len(row) > 0 and row[0].strip().lower() == matched_category.lower():
                category_row = i
                break
        
        if category_row is None:
            await update.message.reply_text(f"Category '{matched_category}' not found in the sheet.")
            return
        
        # Get the balance from column D
        balance = float(values[category_row][3]) if len(values[category_row]) > 3 and values[category_row][3] else 0
        
        await update.message.reply_text(
            f"💰 Available Balance for {matched_category}: {balance:.2f}"
        )
        
    except Exception as e:
        logger.error(f"Error getting category balance: {e}", exc_info=True)
        await update.message.reply_text("Error retrieving category balance.")

async def delete_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the /delete command to delete the latest expense."""
    global expense_history
    
    try:
        # Get the message object - check if we're dealing with a callback query
        # If it is a CallbackQuery object directly (from button_callback)
        if hasattr(update, 'message') and update.message:
            message = update.message
            user_id = update.message.from_user.id
        elif hasattr(update, 'callback_query') and update.callback_query:
            message = update.callback_query.message
            user_id = update.callback_query.from_user.id
        elif isinstance(update, telegram.CallbackQuery):
            # Direct CallbackQuery object from button handler
            message = update.message
            user_id = update.from_user.id
        else:
            logger.error(f"Unknown update type in delete_command: {type(update)}")
            return

        logger.info(f"Delete command triggered. Expense history length: {len(expense_history)}")
        
        if not expense_history:
            await message.reply_text("No recent expenses found to delete.")
            return

        # Get the most recent expense entry
        expense = expense_history.pop()
        
        logger.info(f"Attempting to delete expense: {expense}")
        
        # Extract relevant information
        category = expense["category"]
        amount = expense["amount"]
        row = expense["row"]
        sheet_name = expense["sheet_name"]
        sheet_id = expense["sheet_id"]
        timestamp = expense["timestamp"]
        
        sheet = service.spreadsheets()
        
        # 1. Update the amount in the cell (subtract the deleted expense)
        try:
            # Get current amount
            result = sheet.values().get(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{sheet_name}!C{row}"
            ).execute()
            
            current_amount = float(result.get("values", [[0]])[0][0].replace('₪', '').replace(',', '').strip() or 0) if result.get("values") else 0
            new_amount = max(0, current_amount - amount)
            
            logger.info(f"Updating amount from {current_amount} to {new_amount}")
            
            # Update amount
            sheet.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{sheet_name}!C{row}",
                valueInputOption="RAW",
                body={"values": [[new_amount]]}
            ).execute()
            
        except Exception as e:
            logger.error(f"Error updating amount: {str(e)}", exc_info=True)
            await message.reply_text("❌ Failed to update amount. Please try again.")
            return
            
        # 2. Remove the entry from the note
        try:
            # Get current note
            cell_range = f"{sheet_name}!C{row}"
            notes_result = sheet.get(
                spreadsheetId=SPREADSHEET_ID,
                ranges=[cell_range],
                fields="sheets(data(rowData(values(note))))"
            ).execute()
            
            current_note = ""
            if (notes_result.get("sheets") and
                notes_result["sheets"][0].get("data") and
                notes_result["sheets"][0]["data"][0].get("rowData") and
                notes_result["sheets"][0]["data"][0]["rowData"][0].get("values")):
                current_note = notes_result["sheets"][0]["data"][0]["rowData"][0]["values"][0].get("note", "")
                
            logger.info(f"Current note content length: {len(current_note)}")
            
            # Find and remove the specific entry with matching timestamp
            entries = current_note.split('\n')
            new_entries = []
            removed_entry = False
            
            for entry in entries:
                if timestamp in entry:  # This is the entry to remove
                    logger.info(f"Found entry to remove: {entry}")
                    removed_entry = True
                else:
                    new_entries.append(entry)
                    
            if not removed_entry:
                logger.warning(f"Entry with timestamp {timestamp} not found in note")
                
            new_note = '\n'.join(new_entries)
            
            # Update the note
            request = {
                "requests": [{
                    "updateCells": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": row - 1,
                            "endRowIndex": row,
                            "startColumnIndex": 2,  # Column C
                            "endColumnIndex": 3
                        },
                        "rows": [{
                            "values": [{
                                "note": new_note
                            }]
                        }],
                        "fields": "note"
                    }
                }]
            }
            
            sheet.batchUpdate(
                spreadsheetId=SPREADSHEET_ID,
                body=request
            ).execute()
            
            logger.info("Successfully updated note")
            
        except Exception as e:
            logger.error(f"Error updating note: {str(e)}", exc_info=True)
            await message.reply_text("❌ Failed to update note. Amount was updated but note wasn't modified.")
            return

        # Remove gamification code that was causing the error
        # and just send the success message directly
        
        await message.reply_text(
            f"✅ Successfully deleted expense:\n"
            f"Amount: ₪{amount:.2f}\n"
            f"Category: {category}\n"
            f"Timestamp: {timestamp}"
        )
            
    except Exception as e:
        logger.error(f"Error in delete_command: {str(e)}", exc_info=True)
        await message.reply_text("❌ An error occurred while deleting the expense. Please try again.")

async def mapping_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show keywords for a specific category"""
    if not context.args:
        await update.message.reply_text("Please specify a category. Example: /mapping groceries")
        return
        
    # Join all arguments to handle categories with spaces
    search_query = " ".join(context.args).strip().lower()
    
    # First try to match the exact category name
    exact_match = None
    for category, keywords in category_map.items():
        if category.lower() == search_query:
            exact_match = category
            break
    
    # If no exact match, try to match using keywords
    if not exact_match:
        for category, keywords in category_map.items():
            if search_query in [kw.lower() for kw in keywords]:
                exact_match = category
                break
    
    if not exact_match:
        await update.message.reply_text(
            f"Category '{search_query}' not found. Use /categories to see all available categories."
        )
        return
    
    # Format the keywords for display
    keywords_text = f"🔍 Keywords for '{exact_match}':\n\n"
    
    # Get all keywords for the found category
    keywords = category_map[exact_match]
    keywords_text += "\n".join([f"• {keyword}" for keyword in keywords])
    
    await update.message.reply_text(keywords_text)

async def generate_expense_chart(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Generate a pie chart of expenses by category"""
    try:
        now = datetime.now()
        
        # Get the month from arguments, if provided
        if context.args and len(context.args) > 0:
            # Get the month name from the argument
            month_name = context.args[0].lower().capitalize()
            
            # Map month name to number
            month_map = {
                "January": "01", "February": "02", "March": "03", "April": "04",
                "May": "05", "June": "06", "July": "07", "August": "08",
                "September": "09", "October": "10", "November": "11", "December": "12",
                # Add abbreviations
                "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04", "Jun": "06", 
                "Jul": "07", "Aug": "08", "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12"
            }
            
            if month_name not in month_map:
                await update.message.reply_text(f"Invalid month name: {month_name}. Please use a valid month name like 'January' or 'Jan'.")
                return
                
            month_number = month_map[month_name]
            sheet_name = f"{month_number}{now.strftime('%y')}"
            month_text = month_name
        else:
            sheet_name = now.strftime("%m%y")
            month_text = now.strftime("%B")
        
        # Check if the sheet exists
        sheet = service.spreadsheets()
        sheet_metadata = sheet.get(spreadsheetId=SPREADSHEET_ID).execute()
        available_sheets = [s.get("properties", {}).get("title") for s in sheet_metadata.get("sheets", [])]
        
        if sheet_name not in available_sheets:
            await update.message.reply_text(f"Data for {month_text} {now.strftime('%Y')} is not available.")
            return
        
        # Send a message to indicate that the chart is being generated
        message = await update.message.reply_text(f"Generating expense chart for {month_text} {now.strftime('%Y')}... This may take a moment.")
        
        # Get all values from the sheet
        result = sheet.values().get(spreadsheetId=SPREADSHEET_ID,
                                  range=f"{sheet_name}!A:C").execute()
        values = result.get("values", [])
        
        if not values:
            await message.edit_text(f"No data found for {month_text} {now.strftime('%Y')}.")
            return
        
        # Collect expense data for pie chart
        categories = []
        amounts = []
        
        # Skip header row
        for row in values[1:]:
            if len(row) >= 3 and row[0] and row[2]:
                try:
                    # Check if this is a category row (not a total or header)
                    # Skip rows that contain 'Total' or are empty
                    if not any(keyword in row[0] for keyword in ['Total', 'Budget', 'Income']):
                        category = row[0]
                        actual_amount = float(row[2]) if row[2] else 0
                        
                        # Only include categories with non-zero amounts
                        if actual_amount > 0:
                            categories.append(category)
                            amounts.append(actual_amount)
                except (ValueError, IndexError):
                    # Skip rows with invalid data
                    continue
        
        if not categories:
            await message.edit_text(f"No expense data found for {month_text} {now.strftime('%Y')}.")
            return
        
        # Generate pie chart
        plt.figure(figsize=(10, 7))
        
        # Create beautiful pie chart with better colors and spacing
        colors = plt.cm.viridis(range(len(categories)))
        wedges, texts, autotexts = plt.pie(
            amounts, 
            labels=None,  # We'll add custom legend instead
            autopct='%1.1f%%', 
            startangle=90, 
            shadow=False, 
            colors=colors,
            wedgeprops={'linewidth': 1, 'edgecolor': 'white'},
            textprops={'fontsize': 12, 'fontweight': 'bold', 'color': 'white'}
        )
        
        # Add a circle at the center to make it a donut chart
        plt.gca().add_artist(plt.Circle((0, 0), 0.3, fc='white'))
        
        # Add title and legend
        plt.title(f'Expenses by Category - {month_text} {now.strftime("%Y")}', fontsize=16, fontweight='bold')
        plt.legend(
            wedges, 
            [f"{cat} (₪{amt:.0f})" for cat, amt in zip(categories, amounts)],
            title="Categories",
            loc="center left",
            bbox_to_anchor=(1, 0, 0.5, 1)
        )
        
        plt.tight_layout()
        
        # Save the chart to a temporary file
        with NamedTemporaryFile(suffix='.png', delete=False) as temp_file:
            plt.savefig(temp_file.name, format='png', dpi=100, bbox_inches='tight')
            temp_file_name = temp_file.name
        
        plt.close()  # Close the plot to free memory
        
        # Send the chart
        with open(temp_file_name, 'rb') as photo:
            await message.delete()  # Delete the "generating" message
            await update.message.reply_photo(
                photo=photo,
                caption=f"📊 Expense Distribution for {month_text} {now.strftime('%Y')}\nTotal: ₪{sum(amounts):.2f}"
            )
        
        # Delete the temporary file
        try:
            os.unlink(temp_file_name)
        except Exception as e:
            logger.error(f"Error deleting temporary file: {str(e)}")
            
    except Exception as e:
        logger.error(f"Error generating chart: {str(e)}", exc_info=True)
        await update.message.reply_text("Error generating the expense chart. Please try again later.")

def check_budget_thresholds(category, sheet_name, user_id):
    """
    Check if a category has crossed any budget thresholds.
    Thresholds are: 75%, 90%, 100%, and every 10% over budget.
    Returns a notification message if a threshold was crossed, None otherwise.
    """
    try:
        # Get the spreadsheet
        sheet = service.spreadsheets()
        
        # Find the category row in the sheet
        result = sheet.values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{sheet_name}!A:D"
        ).execute()
        
        values = result.get("values", [])
        
        # Find the row with the matching category
        category_row = None
        for i, row in enumerate(values):
            if len(row) > 0 and row[0].strip().lower() == category.lower():
                category_row = i + 1  # Google Sheets is 1-indexed
                break
        
        if not category_row:
            logger.warning(f"Category '{category}' not found in sheet for threshold checking")
            return None
        
        # Get budget and actual values
        row_data = values[category_row - 1]
        budget = float(row_data[1]) if len(row_data) > 1 and row_data[1] else 0
        actual = float(row_data[2]) if len(row_data) > 2 and row_data[2] else 0
        
        # Skip if budget is zero or negative (can't calculate percentage)
        if budget <= 0:
            return None
        
        # Calculate what percentage of the budget has been used
        percentage_used = (actual / budget) * 100
        
        # Store notification history in user_data if not already there
        user_key = f"notified_thresholds_{sheet_name}"
        
        # Get or initialize the notification history
        notification_history = get_user_data(user_id, user_key, {})
        
        # Initialize category in history if not present
        if category not in notification_history:
            notification_history[category] = []
        
        # Define the thresholds to check
        thresholds = [75, 90, 100]
        
        # Add thresholds for over budget (110%, 120%, etc.)
        if percentage_used > 100:
            over_budget_threshold = int((percentage_used // 10) * 10)
            for t in range(110, over_budget_threshold + 10, 10):
                if t not in thresholds:
                    thresholds.append(t)
        
        # Check if we've crossed any thresholds that we haven't notified about
        crossed_threshold = None
        for threshold in thresholds:
            if percentage_used >= threshold and threshold not in notification_history[category]:
                notification_history[category].append(threshold)
                crossed_threshold = threshold
                # Break at the highest threshold crossed
                break
        
        # Save updated notification history
        save_user_data(user_id, user_key, notification_history)
        
        # Generate notification if a threshold was crossed
        if crossed_threshold:
            if crossed_threshold < 100:
                emoji = "⚠️"
                message = f"{emoji} Budget Alert: You've used {crossed_threshold}% of your {category} budget for this month.\n"
                message += f"Budget: ₪{budget:.2f}\nSpent: ₪{actual:.2f}"
                return message
            elif crossed_threshold == 100:
                emoji = "🚨"
                message = f"{emoji} Budget Limit Reached: You've reached 100% of your {category} budget for this month.\n"
                message += f"Budget: ₪{budget:.2f}\nSpent: ₪{actual:.2f}"
                return message
            else:
                emoji = "😱"
                over_percentage = crossed_threshold - 100
                message = f"{emoji} Over Budget Alert: You're now {over_percentage}% over your {category} budget for this month.\n"
                message += f"Budget: ₪{budget:.2f}\nSpent: ₪{actual:.2f}\nOver by: ₪{actual-budget:.2f}"
                return message
        
        return None
        
    except Exception as e:
        logger.error(f"Error checking budget thresholds: {str(e)}", exc_info=True)
        return None

def get_user_data(user_id, key, default=None):
    """
    Get user-specific data from a JSON file.
    Creates the file if it doesn't exist.
    """
    try:
        user_id = str(user_id)  # Ensure user_id is a string
        filename = f"user_data_{user_id}.json"
        
        # Create file if it doesn't exist
        if not os.path.exists(filename):
            with open(filename, 'w') as f:
                json.dump({}, f)
            return default
        
        # Read the data
        with open(filename, 'r') as f:
            data = json.load(f)
        
        return data.get(key, default)
    except Exception as e:
        logger.error(f"Error getting user data: {str(e)}", exc_info=True)
        return default

def save_user_data(user_id, key, value):
    """
    Save user-specific data to a JSON file.
    Ensures all data is JSON serializable.
    """
    try:
        user_id = str(user_id)  # Ensure user_id is a string
        filename = f"user_data_{user_id}.json"
        
        # Create or read the existing data
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                data = json.load(f)
        else:
            data = {}
        
        # Convert any non-JSON serializable types
        value = make_json_serializable(value)
        
        # Update the data
        data[key] = value
        
        # Save back to file
        with open(filename, 'w') as f:
            json.dump(data, f)
            
        return True
    except Exception as e:
        logger.error(f"Error saving user data: {str(e)}", exc_info=True)
        return False

def make_json_serializable(obj):
    """Convert Python objects to JSON serializable types"""
    if isinstance(obj, set):
        return list(obj)
    elif isinstance(obj, dict):
        return {k: make_json_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [make_json_serializable(item) for item in obj]
    elif hasattr(obj, '__dict__'):  # Handle custom objects
        return make_json_serializable(obj.__dict__)
    else:
        return obj


async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show gamification stats"""
    try:
        logger.info("Stats command triggered")
        user_id = update.effective_user.id
        logger.info(f"Processing stats for user: {user_id}")
        
        # Create basic stats message
        stats_message = f"🎮 Your Expense Tracker Stats 🎮\n\n"
        
        try:
            # Try to directly open and read the user's gamification data file
            filename = f"user_gamification_{user_id}.json"
            
            if os.path.exists(filename):
                # The file exists, so read it directly
                try:
                    with open(filename, 'r') as f:
                        user_data = json.load(f)
                    
                    logger.info(f"Successfully read user data from {filename}")
                    
                    # Extract basic stats that should always be there
                    xp = user_data.get("xp", 0)
                    current_streak = user_data.get("current_streak", 0)
                    longest_streak = user_data.get("longest_streak", 0)
                    total_expenses = user_data.get("total_expenses_logged", 0)
                    
                    # Calculate level manually
                    level = 1
                    for i, threshold in enumerate(gamification.LEVEL_THRESHOLDS):
                        if xp >= threshold:
                            level = i + 1
                    
                    # Calculate XP to next level
                    next_level_xp = gamification.LEVEL_THRESHOLDS[-1]  # Default to max level
                    for threshold in gamification.LEVEL_THRESHOLDS:
                        if threshold > xp:
                            next_level_xp = threshold
                            break
                    
                    xp_needed = next_level_xp - xp
                    
                    # Count achievements
                    achievement_count = 0
                    achievements = user_data.get("achievements_unlocked", {})
                    if isinstance(achievements, dict):
                        for achievement_id, levels in achievements.items():
                            if isinstance(levels, list):
                                achievement_count += len(levels)
                            elif isinstance(levels, dict):
                                achievement_count += len(levels)
                    
                    # Format a nice message with what we have
                    stats_message += f"👤 Level {level}\n"
                    stats_message += f"✨ XP: {xp} (Need {xp_needed} more for next level)\n\n"
                    
                    stats_message += f"🔥 Current Streak: {current_streak} days\n"
                    stats_message += f"🏆 Longest Streak: {longest_streak} days\n"
                    stats_message += f"❄️ Streak Freezes: {user_data.get('streak_freezes', 0)}\n\n"
                    
                    stats_message += f"📝 Total Expenses Logged: {total_expenses}\n"
                    
                    # Handle unique categories, which might be a list or a set
                    unique_cats = user_data.get("unique_categories_used", [])
                    if isinstance(unique_cats, list):
                        unique_count = len(unique_cats)
                    elif isinstance(unique_cats, set):
                        unique_count = len(unique_cats)
                    else:
                        unique_count = 0
                    
                    stats_message += f"🧭 Unique Categories Used: {unique_count}\n"
                    stats_message += f"📊 Reports Viewed: {user_data.get('reports_viewed', 0)}\n"
                    stats_message += f"💰 Months Under Budget: {user_data.get('months_under_budget', 0)}\n"
                    stats_message += f"🏅 Achievements Unlocked: {achievement_count}\n"
                    
                    # Also log this view if possible
                    try:
                        # Try to initialize gamification system just for logging the view
                        gs = GamificationSystem(user_id)
                        gs.log_report_view('stats')
                        logger.info("Report view logged successfully")
                    except Exception as report_error:
                        logger.error(f"Error logging report view: {str(report_error)}")
                
                except Exception as read_error:
                    logger.error(f"Error reading user file: {str(read_error)}", exc_info=True)
                    stats_message += "You haven't earned any gamification data yet.\n"
                    stats_message += "Start logging expenses to earn XP and unlock achievements!"
            else:
                # File doesn't exist - new user
                logger.info(f"No gamification data file found for user {user_id}")
                stats_message += "You're just getting started! Log some expenses to begin earning XP.\n\n"
                stats_message += "• Each logged expense earns XP\n"
                stats_message += "• Track daily to build streaks\n"
                stats_message += "• Complete challenges to level up faster\n"
                stats_message += "• View reports to discover insights"
            
            # Send the message
            await update.message.reply_text(stats_message)
            logger.info("Stats message sent successfully")
            
        except Exception as e:
            logger.error(f"Error generating stats: {str(e)}", exc_info=True)
            # Fallback message that still works
            fallback_message = "Your expense tracker includes a gamification system!\n\n"
            fallback_message += "Keep logging expenses to earn XP and unlock achievements."
            await update.message.reply_text(fallback_message)
    
    except Exception as outer_e:
        logger.error(f"Critical error in stats_command: {str(outer_e)}", exc_info=True)
        try:
            await update.message.reply_text("Stats tracking is available. Try again later!")
        except:
            pass

async def achievements_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show achievements"""
    try:
        if not gamification:
            await update.message.reply_text("Gamification features are not available.")
            return
            
        user_id = update.effective_user.id
        
        try:
            gs = GamificationSystem(user_id)
            achievements = gs.get_achievements()
            
            achievements_message = "🏆 YOUR ACHIEVEMENTS 🏆\n\n"
            
            # Process achievements safely
            for achievement in achievements:
                # Add achievement name
                achievements_message += f"{achievement['name']}\n"
                
                # Process levels
                for level in achievement.get('levels', []):
                    try:
                        # Check if unlocked safely
                        if level.get('unlocked', False):
                            achievements_message += f"✅ {level.get('emoji', '🏆')} {level.get('description', 'Achievement level')}\n"
                        else:
                            achievements_message += f"⬜ {level.get('emoji', '🏆')} {level.get('description', 'Achievement level')}\n"
                    except Exception as level_error:
                        logger.error(f"Error processing achievement level: {str(level_error)}")
                        # Skip this level but continue with others
                        continue
                
                achievements_message += "\n"
                
            # Check if the message is too long for Telegram
            if len(achievements_message) > 4000:
                achievements_message = achievements_message[:3950] + "...\n\nMessage too long to display all achievements."
                
            await update.message.reply_text(achievements_message)
            
            # Record this as a report view for gamification
            try:
                gs.log_report_view('achievements')
            except Exception as report_error:
                logger.error(f"Error logging report view: {str(report_error)}")
            
        except Exception as e:
            logger.error(f"Error processing achievements: {str(e)}", exc_info=True)
            if ">=" in str(e) and "set" in str(e) and "int" in str(e):
                # Handle the specific comparison error
                await update.message.reply_text("There was an error calculating your achievements progress. Your achievements are still being tracked correctly.")
            else:
                await update.message.reply_text(f"Error retrieving achievements: {str(e)}")
            
    except Exception as e:
        logger.error(f"Error in achievements_command: {str(e)}", exc_info=True)
        await update.message.reply_text("Sorry, there was an error processing your achievements command. Please try again later.")

async def challenge_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show current challenge or get a new one"""
    try:
        if not gamification:
            await update.message.reply_text("Gamification features are not available.")
            return
            
        user_id = update.effective_user.id
        
        try:
            gs = GamificationSystem(user_id)
            
            # Check if the user wants a new challenge
            new_challenge_requested = False
            if context.args and context.args[0].lower() == "new":
                new_challenge_requested = True
                current_challenge = gs._assign_new_challenge()
            else:
                stats = gs.get_user_stats()
                current_challenge = stats.get('current_challenge')
            
            if current_challenge:
                emoji = current_challenge['data']['emoji']
                challenge_message = f"🎯 *YOUR WEEKLY CHALLENGE* 🎯\n\n"
                challenge_message += f"{emoji} *{current_challenge['description']}*\n\n"
                
                # Show completion status
                if current_challenge.get('completed', False):
                    if current_challenge.get('success', False):
                        challenge_message += "✅ Challenge completed! Congratulations!\n"
                        challenge_message += f"You earned {current_challenge['data']['xp_reward']} XP!\n\n"
                    else:
                        challenge_message += "❌ Challenge failed. Don't worry, try again next week!\n\n"
                else:
                    challenge_message += f"📅 Ends on: {current_challenge['end_date']}\n"
                    challenge_message += f"🎁 Reward: {current_challenge['data']['xp_reward']} XP\n\n"
                    
                    # Show progress if available
                    if current_challenge['data']['type'] == 'category_under_budget' and 'current_spending' in current_challenge:
                        challenge_message += f"📊 Current spending: ₪{current_challenge['current_spending']:.2f}\n\n"
                    elif current_challenge['data']['type'] == 'use_features' and 'features_used' in current_challenge:
                        used = current_challenge.get('features_used', [])
                        all_features = current_challenge['data']['features']
                        
                        challenge_message += "📊 Your progress:\n"
                        for feature in all_features:
                            check = "✅" if feature in used else "⬜"
                            feature_name = feature.capitalize()
                            challenge_message += f"{check} {feature_name}\n"
                        challenge_message += "\n"
                        
                if new_challenge_requested:
                    challenge_message += "You've requested a new challenge. Good luck!\n"
                else:
                    challenge_message += "Use `/challenge new` to get a new challenge (cancels the current one).\n"
                    
                await update.message.reply_text(challenge_message, parse_mode='Markdown')
            else:
                await update.message.reply_text("No active challenge found. Use `/challenge new` to get a new challenge.")
                
            # Record this as a report view for gamification
            gs.log_report_view('challenge')
            
        except Exception as e:
            logger.error(f"Error processing challenge: {str(e)}", exc_info=True)
            await update.message.reply_text(f"Error with challenge: {str(e)}")
            
    except Exception as e:
        logger.error(f"Error in challenge_command: {str(e)}", exc_info=True)
        await update.message.reply_text("Sorry, there was an error processing your challenge command. Please try again later.")

async def buy_freeze_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Buy a streak freeze"""
    try:
        if not gamification:
            await update.message.reply_text("Gamification features are not available.")
            return
            
        user_id = update.effective_user.id
        
        try:
            gs = GamificationSystem(user_id)
            
            # Try to buy a streak freeze
            success, freeze_count = gs.buy_streak_freeze()
            
            if success:
                await update.message.reply_text(
                    f"✅ You bought a streak freeze for {gamification.STREAK_FREEZE_COST} XP!\n"
                    f"You now have {freeze_count} streak freezes.\n\n"
                    f"They will be used automatically if you miss a day."
                )
            else:
                # Get user stats to show current XP
                stats = gs.get_user_stats()
                
                await update.message.reply_text(
                    f"❌ Not enough XP to buy a streak freeze!\n"
                    f"Cost: {gamification.STREAK_FREEZE_COST} XP\n"
                    f"Your XP: {stats['xp']} XP\n\n"
                    f"Keep logging expenses to earn more XP!"
                )
                
        except Exception as e:
            logger.error(f"Error buying streak freeze: {str(e)}", exc_info=True)
            await update.message.reply_text(f"Error buying streak freeze: {str(e)}")
            
    except Exception as e:
        logger.error(f"Error in buy_freeze_command: {str(e)}", exc_info=True)
        await update.message.reply_text("Sorry, there was an error processing your buy freeze command. Please try again later.")

def get_sheet_name_for_current_month():
    """
    Utility function to determine the correct sheet name format for the current month.
    Checks available sheets to see if they use MMYY or Month YYYY format.
    Returns the appropriate sheet name for the current month.
    """
    try:
        now = datetime.now()
        
        # Get sheets metadata
        sheet_metadata = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
        available_sheets = [s.get('properties', {}).get('title') for s in sheet_metadata.get('sheets', [])]
        
        # Try both formats
        sheet_name_mmyy = now.strftime('%m%y')  # Format: 0423 for April 2023
        sheet_name_month_year = now.strftime('%B %Y')  # Format: April 2023
        
        # Check if either format exists
        if sheet_name_mmyy in available_sheets:
            logger.info(f"Using sheet name format MMYY: {sheet_name_mmyy}")
            return sheet_name_mmyy
            
        if sheet_name_month_year in available_sheets:
            logger.info(f"Using sheet name format Month YYYY: {sheet_name_month_year}")
            return sheet_name_month_year
            
        # Try to determine format from existing sheets
        for sheet_title in available_sheets:
            if re.match(r'^(0[1-9]|1[0-2])\d{2}$', sheet_title):
                logger.info(f"Found MMYY pattern in sheets, using: {sheet_name_mmyy}")
                return sheet_name_mmyy
                
            if re.match(r'^[A-Z][a-z]+ \d{4}$', sheet_title):
                logger.info(f"Found Month YYYY pattern in sheets, using: {sheet_name_month_year}")
                return sheet_name_month_year
                
        # Default to MMYY if can't determine
        logger.warning(f"Could not determine sheet format. Defaulting to MMYY: {sheet_name_mmyy}")
        return sheet_name_mmyy
        
    except Exception as e:
        logger.error(f"Error determining sheet name: {e}", exc_info=True)
        # Default to MMYY format if there's an error
        return datetime.now().strftime('%m%y')

def get_last_row_with_data():
    """Get the last row that contains data in the sheet"""
    try:
        # Get appropriate sheet name for current month
        sheet_name = get_sheet_name_for_current_month()
        
        sheet = service.spreadsheets()
        result = sheet.values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{sheet_name}!A:D"
        ).execute()
        
        values = result.get("values", [])
        if not values:
            return None
            
        # Find the last row with data
        for i in range(len(values) - 1, -1, -1):
            if any(values[i]):  # Check if row has any non-empty cells
                return i + 1  # Google Sheets is 1-indexed
                
        return None
    except Exception as e:
        logger.error(f"Error getting last row: {str(e)}")
        return None

def reset_telegram_api_sessions():
    """
    Reset any stale Telegram API sessions by directly calling the API.
    This helps work around the "terminated by other getUpdates request" errors.
    """
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not bot_token:
        logger.warning("No bot token available, skipping session reset")
        return False
        
    try:
        from urllib.request import urlopen, Request
        import urllib.error
        import json
        import time
        
        # First make a getUpdates call with a negative offset to reset
        reset_url = f"https://api.telegram.org/bot{bot_token}/getUpdates?offset=-1&limit=1&timeout=1"
        logger.info("Attempting to reset Telegram API sessions")
        
        req = Request(reset_url)
        response = urlopen(req, timeout=5).read().decode('utf-8')
        
        # Now make a deleteWebhook call to be sure
        delete_webhook_url = f"https://api.telegram.org/bot{bot_token}/deleteWebhook"
        req = Request(delete_webhook_url)
        response = urlopen(req, timeout=5).read().decode('utf-8')
        
        # Sleep to ensure changes take effect
        time.sleep(2)
        return True
    except Exception as e:
        logger.error(f"Error resetting Telegram API sessions: {str(e)}")
        return False


async def main():
    # Check for running instances using a lock file
    lock_file = "bot_instance.lock"
    
    try:
        # Check if lock file exists
        if os.path.exists(lock_file):
            # Check if the process is still running
            try:
                with open(lock_file, 'r') as f:
                    pid = int(f.read().strip())
                
                # Check if process with this PID exists
                try:
                    # For UNIX systems, sending signal 0 just checks if process exists
                    os.kill(pid, 0)
                    logger.error(f"Another bot instance is already running with PID {pid}. Exiting.")
                    print(f"ERROR: Another bot instance is already running with PID {pid}. Exiting.")
                    return
                except OSError:
                    # Process not found, we can continue and overwrite the lock file
                    logger.warning(f"Stale lock file found with PID {pid}. Process is not running. Continuing.")
            except Exception as e:
                logger.warning(f"Invalid lock file format: {e}. Continuing.")
        
        # Create new lock file with current process ID
        with open(lock_file, 'w') as f:
            f.write(str(os.getpid()))
        
        logger.info(f"Created lock file with PID {os.getpid()}")

        # Initialize bot
        bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
        if not bot_token:
            raise ValueError("Environment variable TELEGRAM_BOT_TOKEN is not set.")

        # Ensure expense_history is initialized
        global expense_history
        expense_history = []
        logger.info("Initialized expense history")
        
        application = Application.builder().token(bot_token).build()
        
        # Register command handlers
        logger.info("Registering command handlers")
        
        # Basic commands - removed /start as requested
        application.add_handler(CommandHandler("help", help_command))
        
        # Reporting commands
        application.add_handler(CommandHandler("categories", categories_command))
        application.add_handler(CommandHandler("category", category_summary))
        # application.add_handler(CommandHandler("summary", show_summary))
        application.add_handler(CommandHandler("monthly", monthly_summary))
        application.add_handler(CommandHandler("budget", budget_status))
        application.add_handler(CommandHandler("balance", category_balance))
        application.add_handler(CommandHandler("delete", delete_command))
        application.add_handler(CommandHandler("mapping", mapping_command))
        application.add_handler(CommandHandler("chart", generate_expense_chart))
        application.add_handler(CommandHandler("reset_notifications", reset_notifications))
        application.add_handler(CommandHandler("overview", overview_command))
        
        # Gamification commands
        application.add_handler(CommandHandler("stats", stats_command))
        application.add_handler(CommandHandler("achievements", achievements_command))
        application.add_handler(CommandHandler("challenge", challenge_command))
        application.add_handler(CommandHandler("buyfreeze", buy_freeze_command))
        
        # Debug command for finding rogue instances
        application.add_handler(CommandHandler("debug_env", debug_env_command))
        application.add_handler(CommandHandler("trackbot", tracking_command))
        
        # Add button callback handler
        application.add_handler(CallbackQueryHandler(button_callback))
        
        # Add only one message handler for text messages
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

        logger.info("Command handlers registered successfully")
        logger.info("Starting services...")
        start_time = datetime.now()

        # Start web server
        web_server_task = asyncio.create_task(run_web_server())

        # Set longer timeout for better stability with Telegram API if possible
        try:
            if hasattr(application, 'bot') and application.bot and hasattr(application.bot, 'defaults'):
                application.bot.defaults.timeout = 30  # 30 seconds timeout instead of default
                logger.info("Set bot timeout to 30 seconds")
            else:
                logger.warning("Could not set bot timeout - bot or bot.defaults not initialized")
        except Exception as e:
            logger.warning(f"Error setting bot timeout: {e}")

        # Start polling with more robust error handling
        try:
            await application.initialize()
            await application.start()
            
            # Sleep briefly to ensure no conflicts with previous connections
            await asyncio.sleep(2)
            
            # Configure polling parameters based on what's supported
            polling_kwargs = {
                "allowed_updates": Update.ALL_TYPES,
            }
            
            # Add these parameters only if they're supported by the library version
            try:
                # Create a custom polling loop with error handling
                async def custom_polling():
                    """Custom polling implementation with better error handling for conflicts"""
                    offset = 0
                    error_count = 0
                    max_errors = 5
                    conflict_encountered = False
                    
                    while True:
                        try:
                            # If we had a conflict, wait longer before trying again
                            if conflict_encountered:
                                logger.warning("Attempting to recover from Telegram API conflict")
                                conflict_encountered = False
                                await asyncio.sleep(5)  # Wait longer after a conflict
                            
                            # Get updates with standard parameters - omit the unsupported parameter
                            updates = await application.bot.get_updates(
                                offset=offset,
                                limit=100,
                                timeout=20,
                                allowed_updates=Update.ALL_TYPES
                            )
                            
                            # Process updates if we got any
                            if updates:
                                for update in updates:
                                    asyncio.create_task(application.process_update(update))
                                    offset = update.update_id + 1
                                error_count = 0  # Reset error count on success
                            
                            # Brief pause to prevent excessive API usage
                            await asyncio.sleep(0.1)
                            
                        except telegram.error.Conflict as conflict_error:
                            conflict_encountered = True
                            logger.error(f"Telegram API conflict encountered: {conflict_error}")
                            error_count += 1
                            if error_count >= max_errors:
                                logger.critical(f"Too many conflicts ({error_count}), resetting session completely")
                                error_count = 0
                                offset = 0
                            await asyncio.sleep(5)  # Wait longer after a conflict
                            
                        except TypeError as type_error:
                            # Handle parameter errors by logging and trying simpler parameters
                            logger.error(f"Parameter error in get_updates: {type_error}")
                            try:
                                # Try with minimal parameters
                                updates = await application.bot.get_updates(offset=offset)
                                
                                # Process updates if we got any
                                if updates:
                                    for update in updates:
                                        asyncio.create_task(application.process_update(update))
                                        offset = update.update_id + 1
                                    error_count = 0  # Reset error count on success
                            except Exception as fallback_error:
                                logger.error(f"Even fallback polling failed: {fallback_error}")
                                error_count += 1
                            await asyncio.sleep(1)
                            
                        except Exception as e:
                            logger.error(f"Error in custom polling: {e}", exc_info=True)
                            error_count += 1
                            if error_count >= max_errors:
                                logger.critical(f"Too many errors ({error_count}), resetting")
                                error_count = 0
                            await asyncio.sleep(1)  # Wait a bit after an error
                
                # Create task for our custom polling
                update_task = asyncio.create_task(custom_polling())
                logger.info("Started custom polling with enhanced error handling")
                
            except Exception as e:
                logger.error(f"Error setting up custom polling, falling back to standard: {e}", exc_info=True)
                # Fallback - try standard polling
                try:
                    update_task = asyncio.create_task(
                        application.updater.start_polling(
                            **polling_kwargs,
                            timeout=20,
                            read_timeout=30
                        )
                    )
                    logger.info("Started standard polling with extended parameters")
                except TypeError:
                    # Fallback for older versions that might not support all parameters
                    update_task = asyncio.create_task(
                        application.updater.start_polling(**polling_kwargs)
                    )
                    logger.info("Started standard polling with basic parameters")
                
            logger.info("Bot is now running and ready to receive messages")
        except Exception as e:
            logger.error(f"Error starting application: {e}", exc_info=True)
            raise

        # Wait for tasks to complete (they won't since they're long-running)
        await asyncio.gather(web_server_task, update_task)
        
    except Exception as e:
        logger.error(f"Error in main: {e}", exc_info=True)
        raise
    finally:
        logger.info("Shutting down...")
        end_time = datetime.now()
        runtime = end_time - start_time
        logger.info(f"Bot runtime: {runtime}")
        
        # Clean up lock file on exit
        if os.path.exists(lock_file):
            try:
                os.remove(lock_file)
                logger.info(f"Removed lock file {lock_file}")
            except Exception as e:
                logger.error(f"Error removing lock file: {e}", exc_info=True)
                
        # Safely stop the application if it was started
        try:
            if 'application' in locals() and application:
                try:
                    await application.stop()
                    logger.info("Application stopped successfully")
                except RuntimeError as e:
                    # This is expected if the application wasn't running
                    logger.info(f"Application stop skipped: {e}")
                except Exception as e:
                    logger.error(f"Error stopping application: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"Error in shutdown cleanup: {e}", exc_info=True)

def delete_telegram_webhook():
    """
    Explicitly delete any existing webhook using HTTP request.
    This helps avoid conflicts with other bot instances.
    """
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not bot_token:
        logger.warning("No bot token available, skipping webhook deletion")
        return
        
    try:
        from urllib.request import urlopen, Request
        import urllib.error
        import json
        import time
        
        # Make a direct HTTP request to delete the webhook
        delete_webhook_url = f"https://api.telegram.org/bot{bot_token}/deleteWebhook"
        logger.info(f"Attempting to delete webhook using URL: {delete_webhook_url.replace(bot_token, 'REDACTED')}")
        
        req = Request(delete_webhook_url)
        response = urlopen(req, timeout=10).read().decode('utf-8')
        
        response_data = json.loads(response)
        if response_data.get('ok'):
            logger.info("Successfully deleted webhook via direct HTTP request")
        else:
            logger.warning(f"Webhook deletion response was not OK: {response_data}")
            
        # Sleep to ensure changes take effect
        time.sleep(2)
        return True
    except Exception as e:
        logger.error(f"Error deleting webhook via HTTP: {str(e)}")
        return False

async def debug_env_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Report environment info to help debug multiple instances"""
    try:
        import socket
        import platform
        import os
        import psutil
        from datetime import datetime
        
        env_info = []
        env_info.append("🔍 BOT ENVIRONMENT INFO 🔍")
        env_info.append(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # System info
        env_info.append("\n💻 System Info:")
        env_info.append(f"Hostname: {socket.gethostname()}")
        try:
            env_info.append(f"IP Address: {socket.gethostbyname(socket.gethostname())}")
        except:
            env_info.append(f"IP Address: Unable to determine")
        env_info.append(f"Platform: {platform.platform()}")
        env_info.append(f"Python Version: {platform.python_version()}")
        
        # Process info
        env_info.append("\n⚙️ Process Info:")
        pid = os.getpid()
        env_info.append(f"Process ID: {pid}")
        
        # Try to get process details with psutil
        try:
            process = psutil.Process(pid)
            create_time = datetime.fromtimestamp(process.create_time())
            env_info.append(f"Process Creation Time: {create_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Calculate uptime
            uptime = datetime.now() - create_time
            hours, remainder = divmod(uptime.total_seconds(), 3600)
            minutes, seconds = divmod(remainder, 60)
            env_info.append(f"Bot Uptime: {int(hours)}h {int(minutes)}m {int(seconds)}s")
            
            env_info.append(f"CPU Usage: {process.cpu_percent()}%")
            env_info.append(f"Memory Usage: {process.memory_info().rss / (1024 * 1024):.2f} MB")
        except Exception as e:
            env_info.append(f"Error getting process details: {str(e)}")
        
        # Environment variables (be careful not to expose sensitive info)
        env_info.append("\n🔐 Environment Variables:")
        
        safe_vars = ['RENDER', 'RENDER_SERVICE_NAME', 'RENDER_SERVICE_TYPE', 'PORT', 
                      'PYTHON_VERSION', 'PWD', 'HOME', 'USER', 'PYTHONPATH']
        
        for var in safe_vars:
            if var in os.environ:
                env_info.append(f"{var}: {os.environ.get(var)}")
        
        # Join all lines and send
        await update.message.reply_text("\n".join(env_info))
        logger.info(f"Environment debug info sent to {update.effective_user.id}")
        
    except Exception as e:
        logger.error(f"Error in debug_env command: {str(e)}", exc_info=True)
        await update.message.reply_text(f"Error gathering environment info: {str(e)}")

async def tracking_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Detailed tracking to find rogue instances"""
    try:
        import socket
        import platform
        import os
        import psutil
        import sys
        import uuid
        import requests
        from datetime import datetime
        
        # Generate a unique tracking ID for this report
        tracking_id = str(uuid.uuid4())
        
        # Start collecting information
        info = []
        info.append(f"🔍 TRACKING REPORT {tracking_id} 🔍")
        info.append(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # System info
        info.append("\n💻 System Info:")
        info.append(f"Hostname: {socket.gethostname()}")
        try:
            info.append(f"IP Address: {socket.gethostbyname(socket.gethostname())}")
            # Try to get public IP
            try:
                public_ip = requests.get('https://api.ipify.org', timeout=5).text
                info.append(f"Public IP: {public_ip}")
            except:
                info.append("Public IP: Unable to determine")
        except:
            info.append(f"IP Address: Unable to determine")
        
        info.append(f"Platform: {platform.platform()}")
        info.append(f"Python Version: {sys.version}")
        
        # Process info
        info.append("\n⚙️ Process Info:")
        pid = os.getpid()
        info.append(f"Process ID: {pid}")
        
        # Try to get process details
        try:
            process = psutil.Process(pid)
            create_time = datetime.fromtimestamp(process.create_time())
            info.append(f"Process Creation: {create_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Calculate uptime
            uptime = datetime.now() - create_time
            hours, remainder = divmod(uptime.total_seconds(), 3600)
            minutes, seconds = divmod(remainder, 60)
            info.append(f"Uptime: {int(hours)}h {int(minutes)}m {int(seconds)}s")
            
            info.append(f"CPU Usage: {process.cpu_percent()}%")
            info.append(f"Memory: {process.memory_info().rss / (1024 * 1024):.2f} MB")
            
            try:
                # List all open files
                files = process.open_files()
                if files:
                    info.append("\nOpen Files:")
                    for file in files[:5]:  # Limit to 5 files
                        info.append(f"- {file.path}")
                    if len(files) > 5:
                        info.append(f"...and {len(files)-5} more")
            except:
                pass
                
            try:
                # List all connections
                connections = process.connections()
                if connections:
                    info.append("\nNetwork Connections:")
                    for conn in connections[:5]:  # Limit to 5 connections
                        info.append(f"- {conn.laddr.ip}:{conn.laddr.port} -> {conn.raddr.ip if conn.raddr else 'N/A'}:{conn.raddr.port if conn.raddr else 'N/A'} ({conn.status})")
                    if len(connections) > 5:
                        info.append(f"...and {len(connections)-5} more")
            except:
                pass
        except Exception as e:
            info.append(f"Error getting process details: {str(e)}")
        
        # Environment variables
        info.append("\n🔐 Environment:")
        safe_vars = ['PATH', 'PYTHONPATH', 'RENDER', 'RENDER_SERVICE_NAME', 
                     'RENDER_SERVICE_TYPE', 'PORT', 'HOME', 'USER', 'PWD']
        
        for var in safe_vars:
            if var in os.environ:
                info.append(f"{var}: {os.environ.get(var)}")
        
        # File system
        info.append("\n📁 Files:")
        try:
            cwd = os.getcwd()
            info.append(f"Working Dir: {cwd}")
            
            # List some files in the directory
            files = os.listdir(cwd)
            info.append(f"Files ({len(files)} total):")
            for file in sorted(files)[:10]:  # Limit to 10 files
                try:
                    size = os.path.getsize(os.path.join(cwd, file))
                    info.append(f"- {file} ({size/1024:.1f} KB)")
                except:
                    info.append(f"- {file}")
            if len(files) > 10:
                info.append(f"...and {len(files)-10} more")
        except Exception as e:
            info.append(f"Error listing files: {str(e)}")
            
        # Join info and send
        response = "\n".join(info)
        
        # Send the message
        if len(response) > 4000:
            # If too long, split it
            await update.message.reply_text(response[:4000])
            await update.message.reply_text(response[4000:])
        else:
            await update.message.reply_text(response)
            
        logger.info(f"Tracking info sent to {update.effective_user.id} with tracking ID {tracking_id}")
        
    except Exception as e:
        logger.error(f"Error in tracking command: {str(e)}", exc_info=True)
        await update.message.reply_text(f"Error gathering tracking info: {str(e)}")


if __name__ == "__main__":
    try:
        # Try to kill any existing bot instances
        current_pid = os.getpid()
        current_script = os.path.basename(sys.argv[0])
        
        logger.info(f"Current process: PID {current_pid}, script {current_script}")
        
        # Find and terminate any other Python processes running the same script
        import time
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                # Skip the current process
                if proc.info['pid'] == current_pid:
                    continue
                    
                # Check if it's a Python process
                if proc.info['name'] and 'python' in proc.info['name'].lower():
                    cmdline = proc.info['cmdline']
                    # Check if it's running this script
                    if cmdline and any(current_script in cmd for cmd in cmdline):
                        logger.warning(f"Found existing bot instance: PID {proc.info['pid']}. Terminating...")
                        print(f"Terminating existing bot process with PID {proc.info['pid']}")
                        
                        try:
                            # Try SIGTERM first
                            proc.terminate()
                            
                            # Wait for process to terminate
                            gone, still_alive = psutil.wait_procs([proc], timeout=3)
                            
                            # If it's still alive, use SIGKILL
                            if still_alive:
                                for p in still_alive:
                                    logger.warning(f"Process {p.pid} did not terminate with SIGTERM. Using SIGKILL.")
                                    p.kill()
                                    
                            logger.info(f"Process {proc.info['pid']} terminated.")
                        except psutil.NoSuchProcess:
                            logger.info(f"Process {proc.info['pid']} already terminated.")
                        except Exception as term_err:
                            logger.error(f"Error terminating process {proc.info['pid']}: {term_err}")
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess, Exception) as e:
                logger.error(f"Error checking/terminating process: {e}")
                continue
                
        # Remove any stale lock files
        lock_file = "bot_instance.lock"
        if os.path.exists(lock_file):
            try:
                os.remove(lock_file)
                logger.info(f"Removed stale lock file {lock_file}")
            except Exception as e:
                logger.error(f"Error removing stale lock file: {e}")
                
        # Wait a moment for resources to free up
        time.sleep(2)
    except ImportError:
        logger.warning("psutil not installed, skipping process termination check")
    except Exception as e:
        logger.error(f"Error in process cleanup: {e}", exc_info=True)
        
    # Run the main function
    asyncio.run(main())
