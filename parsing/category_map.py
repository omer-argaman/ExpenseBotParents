# Category map: canonical category name -> list of keywords that trigger it.
# Keys must exactly match the row names in column A of the Google Sheet.
# Keywords are matched case-insensitively.
#
# This is the parents' mapping — preserved 1:1 from the previous bot so that
# the existing Google Sheet rows continue to resolve correctly.

CATEGORY_MAP = {

    # Personal Categories
    "Mom":          ["Mom", "Ilanit", "Business", "Sapakim"],
    "Dad":          ["Hafenix", "Bitoach Leumi", "BLL"],
    "Ramat Gan":    ["Noy", "Avigdor", "Yigal"],
    "Extra":        ["Extra", "Bonus"],

    # Savings and Insurance
    "Emergency Fund": ["emergency fund", "savings", "rainy day"],
    "Kupat Gemel":    ["Gemel", "Kupat Gemel", "Kupat Gemel Mom", "Mom Kupat Gemel"],
    "Savings":        ["Savings", "Saveings", "IBI"],

    # Home Expenses
    "Mortgage Payment": ["mortgage", "mortage", "Mortgage Payment"],
    "Electricity":      ["electricity", "electric", "energy", "utility", "hashmal"],
    "Gas":              ["gas", "gas home"],
    "Water":            ["water", "sewer", "trash", "garbage", "utilities", "waste"],
    "Property Tax":     ["Property Tax", "Tax", "Arnona"],
    "Internet":         ["internet", "wifi", "broadband", "net"],
    "TV":               ["TV"],
    "Cellular":         ["Cellular", "Phone", "Pelephone", "phone bill", "Telephone"],
    "Gardner":          ["Gardner"],
    "Heating":          ["Heating", "Oil"],
    "Elza":             ["Elza food", "Elza", "Vet", "Veterinar", "Vetrinar", "Dog", "Dog Food"],
    "Cleaner":          ["Cleaner", "House Keeper", "House Keeping"],
    "Maintenance":      ["maintenance", "improvement", "repair", "fix", "home",
                         "flowers", "flower", "upgrade", "repairs"],

    # Transportation
    "Fuel Toyota":          ["Fuel Toyota", "fuel toyota", "diesel", "gas toyota"],
    "Fuel MG":              ["Fuel MG", "petrol", "fuel mg", "gas MG"],
    "Road Talls":           ["Road Talls", "Talls", "Road six", "Kvish shesh", "Kvish", "Road"],
    "Public Transportation": ["train", "taxi", "bus", "metro", "tram", "cab", "subway",
                              "ride", "public", "transport"],
    "Parking":              ["parking", "garage", "space", "charging", "park",
                             "pango", "cello", "cellopark"],
    "Car Insurance Toyota": ["Car Insurance Toyota", "Insurance toyota"],
    "Car Insurance MG":     ["Car Insurance MG", "Insurance MG"],
    "Maintenance Toyota":   ["Test Toyota", "Tipul toyota", "Musah toyota",
                             "Garage toyota", "Mechanic toyota"],
    "Maintenance MG":       ["Test MG", "Tipul MG", "Musah mg", "Garage MG", "Mechanic MG"],
    "Other (Trans)":        ["transportation other", "other transportation", "Other (Trans)",
                             "Other Trans", "Other Transportation"],

    # Insurances
    "Life":     ["life insurance", "life insurance Dad", "life insurance Mom",
                 "life insurance Risk"],
    "Nursing":  ["Nursing insurance", "Nursing"],
    "Structure": ["Structure insurance", "Structure"],
    "Health Care": ["Health insurance", "Clalit", "Clalit Shahar", "Clalit Ilanit",
                    "Clalit Nadav", "Clalit Omer", "Clalit Alon", "Clalit Naama",
                    "Siudi Clalit Shahar", "Siudi Clalit Ilanit", "Siudi Clalit Nadav",
                    "Siudi Clalit Omer", "Siudi Clalit Alon", "Siudi Clalit Naama",
                    "Siudi Shahar", "Siudi Ilanit", "Siudi Nadav", "Siudi Omer",
                    "Siudi Alon", "Siudi Naama", "Siudi"],
    "Mortgage Insurance": ["Mortgage insurance"],
    "Health Insurance":   ["Health Insurance"],

    # Daily Living
    "Groceries":    ["groceries", "supermarket", "market", "super", "store",
                     "shopping", "food", "dani", "danny"],
    "Vitamins":     ["Vitamins", "Omega three", "Vitamin", "Omega"],
    "Dining Out":   ["dining", "restaurant", "meal", "food", "breakfast", "lunch",
                     "dinner", "eat", "takeout", "delivery"],
    "Beer / Wine":  ["beer", "wine", "alcohol", "bar", "cocktail", "drink",
                     "vodka", "whiskey", "liquor"],
    "Other (Daily)": ["daily living other", "other daily living", "miscellaneous living",
                      "Other (Daily)", "Other Daily", "other", "cosmetics", "laser",
                      "personal", "gym", "present", "presents"],
    "Cloths":       ["cloths", "shirt", "pants", "dress", "clothes", "cloth",
                     "tshirt", "t-shirt", "t shirt"],
    "Kids":         ["Naama", "Alon", "Nadav", "Omer", "Naama tzofim", "tzofim",
                     "Naama Studio", "Studio", "Naama Driving Licence",
                     "Driving Licence", "Driving Lessons", "Driving Lesson", "Matnas"],
    "Education":    ["School", "Naama school", "Tel Aviv Univercity", "Uni",
                     "Univercity", "Education", "Edu", "Private lesson",
                     "Privet Lesson", "Shiur prati", "Lesson", "Shiur"],
    "Entertainment": ["entertainment", "movie", "Movies", "theater", "show",
                      "concert", "game", "festival", "fun", "games",
                      "Haaretz", "News Paper", "News", "newspaper", "Haarets"],
    "Vacation":     ["vacation", "holiday", "trip", "travel", "hotel", "flight",
                     "beach", "resort", "Jip Trip", "Jeep trip", "Jeep trips"],
    "Health":       ["health", "doctor", "medicine", "hospital", "clinic", "checkup",
                     "insurance", "medical", "healthcare"],
    "Pharm":        ["Pharm", "pharm", "superpharm", "super pharm", "super-pharm", "pharmacy"],
    "Beauty":       ["Beauty", "Tipuah", "Haircut", "Hair cut", "Ilana", "Nails",
                     "Hair Color", "personal care"],
    "Treatment":    ["Treatment", "RINA", "Rina", "pool", "Alon Rosenberg",
                     "Rosenberg", "Tipul"],

    # Business
    "Contructors":      ["Contructors"],
    "Software":         ["Application", "Software", "Autocad", "Autodesk"],
    "Insurance":        [],
    "Office Appliance": ["office appliance", "Office", "Misrad"],
    "Accountant":       ["Accountant", "Accounting", "Ziv Shifer", "Ziv"],
    "Other (Business)": ["Other Business", "Other esek", "Esek"],
}


# How categories are grouped into broad sections for /summary.
# Keys must match the broad-section header names in column A of the Google Sheet
# (case-insensitive match at runtime).
# Subcategory list order and count must reflect the actual sheet layout so
# /summary can correctly locate each section's total row.
#
# Only sections that change month to month are listed here. Personal,
# Savings/Insurance and the standalone Insurance categories stay in
# CATEGORY_MAP so they remain loggable, but are intentionally absent from
# /summary because their amounts don't fluctuate.
BROAD_CATEGORIES = {
    "Home": [
        "Mortgage Payment", "Electricity", "Gas", "Water",
        "Property Tax", "Internet", "TV", "Cellular",
        "Gardner", "Heating", "Elza", "Cleaner", "Maintenance",
    ],
    "Transportation": [
        "Fuel Toyota", "Fuel MG", "Road Talls", "Public Transportation",
        "Parking", "Car Insurance Toyota", "Car Insurance MG",
        "Maintenance Toyota", "Maintenance MG", "Other (Trans)",
    ],
    "Daily Living": [
        "Groceries", "Vitamins", "Dining Out", "Beer / Wine",
        "Other (Daily)", "Cloths", "Kids", "Education",
        "Entertainment", "Vacation", "Health", "Pharm",
        "Beauty", "Treatment",
    ],
    "Business": [
        "Contructors", "Software", "Insurance",
        "Office Appliance", "Accountant", "Other (Business)",
    ],
}
