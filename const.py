#python 3.5 code
"""
**********************************************************************
                    
                             TEXTFILE DB API
                             ===============
                            
                    Copyright © 2018 by Giuliano Jordao
                    
***********************************************************************
"""

"""
***********************************
		 	User Settings
 		 	  Constants 
************************************
"""

_DEBUG = 0                  # 0=Debug disabled, 1=Debug enabled
_TXTDBAPI_DEBUG = _DEBUG	# ALIAS TO THE USER DEFINITION

""" *** Even more Debug infos? """
_TXTDBAPI_VERBOSE_DEBUG = 0 # # 0=NO_VERBOSE, 1=VERBOSE
_LIKE_CASE_SENSITIVE = 0    # 0=LIKE is case insensitive, 1=LIKE is case sensitive
_ORDER_CASE_SENSITIVE = 0   # 0=ORDER BY is case insensitive, 1=ORDER BY is case sensitive

""""
# This constant doesn't limit the max size of a record it's only the assmued size 
# of a record when a table is read for appending. If not a whole record is
# contained in ASSUMED_RECORD_SIZE bytes, the the number of bytes read 
# is increased until a whole record was read. Choosing this value wisely may
# result in a better INSERT performance
"""
_ASSUMED_RECORD_SIZE = 30   # Set this to the average size of one record, if in doubt 
                            # leave the default value. DON'T set it to <1! int's only!

_PRINT_ERRORS = 1           # 0 = Errors are NOT displayed, 1 = Errors are displayed
_PRINT_WARNINGS = 0         # 0 = Warnings are NOT displayed, 1 = Warnings are displayed

"""
# Version
"""
_TXTDBAPI_VERSION = "0.3.1-Beta-01"

"""
# General
"""
_NOT_FOUND = -1

"""
# File parsing
"""
_COLUMN_SEPARATOR_CHAR = '#'    # Char which seperates the columns in the table file
                                # This MUST be a sigle character and NOR a string
_TABLE_FILE_ESCAPE_CHAR  = '%'  # Char to Escape COLUMN_SEPARATOR_CHAR in the Table Files
_TABLE_FILE_OPEN_MODE = 'b'     # "b" or ""

"""
# Timeouts
"""
_OPEN_TIMEOUT = 10 		# Timeout in seconds to try opening a still locked Table
_LOCK_TIMEOUT = 10 		# Timeout in seconds to try locking a still locked Table
_LOCKFILE_TIMEOUT = 30 	# Timeout for the maximum time a lockfile can exist

# Predefined Databases
_ROOT_DATABASE = ""

# Order Types
_ORDER_ASC = 1
_ORDER_DESC = 2

# Join Types
_JOIN_INNER = 1
_JOIN_LEFT = 2
_JOIN_RIGHT = 3

# Row Flags
_FLAG_KEEP = 0x1

# Column Types
_COL_TYPE_INC = "inc"
_COL_TYPE_INT = "int"
_COL_TYPE_STRING = "str"

# Column Function Types
_COL_FUNC_TYPE_SINGLEREC = 1
_COL_FUNC_TYPE_GROUPING = 2

# File Extensions
_TABLE_FILE_EXT = ".txt"
_LOCK_FILE_EXT = ".lock"