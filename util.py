#python 3.5 code
"""
**********************************************************************
                     
                             TEXTFILE DB API
                             ===============
                            
                    Copyright © 2018 by Giuliano Jordao
                    
***********************************************************************
"""

import const
import re

"""
/**********************************************************************
			A SMALL CLASS TO MAKE SOME TRICKS ON DISPLAY
***********************************************************************/
"""
class color:
   PURPLE 		= '\033[95m'
   CYAN 		= '\033[96m'
   DARKCYAN 	= '\033[36m'
   BLUE 		= '\033[94m'
   GREEN 		= '\033[92m'
   YELLOW 		= '\033[93m'
   RED 			= '\033[91m'
   BOLD 		= '\033[1m'
   UNDERLINE 	= '\033[4m'
   END 			= '\033[0m'

"""
/**********************************************************************
							Global vars 
***********************************************************************/
"""

_g_txtdbapi_errors = []

"""
/**********************************************************************
							Util Functions
***********************************************************************/
"""
"""
/***********************************
	 	Version Functions
************************************/
"""
def txtdbapi_version() :
	return TXTDBAPI_VERSION
#end txtdbapi_version


"""
/***********************************
	 	Debug Functions
************************************/
"""
def debug_printb(self, _str) :
	if(TXTDBAPI_DEBUG == 1 or TXTDBAPI_DEBUG == True) :
		print(color.BOLD + _str + color.END)
	#endif
#enddef

def debug_print(self, _str) :
	if(TXTDBAPI_DEBUG == 1 or TXTDBAPI_DEBUG == True) :
		print(_str)
	#endif
#enddef

def verbose_debug_print(self,_str) :
	if(TXTDBAPI_DEBUG == 1 or TXTDBAPI_DEBUG == True) :
		print(_str)
	#endif
#enddef

"""
/***********************************
	 	Char Functions
************************************/
"""
def last_char(self, _string) :
	if (len(_string) < 1) :
		return ""
	else :
		return _string[-1]
	#endif
#enddef

def remove_last_char(self, _string) :
	return _string[:-1]
#enddef

"""
/***********************************
	 	String Functions
************************************/
"""
##// returns _length chars from the right side of _string
def substr_right(self, _string, _length) :
	return _string[- _length]
#enddef

"""
/***********************************
	 	Array Functions
************************************/
"""
def array_walk_trim(self, _value, _key) :
	return _value[_key].strip()
#enddef

def create_array_fill(self, _size, _value) :
	_arr = []
	for _i in range(0,_size) :
		_arr[_i] = _value
	return _arr
#enddef

##// searches the first n chars of _string in _array
##// where n is the length of reach _array element
##// returns the value of _array if found or false
def array_search_str_start(self, _string, _array) :
	for _i in range(0,len(_array)) :
		if (_array[_i].startswith(_string)) :
			return _array[_i]
		#endif
	#endfor
	return False
#enddef

##// as above but case insenitive
def array_search_stri_start(self, _string, _array) :
	for _i in range(0, len(_array)) :
		_test = _array[_i].upper().startswith(_string.upper())
		if (_test == True) :
			return _array[_i]
		#endif
	#endfor
	return False
#enddef

"""	
/***********************************
	 	Type Functions
************************************/
"""
def dump_retval_type(self, _var) :
	if(bool(_var) and _var != "" and _var != None) :
		print("The value is FALSE") 
	if(int(_var) and _var != "" and _var != None  and _var != False) : 
		print("The value is 0") 
	if(_var == None) :
		print("The value is NULL")
	if(isinstance(_var, str) and _var != "" and _var != None  and _var != False) : 
		print("The value is \"\"") 
	if(isinstance(_var, str) and _var=="0"  and _var != False) : 
		print("The value is \"0\"") 
	if(_var != None and _var != "" and _var != False) :
		print("The value is a TRUE or something other then 0 or FALSE") 
#enddef 

def is_false(self,_var) :
	if (bool(_var) and _var != "" and _var != None) :
		return (bool(_var))
#enddef
	
def is_0(self, _var) :
	if (int(_var) and _var != "" and _var != None) :
		return int(_var_)
#enddef
##// _ at the front, cause is_null exists
def is_null(self, _var) :
	if (_var == None) :
		return True
	else :
		return False
#enddef
def is_empty_str(self, _var) :
	if (_var == "" and _var != None):
		return True
	else :
		return False
#enddef
"""
/***********************************
	 	SQL Util Functions
************************************/
"""
##// compares 2 values by _operator, and returns true or false
def compare(self, _value1, _value2, _operator) :
	if(_operator == "=") :
		if (_value1 == _value2) :
			return True
		else :
			return False

	if(_operator == ">") :
		if (_value1 > _value2) :
			return True
		else :
			return False
		
	if(_operator == "<") :
		if (_value1 < _value2) :
			return True
		else :
			return False
	if(_operator == "<>" or _operator == "!=") :
		if (_value1 != _value2) :
			return True
		else :
			return False
	if(_operator == ">=") :
		if (_value1 >= _value2) :
			return True
		else :
			return False
	if(_operator == "<=") :
		if (_value1 <= _value2) :
			return True
		else :
			return False
	if(_operator == "LIKE") :
		if (compare_like(_value1,_value2)) :		
			return True
		else :
			return False
	if(_operator == "NOT LIKE") :
		if (compare_like(_value1,_value2)) :		
			return False
		else :
			return True
	if(_operator == "IN") :
		_list = re.compile(_value, re.VERBOSE)
		for _listVal in _list :
			if(not _listVal.isnumeric()):
				if(has_quotes(_listVal)) :
					_listVal = remove_quotes(_listVal)
				if(_listVal == _value1) :
					return 1
			elif(_listVal == _value1) :
				return 1
		return 0
	if(_operator == "NOT IN") :
		_list = re.compile(_value, re.VERBOSE)
		for _listVal in _list :
			if(not _listVal.isnumeric()):
				if(has_quotes(_listVal)) :
					_listVal = remove_quotes(_listVal)
				if(_listVal == _value1) :
					return 0
			elif(_listVal == _value1) :
				return 0
		return 1
	return False


def compare_like(self, _value1, _value2) : 
	_patterns = [] 

	##// Lookup precomputed pattern 
	if(_patterns[_value2] != None) : 
		_pat = _patterns[_value2] 
	else : 
		##// Calculate pattern 
		_rc = 0 
		_mod = "" 
		_prefix = "/^" 
		_suffix = "_/" 
       
		##// quote regular expression characters 
		_str = e.escape(_value2) 
       
		##// unquote \ 
		_str = _str.replace("\\\\", "\\") 
       
		##// Optimize leading/trailing wildcards 
		if(_str[0] == '%') : 
			_str = _str[1:] 
			_prefix = "/" 
		if(_str[-1] == '% '  and _str[-1] != '\\') : 
			_str = _str[0: - 1] 
			_suffix = " / " 
       
		##// case sensitive ? 
		if(LIKE_CASE_SENSITIVE == False or LIKE_CASE_SENSITIVE == 1) :
			_mod = "i" 
          
		##// setup a StringParser and replace unescaped '%' with '.*' 
		_sp = StringParser() 
		_sp.setConfig([] ,"\\",[]) 
		_sp.setString(_str) 
		_str = _sp.replaceCharWithStr("%",".*") 
		##// replace unescaped '_' with '.' 
		_sp.setString(_str) 
		_str=_sp.replaceCharWithStr("_",".") 
		_pat = _prefix . _str . _suffix . _mod 

		##// Stash precomputed value 
		_patterns[_value2] = _pat 
	       
		_matches = re.search(_pat, _value1)
	return _matches

##// splits a full column name into its subparts (name, table, function)
##// return true or false on error
def split_full_colname(_fullColName, _colName, _colTable, _colFunc) :
	
	_colName = ""
	_colTable = ""
	_colFunc = ""
	
	#// direct value ?
	if(_fullColName.insnumeric() or has_quotes(_fullColName)) :
		_colName = _fullColName.strip()
		return True
	
	_pos = _fullColName.find("(")
	if(not is_false(_pos) ) :
		_colFunc = _fullColName[0:_pos].upper().strip()
		_fullColName = _fullColName[_pos + 1:]

	_pos = _fullColName.find(".")	
	if(not is_false (_pos) and _colFunc != "EVAL") :
		_colTable = _fullColName[0:_pos]
		_colName = (_fullColName[_pos + 1:])
	else :
		_colName = _fullColName
	
	_colName = _colName.strip()
	if(_colFunc != None or _colFunc != "") :
		if(_colName[-1] == ")") :
			_colName = _colName[0:-1]
		else :
			print_error_msg(") expected after _colName!")
			return False
	_colName = _colName.strip()
	_colTable = _colTable.strip()
	
import time
import datetime
		
def doFuncUNIX_TIMESTAMP() :
	return datetime.datetime.fromtimestamp(time.time())
#enddef
					
def doFuncABS(self, _param) :
	return abs(_param)
#enddef

def doFuncLCASE(self, _param) :
	return _param.lower()
#enddef

def doFuncUCASE(self, _param) :
	return _param.upper()
#enddef

def execGroupFunc(self, _func, _params) :
	
	if (_func == "MAX") :
		return doFuncMAX(_params)
	elif (_func == "MIN") :
		return doFuncMIN(_params)
	elif (_func == "COUNT") :
		return doFuncCOUNT(_params)
	elif (_func == "SUM") :
		return doFuncSUM(_params)
	elif (_func == "AVG") :
		return doFuncAVG(_params)
	else :		
		print_error_msg("Function '_func' not supported!!!")
	
	return _col
#enddef

def doFuncMAX(self, _params) :
	_maxVal = _params[0]
	for _i in range(_params) :
		_maxVal = max(_maxVal, _params[_i])
	
	return _maxVal
#enddef

def doFuncMIN(self, _params) :
	_minVal = _params[0]
	for _i in range(_params) :
		_minVal = min(_minVal,_params[_i])
	
	return _minVal
#enddef

def doFuncCOUNT(self, _params) :
	return len(_params)
#enddef

def returnNumber(self, _value) :
	digit = lambda x: int(filter(str.isdigit, x) or 0)
	return digit(_value)
#enddef

def doFuncSUM(self, _params) :
	_sum = 0
	for _i in range(_params) :
		_sum = returnNumber(_sum) + returnNumber(_params[_i])

	return _sum
#enddef

def doFuncAVG(self, _params) :
	_sum = doFuncSUM(_params)
	_res = _sum / len(_params)
	return _res
#enddef

"""
/***********************************
	 	Error Functions
************************************/
"""
def print_error_msg(self, _text, _nr ) :
	if (_nr == "" or _nr == None) :
		_nr = -1
	global _g_txtdbapi_errors
	
	_g_txtdbapi_errors = _g_txtdbapi_errors + _text
	if(PRINT_ERRORS == 0 or PRINT_ERRORS == False) :
		return

	if(_nr == -1) :
		print("\n" + color.BOLD + "Txt-Db-Access Error:" + color.END + "\n")
	else :
		print("\n Txt-Db-Access Error Nr: " + _nr +"\n")
	print(_text + "\n")
#enddef

def print_warning_msg(self, _text, _nr) :
	if (_nr == "" or _nr == None) :
		_nr = -1
	if(PRINT_WARNINGS == 0 or PRINT_WARNINGS == False) :
		return
		
	if(_nr==-1) :
		print("\n" + color.BOLD + "Txt-Db-Access Warning:" + color.END + "\n")
	else :
		print("\n Txt-Db-Access Warning Nr: " + _nr +"\n")
	print(_text + "\n")
#enddef

#// returns true if errors occurred
def txtdbapi_error_occurred() :
	global _g_txtdbapi_errors
	if (len(_g_txtdbapi_errors) > 0) : 
		return True
	else :
		return False
#enddef

def txtdbapi_get_last_error() :
	global _g_txtdbapi_errors
	if(not txtdbapi_error_occurred()) :
	    return ""
	return array_pop(_g_txtdbapi_errors)
#enddef

def txtdbapi_get_errors() :
	global _g_txtdbapi_errors
	
	if(not txtdbapi_error_occurred()) :
	    return []
	_arr = _g_txtdbapi_errors
	_g_txtdbapi_errors = []
	return _arr
#enddef

def txtdbapi_clear_errors() :
	global _g_txtdbapi_errors
	_g_txtdbapi_errors = []
#enddef

#// error handler function
def txtdbapi_error_handler (self, _errno, _errstr, _errfile, _errline) :
	_prefix = "Python Error: "
	if (_errno ==  E_USER_ERROR) :
		print_error_msg(_prefix + "FATAL [_errno] _errstr [Line: " + _errline + "] [File: " + _errfile + "]")
	else :
		print_error_msg(_prefix + "[_errno] _errstr [Line: " + _errline + "] [File: " + _errfile + "]")
    	
	return
	#endif
#enddef

"""
/***********************************
	 	Quote Functions
************************************/
"""
def has_quotes(self, _str) :
	if(_str == ""):
		return False
	if ((_str[0] == "'" or _str[0] == '"') and (_str[-1] == "'" or _str[-1] == '"')) :
		return True
	else :
		return False
#enddef

def remove_quotes(self, _str) :
	return _str[1,-1]
#enddef

def array_walk_remove_quotes(self, _value,  _key) :
	if(has_quotes(_value)) :
		return remove_quotes(_value)
#enddef

"""
/***********************************
	 	Time Functions
************************************/
"""
def getmicrotime() :
	return lambda : int(round(time.time() * 1000))
#end getmicrotime

#// ensures that all timestamp requests of one execution have the same time
def get_static_timestamp() :
	_t = 0
	if(_t == 0) :
		_t = time.time()
	return _t
#enddef