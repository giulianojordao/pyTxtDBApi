#python 3.5 code
"""
**********************************************************************
                     
                             TEXTFILE DB API
                             ===============
                            
                    Copyright © 2018 by Giuliano Jordao
                    
***********************************************************************
"""
import const
import util
import resultset
import sql
import expression
import os 
import os.path
import time

import portalocker

"""
***********************************************************************
                       Python FILEOPEN MODES
                       ====== ======== =====
                       
        CHARACTER   MEANING
        'r'         open for reading (default)
        'w'         open for writing, truncating the file first
        'x'         open for exclusive creation, failing if the 
                    file already exists
        'a'         open for writing, appending to the end of 
                    the file if it exists
        'b'         binary mode
        't'         text mode (default)
        '+'         open a disk file for updating (reading and 
                    writing)
        'U'         universal newlines mode (deprecated)
        
***********************************************************************
"""

"""
***********************************************************************
                            LOCK MODES
        
        LOCK_SH para adquirir um bloqueio compartilhado (leitura).
        LOCK_EX para adquirir um bloqueio exclusivo (escrita).
        LOCK_UN para liberar um bloqueio (compartilhado ou exclusivo).
        
***********************************************************************
"""

"""
/**********************************************************************
                            DATABASE 
                        (class / module)
***********************************************************************/
"""

# Represents a Database and has functions to execute Sql-Queries on it
class Database :
    #def __init__(self):
    _dbFolder = ""
    _lastInsertId = 0 
    
    _lastErrorMsgs = [ ]
    
    """
    /***********************************
                Constructor
    ************************************/
    """
    def Database(self, _dbFolder) :
        if (_dbFolder == "" ):
            _dbFolder = "defaultDb/"
            
        self.dbFolder = _DB_DIR + _dbFolder
        
        if(self.dbFolder[-1] != "/"):
            self.dbFolder = self.dbFolder + "/"
        
    
    """
    /***********************************
             Insert Id Functions
    ************************************/
    """
    def updateLastInsertId(self, _resultSet) : 
        _resultSet.end()
        
        for _i in xrange(0, len(_resultSet.colTypes)) : 
            if (_resultSet.colTypes[_i] == _COL_TYPE_INC) : 
                self.lastInsertId = _resultSet.getCurrentValueByNr(_i) 
                debug_print("Setting lastInsertId to " + _this.lastInsertId)
                return 
            #endif
        #endfor
        # if no inc column exists set lastInsertId to 0 
        self.lastInsertId = 0 
        debug_print("Setting lastInsertId to " + self.lastInsertId )
    #end updateLastInsertId 
    
    def getLastInsertId(self) :
        return self.lastInsertId
    #end getLastInsertId    
    
    """
    /***********************************
         Table open/close Functions
    ************************************/
    """
                         
    # Does open a Table for writing
    def openTableWrite(self, _tableName) :
        _filename = self._dbFolder + _tableName + _TABLE_FILE_EXT
                         
        debug_print("openTableWrite \n" + _filename)
        
        if (os.path.isfile(_filename) == False):
            debug_print("FileIOError: File not found " + _filename)
            return 0
        
        try :
            _fp = open (_filename, 'r+') # Open the table file for read and write
            portalocker.lock(_filename, portalocker.LOCK_EX) # lock the file for exclusive
            
        except IOError :
            debug_print("FileIOError: File not found " + _filename)
            return 0

        else :
            return _fp
    #end openTableWrite
    """
    #// same rules as for openTableWrite()
    #// does open a table for appending
    """
    def openTableAppend(self, _tableName) :
        _filename = self._dbFolder + _tableName + TABLE_FILE_EXT
        debug_print("openTableAppend \n" + _filename)

        if (os.path.isfile(_filename) == False):
            debug_print("FileIOError: File not found " + _filename)
            return 0

        try :
            _fp = open (_filename, 'a') # Open the table file for apend mode
            portalocker.lock(_filename, portalocker.LOCK_EX) # lock the file for exclusive
            
        except IOError :
            debug_print("FileIOError: File not found " + _filename)
            return 0

        else :
            return _fp
    #end openTableappend
    """    
    #// Opens a Table for reading
    #// This function is used by SELECT
    #// returns a FilePointer or False
    """
    def openTableRead(self, _tableName) :
        _filename = self._dbFolder + _tableName + TABLE_FILE_EXT
        debug_print("openTableAppend \n" + _filename)

        if (os.path.isfile(_filename) == False):
            debug_print("FileIOError: File not found " + _filename)
            return 0

        try :
            _fp = open (_filename, 'r') # Open the table file for read
            portalocker.lock(_filename, portalocker.LOCK_SH) # lock the file for exclusive
            
        except IOError :
            debug_print("FileIOError: File not found " + _filename)
            return 0

        else :
            return _fp
    #end openTableRead            
    """
    #// Closes a Table
    """
    def closeTable(self, _fp) :
        debug_print("Table " + _fp + " closed\n")
        portalocker.lock(_fp, portalocker.LOCK_UN) # unlock the file
        return _fp.close()        
    #end closeTable
    
    """
    /***********************************
         Table read/write Functions
    ************************************/
    """
    """
    #// Reads a Table into a ResultSet
    #// Returns a ResultSet or null (the function opens an closes the file itself)
    """
    def readTable(self, _tableName) :
        debug_print("readTable " + _tableName + " to parse. \n")
        _parser = ResultSetParser()
        
        _fd = self.openTableRead(_tableName)
        
        if( _fd  == False ) :
              print_error_msg("readTable(): Cannot open Table " + _tableName)
              return None
        #endif
        _rs = _parser.parseResultSetFromFile(_fd)
        self.closeTable(_fd)
        return _rs
    #end readTable
    
    """
    #// Reads a Table into a ResultSet
    #// But only the column names and types + the last record
    #// thats usefull for appending record
    """
    def readTableForAppend(_tableName) :
        debug_print("readTableForAppend \n")
        _parser = ResultSetParser()
        
        _fd = self.openTableRead(_tableName)
        if(_fd == False) :
              print_error_msg("readTableForAppend(): Cannot open Table " |+ _tableName)
              return None
        #endif
        _rs = _parser.parseResultSetFromFileForAppend(_fd)
        self.closeTable(_fd)
        return _rs
    #end readTableForAppend

    """
    #// writes the table by using the FilePointer _fd 
    #// _fd has to be opened an closed by the caller
    """
    def writeTable(self, _fd, _resultSet) :
        debug_print("writeTable\n")
        _parser = ResultSetParser()
        return _parser.parseResultSetIntoFile(_fd, _resultSet)
    #end writeTable
    
    """
    #// appends to the table by using the FilePointer _fd 
    #// _fd has to be opened an closed by the caller
    """
    def appendToTable(self, _fd, _resultSet, _recordCount) :
        debug_print("appendToTable\n")
        _parser = ResultSetParser()
        while _resultSet.getRowCount() > _recordCount :
            _resultSet.reset()
            _resultSet.next()
            _resultSet.deleteCurrentRow()
        #end While
        return _parser.parseResultSetIntoFileAppend(_fd, _resultSet)
    #end appendToTable
    
    """
    /***********************************
             Query dispatcher
    ************************************/
    """
    """
    #// _sql_query_str is an unparsed SQL Query String
    #// Return Values:
    #// SELECT Queries: Returns a ResultSet Object or False
    #// CREATE TABLE: Returns True or False
    #// All other types: Returns the number of rows affected
    """
    def executeQuery(self, _sql_query_str) :
        set_error_handler("txtdbapi_error_handler")
        txtdbapi_clear_errors()
        
        debug_print("[executeQuery] Query: " + _sql_query_str + "\n")        
        # Parse Query

        _start = self.getmicrotime()
        _sqlParser = SqlParser(_sql_query_str)
        _sqlQuery = _sqlParser.parseSqlQuery()
        debug_print("parseSqlQuery: " + (self.getmicrotime() - _start) + " seconds elapsed \n")
        
        # free _sqlParser
        _sqlParser = None
        _sqlParser = ""
        
        # Test Query
        if ((_sqlQuery == None or _sqlQuery == "") or (_sqlQuery.test() == None or _sqlQuery.test() == "" )) :
            restore_error_handler()
            return False
        #endif
        
        _start = self.getmicrotime()
    
        if(TXTDBAPI_DEBUG == 1) :
            debug_print("[executeQuery] Parsed Query: \n" + _sqlQuery.dump())
        #endif

        # Dispatch
        if (_sqlQuery.type == "SELECT") :
            _rc = self.executeSelectQuery(_sqlQuery)
        elif (_sqlQuery.type == "INSERT") :
            _rc = self.executeInsertQuery(_sqlQuery)
        elif (_sqlQuery.type == "DELETE") :
            _rc = self.executeDeleteQuery(_sqlQuery)
        elif (_sqlQuery.type == "UPDATE") :
            _rc = self.executeUpdateQuery(_sqlQuery)
        elif (_sqlQuery.type == "CREATE TABLE") :
            _rc = self.executeCreateTableQuery(_sqlQuery)
        elif (_sqlQuery.type == "DROP TABLE" ):
            _rc = self.executeDropTableQuery(_sqlQuery)
        elif (_sqlQuery.type == "CREATE DATABASE") :
            _rc = self.executeCreateDatabaseQuery(_sqlQuery)
        elif (_sqlQuery.type == "DROP DATABASE") :
            _rc = self.executeDropDatabaseQuery(_sqlQuery)
        elif (_sqlQuery.type == "LIST TABLES") :
            _rc = self.executeListTablesQuery(_sqlQuery)
        else :
            print_error_msg("Invalid or unsupported Query Type: " + _sqlQuery.type)
            restore_error_handler()
            return False
        #endif
        
        if( _rc == False ) :
            print_error_msg("Query '" + _sql_query_str + "' failed\n")
        #endif
        
        debug_print("[executeQuery] Query execution done: " + (self.getmicrotime() - _start) + " seconds elapsed\n")
        restore_error_handler()
        return _rc
    #end executeQuery
    
    """
    /***********************************
             Delete Query
    ************************************/
    """
    def executeDeleteQuery(self, _sqlQuery) :
        # Read Table
        _rs = self.readTable(_sqlQuery.tables[0])
        if(_rs == None or _rs == False or _rs == "") :
            print_error_msg("Reading the Table " + _sqlQuery.tables[0] + " failed\n")
            return False
        #endif
        
        _rowsAffected = 0
        
        if(_sqlQuery.where_expr == None or _sqlQuery.where_expr == "" or _sqlQuery.where_expr == False) :
            _rowsAffected = _rs.getRowCount()
            _rs.deleteAllRows()
        else :
            ##// set row ids
            _rId = -1
            _rs.reset()
            while(_rs.next()): 
                _rId += 1
                _rs.setCurrentRowId(_rId)
            #end While
            _rs.reset()
            
            ##// calc current column count
            _colCount = len(_rs.colNames)
            
            ##// generate additional columns from the WHERE-expression
            _rs.generateAdditionalColumnsFromWhereExpr(_sqlQuery.where_expr)
        
            ##// execute the new single-rec functions
            _rs.executeSingleRecFuncs()
            
            ##// apply WHERE Expression
            _ep = ExpressionParser()
            _et = _ep.getExpressionTree(_sqlQuery.where_expr)
            _rsFiltered = _et.getFilteredResultSet(_rs)
            
            if(_rsFiltered == None or _rsFiltered == False or _rsFiltered == "") :
                return 0            
            
            ##// Delete rows..
            _rsFiltered.reset()
            while(_rsFiltered.next()) :
                _rowId = _rsFiltered.getCurrentRowId()
                _rs.deleteRow(_rs.searchRowById(_rowId))
            #end While
            
            _rowsAffected = _rsFiltered.getRowCount()
            
            ##// Remove columns added from WHERE Expression
            while(len(_rs.colNames) > _colCount) :
                _rs.removeColumn(len(_rs.colNames)-1)
            #end While
        #endif     
        
        ##// Open Table
        _fp = self.openTableWrite(_sqlQuery.tables[0])
        if(_fp == None or _fp == False or _fp == "") :
            print_error_msg("Open the Table " + _sqlQuery.tables[0] + " (for WRITE) failed\n")
            return False
        #endif
        
        ##// Write Table
        self.writeTable(_fp, _rs)
        self.closeTable(_fp)
        return _rowsAffected
    #end executeDeleteQuery


    """
    /***********************************
             Insert Query
    ************************************/
    """
    ##// returns the affected Row count or False
    def executeInsertQuery(self, _sqlQuery) :
        
        #// Read Table
        _rs = self.readTableForAppend(_sqlQuery.tables[0])
        
        if(TXTDBAPI_VERBOSE_DEBUG == 1) :
            print_error_msg("executeInsertQuery(): Last Record read for appending: \n" + _rs.dump() + "\n")
        #end
        
        if(_rs == None or _rs == False or _rs == "") :
            print_error_msg("Reading the Table " + _sqlQuery.tables[0] + " failed \n")
            return False
        #end
        
        #// Open Table
        _fp = self.openTableAppend(_sqlQuery.tables[0])
        if(_fp == False or _fp == "" or _fp == None) :
            print_error_msg("Open the Table " + _sqlQuery.tables[0] + " (for APPEND) failed \n")
            return False
        #end
        
        ##// a INSERT INTO table () VALUES () query: just write the default values
        if(len(_sqlQuery.colNames) == 0 and len(_sqlQuery.fieldValues) == 0) :
                _rs.append()
                self.updateLastInsertId(_rs)
                self.appendToTable(_fp,_rs,1)
                self.closeTable(_fp)
                return 1 #// Error Handling ??
        #end
                
        #// execute functions on the values
        _colName = ""
        _colTable = ""
        _colFunc = ""
        
        for _i in range(_sqlQuery.fieldValues) :
            split_full_colname(_sqlQuery.fieldValues[_i],_colName,_colTable,_colFunc)
            if(_colFunc == 1 or _colFunc == True) :
                if((_colName == 1 or _colName == True) and (has_quotes(_colName)))  :
                    _colName = remove_quotes(_colName)
                #end
                _sqlQuery.fieldValues[_i] = execFunc(_colFunc, _colName)
            #end else :
                if(has_quotes(_sqlQuery.fieldValues[_i])) :
                    _sqlQuery.fieldValues[_i] = remove_quotes(_sqlQuery.fieldValues[_i])
                #end
            #end
        #end
                
        _rc = True
        if (len(_sqlQuery.colNames) == 0) :
            _rs.appendRow(_sqlQuery.fieldValues)
            self.updateLastInsertId(_rs)
            self.appendToTable(_fp,_rs,1)
            self.closeTable(_fp)
            return _rc
        else :
            _rs.append()
            for _i in range(_sqlQuery.colNames) :
                if(_rs.setCurrentValueByName(_sqlQuery.colNames[_i],_sqlQuery.fieldValues[_i]) == None or _rs.setCurrentValueByName(_sqlQuery.colNames[_i],_sqlQuery.fieldValues[_i]) == False or _rs.setCurrentValueByName(_sqlQuery.colNames[_i],_sqlQuery.fieldValues[_i]) == "") :
                    _rc = False
                #end
            #end
            if(_rc == True or _rc==1) :
                self.updateLastInsertId(_rs)
                self.appendToTable(_fp,_rs,1)
            #end
            self.closeTable(_fp)
            return _rc
        #end
    #end
    
    """
    /***********************************
             Update Query
    ************************************/
    """
    #// returns the affected Row count or False
    def executeUpdateQuery(self, _sqlQuery) :
        #// Read Table
        _rs = self.readTable(_sqlQuery.tables[0])
        if(_rs == None or _rs == False or _rs == "") :
            print_error_msg("Reading the Table " + _sqlQuery.tables[0] + " failed \n")
            return False
        #end
        
        #// calc original column count
        _colCount = len(_rs.colNames)

        _rs.generateAdditionalColumnsFromArray(_sqlQuery.fieldValues)

        if(txtdbapi_error_occurred()) :
            return False

        _rs.executeSingleRecFuncs()

        #// check if there are wrong functions
        for _i in range(_rs.colFuncs) :
            if(_rs.colFuncs[_i] == True and (_rs.colFuncsExecuted[_i] == None or _rs.colFuncsExecuted[_i] == False or _rs.colFuncsExecuted[_i] == "")) :
                print_error_msg("Function '" + _rs.colFuncs[_i]  + "' not supported in UPDATE statements\n")
                return False
            #end
        #end
        
        
        if(TXTDBAPI_DEBUG == 1 or TXTDBAPI_DEBUG == True) :
            debug_print("[executeUpdateQuery] ResultSet dump after generating columns: \n" + _rs.dump())
        #end
        
        #// No where_expr ? update all
        if (_sqlQuery.where_expr == None  or  _sqlQuery.where_expr == False or _sqlQuery.where_expr == "") :
            #// update 
            _rs.reset()
            while(_rs.next()) :
                for _i in range(_sqlQuery.colNames)  :
                    _rc = _rs.setCurrentValueByName(_sqlQuery.colNames[_i], _rs.getCurrentValueByName(_sqlQuery.fieldValues[_i]))
                    if(_rc == False or _rc == None or _rc == "")  :
                        return False
                    #end
                #end
            #end
            
            #// Remove columns added from WHERE Expression
            while(len(_rs.colNames) > _colCount) :
                _rs.removeColumn(len(_rs.colNames)-1)
            #end
            
            #// Open Table
            _fp = self.openTableWrite(_sqlQuery.tables[0])
            if(_fp == False or _fp == None or _fp == "") :
                print_error_msg("Open the Table " + _sqlQuery.tables[0] + " (for WRITE) failed \n")
                return False
            #end
            #// Write Table
            self.writeTable(_fp,_rs)
            self.closeTable(_fp)
            return _rs.getRowCount()

        #end else :
            #// set row id's
            _rs.reset()
            _rId = -1
            while(_rs.next()) :
                _rId += 1
                _rs.setCurrentRowId(_rId)
            
            #// generate additional columns from the WHERE-expression
            _rs.generateAdditionalColumnsFromWhereExpr(_sqlQuery.where_expr)
        
            #// execute the new single-rec functions
            _rs.executeSingleRecFuncs()
            
            #// create a copy 
            _rsFiltered=_rs

            #// filter by where expression
            _ep = ExpressionParser()
            _et = _ep.getExpressionTree(_sqlQuery.where_expr)
            _rsFiltered = _et.getFilteredResultSet(_rsFiltered)
            
            if(_rsFiltered.getRowCount()<1) :
                return 0
                
            #// update 
            _rsFiltered.reset()
            while(_rsFiltered.next()) :
                for _i in range(_sqlQuery.colNames) :
                    _rc = _rsFiltered.setCurrentValueByName(_sqlQuery.colNames[_i], _rsFiltered.getCurrentValueByName(_sqlQuery.fieldValues[_i]))
                    if(_rc == False or _rc == None or _rc == "")  :
                        return False
                    #end
                    
                #end
            #end
                        
            #// put filtered part back in the original ResultSet
            _rowNr = 0
            _putBack = 0
            _rs.reset()
            _rsFiltered.reset()
            while(_rs.next()) :
                _rowNr = _rsFiltered.searchRowById(_rs.getCurrentRowId())
                if(_rowNr != NOT_FOUND) :
                    _rs.setCurrentValues(_rsFiltered.getValues(_rowNr))
                    _putBack += 1
                #end
            #end
            if(_putBack < _rsFiltered.getRowCount()) :
                print_error_msg("UPDATE: Could not put Back all filtered Values\n")
                return False
            #end
            
            
            #// Remove columns added from WHERE Expression
            while(len(_rs.colNames) > _colCount) :
                _rs.removeColumn(len(_rs.colNames)-1)
            #end
            
            #// Open Table
            _fp = self.openTableWrite(_sqlQuery.tables[0])
            if(_fp == None or _fp == False or _fp == "") :
                print_error_msg("Open the Table " + _sqlQuery.tables[0] + " (for WRITE) failed \n")
                return False
            #end
            self.writeTable(_fp,_rs)
            self.closeTable(_fp)
            return _rsFiltered.getRowCount()
        #end
    #end
    
    """
    /***********************************
             Create Table Query
    ************************************/
    """
    #// executes a SQL CREATE TABLE Statement
    #// param: SqlQuery Object
    #// returns True or False
    def executeCreateTableQuery(self, _sqlQuery) :
        clearstatcache()
        _filename = self.dbFolder + _sqlQuery.tables[0] + TABLE_FILE_EXT
        
        #// checks
        if(_sqlQuery.tables[0] == False or _sqlQuery.tables[0] == None or _sqlQuery.tables[0] == "") :
            print_error_msg("Invalid Table " + _sqlQuery.tables[0])
            return False
        #end
        if(os.path.isfile(_filename) != False) :
            print_error_msg("Table " + _sqlQuery.tables[0] + " allready exists \n")
            return False
        #end
        if(len(_sqlQuery.colNames)!=len(_sqlQuery.colTypes)) :
            print_error_msg("There's not a type defined for each column \n")
            return False
        #end
        for _i in range(_sqlQuery.colTypes) :
            _tmp = _sqlQuery.colTypes[_i].lower()
            if( not (_tmp == COL_TYPE_INT or _tmp == COL_TYPE_STRING or _tmp==COL_TYPE_INC) ) :
                print_error_msg("Column Type " + _tmp + " not supported \n")
                return False
            #end
        #end
            
        
        #// write file    
        _fp = open (_filename, "w")
        
        _rsParser = ResultSetParser()
        
        _fp.write(_rsParser.parseLineFromRow(_sqlQuery.colNames))
        _fp.write("\n")
        _fp.write(_rsParser.parseLineFromRow(_sqlQuery.colTypes))
        _fp.write("\n")
        _fp.write(_rsParser.parseLineFromRow(_sqlQuery.fieldValues))
                
        _fp.close()
        os.chmod(_filename, "0777")
        return True    
    #end
    
    """
    /***********************************
             Drop Table Query
    ************************************/
    """
    #// executes a SQL DROP TABLE Statement 
    #// param: SqlQuery Object
    #// returns True or False
    def executeDropTableQuery(self, sqlQuery) :
        clearstatcache()
        if(_sqlQuery.colNames[0] == None or _sqlQuery.colNames[0] == False or _sqlQuery.colNames[0] == "") :
            return False
        
        for i in range(_sqlQuery.colNames) :
            _filename = self.dbFolder + _sqlQuery.colNames[_i]  + TABLE_FILE_EXT
            _rc = os.remove(_filename)
            if(_rc == False or _rc == None or _rc == "" ) :
                print_error_msg("DROP TABLE " + _sqlQuery.colNames[_i] + " failed \n")
                return False
            #end
        #end
        return True    
    #end
    
    """
    /***********************************
             List Tables Query
    ************************************/
    """
    #// executes a LIST TABLES Statement 
    #// param: SqlQuery Object
    #// returns: A ResultSet Object with a single column "table"
    def executeListTablesQuery(self, sqlQuery) :
        _rs = ResultSet()
        _rs.colNames = ["table"]
        _rs.colAliases = [""]
        _rs.colTables = [""]
        _rs.colTableAliases = [""]
        _rs.colTypes = [COL_TYPE_STRING]
        _rs.colDefaultValues = [""]
        _rs.colFuncs = [""]
        _rs.colFuncsExecuted = [False]
        
        
        _handle = os.purePath(self.dbFolder)
        
        
        _rs.reset()
        _file = os.listdir(_handle)
        while (_file) : 
            _filename1 = self.dbFolder + _file
            if (_file != "." and _file != ".." and os.path.isfile(_filename1)) : 
                _strSize = len(_file) - len(TABLE_FILE_EXT)
                _rs.appendRow([_file[0:_strSize]])
            #end 
        #end
        
        #// apply WHERE Statement
        if(_sqlQuery.where_expr) :
            _ep = ExpressionParser()
            _et = _ep.getExpressionTree(_sqlQuery.where_expr)
            _rs = _et.getFilteredResultSet(_rs)
        #end 
            
        #// Order ResultSet
        if(len(_sqlQuery.orderColumns) > 0) :
            _rs.orderRows(_sqlQuery.orderColumns,_sqlQuery.orderTypes)
        #end

        #// Group ResultSet (process GROUP BY)
        if(len(_sqlQuery.groupColumns)>0) :
            _rs = _rs.groupRows(_sqlQuery.groupColumns, _sqlQuery.limit)
        #end

        #// Apply Limit        
        _rs.reset()
        _rs = _rs.limitResultSet(_sqlQuery.limit)
        
        _rs.reset()
        return _rs
    #end
    
    """
    /***********************************
             Create Database Query
    ************************************/
    """
    #// executes a SQL CREATE DATABASE Statement 
    #// param: SqlQuery Object
    #// returns True or False
    def executeCreateDatabaseQuery(self, sqlQuery) :
        clearstatcache()
        if(self.dbFolder != (DB_DIR + ROOT_DATABASE) and self.dbFolder != (DB_DIR + ROOT_DATABASE + "/")) :
            print_error_msg("Databases can only be created with a ROOT_DATABASE instance!\n")
            return False
        #end
        if(_sqlQuery.colNames[0] == False or _sqlQuery.colNames[0] == None or _sqlQuery.colNames[0] == "") :
            return False
        
        _directory = self.dbFolder + _sqlQuery.colNames[0]
        if not os.path.exists(_directory):
            os.makedirs(_directory)
            os.chmod(_directory, "0777")
        if(_rc == False or _rc == None or _rc == "") :
            print_error_msg("Cannot create Database " + _sqlQuery.colNames[0] + "\n")
            return False
        #end
        return True
    #end
    
    """
    /***********************************
             Drop Database Query
    ************************************/
    """
    #// executes a SQL DROP DATABASE Statement 
    #// param: SqlQuery Object
    #// returns True or False
    def executeDropDatabaseQuery(self, sqlQuery) :
        clearstatcache()
        if((self.dbFolder != DB_DIR + ROOT_DATABASE) and (self.dbFolder != DB_DIR + ROOT_DATABASE + "/")) :
            print_error_msg("Databases can only be deleted with a ROOT_DATABASE instance! \n")
            return False
        #end
        if(_sqlQuery.colNames[0] == False or _sqlQuery.colNames[0] == None or _sqlQuery.colNames[0] == "") :
            return False
        
        _directory = self.dbFolder + _sqlQuery.colNames[0]
        
        #// delete all tables
        _dirHandle = os.purePath(_directory)
        _file = os.listdir(_dirHandle)
        while (_file) :
            _newFilename = _directory + "/" + _file
            if (_file != "." and _file != ".." and os.path.isfile(_newFilename)) : 
                _rc = os.unlink(_newFilename)
                if(_rc == False or _rc == None or _rc == "") :
                    print_error_msg("Cannot drop Database: Deleting the table _file failed\n")
                #end
                debug_print(_file + " \n")
            #end 
        #end
        
        _rc= os.path.rmdir(_directory)
        if(_rc == False or _rc == None or _rc == "") :
            print_error_msg("Cannot drop Database " + _sqlQuery.colNames[0])
            return False
        #end
        return True    
    #end
    
    """
    /***********************************
             Select Query
    ************************************/
    """
    #// executes a SQL SELECT STATEMENT and returns a ResultSet 
    #// param: SqlQuery Object
    def executeSelectQuery(self, sqlQuery) :

        global _g_sqlGroupingFuncs
        global _g_sqlSingleRecFuncs



        _resultSets=array()        
        
        #// create a copy
        _aliases=_sqlQuery.colAliases
        _funcs=_sqlQuery.colFuncs
            
            
        #// Read all Tables
        for _i in range(_sqlQuery.tables) :
            debug_print ("[executeSelectQuery] Reading table " + _sqlQuery.tables[_i] + " \n") 
            _resultSets[_i] = self.readTable(_sqlQuery.tables[_i])
            if( _resultSets[_i] == False or _resultSets[_i] == None or _resultSets[_i] == "") :
                print_error_msg("Reading Table " + _sqlQuery.tables[_i] + " failed \n")
                return False
            #end
            _resultSets[_i].setColumnTableForAll(_sqlQuery.tables[_i])
            _resultSets[_i].setColumnTableAliasForAll(_sqlQuery.tableAliases[_i])
            
            #// set all functions to the ResultSet of the current table
            #// if table and column name matches
            debug_print("[executeSelectQuery] Setting functions for the current table: \n")
            for _j in range(_funcs) :
                if((_funcs[_j] == None or _funcs[_j] == False or _funcs[_j] == "") or (_sqlQuery.colNames[_j] == None or _sqlQuery.colNames[_j] == False or _sqlQuery.colNames[_j] == "")) :
                    continue
                                
                if(_sqlQuery.colTables[_j] ==_sqlQuery.tables[_i] or (len(_sqlQuery.tableAliases[_i]) > 0 and (_sqlQuery.colTables[_j] == _sqlQuery.tableAliases[_i]))) :
                    _colNr = _resultSets[_i].findColNrByAttrs(_sqlQuery.colNames[_j],_sqlQuery.colTables[_j],"","str","",_funcs[_j],"",True)
                    
                    if(_colNr == NOT_FOUND) :
                        continue
                    #end                    
                    
                    #// create a new column for each function
                    _resultSets[_i].addColumn(_sqlQuery.colNames[_j],_sqlQuery.colAliases[_j],_sqlQuery.colTables[_j],"","str","",_funcs[_j],"",True)
                    _funcs[_j] = ""
                #end
                
            #end
            
            #// set all aliases where table, column name and function matches
            debug_print("[executeSelectQuery] Setting aliases for the current table: \n")
            for _j in range(_aliases)  :
                if(_aliases[_j] == False or _aliases[_j] == None or _aliases[_j] == "") :
                    continue
                
                if(_sqlQuery.colTables[_j]==_sqlQuery.tables[_i] or _sqlQuery.colTables[_j]==_sqlQuery.tableAliases[_i]) :    
                    _colNr = _resultSets[_i].findColNrByAttrs(_sqlQuery.colNames[_j],_sqlQuery.colTables[_j],_sqlQuery.colFuncs[_j])
                    if(_colNr == NOT_FOUND) :
                        _resultSets[_i].setColumnAlias(_colNr,_aliases[_j])
                        _aliases[_j] = ""                
                    #end
                #end
            #end
            
            if(TXTDBAPI_DEBUG == 1 or TXTDBAPI_DEBUG == True) :
                debug_print(" \n[executeSelectQuery] Dump of Table _i (" + _sqlQuery.tables[_i] + "): \n" + _resultSets[_i].dump())
            #end
        #end
        
        #// set remaining functions to the ResultSet where column name matches
        debug_print("[executeSelectQuery] Setting remaining functions where column name matches: \n")
        for _i in range(_resultSets)  :
            for _j in range(_funcs)  :
                if((_funcs[_j] == False or _funcs[_j] == None or _funcs[_j] == "") or (_sqlQuery.colNames[_j] == False or _sqlQuery.colNames[_j] == "" or _sqlQuery.colNames[_j] == None)) :
                    continue
                                
                _colNr = _resultSets[_i].findColNrByAttrs(_sqlQuery.colNames[_j],"","str","",_funcs[_j],"",True)
                if(_colNr == NOT_FOUND) :
                    #// 'text' or 123 ? => add column
                    if( not _sqlQuery.colNames[_j].isnumeric() or (has_quotes(_sqlQuery.colNames[_j]))) :
                        continue
                    #end
                    debug_print("Adding function with quoted string or number paremeter! \n")
                #end
                    
                #// create a new column for each function
                _resultSets[_i].addColumn(_sqlQuery.colNames[_j],_sqlQuery.colAliases[_j],_sqlQuery.colTables[_j],"","str","",_funcs[_j],"",True)
                _funcs[_j] = ""

            #end
        #end
        
        #// set remaining aliases where column name and function matches
        debug_print("[executeSelectQuery] Setting remaining aliases where column name and function matches: \n")
        for _i in range(_resultSets) :
            for _j in range(_aliases)  :
                if(_aliases[_j] == False or _aliases[_j] == None or _aliases[_j] == "") :
                    continue
                _colNr = _resultSets[_i].findColNrByAttrs(_sqlQuery.colNames[_j],"",_sqlQuery.colFuncs[_j])
                if(_colNr != NOT_FOUND) :
                    _resultSets[_i].setColumnAlias(_colNr,_aliases[_j])
                    _aliases[_j] = ""
                #end
            #end
        #end


        debug_print("[executeSelectQuery] Executing single-rec functions (on the separate ResultSet's): \n")
        #// execute single-rec functions (on the separate ResultSet's)
        for _i in range(_resultSets) :
            _resultSets[_i].executeSingleRecFuncs()
        #end
        
        
        #// A query without tables ? => make a dummy ResultSet
        _dummyResultSet = False
        if(len(_sqlQuery.tables) == 0) :
            _dummyResultSet = True
            _rsMaster = ResultSet()    
            _rsMaster.addColumn ("(dummy)", "(dummy)", "(dummy)", "(dummy)", "str", "(dummy)", "", "", True)
            _rsMaster.append()
        
        #// else: real ResultSet
        #end 
        else :
            _dummyResultSet = False
            
        #// Perform JOIN's
        for _join in _sqlQuery.joins :
                if(_rsMaster == False or _rsMaster == None or _rsMaster == "") :
                    _rsMaster = _resultSets[_join.leftTableIndex]
                    _resultSets[_join.leftTableIndex] = 0
                #end
                
                #// INNER => Nothing special
                if(_join.type == JOIN_INNER) :
                    _rsMaster = _rsMaster.joinWithResultSet(_resultSets[_join.rightTableIndex])
                    _ep = ExpressionParser()
                    _et = _ep.getExpressionTree(_join.expr)
                    _rsMaster = _et.getFilteredResultSet(_rsMaster)
                    _resultSets[_join.rightTableIndex]=0
                    
                #// LEFT OUTER JOIN => keep all of the left
                #end 
                elif(_join.type == JOIN_LEFT) :
                    #// generate new row id's for the left
                    _rsMaster.generateRowIds()
                    #// copy the left
                    _copy = _rsMaster.copy()
                    _rsMaster = _rsMaster.joinWithResultSet(_resultSets[_join.rightTableIndex],0)
                    _ep = ExpressionParser()
                    _et = _ep.getExpressionTree(_join.expr)
                    _rsMaster = _et.getFilteredResultSet(_rsMaster)
                    _resultSets[_join.rightTableIndex]=0
                    #// add back missing
                    _rsMaster.addMissingRows(_copy)
                    
                    
                #// RIGHT OUTER JOIN => keep all of the right
                #end 
                elif(_join.type == JOIN_RIGHT) :
                    #// generate new row id's for the right
                    _resultSets[_join.rightTableIndex].generateRowIds()
                    #// copy the right
                    _copy =_resultSets[_join.rightTableIndex].copy()
                    _rsMaster = _rsMaster.joinWithResultSet(_resultSets[_join.rightTableIndex],1)
                    _ep = ExpressionParser()
                    _et = _ep.getExpressionTree(_join.expr)
                    _rsMaster = _et.getFilteredResultSet(_rsMaster)
                    _resultSets[_join.rightTableIndex]=0                
                    #// add back missing
                    _rsMaster.addMissingRows(_copy)
                #end
            #end
        
                #// join the remaining ResultSet's
                if(_rsMaster == False or _rsMaster == None or _rsMaster == "") :
                    _rsMaster = _resultSets[0]
                    _i = 1
                #end 
                else :
                    _i = 0
                #end
                for _i in range(_resultSets) :
                    if(_resultSets[_i] != 0) :
                        _rsMaster = _rsMaster.joinWithResultSet(_resultSets[_i])
                    #end
                #end
            #end
        
        
        
        
        #// from here we only work with _rsMaster and can free the other ResultSet's
        _resultSets = None
        _resultSets = ""
        
        
        #// generate additional columns for COUNT(*) functions
        debug_print("[executeSelectQuery] Adding COUNT(*) functions \n ")
        for _i in range(_funcs) :
            if((_funcs[_i] != None or _funcs[_i] != False or _funcs[_i] != "") and _sqlQuery.colNames[_i] == "*") :
                _rsMaster.addColumn(_sqlQuery.colNames[_i],_sqlQuery.colAliases[_i],_sqlQuery.colTables[_i],"","","str","",_funcs[_i],execFunc(_funcs[_i],""))
                _funcs[_i] = ""
            #end
        #end
        
        
        #// generate additional columns for the remaining functions (functions without params)
        for _i in range(_funcs) :
            if(_funcs[_i] != False or _funcs[_i] != None or _funcs[_i] != "") :
                _rsMaster.addColumn(_sqlQuery.colNames[_i],_sqlQuery.colAliases[_i],"","","str","",_funcs[_i],execFunc(_funcs[_i],""))
            #end
        #end


        #// generate additional columns from the WHERE-expression
        _rsMaster.generateAdditionalColumnsFromWhereExpr(_sqlQuery.where_expr)
        
        #// generate additional columns from ORDER BY
        _rsMaster.generateAdditionalColumnsFromArray(_sqlQuery.orderColumns)
        
        #// generate additional columns from GROUP BY
        _rsMaster.generateAdditionalColumnsFromArray(_sqlQuery.groupColumns)
        
        #// execute the new single-rec functions (on the Master ResultSet)
        _rsMaster.executeSingleRecFuncs()
        
        
        #// generate new row id's 
        _rsMaster.generateRowIds()
        
        
        
        
            
        debug_print(" \n[executeSelectQuery] Master ResultSet:</b> \n")
        if(TXTDBAPI_DEBUG == 1 or TXTDBAPI_DEBUG == True) :
            _rsMaster.dump()
        
        
        #// apply WHERE expression
        if(_sqlQuery.where_expr) :
            _ep = ExpressionParser()
            _et = _ep.getExpressionTree(_sqlQuery.where_expr)
            _rsMaster = _et.getFilteredResultSet(_rsMaster)
        #end 
        #// free _ep
        _ep = None
        _ep = ""
        
        #// stop if the WHERE expression failed
        if(txtdbapi_error_occurred()) :
            return False
        #end
        

        #// check if we can use some optimization 
        #// (use the limit in group by, but only if there are no grouping function
        #// in the groupRows. To be able to do this we must order before grouping)
        _optimizedPath = True
        if((_sqlQuery.limit == False or _sqlQuery.limit == None or _sqlQuery.limit == "") or (_sqlQuery.orderColumns == False or _sqlQuery.orderColumns == None or _sqlQuery.orderColumns == "")) :
            _optimizedPath = False
        #end 
        else :
            for _i in range(_sqlQuery.colFuncs) :
                if(_sqlQuery.colFuncs[_i] == _g_sqlGroupingFuncs) :
                    _optimizedPath = False
                    break
                #end
            #end                
        #end
        if(_optimizedPath) :
            debug_print("[executeSelectQuery] Using optimized path! \n")
        #end 
        else :
            debug_print("[executeSelectQuery] Using normal path! \n")
        #end
        
        #// Order ResultSet (if optimizedPath)
        if(_optimizedPath) :    
            debug_print("[executeSelectQuery] Calling orderRows() (optimized path).. \n")
            if(len(_sqlQuery.orderColumns) > 0) :
                _rsMaster.orderRows(_sqlQuery.orderColumns,_sqlQuery.orderTypes)
            #end
        #end

        
        #// Group ResultSet (process GROUP BY)
        _numGroupingFuncs = 0
        for _i in range(_sqlQuery.colFuncs) :
            if((_sqlQuery.colFuncs[_i] != False or _sqlQuery.colFuncs[_i] != None or _sqlQuery.colFuncs[_i] != "") and _sqlQuery.colFuncs[_i] == _g_sqlGroupingFuncs) :
                _numGroupingFuncs += 1
                break
            #end
        #end
        if(_numGroupingFuncs > 0 or len(_sqlQuery.groupColumns) > 0) :
            debug_print("[executeSelectQuery] Calling groupRows().. \n")
            _rsMaster = _rsMaster.groupRows(_sqlQuery,_optimizedPath)
        #end
        
        #// Order ResultSet (if NOT optimizedPath)
        if(_optimizedPath == False or _optimizedPath == None or _optimizedPath == "") :    
            debug_print("[executeSelectQuery] Calling orderRows() (normal path).. \n")
            if(len(_sqlQuery.orderColumns)>0) :
                _rsMaster.orderRows(_sqlQuery.orderColumns,_sqlQuery.orderTypes)
            #end
        #end

        #// add direct value columns
        debug_print("[executeSelectQuery] Adding direct value columns.. \n")
        for _i in range(_sqlQuery.colNames) :
            if((_sqlQuery.colNames[_i] != False or _sqlQuery.colNames[_i] != None or _sqlQuery.colNames[_i] != "") and 
               (_sqlQuery.colNames[_i].isnumeric()) or (has_quotes(_sqlQuery.colNames[_i])) and 
               (_sqlQuery.colTables[_i] != False or _sqlQuery.colTables[_i] != None or _sqlQuery.colTables[_i] != "") and 
               (_sqlQuery.colFuncs[_i] == False or _sqlQuery.colFuncs[_i] == None or _sqlQuery.colFuncs[_i] == "") and 
               _rsMaster.findColNrByAttrs(_sqlQuery.colNames[_i],"","") == NOT_FOUND) :
                   _value = _sqlQuery.colNames[_i]
                   if(has_quotes(_value)) :
                       remove_quotes(_value)
                   #end
                   _rsMaster.addColumn(_sqlQuery.colNames[_i],_sqlQuery.colAliases[_i],"","","str","","",_value,True)
            #end
        #end
        
        #// return only the requested columns
        debug_print("[executeSelectQuery] Removing unwanted columns... \n")
        _rsMaster.filterByColumnNamesInSqlQuery(_sqlQuery)
        
        
        #// order columns (not their data)
        debug_print("[executeSelectQuery] Ordering columns (amog themself)... \n")
        if(_rsMaster.orderColumnsBySqlQuery(_sqlQuery) == None or _rsMaster.orderColumnsBySqlQuery(_sqlQuery) == False or _rsMaster.orderColumnsBySqlQuery(_sqlQuery) == "" ) :
            print_error_msg("Ordering the Columns (themself) failed\n")
            return False
        #end    
        
        #// process DISTINCT
        if(_sqlQuery.distinct == 1) :
            _rsMaster = _rsMaster.makeDistinct(_sqlQuery.limit)
        #end
        
        #// Apply Limit        
        _rsMaster.reset()
        _rsMaster = _rsMaster.limitResultSet(_sqlQuery.limit)
        verbose_debug_print (" \nLimited ResultSet: \n")
        if(TXTDBAPI_VERBOSE_DEBUG == 1 or TXTDBAPI_VERBOSE_DEBUG == True) :
            _rsMaster.dump()

        _rsMaster.reset()
        return _rsMaster
    #end
    

#end