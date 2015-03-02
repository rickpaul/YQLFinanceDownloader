import pprint
import string
import sqlite3 as sq
from YQLFinanceTemplates import FinDBFileName as defaultDB

########################################Common Insert and Update Statements
def stringify(someString):
	return '"' + someString + '"'

def generateInsertStatement(table, columns, values):
	columnsString = ' ( ' + string.join(columns,', ') + ' ) '
	valuesString = ' ( ' + string.join(values,', ') + ' ) '
	return ('insert into ' + table + columnsString + 'values' + valuesString + ';')

def generateUpdateStatement(table, setColumns, setValues, whereColumns, whereValues):
	setStatements = [(a + '=' + str(b)) for (a,b) in zip(setColumns,setValues)]
	whereStatements = [(a + '=' + str(b)) for (a,b) in zip(whereColumns,whereValues)]
	setString = string.join(setStatements,' , ')
	whereString = string.join(whereStatements,' and ')
	return ('update ' + table + ' set ' + setString + ' where ' + whereString + ';')

def get_insert_SectorIndustryInfo_into_SectorIndustryDB_Statement(indID, indName, sectName):
	table = 'T_SECTOR_INDUSTRY';
	columns = ['int_industry_id','txt_industry_name','txt_sector_name']
	if (sectName[0] is not '"') and (sectName[0] is not "'"):
		sectName = stringify(sectName)
	if (indName[0] is not '"') and (indName[0] is not "'"):
		indName = stringify(indName)
	values = [str(indID), indName, sectName]
	return generateInsertStatement(table, columns, values)

def get_insert_Ticker_into_TickerDB_Statement(compTicker):
	table = 'T_TICKER';
	columns = ['txt_ticker']
	if (compTicker[0] is not '"') and (compTicker[0] is not "'"):
		compTicker = stringify(compTicker)
	values = [compTicker]
	return generateInsertStatement(table, columns, values)

def get_insert_Basics_into_StockInfoDB_Statement(compTicker, compName, indID):
	table = 'T_STOCK_INFORMATION';
	columns = ['txt_ticker', 'txt_company_name', 'int_industry_id']
	if (compTicker[0] is not '"') and (compTicker[0] is not "'"):
		compTicker = stringify(compTicker)
	if (compName[0] is not '"') and (compName[0] is not "'"):
		compName = stringify(compName)
	values = [compTicker, compName, str(indID)]
	return generateInsertStatement(table, columns, values)

def get_update_ReadTime_into_SectorIndustryDB_Statement(time, indID):
	table = 'T_SECTOR_INDUSTRY'
	setColumns = ['dt_last_accessed']
	setValues = [str(time)]
	whereColumns = ['int_industry_id']
	whereValues = [str(indID)]
	return generateUpdateStatement(table, setColumns, setValues, whereColumns, whereValues)

########################################Useful Select Statements
def checkIfTickerAlreadyInDB(conn, cursor, compTicker, verbose = False):
	if (compTicker[0] is not '"') and (compTicker[0] is not "'"):
		compTicker = stringify(compTicker)
	statement = 'select count(*) from T_TICKER where txt_ticker = ' + compTicker
	cursor.execute(statement)
	conn.commit()
	exists = cursor.fetchall()[0][0] > 0
	if verbose and exists:
			print ('Ticker ' + compTicker + ' already in Ticker Database')
	return exists

########################################Database Helper Code
########################################Database Helper Code / Commiting Code with Silent Failure
#TODO: You can do batch inserts in sql... maybe worth a try later
def commitDBStatement(conn, cursor, statement, verbose = False):
	try:
		if verbose:
			print "Attempting ..." + statement,
		cursor.execute(statement)
		conn.commit()
		if verbose:
			print "... Succeeded!"
		return (True, "")
	except Exception as e:
		if not verbose:
			print statement + " Failed!"
		else:
			print "... Failed!"
		print e
		return (False, str(e))

########################################Outward-facing Table Peeking for Debugging
def executeSimpleDatabaseStatement(statement, showCols=True, databaseFile=defaultDB):
	# Open DB Connection
	conn = sq.connect(databaseFile)
	c = conn.cursor()
	try:
		queryType = string.lower(statement[0:string.find(statement,' ')])
		if queryType == 'select':
			if showCols:
				tableNameStart = string.find(statement,'from ') + 5
				tableNameEnd = string.find(statement,' ',tableNameStart)
				tableName = statement[tableNameStart:tableNameEnd]
				pragmaCommand = 'pragma table_info("' + tableName + '")'
				c.execute(pragmaCommand)
				pragma = c.fetchall()
				print 'COLUMN NAMES:'
				print [col[1] for col in pragma]
				print 'COLUMN TYPES:'	
				print [col[2] for col in pragma]
			print 'VALUES:'
			c.execute(statement)
			results = c.fetchall()
			if len(results) == 0:
				print 'NOTHING FOUND'
			else:
				pprint.pprint(results)
		elif queryType == 'insert' or queryType == 'update':
			c.execute(statement)
			c.commit()
		else:
			raise NameError('simple database query type not recognized')
	except:
		raise
	finally:
		# Close DB Connection
		conn.close()

def peerIntoDatabase(tableName, databaseFile=defaultDB, displayPragma=False, limit=10):
	# Open DB Connection
	conn = sq.connect(databaseFile)
	c = conn.cursor()
	try:
		# Print Table Length
		viewLengthCommand = 'select count(*) from ' + tableName
		c.execute(viewLengthCommand)
		print 'There are ' + str(c.fetchall()[0][0]) + ' rows in ' + tableName
		# Get Table Column Names and Types
		pragmaCommand = 'pragma table_info("' + tableName + '")'
		c.execute(pragmaCommand)
		pragma = c.fetchall()
		#		(pragma columns are as follows:)
		#		(cid,name,type,notnull,dflt_value,pk)
		columnNames = [col[1] for col in pragma]
		columnTypes = [col[2] for col in pragma]
		if displayPragma:
			pprint.pprint(pragma)
		# Print First Few Rows of Table		
		viewFirstFewCommand = 'select * from ' + tableName + ' limit ' + str(limit)
		c.execute(viewFirstFewCommand)
		firstFew = c.fetchall()
		print columnTypes
		print columnNames
		pprint.pprint(firstFew)
	except:
		raise
	finally:
		# Close DB Connection
		conn.close()