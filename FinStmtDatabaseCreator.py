import os
import sys
import sqlite3 as sq
import YQLFinanceTemplates as YQLtmplts


fullTableCreationInstructions = {}

createSectorIndustryTable = '''
CREATE TABLE T_SECTOR_INDUSTRY
(
int_industry_id INTEGER NOT NULL UNIQUE PRIMARY KEY,
txt_industry_name TEXT NOT NULL UNIQUE,
txt_sector_name TEXT NOT NULL,
dt_last_accessed REAL DEFAULT 0
);
'''
fullTableCreationInstructions['T_SECTOR_INDUSTRY'] = createSectorIndustryTable

createTickerTable = '''
CREATE TABLE T_TICKER
(
txt_ticker TEXT UNIQUE NOT NULL PRIMARY KEY,
dt_latest_BS_download REAL DEFAULT 0,
dt_latest_IS_download REAL DEFAULT 0,
dt_latest_CF_download REAL DEFAULT 0,
bool_watching INTEGER DEFAULT 0 /*Are we watching this stock?*/
);
'''
fullTableCreationInstructions['T_TICKER'] = createTickerTable

createStockInformationTable = '''
CREATE TABLE T_STOCK_INFORMATION
(
txt_ticker TEXT NOT NULL PRIMARY KEY,
txt_company_name TEXT,
txt_exchange TEXT,
int_industry_id INTEGER,
dt_start_trading REAL,
dt_end_trading REAL,
int_market_cap INTEGER,
int_average_daily_vol INTEGER,
dt_date_retrieved REAL DEFAULT 0,
FOREIGN KEY (txt_ticker) REFERENCES T_TICKER (txt_ticker)
FOREIGN KEY (int_industry_id) REFERENCES T_SECTOR_INDUSTRY (int_industry_id)
);
'''
fullTableCreationInstructions['T_STOCK_INFORMATION'] = createStockInformationTable

# createDeepStockInformationTable = '''
# CREATE TABLE T_DEEP_STOCK_INFORMATION
# (
# txt_ticker TEXT NOT NULL PRIMARY KEY,
# txt_metric TEXT NOT NULL,
# dt_date_retrieved REAL NOT NULL,
# dt_end_period REAL NOT NULL,
# bool_found INTEGER NOT NULL,
# FOREIGN KEY (txt_ticker) REFERENCES T_TICKER (txt_ticker)
# );
# '''
# fullTableCreationInstructions['T_DEEP_STOCK_INFORMATION'] = createDeepStockInformationTable

# createCashFlowStatementTable = '''
# CREATE TABLE T_CASH_FLOW_STATEMENT
# (
# txt_ticker TEXT NOT NULL PRIMARY KEY,
# txt_metric TEXT NOT NULL,
# dt_date_retrieved REAL NOT NULL,
# dt_begin_period REAL NOT NULL,
# dt_end_period REAL NOT NULL,
# bool_found INTEGER NOT NULL,
# FOREIGN KEY (txt_ticker) REFERENCES T_TICKER (txt_ticker)
# );
# '''
# fullTableCreationInstructions['T_CASH_FLOW_STATEMENT'] = createCashFlowStatementTable

# createIncomeStatementTable = '''
# CREATE TABLE T_INCOME_STATEMENT
# (
# txt_ticker TEXT NOT NULL PRIMARY KEY,
# txt_metric TEXT NOT NULL,
# dt_date_retrieved REAL NOT NULL,
# dt_begin_period REAL NOT NULL,
# dt_end_period REAL NOT NULL,
# bool_found INTEGER NOT NULL,
# FOREIGN KEY (txt_ticker) REFERENCES T_TICKER (txt_ticker)
# );
# '''
# fullTableCreationInstructions['T_INCOME_STATEMENT'] = createIncomeStatementTable

# createBalanceSheetTable = '''
# CREATE TABLE T_BALANCE_SHEET
# (
# txt_ticker TEXT NOT NULL PRIMARY KEY,
# txt_metric TEXT NOT NULL,
# dt_date_retrieved REAL NOT NULL,
# dt_end_period REAL NOT NULL,
# bool_found INTEGER NOT NULL,
# FOREIGN KEY (txt_ticker) REFERENCES T_TICKER (txt_ticker)
# );
# '''
# fullTableCreationInstructions['T_BALANCE_SHEET'] = createBalanceSheetTable

# createUnrecordedMetricsTable = '''
# CREATE TABLE T_UNRECORDED_METRICS
# (
# int_record_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
# txt_ticker TEXT NOT NULL,
# dt_date_retrieved REAL NOT NULL,
# txt_metric TEXT NOT NULL,
# FOREIGN KEY (txt_ticker) REFERENCES T_TICKER (txt_ticker)
# );
# '''
# fullTableCreationInstructions['T_UNRECORDED_METRICS'] = createUnrecordedMetricsTable

manualTableCreationInstructions = {
			# 'T_SECTOR_INDUSTRY':createSectorIndustryTable,
			# 'T_TICKER':createTickerTable,
			'T_STOCK_INFORMATION':createStockInformationTable,
			# 'T_DEEP_STOCK_INFORMATION':createDeepStockInformationTable,
			# 'T_CASH_FLOW_STATEMENT':createCashFlowStatementTable,
			# 'T_INCOME_STATEMENT':createIncomeStatementTable,
			# 'T_BALANCE_SHEET':createBalanceSheetTable,
			# 'T_UNRECORDED_METRICS':createUnrecordedMetricsTable
		}

def get_TableExists_Statement(tableName):
	return 'select name from sqlite_master where type="table" and name = "' + tableName + '";'

def get_DropTable_Statement(tableName):
	return 'drop table if exists ' + tableName + ';'

def checkIfDBExistsWithTables(dbPath=YQLtmplts.FinDBFileName, manual=False):
	# Check if DB File Exists
	if not os.path.isfile(dbPath):
		return False

	# Find List of DBs to Create
	if manual:
		tableCreationInstructions = manualTableCreationInstructions
	else:
		tableCreationInstructions = fullTableCreationInstructions

	conn = sq.connect(dbPath)
	c = conn.cursor()
	# Check for Existence of Individual Tables
	try:
		for (tableName, instruction) in tableCreationInstructions.iteritems():
			statement = get_TableExists_Statement(tableName)
			print "Checking existence of "  + tableName + "...",
			c.execute(statement)
			if c.fetchall()[0][0] == 0:
				print "..." + tableName + " doesn't exist."
				return False
			print "... Exists!"
		print "DB exists with appropriate tables"
		return True
	except:
		raise
	finally:
		conn.close()


def doOneTimeDBCreation(force=False, dbPath=YQLtmplts.FinDBFileName, manual=False):
	# Find List of DBs to Create
	if manual:
		tableCreationInstructions = manualTableCreationInstructions
	else:
		tableCreationInstructions = fullTableCreationInstructions

	# Find Database Directory
	directory = os.path.dirname(dbPath)
	# Create Directory if Necessary
	if not os.path.exists(directory):
		os.makedirs(directory)
	# Connect to Database
	conn = sq.connect(dbPath)
	c = conn.cursor()
	try:
		for (tableName, instruction) in tableCreationInstructions.iteritems():
			# Find List of DBs to Drop, if Necessary
			if force:
				statement = get_DropTable_Statement(tableName)
				print "Dropping "  + tableName + "...",
				c.execute(statement)
				conn.commit()
				print "... Dropped!"
			summaryString = instruction[1:instruction.find('\n',3)]
			print "Executing "  + summaryString,
			c.execute(instruction)
			conn.commit()
			print "... Executed!"
	except:
		print "... Failed!"
		raise
	finally:
		conn.close()

if __name__ == '__main__':
	# Read Arguments
	force = len(sys.argv) > 1 and '-f' in sys.argv[1:]
	if len(sys.argv) > 1 and '-t' in sys.argv[1:]:
		dbPath=YQLtmplts.TestDBFileName
	else:
		dbPath=YQLtmplts.FinDBFileName
	manual = len(sys.argv) > 1 and '-m' in sys.argv[1:]
	# Do DB Creation
	try:
		doOneTimeDBCreation(force=force,dbPath=dbPath,manual=manual)
	except sq.OperationalError as e:
		print 'Database Creation Failed.'
		if 'already exists' in str(e):
			print '\tDatabases already exist. \n\tIf overwriting was your intent, use -f flag to force creation.'
		else:
			raise e
