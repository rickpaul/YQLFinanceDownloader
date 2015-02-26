import os
import sqlite3 as sq
import sys
import string
import pprint
import YQLFinanceTemplates as tmplts


#TODO: Give ability to create only one database at a time


tableCreationInstructions = []

createSectorIndustryTable = '''
CREATE TABLE T_SECTOR_INDUSTRY
(
int_industry_id INTEGER NOT NULL UNIQUE PRIMARY KEY,
txt_industry_name TEXT NOT NULL UNIQUE,
txt_sector_name TEXT NOT NULL,
dt_last_accessed REAL DEFAULT 0
);
'''
tableCreationInstructions.append(createSectorIndustryTable)

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
tableCreationInstructions.append(createTickerTable)

createStockInformationTable = '''
CREATE TABLE T_STOCK_INFORMATION
(
txt_ticker TEXT NOT NULL PRIMARY KEY,
txt_company_name TEXT,
txt_exchange TEXT,
int_industry_id INTEGER,
dt_date_retrieved REAL,
dt_start_trading REAL,
dt_end_trading REAL,
FOREIGN KEY (txt_ticker) REFERENCES T_TICKER (txt_ticker)
FOREIGN KEY (int_industry_id) REFERENCES T_SECTOR_INDUSTRY (int_industry_id)
);
'''
tableCreationInstructions.append(createStockInformationTable)

createDeepStockInformationTable = '''
CREATE TABLE T_COMPANY_INFORMATION
(
txt_ticker TEXT NOT NULL PRIMARY KEY,
txt_metric TEXT NOT NULL,
dt_date_retrieved REAL NOT NULL,
dt_end_period REAL,
bool_found INTEGER,
FOREIGN KEY (txt_ticker) REFERENCES T_TICKER (txt_ticker)
);
'''
tableCreationInstructions.append(createStockInformationTable)

createCashFlowStatementTable = '''
CREATE TABLE T_CASH_FLOW_STATEMENT
(
txt_ticker TEXT NOT NULL PRIMARY KEY,
txt_metric TEXT NOT NULL,
dt_date_retrieved REAL NOT NULL,
dt_begin_period REAL,
dt_end_period REAL,
bool_found INTEGER,
FOREIGN KEY (txt_ticker) REFERENCES T_TICKER (txt_ticker)
);
'''
tableCreationInstructions.append(createCashFlowStatementTable)

createIncomeStatementTable = '''
CREATE TABLE T_INCOME_STATEMENT
(
txt_ticker TEXT NOT NULL PRIMARY KEY,
txt_metric TEXT NOT NULL,
dt_date_retrieved REAL NOT NULL,
dt_begin_period REAL,
dt_end_period REAL,
bool_found INTEGER,
FOREIGN KEY (txt_ticker) REFERENCES T_TICKER (txt_ticker)
);
'''
tableCreationInstructions.append(createIncomeStatementTable)

createBalanceSheetTable = '''
CREATE TABLE T_BALANCE_SHEET
(
txt_ticker TEXT NOT NULL PRIMARY KEY,
txt_metric TEXT NOT NULL,
dt_date_retrieved REAL NOT NULL,
dt_end_period REAL,
bool_found INTEGER,
FOREIGN KEY (txt_ticker) REFERENCES T_TICKER (txt_ticker)
);
'''
tableCreationInstructions.append(createBalanceSheetTable)

createUnrecordedMetricsTable = '''
CREATE TABLE T_UNRECORDED_METRICS
(
int_record_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
txt_ticker TEXT,
dt_date_retrieved REAL NOT NULL,
txt_metric TEXT NOT NULL,
FOREIGN KEY (txt_ticker) REFERENCES T_TICKER (txt_ticker)
);
'''
tableCreationInstructions.append(createUnrecordedMetricsTable)

def doOneTimeDBCreation(force = False):
	# Find database path and directory
	filename = tmplts.DBFileName
	directory = os.path.dirname(filename)
	# Create directory if necessary
	try: 
		os.stat(directory)
	except:
		os.mkdir(directory)
	# Remove file if it already exists
	if force:
		try: 
			os.stat(filename)
			os.remove(filename)
		except:
			pass
	# Connect to Database
	conn = sq.connect(filename)
	c = conn.cursor()
	# Write Create Table Commands
	for instruction in tableCreationInstructions:
		summaryString = instruction[1:instruction.find('\n',3)]
		print "Executing "  + summaryString,
		try:
			c.execute(instruction)
			conn.commit()
		except:
			print "... Failed!"
			raise
		else:
			print "... Executed!"
	# Close Database connections
	conn.close()

if __name__ == '__main__':
	if len(sys.argv) > 1 and '-f' in sys.argv[1:]: #f for force
		doOneTimeDBCreation(True)
	else:
		try:
			doOneTimeDBCreation()
		except sq.OperationalError as e:
			print 'Database Creation Failed.'
			if 'already exists' in str(e):
				print '\tDatabases already exist. \n\tIf overwriting was your intent, use -f flag to force creation.'
			else:
				raise e
