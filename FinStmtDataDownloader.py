import sqlite3 as sq
import urllib2
import json
import time
import pprint
import YQLFinanceTemplates as YQLtmplts
import FinStmtDatabaseHelper as finDB

ONE_DAY = 60*60*24
ONE_WEEK = ONE_DAY*7
ONE_MONTH = ONE_DAY*30
TIME_BETWEEN_COMPANY_INFO_UPDATES = ONE_WEEK #CONSIDER: make this a month? Depends on how much you care about start/end trading dates
TIME_BETWEEN_COMPANY_LIST_UPDATES = ONE_MONTH
SECTOR_LIST_INDUSTRY_UPDATE_LIMIT = 10
COMPANY_LIST_INDUSTRY_UPDATE_LIMIT = 20

def to_DB_fillCompanyInfoDatabase(	companyNumUpdateLimit=COMPANY_LIST_INDUSTRY_UPDATE_LIMIT,
									companyTimeUpdateLimit=TIME_BETWEEN_COMPANY_INFO_UPDATES,
									DBFileName=YQLtmplts.FinDBFileName,
									verbose=False):
	# Grab a few Companies to Update
	updateableCompanies = from_DB_getUpdateableCompanyList(	DBFileName=DBFileName, 
															updateableTimeLimit=companyTimeUpdateLimit)
	numRemaining = len(updateableCompanies)
	print 'There are ' + str(numRemaining) + ' companies that need updating.'
	updateableCompanies = updateableCompanies[0:companyNumUpdateLimit]
	numUpdating = len(updateableCompanies)
	print 'This update will update the following ' + str(numUpdating) + ' companies: ' + str(updateableCompanies)
	# Open DB Connection
	conn = sq.connect(DBFileName)
	c = conn.cursor()
	try:
		now = str(int(time.time()))
		# Retrieve data from YQL
		exchangeEtcWriteRows = from_YQL_getCompanyExchangeVolumeMktCap(updateableCompanies)
		startEndWriteRows = from_YQL_getCompanyStartEndTradingDates(updateableCompanies)
		# pprint.pprint(exchangeEtcWriteRows) #TEST
		# pprint.pprint(startEndWriteRows) #TEST
		# return #TEST
		numSuccesses = 0
		for company in updateableCompanies:
			exchangeEtcInfo = exchangeEtcWriteRows[company]
			startEndInfo = startEndWriteRows[company]
			updateStatement = finDB.get_update_into_StockInfoDB_Statement(	company, 
																			exchangeEtcInfo['exchange'], 
																			exchangeEtcInfo['mktCap'], 
																			exchangeEtcInfo['avgVol'], 
																			startEndInfo['startTrade'], 
																			startEndInfo['endTrade'], 
																			now)
			numSuccesses += finDB.commitDBStatement(conn, c, updateStatement, verbose=verbose)[0]
		return (numRemaining - numSuccesses) > 0
	except:
		# print "**** **** **** **** " + "total rows to be added"
		# pprint.pprint(exchangeEtcWriteRows)
		# pprint.pprint(startEndWriteRows)
		# print "**** **** **** **** " + "specific company row information"
		# print company
		# pprint.pprint(exchangeEtcInfo)
		# pprint.pprint(startEndInfo)
		raise
	finally:
		# Close DB Connection
		conn.close()

########################################Code to Fill Company Ticker Database / Find industries with non-recent updates
def from_DB_getUpdateableCompanyList(	DBFileName=YQLtmplts.FinDBFileName, 
										updateableTimeLimit=TIME_BETWEEN_COMPANY_INFO_UPDATES):
	# Open DB Connection
	conn = sq.connect(DBFileName)
	c = conn.cursor()
	try:
		# Get Time Delay
		updateableTime = int(time.time()) - updateableTimeLimit;
		# Generate Statement
		getListCommand = (	'select txt_ticker from T_STOCK_INFORMATION where dt_date_retrieved < ' + str(updateableTime) +
							' and txt_ticker not like "%.%"')
		# Execute Statement
		c.execute(getListCommand)
		companyList = c.fetchall()
		# Flatten List
		companyList = [record[0] for record in companyList]
		# Return Data
		return companyList
	except:
		raise
	finally:
		# Close DB Connection
		conn.close()

########################################Code to Fill Company Ticker Database / Find Exchange
def from_YQL_getCompanyExchangeVolumeMktCap(tickerList):
	# Read Data from Yahoo as JSON query
	url = YQLtmplts.createFriendlyURL(YQLtmplts.EXCHANGE_FROM_SYMBOL_TEMPLATE_URL, tickerList=tickerList)
	raw_data = urllib2.urlopen(url).read()	
	queryResult = json.loads(raw_data)['query']['results']['quote']
	writeRows = {}
	try:
		# Parse Query Results
		if type(queryResult) is dict:
			queryResult = [queryResult]
		# Parse Query Results / Read Company
		for stock in queryResult:
				# Parse Query Results / Read Company / Handle Null Results
				exchange = finDB.SQL_NULL if stock['StockExchange'] is None else stock['StockExchange']
				mktCap = finDB.SQL_NULL if stock['MarketCapitalization'] is None else YQLtmplts.shorthand2i(stock['MarketCapitalization'])
				vol = finDB.SQL_NULL if stock['AverageDailyVolume'] is None else stock['AverageDailyVolume']
				# Parse Query Results / Read Company / Append to Row
				writeRows[stock['symbol']] = {	'exchange':exchange,
												'mktCap':mktCap,
												'avgVol':vol
				}
		return writeRows
	except:
		print "**** **** **** **** " + "total queryResult"
		pprint.pprint(queryResult)
		print "**** **** **** **** " + "specific stock queryResult"
		pprint.pprint(stock)
		raise


########################################Code to Fill Company Ticker Database / Find Start and End Trading Dates, (And Number of Employees)
def from_YQL_getCompanyStartEndTradingDates(tickerList):
	# Read Data from Yahoo as JSON query
	url = YQLtmplts.createFriendlyURL(YQLtmplts.STOCKS_START_END_TRADING_URL, tickerList=tickerList)
	raw_data = urllib2.urlopen(url).read()	
	queryResult = json.loads(raw_data)['query']['results']['stock']
	writeRows = {}
	try:
		# Parse Query Results
		if type(queryResult) is dict:
			queryResult = [queryResult]
		# Parse Query Results / Read Company
		for stock in queryResult:
				try:
					numEmployees = int(stock['FullTimeEmployees'])
				except:
					numEmployees = finDB.SQL_NULL
				try:
					start = YQLtmplts.dtConvert_YMDtoEpoch(stock['start'])
				except:
					start = finDB.SQL_NULL
				try:
					end = YQLtmplts.dtConvert_YMDtoEpoch(stock['end'])
				except:
					end = finDB.SQL_NULL
				writeRows[stock['symbol']] = {	'startTrade':start, 
												'endTrade':end, 
												'FullTimeEmployees': numEmployees}
		return writeRows
	except:
		print "**** **** **** **** " + "total queryResult"
		pprint.pprint(queryResult)
		print "**** **** **** **** " + "specific stock queryResult"
		pprint.pprint(stock)
		raise

#Download Income Statement, Balance Sheet, and Cash Flow Statement
def downloadBasicFinancialStatementData(tickerList, statementName):
    # Read Data from Yahoo as JSON query
    url = YQLtmplts.createFriendlyURL(YQLtmplts.BASIC_FINANCIAL_STATEMENT_TEMPLATE_URL,statementName=statementName,tickerList=tickerList)
    raw_data = urllib2.urlopen(url).read()
    queryResult = json.loads(raw_data)['query']
    if queryResult['count'] != len(tickerList):
        raise NotImplementedError #Throw better error
        pass
    # Find Expected Data Points
    expectedData = YQLtmplts.getExpectedContentKeys(statementName)
    # Get Specific Query Data (strip metadata about query)
    if queryResult['count'] == 1:
        queryData = [queryResult['results'][statementName]]
    else:
        queryData = queryResult['results'][statementName]
    # Parse Query Results
    foundData = []
    for symbolData in queryData:
        # Parse Query Results / Find Symbol
        symbol = symbolData['symbol']
        # Parse Query Results / Check Timeframe
        if symbolData['timeframe'] != 'quarterly':
            warnings.warn(symbol + ' does not have quarterly data')
            raise NotImplementedError
        # Parse Query Results / Check if Data Exists
        if 'statement' not in symbolData:
            warnings.warn(symbol + ' does not have '+ statementName +' data')
            raise NotImplementedError
            continue
        # Parse Query Results / Read Data by Period
        for periodData in symbolData['statement']:
            # Parse Query Results / Read Data by Period / Read Period
            period = periodData['period']
            # Parse Query Results / Read Data by Period / Read Expected Data
            for dataPoint in expectedData:
                # Parse Query Results / Read Data by Period / Read Expected Data
                if dataPoint in periodData:
                    value = periodData[dataPoint]['content']
                    if value == '-':
                        value = 0
                    else:
                        value = float(value)
                else:
                    warnings.warn(symbol + ' does not have '+ dataPoint +' data')
                    value = 'NF'
                # Parse Query Results / Read Data by Period / Write Data to List
                entry = {   'symbol' : symbol,
                            'period' : period,
                            'dataPoint' : dataPoint, 
                            'value' : value}
                foundData.append(entry)
            # Parse Query Results / Read Data by Period / Output if Extra Data Found
            for dataPoint in (periodData.keys()):
                if dataPoint not in (expectedData.keys()+['period']):
            		warnings.warn(dataPoint + ' (from ' + symbol + ') not found in ' + statementName + ' expected data list')
            		raise NotImplementedError
            		continue
    return {statementName: statementName,
    		data: foundData,
    		retrievedTime: int(time.time())}

######################### Completed Code Below

########################################Code to Fill Sector and Industry Database
def fillSectorAndIndustryDatabase(DBFileName=YQLtmplts.FinDBFileName, verbose=False):
	print "Using Database in " + DBFileName
	# Get Sector/Industry Data from Yahoo!
	print "Getting Yahoo!'s' List of all Sectors/Industries..."
	writeRows = from_YQL_getSectorAndIndustryList()
	# Display Data Retrieval Success
	print "\tRetrieved " + str(len(writeRows)) + " sectors' data.",
	if verbose:
		print "\tSectors are as follows:"
		pprint.pprint(writeRows)
	# Put Sector/Industry Data into Database
	print "Putting Retrieved Sector/Industry Data into Database"
	to_DB_fillSectorAndIndustryDatabase(writeRows, DBFileName=DBFileName, verbose=verbose)

########################################Code to Fill Sector and Industry Database / Get Data from Yahoo!
def from_YQL_getSectorAndIndustryList():
	# Get Sector and Industry List from Yahoo!
	url = YQLtmplts.createFriendlyURL(YQLtmplts.SECTOR_LIST_URL)
	raw_data = urllib2.urlopen(url).read()	
	queryResult = json.loads(raw_data)['query']['results']['sector']
	# Parse Query Results
	try:
		writeRows = []
		# Parse Query Results / Read Sector 
		for sector in queryResult:
			# Parse Query Results / Read Sector / Deal with Single Entries
			if type(sector['industry']) is dict:
				sector['industry'] = [sector['industry']]
			# Parse Query Results / Read Sector / Read Industry
			for industry in sector['industry']:
				# Parse Query Results / Read Sector / Read Industry / Write Row Dictionary to List
				writeRows.append({'sectorName':sector['name'],'industryID':industry['id'], 'industryName':industry['name']})
		# Return Query Results
		return writeRows
	except:
		print "**** **** **** **** " + "total queryResult"
		pprint.pprint(queryResult)
		raise

########################################Code to Fill Sector and Industry Database / Push Data into Database
# TODO: This can definitely be batched to add all at once.
def to_DB_fillSectorAndIndustryDatabase(writeRows, DBFileName=YQLtmplts.FinDBFileName, verbose=False):
	numSuccesses = 0
	# Open DB Connection
	conn = sq.connect(DBFileName)
	c = conn.cursor()
	try:
		# Iterate over Retrieved Data
		for row in writeRows:
			# Iterate over Retrieved Data / Create Data Insertion Line
			statement = finDB.get_insert_SectorIndustryInfo_into_SectorIndustryDB_Statement(
									row['industryID'],row['industryName'],row['sectorName'])
			# Iterate over Retrieved Data / Perform Database Insert
			numSuccesses += finDB.commitDBStatement(conn, c, statement, verbose=verbose)[0]
		# Display DB Insertion Statistics
		print "\tSuccessfully inserted " + str(numSuccesses) + " sectors' data."
		print "\tFailed to Insert " + str((len(writeRows) - numSuccesses)) + " sectors' data."
	except:
		raise
	finally:
		# Close DB Connection
		conn.close()

def to_DB_updateIndustryUpdateTime(conn, cursor, nowTime, industryID, industryName, verbose=False):
	statement = finDB.get_update_ReadTime_into_SectorIndustryDB_Statement(nowTime, industryID)
	if finDB.commitDBStatement(conn, cursor, statement, verbose=verbose)[0]:
		print (	'Successfully updated Industry Read Time for Industry ' + 
				str(industryID) +' (' + industryName + ') to SectorIndustry Table')


########################################Code to Fill Company Ticker Database
def to_DB_fillTickerDatabase(industryUpdateLimit=SECTOR_LIST_INDUSTRY_UPDATE_LIMIT, DBFileName=YQLtmplts.FinDBFileName, verbose=False):
	# Grab a few Industries to Update
	UpdateableIndustries = from_DB_getUpdateableIndustryList(DBFileName=DBFileName, updateableTimeLimit=TIME_BETWEEN_COMPANY_LIST_UPDATES)
	numRemaining = len(UpdateableIndustries)
	print 'There are ' + str(numRemaining) + ' sectors that need updating.'
	UpdateableIndustries = UpdateableIndustries[0:industryUpdateLimit]
	numUpdating = len(UpdateableIndustries)
	print 'This update will update the following ' + str(numUpdating) + ' industry IDs: ' + str(UpdateableIndustries)
	# Open DB Connection
	conn = sq.connect(DBFileName)
	c = conn.cursor()
	numSuccesses = 0
	try:
		now = str(int(time.time()))
		for industryID in UpdateableIndustries:
			#CONSIDER: You could get names for all industries at once, IF you can find a way to update updateable times individually
			writeRows = from_YQL_getCompanyNamesByIndustry(industryID)
			if len(writeRows) == 0:
				to_DB_updateIndustryUpdateTime(conn, c, now, industryID, '?', verbose=verbose)
				continue
			industryName = writeRows[0]['industryName']
			tickerExistsCount = 0
			tickerAdditionSuccess = True
			stockInfoAdditionSuccess = True
			for row in writeRows:
				tickerInsertStatement = finDB.get_insert_Ticker_into_TickerDB_Statement(row['ticker'])
				stockInfoInsertStatement = finDB.get_insert_Basics_into_StockInfoDB_Statement(row['ticker'], row['companyName'], industryID)
				# Check if company is in multiple Industries
				if finDB.checkIfTickerAlreadyInDB(conn, c, row['ticker'], verbose=verbose):
					tickerExistsCount += 1
					continue
				tickerAdditionSuccess = (tickerAdditionSuccess and finDB.commitDBStatement(conn, c, tickerInsertStatement, verbose=verbose)[0])
				stockInfoAdditionSuccess = (stockInfoAdditionSuccess and finDB.commitDBStatement(conn, c, stockInfoInsertStatement, verbose=verbose)[0])
			# Update UpdateableTime
			if tickerAdditionSuccess and stockInfoAdditionSuccess:
				print (	'Successfully added ' + str(len(writeRows) - tickerExistsCount) + ' and skipped ' +
						str(tickerExistsCount) + ' companies in Industry ' + 
						str(industryID) + ' (' + industryName  + ') to Ticker and StockInfo Tables')
				to_DB_updateIndustryUpdateTime(conn, c, now, industryID, industryName, verbose=verbose)
				numSuccesses += 1
		return (numRemaining - numSuccesses) > 0
	except:
		raise
	finally:
		# Close DB Connection
		conn.close()

########################################Code to Fill Company Ticker Database / Find industries with non-recent updates
def from_DB_getUpdateableIndustryList(DBFileName=YQLtmplts.FinDBFileName, updateableTimeLimit=TIME_BETWEEN_COMPANY_LIST_UPDATES):
	# Open DB Connection
	conn = sq.connect(DBFileName)
	c = conn.cursor()
	try:
		# Get Time Delay
		updateableTime = int(time.time()) - updateableTimeLimit;
		# Generate Statement
		getListCommand = 'select int_industry_id from T_SECTOR_INDUSTRY where dt_last_accessed < ' + str(updateableTime)
		# Execute Statement
		c.execute(getListCommand)
		IndustryList = c.fetchall()
		# Flatten List
		IndustryList = [record[0] for record in IndustryList]
		# Return Data
		return IndustryList
	except:
		raise
	finally:
		# Close DB Connection
		conn.close()
	

########################################Code to Fill Company Ticker Database / Find companies belonging to each industry
def from_YQL_getCompanyNamesByIndustry(idList):
	if type(idList) is int:
		idList = [idList]
	# Read Data from Yahoo as JSON query
	url = YQLtmplts.createFriendlyURL(YQLtmplts.COMPANY_BY_INDUSTRY_TEMPLATE_URL, idList = idList)
	raw_data = urllib2.urlopen(url).read()	
	queryResult = json.loads(raw_data)['query']['results']['industry']
	writeRows = []
	try:
		# Parse Query Results
		if type(queryResult) is dict:
			queryResult = [queryResult]
		for industry in queryResult:
			# Parse Query Results / Read Industry / Deal with Mistaken or Empty Industries
			if 'company' not in industry:
				print 'Industry ' + industry['id'] + ' appears to have no companies within it'
				continue
			# Parse Query Results / Read Industry / Deal with Single Entries
			if type(industry['company']) is dict:
				industry['company'] = [industry['company']]
			# Parse Query Results / Read Industry / Read Company
			for company in industry['company']:
				# Parse Query Results / Read Industry / Read Company / Write Row Dictionary to List
				if company['name'] == '':
					company['name'] = company['symbol']
				writeRows.append({	'ticker':company['symbol'], 
									'companyName':company['name'], 
									'industryID':industry['id'], 
									'industryName': industry['name']})
		# Return Results
		return writeRows
	except:
		print "**** **** **** **** " + "total queryResult"
		pprint.pprint(queryResult)
		print "**** **** **** **** " + "specific industry queryResult"
		pprint.pprint(industry)
		raise

