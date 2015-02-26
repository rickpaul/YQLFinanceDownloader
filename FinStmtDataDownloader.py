
import sqlite3 as sq
import urllib2
import json
import time
import string
import pprint
import YQLFinanceTemplates as tmplts


class FinStmtDataDownloader:

	ONE_MONTH = 60*60*24*30

	TIME_BETWEEN_COMPANY_LIST_UPDATES = ONE_MONTH
	COMPANY_LIST_INDUSTRY_UPDATE_LIMIT = 10

	def __init__(self):
		pass

	#Download Income Statement, Balance Sheet, and Cash Flow Statement
	def downloadBasicFinancialStatementData(self, symbolList, statementName):
	    # Read Data from Yahoo as JSON query
	    url = tmplts.createFriendlyURL(tmplts.BASIC_FINANCIAL_STATEMENT_TEMPLATE_URL,statementName = statementName,symbolList = symbolList)
	    raw_data = urllib2.urlopen(url).read()
	    queryResult = json.loads(raw_data)['query']
	    if queryResult['count'] != len(symbolList):
	        #Throw Error
	        raise NotImplementedError()
	        pass
	    # Find Expected Data Points
	    expectedData = tmplts.getExpectedContentKeys(statementName)
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
	            raise NotImplementedError()
	        # Parse Query Results / Check if Data Exists
	        if 'statement' not in symbolData:
	            warnings.warn(symbol + ' does not have '+ statementName +' data')
	            raise NotImplementedError()
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
	            		raise NotImplementedError()
            			continue
	    return {statementName: statementName,
	    		data: foundData,
	    		retrievedTime: int(time.time())}

######################### Completed Code Below
	
	########################################Code to Fill Sector and Industry Database
	def fillSectorAndIndustryDatabase(self):
		# Get Sector/Industry Data from Yahoo!
		print "Get Sector/Industry Data from Yahoo!"
		writeRows = self.from_YQL_getSectorAndIndustryList()
		# Display Data Retrieval Success
		print "\tRetrieved " + str(len(writeRows)) + " sectors' data. Sectors are as follows:"
		pprint.pprint(writeRows)
		# Put Sector/Industry Data into Database
		print "Put Sector/Industry Data into Database"
		self.to_DB_fillSectorAndIndustryDatabase(writeRows)

	########################################Code to Fill Sector and Industry Database / Get Data from Yahoo!
	def from_YQL_getSectorAndIndustryList(self):
		# Read Data from Yahoo as JSON query
		url = tmplts.createFriendlyURL(tmplts.SECTOR_LIST_URL)
		raw_data = urllib2.urlopen(url).read()	
		queryResult = json.loads(raw_data)['query']['results']['sector']
		# Parse Query Results
		try:
			writeRows = []
			for sector in queryResult:
				# Parse Query Results / Read Sector / Read Sector Name
				sectorName = '"' + sector['name'] + '"'
				# Parse Query Results / Read Sector / Deal with Single Entries
				if type(sector['industry']) is dict:
					sector['industry'] = [sector['industry']]
				# Parse Query Results / Read Sector / Read Industry
				for industry in sector['industry']:
					# Parse Query Results / Read Sector / Read Industry / Write Row Dictionary to List
					industryID = industry['id']
					industryName = '"' + industry['name'] + '"'
					writeRows.append({'sectorName':sectorName,'industryID':industryID, 'industryName':industryName})
			return writeRows
		except:
			pprint.pprint(queryResult)
			raise

	########################################Code to Fill Sector and Industry Database / Push Data into Database
	def to_DB_fillSectorAndIndustryDatabase(self, writeRows, verbose = False):
		numSuccesses = 0
		# Open Database Connection
		conn = sq.connect(tmplts.DBFileName)
		c = conn.cursor()
		try:
			# Iterate over Retrieved Data
			for row in writeRows:
				# Iterate over Retrieved Data / Create Values Insert
				valueString = '( ' + string.join([row['industryID'],row['industryName'],row['sectorName']],',') + ' )'
				# Iterate over Retrieved Data / Create Data Insertion Line
				statement = [	'INSERT INTO T_SECTOR_INDUSTRY',
								'( int_industry_id, txt_industry_name, txt_sector_name )',
								'VALUES',
								valueString
				]
				statement = string.join(statement, ' ') + ';'
				# Iterate over Retrieved Data / Perform Database Insert
				numSuccesses += self.commitDBStatement(conn, c, statement, verbose=verbose)[0]
			# Display DB Insertion Statistics
			print "\tSuccessfully inserted " + str(numSuccesses) + " sectors' data."
			print "\tFailed to Insert " + str((len(writeRows) - numSuccesses)) + " sectors' data."
		finally:
			# Close Database Connection
			conn.close()

	########################################Code to Fill Company Ticker Database
	def fillTickerDatabase(self, industryUpdateLimit=COMPANY_LIST_INDUSTRY_UPDATE_LIMIT, verbose=False):
		# Grab a few Industries to Update
		UpdateableIndustries = self.from_DB_getUpdateableIndustryList(updateableTimeLimit=self.TIME_BETWEEN_COMPANY_LIST_UPDATES)
		print 'There are ' + str(len(UpdateableIndustries)) + ' sectors that need updating.'
		UpdateableIndustries = UpdateableIndustries[0:industryUpdateLimit]
		print 'This update will update the following industry IDs: ' + str(UpdateableIndustries)
		
		# Open DB Connection
		conn = sq.connect(tmplts.DBFileName)
		c = conn.cursor()
		try:
			tickerTableInsertStatement = (	'INSERT INTO T_TICKER '
											'(txt_ticker) '
											'VALUES '
											'(<TICKER_HERE>);')
			stockInfoInsertStatement = (	'INSERT INTO T_STOCK_INFORMATION '
											'(txt_ticker, txt_company_name, int_industry_id) ' 
											'VALUES '
											'(<TICKER_HERE>, <NAME_HERE>, <INDUSTRY_ID_HERE>);')
			updateIndustryReadTimeStatement = (	'UPDATE T_SECTOR_INDUSTRY SET '
												'dt_last_accessed = <TIME_HERE> ' 
												'WHERE '
												'int_industry_id = <INDUSTRY_ID_HERE>;')
			now = str(int(time.time()))
			for industryID in UpdateableIndustries:
				#CONSIDER: You can get them all at once, IF you can find a way to update updateable times individually
				tickerSuccess = True
				stockInfoSuccess = True
				writeRows = self.from_YQL_getCompanyNamesByIndustry(UpdateableIndustries)
				if len(writeRows == 0):
					updateIndustryReadTimeStatement_tmp = updateIndustryReadTimeStatement.replace('<INDUSTRY_ID_HERE>', str(industryID))
					updateIndustryReadTimeStatement_tmp = updateIndustryReadTimeStatement_tmp.replace('<TIME_HERE>', str(now))
					if self.commitDBStatement(conn, c, updateIndustryReadTimeStatement_tmp, verbose=verbose)[0]:
						print (	'Successfully added time stamp of read for Industry ' + 
								str(industryID) + 
								' (' + row['industryName'] + 
								') to Ticker and StockInfo Tables')
					continue
				for row in writeRows:
					tickerTableInsertStatement_tmp = tickerTableInsertStatement.replace('<TICKER_HERE>', row['ticker'])
					stockInfoInsertStatement_tmp = stockInfoInsertStatement.replace('<TICKER_HERE>', row['ticker'])
					stockInfoInsertStatement_tmp = stockInfoInsertStatement_tmp.replace('<NAME_HERE>', row['companyName'])
					stockInfoInsertStatement_tmp = stockInfoInsertStatement_tmp.replace('<INDUSTRY_ID_HERE>', str(industryID)) 
					# Some companies are in multiple industries, requiring some ugly code
					# TODO: Clean up! Split into inserting tickers and inserting stockInfo
					# TODO: Clean up! Check if the UNIQUE constraint will fail before adding!
					successStatement = 	self.commitDBStatement(conn, c, tickerTableInsertStatement_tmp, verbose=verbose)[0]
					tickerSuccess = 	(tickerSuccess and
										(successStatement[0] or
										('UNIQUE constraint failed' in successStatement[1])))
					successStatement = 	self.commitDBStatement(conn, c, stockInfoInsertStatement_tmp, verbose=verbose)[0]
					stockInfoSuccess = 	(stockInfoSuccess and
										(successStatement[0] or
										('UNIQUE constraint failed' in successStatement[1])))
				#TODO: Update updateableTime
				if tickerSuccess and stockInfoSuccess:
					print (	'Successfully added companies from Industry ' + 
							str(industryID) + 
							' (' + row['industryName'] + 
							') to Ticker and StockInfo Tables)')
					updateIndustryReadTimeStatement_tmp = updateIndustryReadTimeStatement.replace('<INDUSTRY_ID_HERE>', str(industryID))
					updateIndustryReadTimeStatement_tmp = updateIndustryReadTimeStatement_tmp.replace('<TIME_HERE>', str(now))
					if self.commitDBStatement(conn, c, updateIndustryReadTimeStatement_tmp, verbose=verbose)[0]:
						print (	'Successfully added time stamp of read for Industry ' + 
								str(industryID) + 
								' (' + row['industryName'] + 
								') to Ticker and StockInfo Tables')

		finally:
			# Close DB Connection
			conn.close()

	########################################Code to Fill Company Ticker Database / Find industries with non-recent updates
	def from_DB_getUpdateableIndustryList(self,updateableTimeLimit=TIME_BETWEEN_COMPANY_LIST_UPDATES):
		# Open Database Connection
		conn = sq.connect(tmplts.DBFileName)
		c = conn.cursor()
		try:
			# Get Time Delay
			updateableTime = int(time.time()) - updateableTimeLimit;
			getCountCommand = 'select int_industry_id from T_SECTOR_INDUSTRY where dt_last_accessed < ' + str(updateableTime)
			c.execute(getCountCommand)
			IndustryList = c.fetchall()
			# Flatten List
			IndustryList = [record[0] for record in IndustryList]
			# Return Data
			return IndustryList
		# Close Database Connection
		finally:
			conn.close()
		

	########################################Code to Fill Company Ticker Database / Find companies belonging to each industry
	def from_YQL_getCompanyNamesByIndustry(self, idList):
		# Read Data from Yahoo as JSON query
		url = tmplts.createFriendlyURL(tmplts.COMPANY_BY_INDUSTRY_TEMPLATE_URL, idList = idList)
		raw_data = urllib2.urlopen(url).read()	
		queryResult = json.loads(raw_data)['query']['results']['industry']
		writeRows = []
		try:
			# Parse Query Results
			if type(queryResult) is dict:
				queryResult = [queryResult]
			for industry in queryResult:
				# Parse Query Results / Read Industry / Read Industry Name and ID
				industryName = '"' + industry['name'] + '"'
				industryID = '"' + industry['id'] + '"'
				# Parse Query Results / Read Industry / Deal with Mistaken/Empty Industries
				if 'company' not in industry:
					continue
				# Parse Query Results / Read Industry / Deal with Single Entries
				if type(industry['company']) is dict:
					industry['company'] = [industry['company']]
				# Parse Query Results / Read Industry / Read Company
				for company in industry['company']:
					# Parse Query Results / Read Industry / Read Company / Write Row Dictionary to List
					companyName = '"' +  company['name']  + '"'
					companyTicker = '"' + company['symbol'] + '"'
					writeRows.append({'ticker':companyTicker, 'companyName':companyName, 'industryID':industryID, 'industryName': industryName})
			# Return Results
			return writeRows
		except:
			print "**** **** **** **** " + "total queryResult"
			pprint.pprint(queryResult)
			print "**** **** **** **** " + "specific industry queryResult"
			pprint.pprint(industry)
			raise

	########################################Database Helper Code
	########################################Database Helper Code / Commiting Code with Silent Failure
	#TODO: You can do batch inserts in sql... maybe worth a try
	def commitDBStatement(self, conn, cursor, statement, verbose = False):
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

	########################################Database Helper Code / Outward-facing Table Peeking
	def peerIntoDatabase(self, TableName, displayPragma = False, limit = 10):
		# Open DB Connection
		conn = sq.connect(tmplts.DBFileName)
		c = conn.cursor()
		try:
			# Print Table Length
			viewLengthCommand = 'select count(*) from ' + TableName
			c.execute(viewLengthCommand)
			print 'There are ' + str(c.fetchall()[0][0]) + ' rows in ' + TableName
			# Get Table Column Names and Types
			pragmaCommand = 'pragma table_info("' + TableName + '")'
			c.execute(pragmaCommand)
			pragma = c.fetchall()
			#		(pragma columns are as follows:)
			#		(cid,name,type,notnull,dflt_value,pk)
			columnNames = [col[1] for col in pragma]
			columnTypes = [col[2] for col in pragma]
			if displayPragma:
				pprint.pprint(pragma)
			# Print First Few Rows of Table		
			viewFirstFewCommand = 'select * from ' + TableName + ' limit ' + str(limit)
			c.execute(viewFirstFewCommand)
			firstFew = c.fetchall()
			print columnTypes
			print columnNames
			pprint.pprint(firstFew)
		finally:
			# Close DB Connection
			conn.close()
