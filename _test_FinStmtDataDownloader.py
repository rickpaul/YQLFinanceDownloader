#Yes, I do know the following things:
#1) that there are testing frameworks that exist
#2) that I've not designed these tests well. 

import os
import sqlite3 as sq
from pprint import pprint
import FinStmtDataDownloader as finDld
import FinStmtDatabaseCreator as finDBMaker
import YQLFinanceTemplates as YQLtmplts

def test_DatabaseCreation():
	DBFileName = YQLtmplts.TestDBFileName
	try:
		print '\tEnsure the DB can be created...'
		finDBMaker.doOneTimeDBCreation(force=True,dbPath=DBFileName)
		print ' ... seems OK.'
	except:
		print '... Failed!'
		raise
	print '\tEnsure DB is in expected location... ',
	assert os.path.isfile(DBFileName)
	print ' ... seems OK.'

	conn = sq.connect(DBFileName)
	c = conn.cursor()
	try:
		print '\tEnsure at least one table (T_TICKER) was created',
		statement = 'select count(*) from T_TICKER'
		c.execute(statement)
		assert len(c.fetchall()) > 0
		print ' ... seems OK.'
	except:
		print '... Failed!'
		raise
	finally:
		conn.close()

def test_from_YQL_getSectorAndIndustryList():
	sectIndList = finDld.from_YQL_getSectorAndIndustryList()
	print '\tEnsure the sector/industry list is not changing drastically in size...',
	assert len(sectIndList) > 200 and len(sectIndList) < 300 
	print ' ... seems OK.'
	print '\tEnsure some data is actually coming back in list of dictionaries... ',
	sectIndListSample = sectIndList[0]
	assert 'industryID' in sectIndListSample
	assert 'industryName' in sectIndListSample
	assert 'sectorName' in sectIndListSample
	print ' ... seems OK.'

#bad test design. everywhere.
def test_fillSectorAndIndustryDatabase():
	DBFileName = YQLtmplts.TestDBFileName
	try:
		print '\tEnsure getting the Full Sector and Industry List goes through end to end ...'
		finDld.fillSectorAndIndustryDatabase(DBFileName=DBFileName,verbose=False)
		print ' ... seems OK.'
	except:
		print '... Failed!'
		raise

	conn = sq.connect(DBFileName)
	c = conn.cursor()
	try:
		print '\tEnsure specific expected entry is in database...',
		statement = '''	select * from T_SECTOR_INDUSTRY 
						where 
						int_industry_id = 850 and 
						txt_industry_name = "Internet Service Providers" and 
						txt_sector_name = "Technology" '''
		c.execute(statement)
		assert len(c.fetchall()) > 0
		print ' ... seems OK.'
	except:
		print '... Failed!'
		raise
	finally:
		conn.close()	

def test_from_DB_getUpdateableIndustryList():
	DBFileName = YQLtmplts.TestDBFileName
	try:
		print '\tEnsure the List of Updateable Industries comes through ...',
		updateableIndustries = finDld.from_DB_getUpdateableIndustryList(DBFileName=DBFileName, updateableTimeLimit=0)
		print ' ... seems OK.'
	except:
		print '... Failed!'
		raise
	
	conn = sq.connect(DBFileName)
	c = conn.cursor()
	try:
		print '\tEnsure the list is long enough...',
		c.execute('select count(*) from T_SECTOR_INDUSTRY')
		numberOfSectors = c.fetchall()[0][0]
		assert numberOfSectors == len(updateableIndustries)
		print ' ... seems OK.'
	except:
		raise
	finally:
		conn.close()

def test_from_YQL_getCompanyNamesByIndustry():
	DBFileName = YQLtmplts.TestDBFileName
	try:
		print '\tEnsure you can get companies using an ID'
		writeRows = finDld.from_YQL_getCompanyNamesByIndustry([110])
		pprint(writeRows)
		print ' ... seems OK.' 
	except:
		print '... Failed!'
		raise

	try:
		print '\tEnsure you can get companies using multiple IDs'
		writeRows = finDld.from_YQL_getCompanyNamesByIndustry([110, 111, 112, 113, 120, 121, 122, 123, 124, 125])
		pprint(writeRows)
		print ' ... seems OK.' 
	except:
		print '... Failed!'
		raise		

def test_to_DB_fillTickerDatabase():
	DBFileName = YQLtmplts.TestDBFileName
	try:
		print '\tEnsure getting the Tickers using the Industry List goes through end to end (for 1 industry)...'
		finDld.to_DB_fillTickerDatabase(industryUpdateLimit=1, DBFileName=DBFileName, verbose=False)
		print ' ... seems OK.'
	except:
		print '... Failed!'
		raise
	try:
		print '\tEnsure getting the Tickers using the Industry List goes through end to end (for 10 industries)...'
		finDld.to_DB_fillTickerDatabase(industryUpdateLimit=10, DBFileName=DBFileName, verbose=False)
		print ' ... seems OK.'
	except:
		print '... Failed!'
		raise	

if __name__ == '__main__':
	if False:
		test_DatabaseCreation()
	# ...Presumably our Test DB exists at this point
	if False:
		test_from_YQL_getSectorAndIndustryList()
	if False:
		test_fillSectorAndIndustryDatabase()
	# ...Presumably our Test DB has Sectors and Industries filled at this point
	if False:
		test_from_DB_getUpdateableIndustryList()
	if False:
		test_from_YQL_getCompanyNamesByIndustry()
	if True:
		test_to_DB_fillTickerDatabase()
