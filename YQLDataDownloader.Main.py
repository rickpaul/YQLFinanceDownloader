import FinStmtDatabaseCreator as finDBMaker
import FinStmtDataDownloader as finDld
import FinStmtDatabaseCreator as finDBMaker

def createDatabase():
	# Create DB
	if not finDBMaker.checkIfDBExistsWithTables(manual=True):
		finDBMaker.doOneTimeDBCreation(manual=True)

def fillTickerDatabase():
	# Fill Sectors and Industries
	finDld.fillSectorAndIndustryDatabase()
	# Fill Tickers
	while True:
		if not finDld.to_DB_fillTickerDatabase(industryUpdateLimit=10, verbose=False):
			break

if __name__ == '__main__':
	createDatabase()
	fillTickerDatabase()