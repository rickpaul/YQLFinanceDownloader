############### COPY A TABLE
import FinStmtDatabaseCreator as finDBMaker
import YQLFinanceTemplates as YQLtmplts
import sqlite3 as sq 

createTempStatement = '''
create table T_STOCK_INFO_TEMP (
	txt_ticker TEXT, 
	txt_company_name TEXT, 
	int_industry_id INTEGER
);'''

copyIntoTempStatement = '''
insert into T_STOCK_INFO_TEMP 
	(txt_ticker, txt_company_name, int_industry_id) 
select 
	txt_ticker, txt_company_name, int_industry_id 
from T_STOCK_INFORMATION;'''

verifyTempStatement = '''
select * from T_STOCK_INFO_TEMP limit 10
'''

# finDBMaker.doOneTimeDBCreation(force=True,manual=True)

copyIntoRealStatement = '''
insert into T_STOCK_INFORMATION
	(txt_ticker, txt_company_name, int_industry_id) 
select 
	txt_ticker, txt_company_name, int_industry_id 
from T_STOCK_INFO_TEMP;'''

verifyRealStatement = '''
select * from T_STOCK_INFO_TEMP limit 10
'''

dropTempStatement = '''
drop table T_STOCK_INFO_TEMP;
'''


DB = YQLtmplts.FinDBFileName
conn = sq.connect(DB)
c = conn.cursor()
try:
	c.execute(dropTempStatement)
	conn.commit()
	# c.execute(verifyTempStatement)
	# print c.fetchall()
except:
	raise
finally:
	conn.close