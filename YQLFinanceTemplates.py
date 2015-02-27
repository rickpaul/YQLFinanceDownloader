import string	

#For getting the ticker list
SECTOR_LIST_URL = (
                "https://query.yahooapis.com/v1/public/yql?"
                "q=select%20*%20from%20yahoo.finance.sectors"
                "&format=json&diagnostics=true&env=store%3A%2F%2Fdatatables.org%2Falltableswithkeys&callback=")

COMPANY_BY_INDUSTRY_TEMPLATE_URL = (
                "https://query.yahooapis.com/v1/public/yql?"
                "q=select%20*%20from%20yahoo.finance.industry%20where%20id%20in%20(<INDUSTRY_ID_LIST_HERE>)"
                "&format=json&diagnostics=true&env=store%3A%2F%2Fdatatables.org%2Falltableswithkeys&callback=")

# For Balance Sheet, Income Statement, and Cash Flow Statement
BASIC_FINANCIAL_STATEMENT_TEMPLATE_URL =(
                "https://query.yahooapis.com/v1/public/yql?"
            	"q=SELECT%20*%20FROM%20yahoo.finance.<STATEMENT_NAME_HERE>"
            	"%20WHERE%20symbol%20in%20(<SYMBOL_LIST_HERE>)"
            	"&format=json&diagnostics=true&env=store%3A%2F%2Fdatatables.org%2Falltableswithkeys&callback=")

BSContentKeys = [
"CashAndCashEquivalents",
"ShortTermInvestments",
"NetReceivables",
"Inventory",
"OtherCurrentAssets",
"TotalCurrentAssets",
"LongTermInvestments",
"PropertyPlantandEquipment",
"Goodwill",
"IntangibleAssets",
"AccumulatedAmortization",
"OtherAssets",
"DeferredLongTermAssetCharges",
"TotalAssets",
"AccountsPayable",
"Short_CurrentLongTermDebt",
"OtherCurrentLiabilities",
"TotalCurrentLiabilities",
"LongTermDebt",
"OtherLiabilities",
"DeferredLongTermLiabilityCharges",
"MinorityInterest",
"NegativeGoodwill",
"TotalLiabilities",
"MiscStocksOptionsWarrants",
"RedeemablePreferredStock",
"PreferredStock",
"CommonStock",
"RetainedEarnings",
"TreasuryStock",
"CapitalSurplus",
"OtherStockholderEquity",
"TotalStockholderEquity",
"NetTangibleAssets"]

CFContentKeys = [
"NetIncome",
"Depreciation",
"AdjustmentsToNetIncome",
"ChangesInAccountsReceivables",
"ChangesInLiabilities",
"ChangesInInventories",
"ChangesInOtherOperatingActivities",
"TotalCashFlowFromOperatingActivities",
"CapitalExpenditures",
"Investments",
"OtherCashflowsfromInvestingActivities",
"TotalCashFlowsFromInvestingActivities",
"DividendsPaid",
"SalePurchaseofStock",
"NetBorrowings",
"OtherCashFlowsfromFinancingActivities",
"TotalCashFlowsFromFinancingActivities",
"EffectOfExchangeRateChanges",
"ChangeInCashandCashEquivalents"
]

ISContentKeys = [
"TotalRevenue",
"CostofRevenue",
"GrossProfit",
"ResearchDevelopment",
"SellingGeneralandAdministrative",
"NonRecurring",
"Others",
"TotalOperatingExpenses",
"OperatingIncomeorLoss",
"TotalOtherIncome_ExpensesNet",
"EarningsBeforeInterestAndTaxes",
"InterestExpense",
"IncomeBeforeTax",
"IncomeTaxExpense",
"MinorityInterest",
"NetIncomeFromContinuingOps",
"DiscontinuedOperations",
"ExtraordinaryItems",
"EffectOfAccountingChanges",
"OtherItems",
"NetIncome",
"PreferredStockAndOtherAdjustments",
"NetIncomeApplicableToCommonShares"
]

KeyStatsContentKeys = [
"MarketCap",
"EnterpriseValue",
"TrailingPE",
"ForwardPE",
"PEGRatio",
"PriceSales",
"PriceBook",
"EnterpriseValueRevenue",
"EnterpriseValueEBITDA",
"MostRecentQuarter",
"ProfitMargin",
"OperatingMargin",
"ReturnonAssets",
"ReturnonEquity",
"Revenue",
"RevenuePerShare",
"QtrlyRevenueGrowth",
"GrossProfit",
"EBITDA",
"NetIncomeAvltoCommon",
"DilutedEPS",
"QtrlyEarningsGrowth",
"TotalCash",
"TotalCashPerShare",
"TotalDebt",
"TotalDebtEquity",
"CurrentRatio",
"BookValuePerShare",
"OperatingCashFlow",
"LeveredFreeCashFlow",
"p_52_WeekHigh",
"p_52_WeekLow",
"ShortRatio",
"ShortPercentageofFloat"
]

def getExpectedContentKeys(statementName):
    if statementName == 'incomestatement':
        return dict(zip(ISContentKeys,[0]*len(ISContentKeys)))
    elif statementName == 'balancesheet':
        return dict(zip(BSContentKeys,[0]*len(BSContentKeys)))
    elif statementName == 'cashflow':
        return dict(zip(CFContentKeys,[0]*len(CFContentKeys)))
    else:
        raise NameError("Unrecognized Statement Name")

def getExpectedDatabaseName(statementName):
    if statementName == 'incomestatement':
        return 'T_INCOME_STATEMENT'
    elif statementName == 'balancesheet':
        return 'T_BALANCE_SHEET'
    elif statementName == 'cashflow':
        return 'T_CASH_FLOW_STATEMENT'
    else:
        raise NameError("Unrecognized Statement Name")

def createFriendlyURL(  template,
                        statementName="",
                        symbolList=[],
                        idList=[],
                        startDate="",
                        endDate=""):
    symbolList = "'"+string.join(symbolList,"','")+"'"
    idList = string.join(["'"+str(ID)+"'" for ID in idList],",")
    templateURL = template.replace('<SYMBOL_LIST_HERE>',symbolList)
    templateURL = templateURL.replace('<INDUSTRY_ID_LIST_HERE>',idList)
    templateURL = templateURL.replace('<STATEMENT_NAME_HERE>',statementName)
    templateURL = templateURL.replace(' ','')
    templateURL = templateURL.replace('\n','')
    templateURL = templateURL.replace('\t','')    
    return templateURL

FinDBFileName = './database/financialData.db'
TestDBFileName = './database/testFinancialData.db'