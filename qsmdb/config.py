cfg = {
    "postgres":
        {
            "secmaster_db": "secmasterdb",
            "secmaster_user": "postgres",
            "secmaster_password": "postgres",
            "secmaster_host": "192.168.178.8",
            "secmaster_port": "5432"
        },
    "non_ticker_type": ["INDEX", "CMDTY", "FOREX", "CRYPTO"]
}


def daily_non_equity(tsid, data_vendor_id, beg_date, end_date):
    return """SELECT dp.tsid,
        CAST(dp.date AS DATE),
        CAST(dp.ohlc ->> 'open' AS float) AS open,
        CAST(dp.ohlc ->> 'high' AS float) AS high,
        CAST(dp.ohlc ->> 'low' AS float) AS low,
        CAST(dp.ohlc ->> 'close' AS float) AS close,
        (dp.volume ->> 'volume') AS volume
        FROM daily_prices dp
        WHERE dp.tsid = '%s'
        AND dp.vendor_id = '%s'
        AND dp.date>='%s'::date AND dp.date<='%s'::date;""" % (tsid, data_vendor_id, beg_date, end_date)


def daily_equity(tsid, data_vendor_id, beg_date, end_date):
    return """SELECT dp.tsid,
        CAST(dp.date AS DATE),
        CAST(dp.ohlc ->> 'open' AS float) AS open,
        CAST(dp.ohlc ->> 'high' AS float) AS high,
        CAST(dp.ohlc ->> 'low' AS float) AS low,
        CAST(dp.ohlc ->> 'close' AS float) AS close,
        (dp.volume ->> 'volume') AS volume,
        COALESCE(CAST(sp.splits ->> 'splits' AS float), 1.0) AS split,
        COALESCE(CAST(div.dividends ->> 'dividends' AS float), 0.0) AS dividend
        FROM daily_prices dp
        LEFT JOIN equity_splits sp ON sp.vendor_id = dp.vendor_id
        AND sp.tsid = dp.tsid
        AND sp.date = dp.date
        LEFT JOIN equity_dividends div ON div.vendor_id = dp.vendor_id
        AND div.tsid = dp.tsid
        AND div.date = dp.date
        WHERE dp.tsid = '%s'
        AND dp.vendor_id = '%s'
        AND dp.date>='%s'::date AND dp.date<='%s'::date;""" % (tsid, data_vendor_id, beg_date, end_date)


def earnings_history(tsid, beg_date, end_date, data_vendor_id=20):
    return """SELECT tsid,
        date,
        epsactual AS float,
        epsestimate As float,
        epsdifference AS float,
        surprisepercent AS float
        FROM equity_earnings_history
        WHERE tsid = '%s'
        AND vendor_id = '%s'
        AND date>='%s'::date AND date<='%s'::date;""" % (tsid, data_vendor_id, beg_date, end_date)


def equity_earnings_trend(tsid, beg_date, end_date, data_vendor_id=20):
    return """SELECT tsid, 
        extraction_date, 
        date, 
        period, 
        CAST(earnings_trend ->> 'growth' AS float) AS growth,
        CAST(earnings_trend ->> 'epsTrendCurrent' AS float) AS epsTrendCurrent,
        CAST(earnings_trend ->> 'epsTrend7daysAgo' AS float) AS epsTrend7daysAgo,
        CAST(earnings_trend ->> 'epsTrend30daysAgo' AS float) AS epsTrend30daysAgo,
        CAST(earnings_trend ->> 'epsTrend60daysAgo' AS float) AS epsTrend60daysAgo,
        CAST(earnings_trend ->> 'epsTrend90daysAgo' AS float) AS epsTrend90daysAgo,
        CAST(earnings_trend ->> 'epsRevisionsUpLast7days' AS float) AS epsRevisionsUpLast7days,
        CAST(earnings_trend ->> 'epsRevisionsUpLast30days' AS float) AS epsRevisionsUpLast30days,
        CAST(earnings_trend ->> 'epsRevisionsDownLast30days' AS float) AS epsRevisionsDownLast30days,
        CAST(earnings_trend ->> 'epsRevisionsDownLast90days' AS float) AS epsRevisionsDownLast90days,
        CAST(earnings_trend ->> 'revenueEstimateAvg' AS float) AS revenueEstimateAvg,
        CAST(earnings_trend ->> 'revenueEstimateLow' AS float) AS revenueEstimateLow,
        CAST(earnings_trend ->> 'revenueEstimateHigh' AS float) AS revenueEstimateHigh,
        CAST(earnings_trend ->> 'revenueEstimateGrowth' AS float) AS revenueEstimateGrowth,
        CAST(earnings_trend ->> 'revenueEstimateNumberOfAnalysts' AS float) AS revenueEstimateNumberOfAnalysts,
        CAST(earnings_trend ->> 'revenueEstimateYearAgoEps' AS float) AS revenueEstimateYearAgoEps,
        CAST(earnings_trend ->> 'revenueEstimateNumberOfAnalysts' AS float) AS revenueEstimateNumberOfAnalysts,
        CAST(earnings_trend ->> 'earningsEstimateAvg' AS float) AS earningsEstimateAvg,
        CAST(earnings_trend ->> 'earningsEstimateLow' AS float) AS earningsEstimateLow,
        CAST(earnings_trend ->> 'earningsEstimateHigh' AS float) AS earningsEstimateHigh,
        CAST(earnings_trend ->> 'earningsEstimateGrowth' AS float) AS earningsEstimateGrowth,
        CAST(earnings_trend ->> 'earningsEstimateYearAgoEps' AS float) AS earningsEstimateYearAgoEps,
        CAST(earnings_trend ->> 'earningsEstimateNumberOfAnalysts' AS float) AS earningsEstimateNumberOfAnalysts,
        FROM equity_earnings_trend
        WHERE tsid = '%s'
        AND vendor_id = '%s'
        AND extraction_date>='%s'::date AND extraction_date<='%s'::date;""" % (tsid, data_vendor_id, beg_date, end_date)


def equity_cash_flow(tsid, beg_date, end_date, data_vendor_id=20):
    return """SELECT tsid, 
        date, 
        frequency, 
        CAST(cash_flow ->> 'netIncome' AS float) AS netIncome,
        CAST(cash_flow ->> 'filing_date' AS float) AS filing_date,
        CAST(cash_flow ->> 'investments' AS float) AS investments,
        CAST(cash_flow ->> 'changeInCash' AS float) AS changeInCash,
        CAST(cash_flow ->> 'depreciation' AS float) AS depreciation,
        CAST(cash_flow ->> 'dividendsPaid' AS float) AS dividendsPaid,
        CAST(cash_flow ->> 'netBorrowings' AS float) AS netBorrowings,
        CAST(cash_flow ->> 'changeToInventory' AS float) AS changeToInventory,
        CAST(cash_flow ->> 'changeToNetincome' AS float) AS changeToNetincome,
        CAST(cash_flow ->> 'capitalExpenditures' AS float) AS capitalExpenditures,
        CAST(cash_flow ->> 'changeToLiabilities' AS float) AS changeToLiabilities,
        CAST(cash_flow ->> 'salePurchaseOfStock' AS float) AS salePurchaseOfStock,
        CAST(cash_flow ->> 'changeToAccountReceivables' AS float) AS changeToAccountReceivables,
        CAST(cash_flow ->> 'changeToOperatingActivities' AS float) AS changeToOperatingActivities,
        CAST(cash_flow ->> 'totalCashFromFinancingActivities' AS float) AS totalCashFromFinancingActivities,
        CAST(cash_flow ->> 'totalCashFromOperatingActivities' AS float) AS totalCashFromOperatingActivities,
        CAST(cash_flow ->> 'otherCashflowsFromFinancingActivities' AS float) AS otherCashflowsFromFinancingActivities,
        CAST(cash_flow ->> 'otherCashflowsFromInvestingActivities' AS float) AS otherCashflowsFromInvestingActivities,
        CAST(cash_flow ->> 'totalCashflowsFromInvestingActivities' AS float) AS totalCashflowsFromInvestingActivities,
        CAST(cash_flow ->> 'earningsEstimateHigh' AS float) AS earningsEstimateHigh,
        CAST(cash_flow ->> 'earningsEstimateGrowth' AS float) AS earningsEstimateGrowth,
        CAST(cash_flow ->> 'earningsEstimateYearAgoEps' AS float) AS earningsEstimateYearAgoEps,
        CAST(cash_flow ->> 'earningsEstimateNumberOfAnalysts' AS float) AS earningsEstimateNumberOfAnalysts,
        FROM equity_cash_flow
        WHERE tsid = '%s'
        AND vendor_id = '%s'
        AND date>='%s'::date AND date<='%s'::date;""" % (tsid, data_vendor_id, beg_date, end_date)


def equity_balance_sheet(tsid, beg_date, end_date, data_vendor_id=20):
    return """SELECT tsid, 
        date, 
        frequency, 
        CAST(balance_sheet ->> 'cash' AS float) AS cash,
        CAST(balance_sheet ->> 'goodWill' AS float) AS goodWill,
        CAST(balance_sheet ->> 'inventory' AS float) AS inventory,
        CAST(balance_sheet ->> 'otherLiab' AS float) AS otherLiab,
        CAST(balance_sheet ->> 'totalLiab' AS float) AS totalLiab,
        CAST(balance_sheet ->> 'commonStock' AS float) AS commonStock,
        CAST(balance_sheet ->> 'filing_date' AS date) AS filing_date,
        CAST(balance_sheet ->> 'otherAssets' AS float) AS otherAssets,
        CAST(balance_sheet ->> 'totalAssets' AS float) AS totalAssets,
        CAST(balance_sheet ->> 'longTermDebt' AS float) AS longTermDebt,
        CAST(balance_sheet ->> 'treasuryStock' AS float) AS treasuryStock,
        CAST(balance_sheet ->> 'netReceivables' AS float) AS netReceivables,
        CAST(balance_sheet ->> 'accountsPayable' AS float) AS accountsPayable,
        CAST(balance_sheet ->> 'intangibleAssets' AS float) AS intangibleAssets,
        CAST(balance_sheet ->> 'otherCurrentLiab' AS float) AS otherCurrentLiab,
        CAST(balance_sheet ->> 'retainedEarnings' AS float) AS retainedEarnings,
        CAST(balance_sheet ->> 'netTangibleAssets' AS float) AS netTangibleAssets,
        CAST(balance_sheet ->> 'shortLongTermDebt' AS float) AS shortLongTermDebt,
        CAST(balance_sheet ->> 'otherCurrentAssets' AS float) AS otherCurrentAssets,
        CAST(balance_sheet ->> 'totalCurrentAssets' AS float) AS totalCurrentAssets,
        CAST(balance_sheet ->> 'longTermInvestments' AS float) AS longTermInvestments,
        CAST(balance_sheet ->> 'deferredLongTermLiab' AS float) AS deferredLongTermLiab,
        CAST(balance_sheet ->> 'shortTermInvestments' AS float) AS shortTermInvestments,
        CAST(balance_sheet ->> 'totalPermanentEquity' AS float) AS totalPermanentEquity,
        CAST(balance_sheet ->> 'commonStockTotalEquity' AS float) AS commonStockTotalEquity,
        CAST(balance_sheet ->> 'otherStockholderEquity' AS float) AS otherStockholderEquity,
        CAST(balance_sheet ->> 'propertyPlantEquipment' AS float) AS propertyPlantEquipment,
        CAST(balance_sheet ->> 'totalStockholderEquity' AS float) AS totalStockholderEquity,
        CAST(balance_sheet ->> 'additionalPaidInCapital' AS float) AS additionalPaidInCapital,
        CAST(balance_sheet ->> 'totalCurrentLiabilities' AS float) AS totalCurrentLiabilities,
        CAST(balance_sheet ->> 'preferredStockTotalEquity' AS float) AS preferredStockTotalEquity,
        CAST(balance_sheet ->> 'retainedEarningsTotalEquity' AS float) AS retainedEarningsTotalEquity,
        CAST(balance_sheet ->> 'accumulatedOtherComprehensiveIncome' AS float) AS accumulatedOtherComprehensiveIncome,
        CAST(balance_sheet ->> 'noncontrollingInterestInConsolidatedEntity' AS float) AS noncontrollingInterestInConsolidatedEntity,
        CAST(balance_sheet ->> 'temporaryEquityRedeemableNoncontrollingInterests' AS float) AS temporaryEquityRedeemableNoncontrollingInterests
        FROM equity_balance_sheet
        WHERE tsid = '%s'
        AND vendor_id = '%s'
        AND date>='%s'::date AND date<='%s'::date;""" % (tsid, data_vendor_id, beg_date, end_date)


def equity_income_statement(tsid, beg_date, end_date, data_vendor_id=20):
    return """SELECT tsid, 
        date, 
        frequency, 
        CAST(income_statement ->> 'ebit' AS float) AS ebit,
        CAST(income_statement ->> 'netIncome' AS float) AS netIncome,
        CAST(income_statement ->> 'otherItems' AS float) AS otherItems,
        CAST(income_statement ->> 'grossProfit' AS float) AS grossProfit,
        CAST(income_statement ->> 'nonRecurring' AS float) AS nonRecurring,
        CAST(income_statement ->> 'totalRevenue' AS float) AS totalRevenue,
        CAST(income_statement ->> 'costOfRevenue' AS float) AS costOfRevenue,
        CAST(income_statement ->> 'incomeBeforeTax' AS float) AS incomeBeforeTax,
        CAST(income_statement ->> 'interestExpense' AS float) AS interestExpense,
        CAST(income_statement ->> 'operatingIncome' AS float) AS operatingIncome,
        CAST(income_statement ->> 'incomeTaxExpense' AS float) AS incomeTaxExpense,
        CAST(income_statement ->> 'minorityInterest' AS float) AS minorityInterest,
        CAST(income_statement ->> 'extraordinaryItems' AS float) AS extraordinaryItems,
        CAST(income_statement ->> 'researchDevelopment' AS float) AS researchDevelopment,
        CAST(income_statement ->> 'discontinuedOperations' AS float) AS discontinuedOperations,
        CAST(income_statement ->> 'otherOperatingExpenses' AS float) AS otherOperatingExpenses,
        CAST(income_statement ->> 'totalOperatingExpenses' AS float) AS totalOperatingExpenses,
        CAST(income_statement ->> 'effectOfAccountingCharges' AS float) AS effectOfAccountingCharges,
        CAST(income_statement ->> 'netIncomeFromContinuingOps' AS float) AS netIncomeFromContinuingOps,
        CAST(income_statement ->> 'totalOtherIncomeExpenseNet' AS float) AS totalOtherIncomeExpenseNet,
        CAST(income_statement ->> 'sellingGeneralAdministrative' AS float) AS sellingGeneralAdministrative,
        CAST(income_statement ->> 'netIncomeApplicableToCommonShares' AS float) AS netIncomeApplicableToCommonShares
        FROM equity_income_statement
        WHERE tsid = '%s'
        AND vendor_id = '%s'
        AND date>='%s'::date AND date<='%s'::date;""" % (tsid, data_vendor_id, beg_date, end_date)


def equity_key_metrics_highlights(tsid, beg_date, end_date, data_vendor_id=20):
    return """SELECT tsid, 
        date, 
        CAST(highlights ->> 'EBITDA' AS float) AS EBITDA,
        CAST(highlights ->> 'PERatio' AS float) AS PERatio,
        CAST(highlights ->> 'PEGRatio' AS float) AS PEGRatio,
        CAST(highlights ->> 'BookValue' AS float) AS BookValue,
        CAST(highlights ->> 'RevenueTTM' AS float) AS RevenueTTM,
        CAST(highlights ->> 'ProfitMargin' AS float) AS ProfitMargin,
        CAST(highlights ->> 'DilutedEpsTTM' AS float) AS DilutedEpsTTM,
        CAST(highlights ->> 'DividendShare' AS float) AS DividendShare,
        CAST(highlights ->> 'DividendYield' AS float) AS DividendYield,
        CAST(highlights ->> 'EarningsShare' AS float) AS EarningsShare,
        CAST(highlights ->> 'GrossProfitTTM' AS float) AS GrossProfitTTM,
        CAST(highlights ->> 'MostRecentQuarter' AS date) AS MostRecentQuarter,
        CAST(highlights ->> 'ReturnOnAssetsTTM' AS float) AS ReturnOnAssetsTTM,
        CAST(highlights ->> 'ReturnOnEquityTTM' AS float) AS ReturnOnEquityTTM,
        CAST(highlights ->> 'OperatingMarginTTM' AS float) AS OperatingMarginTTM,
        CAST(highlights ->> 'RevenuePerShareTTM' AS float) AS RevenuePerShareTTM,
        CAST(highlights ->> 'EPSEstimateNextYear' AS float) AS EPSEstimateNextYear,
        CAST(highlights ->> 'MarketCapitalization' AS float) AS MarketCapitalization,
        CAST(highlights ->> 'WallStreetTargetPrice' AS float) AS WallStreetTargetPrice,
        CAST(highlights ->> 'EPSEstimateCurrentYear' AS float) AS EPSEstimateCurrentYear,
        CAST(highlights ->> 'EPSEstimateNextQuarter' AS float) AS EPSEstimateNextQuarter,
        CAST(highlights ->> 'MarketCapitalizationMln' AS float) AS MarketCapitalizationMln,
        CAST(highlights ->> 'QuarterlyRevenueGrowthYOY' AS float) AS QuarterlyRevenueGrowthYOY,
        CAST(highlights ->> 'QuarterlyEarningsGrowthYOY' AS float) AS QuarterlyEarningsGrowthYOY,
        FROM key_metrics
        WHERE tsid = '%s'
        AND vendor_id = '%s'
        AND date>='%s'::date AND date<='%s'::date;""" % (tsid, data_vendor_id, beg_date, end_date)


def equity_key_metrics_valuation(tsid, beg_date, end_date, data_vendor_id=20):
    return """SELECT tsid, 
        date, 
        CAST(valuation ->> 'ForwardPE' AS float) AS ForwardPE,
        CAST(valuation ->> 'TralingPE' AS float) AS TrailingPE,
        CAST(valuation ->> 'PriceBookMRQ' AS float) AS PriceBookMRQ,
        CAST(valuation ->> 'PriceSalesTTM' AS float) AS PriceSalesTTM,
        CAST(valuation ->> 'EnterpriseValueEbitda' AS float) AS EnterpriseValueEbitda,
        CAST(valuation ->> 'EnterpriseValueRevenue' AS float) AS EnterpriseValueRevenue
        FROM key_metrics
        WHERE tsid = '%s'
        AND vendor_id = '%s'
        AND date>='%s'::date AND date<='%s'::date;""" % (tsid, data_vendor_id, beg_date, end_date)


def equity_key_metrics_technicals(tsid, beg_date, end_date, data_vendor_id=20):
    return """SELECT tsid, 
        date, 
        CAST(technicals ->> 'Beta' AS float) AS Beta,
        CAST(technicals ->> '50DayMA' AS float) AS 50DayMA,
        CAST(technicals ->> '200DayMA' AS float) AS 200DayMA,
        CAST(technicals ->> '52WeekLow' AS float) AS 52WeekLow,
        CAST(technicals ->> '52WeekHigh' AS float) AS 52WeekHigh,
        CAST(technicals ->> 'ShortRatio' AS float) AS ShortRatio,
        CAST(technicals ->> 'SharesShort' AS float) AS SharesShort,
        CAST(technicals ->> 'ShortPercent' AS float) AS ShortPercent,
        CAST(technicals ->> 'SharesShortPriorMonth' AS float) AS SharesShortPriorMonth
        FROM key_metrics
        WHERE tsid = '%s'
        AND vendor_id = '%s'
        AND date>='%s'::date AND date<='%s'::date;""" % (tsid, data_vendor_id, beg_date, end_date)


def equity_key_metrics_splitdiv(tsid, beg_date, end_date, data_vendor_id=20):
    return """SELECT tsid, 
        date, 
        CAST(splitdiv ->> 'PayoutRatio' AS float) AS PayoutRatio,
        CAST(splitdiv ->> 'DividendDate' AS date) AS DividendDate,
        CAST(splitdiv ->> 'LastSplitDate' AS date) AS LastSplitDate,
        CAST(splitdiv ->> 'ExDividendDate' AS date) AS ExDividendDate,
        CAST(splitdiv ->> 'LastSplitFactor' AS text) AS LastSplitFactor,
        CAST(splitdiv ->> 'ForwardAnnualDividendRate' AS float) AS ForwardAnnualDividendRate,
        CAST(splitdiv ->> 'ForwardAnnualDividendYield' AS float) AS ForwardAnnualDividendYield
        FROM key_metrics
        WHERE tsid = '%s'
        AND vendor_id = '%s'
        AND date>='%s'::date AND date<='%s'::date;""" % (tsid, data_vendor_id, beg_date, end_date)
