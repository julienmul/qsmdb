def check_asset_query(tsid, data_vendor_id):
    return """SELECT exists (SELECT 1 FROM classification 
                            WHERE tsid = '%s'
                            AND vendor_id = '%s' 
                            LIMIT 1);""" % (tsid, data_vendor_id)


def get_asset_type(tsid, data_vendor_id):
    return """SELECT type FROM classification
        WHERE tsid = '%s'
        AND vendor_id = '%s'""" % (tsid, data_vendor_id)


def get_asset_sector(tsid, data_vendor_id):
    return """SELECT sector FROM classification
        WHERE tsid = '%s'
        AND vendor_id = '%s'""" % (tsid, data_vendor_id)


def get_asset_industry(tsid, data_vendor_id):
    return """SELECT industry FROM classification
        WHERE tsid = '%s'
        AND vendor_id = '%s'""" % (tsid, data_vendor_id)


def get_unique_sector(asset_type, data_vendor_id):
    return """SELECT sector, count(sector) FROM classification
            WHERE type = '%s'
            AND vendor_id = '%s'""" % (asset_type, data_vendor_id)


def get_unique_industry(asset_type, kind, data_vendor_id):
    if kind == 'all':
        return """SELECT industry, count(industry) FROM classification
                    WHERE type = '%s'
                    AND vendor_id = '%s'""" % (asset_type, data_vendor_id)
    else:
        return """SELECT sector, count(sector) FROM classification
                            WHERE type = '%s'
                            AND sector = '%s'
                            AND vendor_id = '%s'""" % (asset_type, kind, data_vendor_id)


# def get_sector_list(data_vendor_id):
#     return """SELECT tsid, type, countryiso, industry FROM classification
#                 WHERE tsid = '%s'
#                 AND vendor_id = '%s'""" % (data_vendor_id)


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


def eq_earnings_history(tsid, beg_date, end_date, data_vendor_id=20):
    return """SELECT tsid,
        date,
        CAST(epsactual AS float) AS eps_actual,
        CAST(epsestimate As float) AS eps_estimate,
        CAST(epsdifference AS float) AS eps_difference,
        CAST(surprisepercent AS float) AS surprise_percent
        FROM equity_earnings_history
        WHERE tsid = '%s'
        AND vendor_id = '%s'
        AND date>='%s'::date AND date<='%s'::date;""" % (tsid, data_vendor_id, beg_date, end_date)


def eq_earnings_trend(tsid, beg_date, end_date, data_vendor_id=20):
    return """SELECT tsid, 
        extraction_date, 
        date, 
        period, 
        CAST(earnings_trend ->> 'growth' AS float) AS growth,
        CAST(earnings_trend ->> 'epsTrendCurrent' AS float) AS eps_Trend_Current,
        CAST(earnings_trend ->> 'epsTrend7daysAgo' AS float) AS eps_Trend_7days_Ago,
        CAST(earnings_trend ->> 'epsTrend30daysAgo' AS float) AS eps_Trend_30days_Ago,
        CAST(earnings_trend ->> 'epsTrend60daysAgo' AS float) AS eps_Trend_60days_Ago,
        CAST(earnings_trend ->> 'epsTrend90daysAgo' AS float) AS eps_Trend_90days_Ago,
        CAST(earnings_trend ->> 'epsRevisionsUpLast7days' AS float) AS eps_Revisions_Up_Last_7days,
        CAST(earnings_trend ->> 'epsRevisionsUpLast30days' AS float) AS eps_Revisions_Up_Last_30days,
        CAST(earnings_trend ->> 'epsRevisionsDownLast30days' AS float) AS eps_Revisions_Down_Last_30days,
        CAST(earnings_trend ->> 'epsRevisionsDownLast90days' AS float) AS eps_Revisions_Down_Last_90days,
        CAST(earnings_trend ->> 'revenueEstimateAvg' AS float) AS revenue_Estimate_Avg,
        CAST(earnings_trend ->> 'revenueEstimateLow' AS float) AS revenue_Estimate_Low,
        CAST(earnings_trend ->> 'revenueEstimateHigh' AS float) AS revenue_Estimate_High,
        CAST(earnings_trend ->> 'revenueEstimateGrowth' AS float) AS revenue_Estimate_Growth,
        CAST(earnings_trend ->> 'revenueEstimateNumberOfAnalysts' AS float) AS revenue_Estimate_Number_Of_Analysts,
        CAST(earnings_trend ->> 'revenueEstimateYearAgoEps' AS float) AS revenue_Estimate_Year_Ago_Eps,
        CAST(earnings_trend ->> 'earningsEstimateAvg' AS float) AS earnings_Estimate_Avg,
        CAST(earnings_trend ->> 'earningsEstimateLow' AS float) AS earnings_Estimate_Low,
        CAST(earnings_trend ->> 'earningsEstimateHigh' AS float) AS earnings_Estimate_High,
        CAST(earnings_trend ->> 'earningsEstimateGrowth' AS float) AS earnings_Estimate_Growth,
        CAST(earnings_trend ->> 'earningsEstimateYearAgoEps' AS float) AS earnings_Estimate_Year_Ago_Eps
        FROM equity_earnings_trend
        WHERE tsid = '%s'
        AND vendor_id = '%s'
        AND extraction_date>='%s'::date AND extraction_date<='%s'::date;""" % (tsid, data_vendor_id, beg_date, end_date)


def eq_cash_flow(tsid, beg_date, end_date, data_vendor_id=20):
    return """SELECT tsid, 
        date, 
        frequency, 
        CAST(cash_flow ->> 'netIncome' AS float) AS net_Income,
        CAST(NULLIF(cash_flow ->> 'filing_date', '0000-00-00') AS date) AS filing_date,
        CAST(cash_flow ->> 'investments' AS float) AS investments,
        CAST(cash_flow ->> 'changeInCash' AS float) AS change_In_Cash,
        CAST(cash_flow ->> 'depreciation' AS float) AS depreciation,
        CAST(cash_flow ->> 'dividendsPaid' AS float) AS dividends_Paid,
        CAST(cash_flow ->> 'netBorrowings' AS float) AS net_Borrowings,
        CAST(cash_flow ->> 'changeToInventory' AS float) AS change_To_Inventory,
        CAST(cash_flow ->> 'changeToNetincome' AS float) AS change_To_Netincome,
        CAST(cash_flow ->> 'capitalExpenditures' AS float) AS capital_Expenditures,
        CAST(cash_flow ->> 'changeToLiabilities' AS float) AS change_To_Liabilities,
        CAST(cash_flow ->> 'salePurchaseOfStock' AS float) AS sale_Purchase_Of_Stock,
        CAST(cash_flow ->> 'changeToAccountReceivables' AS float) AS change_To_Account_Receivables,
        CAST(cash_flow ->> 'changeToOperatingActivities' AS float) AS change_To_Operating_Activities,
        CAST(cash_flow ->> 'totalCashFromFinancingActivities' AS float) AS total_Cash_From_Financing_Activities,
        CAST(cash_flow ->> 'totalCashFromOperatingActivities' AS float) AS total_Cash_From_Operating_Activities,
        CAST(cash_flow ->> 'otherCashflowsFromFinancingActivities' AS float) AS other_Cashflows_From_Financing_Activities,
        CAST(cash_flow ->> 'otherCashflowsFromInvestingActivities' AS float) AS other_Cashflows_From_Investing_Activities,
        CAST(cash_flow ->> 'totalCashflowsFromInvestingActivities' AS float) AS total_Cashflows_From_Investing_Activities,
        CAST(cash_flow ->> 'earningsEstimateHigh' AS float) AS earnings_Estimate_High,
        CAST(cash_flow ->> 'earningsEstimateGrowth' AS float) AS earnings_Estimate_Growth,
        CAST(cash_flow ->> 'earningsEstimateYearAgoEps' AS float) AS earnings_Estimate_Year_Ago_Eps,
        CAST(cash_flow ->> 'earningsEstimateNumberOfAnalysts' AS float) AS earnings_Estimate_Number_Of_Analysts
        FROM equity_cash_flow
        WHERE tsid = '%s'
        AND vendor_id = '%s'
        AND date>='%s'::date AND date<='%s'::date;""" % (tsid, data_vendor_id, beg_date, end_date)


def eq_balance_sheet(tsid, beg_date, end_date, data_vendor_id=20):
    return """SELECT tsid, 
        date, 
        frequency, 
        CAST(balance_sheet ->> 'cash' AS float) AS cash,
        CAST(balance_sheet ->> 'goodWill' AS float) AS goodWill,
        CAST(balance_sheet ->> 'inventory' AS float) AS inventory,
        CAST(balance_sheet ->> 'otherLiab' AS float) AS other_Liab,
        CAST(balance_sheet ->> 'totalLiab' AS float) AS total_Liab,
        CAST(balance_sheet ->> 'commonStock' AS float) AS commonStock,
        CAST(NULLIF(balance_sheet ->> 'filing_date', '0000-00-00') AS date) AS filing_date,
        CAST(balance_sheet ->> 'otherAssets' AS float) AS other_Assets,
        CAST(balance_sheet ->> 'totalAssets' AS float) AS total_Assets,
        CAST(balance_sheet ->> 'longTermDebt' AS float) AS long_Term_Debt,
        CAST(balance_sheet ->> 'treasuryStock' AS float) AS treasury_Stock,
        CAST(balance_sheet ->> 'netReceivables' AS float) AS net_Receivables,
        CAST(balance_sheet ->> 'accountsPayable' AS float) AS accounts_Payable,
        CAST(balance_sheet ->> 'intangibleAssets' AS float) AS intangible_Assets,
        CAST(balance_sheet ->> 'otherCurrentLiab' AS float) AS other_Current_Liab,
        CAST(balance_sheet ->> 'retainedEarnings' AS float) AS retained_Earnings,
        CAST(balance_sheet ->> 'netTangibleAssets' AS float) AS net_Tangible_Assets,
        CAST(balance_sheet ->> 'shortLongTermDebt' AS float) AS short_Long_Term_Debt,
        CAST(balance_sheet ->> 'otherCurrentAssets' AS float) AS other_Current_Assets,
        CAST(balance_sheet ->> 'totalCurrentAssets' AS float) AS total_Current_Assets,
        CAST(balance_sheet ->> 'longTermInvestments' AS float) AS long_Term_Investments,
        CAST(balance_sheet ->> 'deferredLongTermLiab' AS float) AS deferred_Long_Term_Liab,
        CAST(balance_sheet ->> 'shortTermInvestments' AS float) AS short_Term_Investments,
        CAST(balance_sheet ->> 'totalPermanentEquity' AS float) AS total_Permanent_Equity,
        CAST(balance_sheet ->> 'commonStockTotalEquity' AS float) AS commonStock_Total_Equity,
        CAST(balance_sheet ->> 'otherStockholderEquity' AS float) AS other_Stockholder_Equity,
        CAST(balance_sheet ->> 'propertyPlantEquipment' AS float) AS property_Plant_Equipment,
        CAST(balance_sheet ->> 'totalStockholderEquity' AS float) AS total_Stockholder_Equity,
        CAST(balance_sheet ->> 'additionalPaidInCapital' AS float) AS additional_Paid_In_Capital,
        CAST(balance_sheet ->> 'totalCurrentLiabilities' AS float) AS total_Current_Liabilities,
        CAST(balance_sheet ->> 'preferredStockTotalEquity' AS float) AS preferredStock_Total_Equity,
        CAST(balance_sheet ->> 'retainedEarningsTotalEquity' AS float) AS retained_Earnings_Total_Equity,
        CAST(balance_sheet ->> 'accumulatedOtherComprehensiveIncome' AS float) AS accumulated_Other_Comprehensive_Income,
        CAST(balance_sheet ->> 'noncontrollingInterestInConsolidatedEntity' AS float) AS non_controlling_Interest_In_Consolidated_Entity,
        CAST(balance_sheet ->> 'temporaryEquityRedeemableNoncontrollingInterests' AS float) AS temporary_Equity_Redeemable_Noncontrolling_Interests
        FROM equity_balance_sheet
        WHERE tsid = '%s'
        AND vendor_id = '%s'
        AND date>='%s'::date AND date<='%s'::date;""" % (tsid, data_vendor_id, beg_date, end_date)


def eq_income_statement(tsid, beg_date, end_date, data_vendor_id=20):
    return """SELECT tsid, 
        date, 
        frequency, 
        CAST(income_statement ->> 'ebit' AS float) AS ebit,
        CAST(income_statement ->> 'netIncome' AS float) AS net_Income,
        CAST(income_statement ->> 'otherItems' AS float) AS other_Items,
        CAST(income_statement ->> 'grossProfit' AS float) AS gross_Profit,
        CAST(income_statement ->> 'nonRecurring' AS float) AS non_Recurring,
        CAST(income_statement ->> 'totalRevenue' AS float) AS total_Revenue,
        CAST(income_statement ->> 'costOfRevenue' AS float) AS cost_Of_Revenue,
        CAST(income_statement ->> 'incomeBeforeTax' AS float) AS income_Before_Tax,
        CAST(income_statement ->> 'interestExpense' AS float) AS interest_Expense,
        CAST(income_statement ->> 'operatingIncome' AS float) AS operating_Income,
        CAST(income_statement ->> 'incomeTaxExpense' AS float) AS income_Tax_Expense,
        CAST(income_statement ->> 'minorityInterest' AS float) AS minority_Interest,
        CAST(income_statement ->> 'extraordinaryItems' AS float) AS extraordinary_Items,
        CAST(income_statement ->> 'researchDevelopment' AS float) AS research_Development,
        CAST(income_statement ->> 'discontinuedOperations' AS float) AS discontinued_Operations,
        CAST(income_statement ->> 'otherOperatingExpenses' AS float) AS other_Operating_Expenses,
        CAST(income_statement ->> 'totalOperatingExpenses' AS float) AS total_Operating_Expenses,
        CAST(income_statement ->> 'effectOfAccountingCharges' AS float) AS effect_Of_Accounting_Charges,
        CAST(income_statement ->> 'netIncomeFromContinuingOps' AS float) AS net_Income_From_Continuing_Ops,
        CAST(income_statement ->> 'totalOtherIncomeExpenseNet' AS float) AS total_Other_Income_Expense_Net,
        CAST(income_statement ->> 'sellingGeneralAdministrative' AS float) AS selling_General_Administrative,
        CAST(income_statement ->> 'netIncomeApplicableToCommonShares' AS float) AS netIncome_Applicable_To_CommonShares
        FROM equity_income_statement
        WHERE tsid = '%s'
        AND vendor_id = '%s'
        AND date>='%s'::date AND date<='%s'::date;""" % (tsid, data_vendor_id, beg_date, end_date)


def eq_key_metrics_highlights(tsid, beg_date, end_date, data_vendor_id=20):
    return """SELECT tsid, 
        date, 
        CAST(highlights ->> 'EBITDA' AS float) AS EBITDA,
        CAST(highlights ->> 'PERatio' AS float) AS PE_Ratio,
        CAST(highlights ->> 'PEGRatio' AS float) AS PEG_Ratio,
        CAST(highlights ->> 'BookValue' AS float) AS Book_Value,
        CAST(highlights ->> 'RevenueTTM' AS float) AS Revenue_TTM,
        CAST(highlights ->> 'ProfitMargin' AS float) AS Profit_Margin,
        CAST(highlights ->> 'DilutedEpsTTM' AS float) AS Diluted_Eps_TTM,
        CAST(highlights ->> 'DividendShare' AS float) AS Dividend_Share,
        CAST(highlights ->> 'DividendYield' AS float) AS Dividend_Yield,
        CAST(highlights ->> 'EarningsShare' AS float) AS Earnings_Share,
        CAST(highlights ->> 'GrossProfitTTM' AS float) AS Gross_Profit_TTM,
        CAST(NULLIF(highlights ->> 'MostRecentQuarter', '0000-00-00') AS date) AS Most_Recent_Quarter,
        CAST(highlights ->> 'ReturnOnAssetsTTM' AS float) AS Return_On_Assets_TTM,
        CAST(highlights ->> 'ReturnOnEquityTTM' AS float) AS Return_On_Equity_TTM,
        CAST(highlights ->> 'OperatingMarginTTM' AS float) AS Operating_Margin_TTM,
        CAST(highlights ->> 'RevenuePerShareTTM' AS float) AS Revenue_Per_Share_TTM,
        CAST(highlights ->> 'EPSEstimateNextYear' AS float) AS EPS_Estimate_NextYear,
        CAST(highlights ->> 'MarketCapitalization' AS float) AS Market_Capitalization,
        CAST(highlights ->> 'WallStreetTargetPrice' AS float) AS WallStreet_Target_Price,
        CAST(highlights ->> 'EPSEstimateCurrentYear' AS float) AS EPS_Estimate_Current_Year,
        CAST(highlights ->> 'EPSEstimateNextQuarter' AS float) AS EPS_Estimate_Next_Quarter,
        CAST(highlights ->> 'MarketCapitalizationMln' AS float) AS Market_Capitalization_Mln,
        CAST(highlights ->> 'QuarterlyRevenueGrowthYOY' AS float) AS Quarterly_Revenue_Growth_YOY,
        CAST(highlights ->> 'QuarterlyEarningsGrowthYOY' AS float) AS Quarterly_Earnings_Growth_YOY
        FROM key_metrics
        WHERE tsid = '%s'
        AND vendor_id = '%s'
        AND date>='%s'::date AND date<='%s'::date;""" % (tsid, data_vendor_id, beg_date, end_date)


def eq_key_metrics_valuation(tsid, beg_date, end_date, data_vendor_id=20):
    return """SELECT tsid, 
        date, 
        CAST(valuation ->> 'ForwardPE' AS float) AS Forward_PE,
        CAST(valuation ->> 'TralingPE' AS float) AS Trailing_PE,
        CAST(valuation ->> 'PriceBookMRQ' AS float) AS Price_Book_MRQ,
        CAST(valuation ->> 'PriceSalesTTM' AS float) AS Price_Sales_TTM,
        CAST(valuation ->> 'EnterpriseValueEbitda' AS float) AS Enterprise_Value_Ebitda,
        CAST(valuation ->> 'EnterpriseValueRevenue' AS float) AS Enterprise_Value_Revenue
        FROM key_metrics
        WHERE tsid = '%s'
        AND vendor_id = '%s'
        AND date>='%s'::date AND date<='%s'::date;""" % (tsid, data_vendor_id, beg_date, end_date)


def eq_key_metrics_technicals(tsid, beg_date, end_date, data_vendor_id=20):
    return """SELECT tsid, 
        date, 
        CAST(technicals ->> 'Beta' AS float) AS Beta,
        CAST(technicals ->> '50DayMA' AS float) AS MA50_Day,
        CAST(technicals ->> '200DayMA' AS float) AS MA200_Day,
        CAST(technicals ->> '52WeekLow' AS float) AS Week52_Low,
        CAST(technicals ->> '52WeekHigh' AS float) AS Week52_High,
        CAST(technicals ->> 'ShortRatio' AS float) AS Short_Ratio,
        CAST(technicals ->> 'SharesShort' AS float) AS Shares_Short,
        CAST(technicals ->> 'ShortPercent' AS float) AS Short_Percent,
        CAST(technicals ->> 'SharesShortPriorMonth' AS float) AS Shares_Short_Prior_Month
        FROM key_metrics
        WHERE tsid = '%s'
        AND vendor_id = '%s'
        AND date>='%s'::date AND date<='%s'::date;""" % (tsid, data_vendor_id, beg_date, end_date)


def eq_key_metrics_splitdiv(tsid, beg_date, end_date, data_vendor_id=20):
    return """SELECT tsid, 
        date, 
        CAST(splitdiv ->> 'PayoutRatio' AS float) AS PayoutRatio,
        CAST(NULLIF(splitdiv ->> 'DividendDate', '0000-00-00') AS date) AS Dividend_Date,
        CAST(NULLIF(splitdiv ->> 'LastSplitDate', '0000-00-00') AS date) AS Last_Split_Date,
        CAST(NULLIF(splitdiv ->> 'ExDividendDate', '0000-00-00') AS date) AS Ex_Dividend_Date,
        CAST(splitdiv ->> 'LastSplitFactor' AS text) AS Last_Split_Factor,
        CAST(splitdiv ->> 'ForwardAnnualDividendRate' AS float) AS Forward_Annual_Dividend_Rate,
        CAST(splitdiv ->> 'ForwardAnnualDividendYield' AS float) AS Forward_Annual_Dividend_Yield
        FROM key_metrics
        WHERE tsid = '%s'
        AND vendor_id = '%s'
        AND date>='%s'::date AND date<='%s'::date;""" % (tsid, data_vendor_id, beg_date, end_date)


# TODO: implement asset allocation ratio
def fund_key_metrics_highlights(tsid, beg_date, end_date, data_vendor_id=20):
    return """SELECT tsid, 
        date, 
        CAST(NULLIF(highlights ->> 'Yield', '-') AS float) AS Yield,
        CAST(NULLIF(highlights -> 'MorningStar' ->> 'Ratio', '-') AS text) AS MorningStar_Ratio,
        CAST(NULLIF(highlights -> 'MorningStar' ->> 'Category_Benchmark', '-') AS text) AS MorningStar_Category_Benchmark,
        CAST(NULLIF(highlights -> 'MorningStar' ->> 'Sustainability_Ratio', '-') AS text) AS MorningStar_Sustainability_Ratio,
        CAST(NULLIF(highlights -> 'Performance' ->> 'Returns_3Y', '-') AS float) AS Returns_3Y,
        CAST(NULLIF(highlights -> 'Performance' ->> 'Returns_5Y', '-') AS float) AS Returns_5Y,
        CAST(NULLIF(highlights -> 'Performance' ->> 'Returns_10Y', '-') AS float) AS Returns_10Y,
        CAST(NULLIF(highlights -> 'Performance' ->> 'Returns_YTD', '-') AS float) AS Returns_YTD,
        CAST(NULLIF(highlights -> 'Performance' ->> '3y_ExpReturn', '-') AS float) AS 3y_Exp_Return,
        CAST(NULLIF(highlights -> 'Performance' ->> '3y_SharpRatio', '-') AS float) AS 3y_Sharp_Ratio,
        CAST(NULLIF(highlights -> 'Performance' ->> '3y_Volatility', '-') AS float) AS 3y_Volatility,
        CAST(NULLIF(highlights ->> 'Inception_Date', '0000-00-00') AS date) AS Most_Recent_Quarter,
        CAST(NULLIF(highlights ->> 'Ongoing_Charge', '-') AS float) AS Ongoing_Charge,
        CAST(NULLIF(highlights ->> 'NetExpenseRatio', '-') AS float) AS Net_Expense_Ratio
        FROM key_metrics
        WHERE tsid = '%s'
        AND vendor_id = '%s'
        AND date>='%s'::date AND date<='%s'::date;""" % (tsid, data_vendor_id, beg_date, end_date)


# TODO: implement query for valuation
def fund_key_metrics_valuation(tsid, beg_date, end_date, data_vendor_id=20):
    return """SELECT tsid, 
        date, 
        CAST(NULLIF(valuation -> 'Valuations_Growth' -> 'Growth_Rates_Portfolio' ->> 'Sales Growth', '-') AS float) AS Sales_Growth,
        CAST(NULLIF(valuation -> 'Valuations_Growth' -> 'Growth_Rates_Portfolio' ->> 'Cash-Flow Growth', '-') AS float) AS CashFlow_Growth,
        CAST(NULLIF(valuation -> 'Valuations_Growth' -> 'Growth_Rates_Portfolio' ->> 'Book-Value Growth', '-') AS float) AS BookValue_Growth,
        CAST(NULLIF(valuation -> 'Valuations_Growth' -> 'Growth_Rates_Portfolio' ->> 'Historical Earnings Growth', '-') AS float) AS Historical_Earnings_Growth,
        CAST(NULLIF(valuation -> 'Valuations_Growth' -> 'Growth_Rates_Portfolio' ->> 'Long-Term Projected Earnings Growth', '-') AS float) AS LongTerm_Projected_Earnings_Growth,
        CAST(NULLIF(valuation -> 'Valuations_Growth' -> 'Valuations_Rates_Portfolio' ->> 'Price/Book', '-') AS float) AS Price_Book,
        CAST(NULLIF(valuation -> 'Valuations_Growth' -> 'Valuations_Rates_Portfolio' ->> 'Price/Sales', '-') AS float) AS Price_Sales,
        CAST(NULLIF(valuation -> 'Valuations_Growth' -> 'Valuations_Rates_Portfolio' ->> 'Price/Cash Flow', '-') AS float) AS Price_Cash_Flow,
        CAST(NULLIF(valuation -> 'Valuations_Growth' -> 'Valuations_Rates_Portfolio' ->> 'Dividend-Yield Factor', '-') AS float) AS Dividend_Yield_Factor,
        CAST(NULLIF(valuation -> 'Valuations_Growth' -> 'Valuations_Rates_Portfolio' ->> 'Price/Prospective Earnings', '-') AS float) AS Price_Prospective_Earnings
        FROM key_metrics
        WHERE tsid = '%s'
        AND vendor_id = '%s'
        AND date>='%s'::date AND date<='%s'::date;""" % (tsid, data_vendor_id, beg_date, end_date)


def fund_key_metrics_technicals(tsid, beg_date, end_date, data_vendor_id=20):
    return """SELECT tsid, 
        date, 
        CAST(NULLIF(technicals ->> 'Beta', '-') AS float) AS Beta,
        CAST(NULLIF(technicals ->> '50DayMA', '-') AS float) AS MA50_Day,
        CAST(NULLIF(technicals ->> '200DayMA', '-') AS float) AS MA200_Day,
        CAST(NULLIF(technicals ->> '52WeekLow', '-') AS float) AS Week52_Low,
        CAST(NULLIF(technicals ->> '52WeekHigh', '-') AS float) AS Week52_High
        FROM key_metrics
        WHERE tsid = '%s'
        AND vendor_id = '%s'
        AND date>='%s'::date AND date<='%s'::date;""" % (tsid, data_vendor_id, beg_date, end_date)


# TODO: implement fund holdings stats fro equity and bond etp
def fund_key_metrics_holdings(tsid, beg_date, end_date, data_vendor_id=20):
    pass
