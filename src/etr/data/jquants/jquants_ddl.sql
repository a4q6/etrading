PRAGMA journal_mode = DELTE;
PRAGMA page_size = 4096;   -- VACUUM を後で必ず実行
PRAGMA foreign_keys = ON;
VACUUM;

-- /listed/info
CREATE TABLE IF NOT EXISTS listed_info (
  Date TEXT NOT NULL,  -- YYYY-MM-DD
  Code TEXT NOT NULL,
  CompanyName TEXT,
  CompanyNameEnglish TEXT,
  Sector17Code TEXT,
  Sector17CodeName TEXT,
  Sector33Code TEXT,
  Sector33CodeName TEXT,
  ScaleCategory TEXT,
  MarketCode TEXT,
  MarketCodeName TEXT,
  ingested_at TEXT DEFAULT (datetime('now')),
  PRIMARY KEY (Date, Code)
);

CREATE TABLE IF NOT EXISTS indices_info (
  Code TEXT NOT NULL,
  IndexName TEXT NOT NULL,
  Sector17CodeName TEXT,
  Sector33CodeName TEXT,
  ingested_at TEXT DEFAULT (datetime('now')),
  PRIMARY KEY (Code, IndexName)
);

-- /prices/daily_quotes
CREATE TABLE IF NOT EXISTS prices_daily_quotes (
  Date TEXT NOT NULL,
  Code TEXT NOT NULL,
  Open REAL,
  High REAL,
  Low REAL,
  Close REAL,
  UpperLimit TEXT,
  LowerLimit TEXT,
  Volume REAL,
  TurnoverValue REAL,
  AdjustmentFactor REAL,
  AdjustmentOpen REAL,
  AdjustmentHigh REAL,
  AdjustmentLow REAL,
  AdjustmentClose REAL,
  AdjustmentVolume REAL,
  -- Morning/Afternoon は Premium のみ取得可能の注記あり（NULLでOK） :contentReference[oaicite:1]{index=1}
  MorningOpen REAL,
  MorningHigh REAL,
  MorningLow REAL,
  MorningClose REAL,
  MorningUpperLimit TEXT,
  MorningLowerLimit TEXT,
  MorningVolume REAL,
  MorningTurnoverValue REAL,
  MorningAdjustmentOpen REAL,
  MorningAdjustmentHigh REAL,
  MorningAdjustmentLow REAL,
  MorningAdjustmentClose REAL,
  MorningAdjustmentVolume REAL,
  AfternoonOpen REAL,
  AfternoonHigh REAL,
  AfternoonLow REAL,
  AfternoonClose REAL,
  AfternoonUpperLimit TEXT,
  AfternoonLowerLimit TEXT,
  AfternoonVolume REAL,
  AfternoonTurnoverValue REAL,
  AfternoonAdjustmentOpen REAL,
  AfternoonAdjustmentHigh REAL,
  AfternoonAdjustmentLow REAL,
  AfternoonAdjustmentClose REAL,
  AfternoonAdjustmentVolume REAL,
  ingested_at TEXT DEFAULT (datetime('now')),
  PRIMARY KEY (Date, Code)
);

-- /markets/trading_calendar
CREATE TABLE IF NOT EXISTS markets_trading_calendar (
  Date TEXT NOT NULL,
  HolidayDivision TEXT NOT NULL,
  ingested_at TEXT DEFAULT (datetime('now')),
  PRIMARY KEY (Date, HolidayDivision)
);

-- /fins/statements
CREATE TABLE IF NOT EXISTS fins_statements (
  DisclosedDate TEXT NOT NULL,
  DisclosedTime TEXT,
  LocalCode TEXT NOT NULL,
  DisclosureNumber TEXT NOT NULL,
  TypeOfDocument TEXT,
  TypeOfCurrentPeriod TEXT,
  CurrentPeriodStartDate TEXT,
  CurrentPeriodEndDate TEXT,
  CurrentFiscalYearStartDate TEXT,
  CurrentFiscalYearEndDate TEXT,
  NextFiscalYearStartDate TEXT,
  NextFiscalYearEndDate TEXT,
  NetSales REAL,
  OperatingProfit REAL,
  OrdinaryProfit REAL,
  Profit REAL,
  EarningsPerShare REAL,
  DilutedEarningsPerShare REAL,
  TotalAssets REAL,
  Equity REAL,
  EquityToAssetRatio REAL,
  BookValuePerShare REAL,
  CashFlowsFromOperatingActivities REAL,
  CashFlowsFromInvestingActivities REAL,
  CashFlowsFromFinancingActivities REAL,
  CashAndEquivalents REAL,
  ResultDividendPerShare1stQuarter REAL,
  ResultDividendPerShare2ndQuarter REAL,
  ResultDividendPerShare3rdQuarter REAL,
  ResultDividendPerShareFiscalYearEnd REAL,
  ResultDividendPerShareAnnual REAL,
  DistributionsPerUnitREIT REAL,
  ResultTotalDividendPaidAnnual REAL,
  ResultPayoutRatioAnnual REAL,
  ForecastDividendPerShare1stQuarter REAL,
  ForecastDividendPerShare2ndQuarter REAL,
  ForecastDividendPerShare3rdQuarter REAL,
  ForecastDividendPerShareFiscalYearEnd REAL,
  ForecastDividendPerShareAnnual REAL,
  ForecastDistributionsPerUnitREIT REAL,
  ForecastTotalDividendPaidAnnual REAL,
  ForecastPayoutRatioAnnual REAL,
  NextYearForecastDividendPerShare1stQuarter REAL,
  NextYearForecastDividendPerShare2ndQuarter REAL,
  NextYearForecastDividendPerShare3rdQuarter REAL,
  NextYearForecastDividendPerShareFiscalYearEnd REAL,
  NextYearForecastDividendPerShareAnnual REAL,
  NextYearForecastDistributionsPerUnitREIT REAL,
  NextYearForecastPayoutRatioAnnual REAL,
  ForecastNetSales2ndQuarter REAL,
  ForecastOperatingProfit2ndQuarter REAL,
  ForecastOrdinaryProfit2ndQuarter REAL,
  ForecastProfit2ndQuarter REAL,
  ForecastEarningsPerShare2ndQuarter REAL,
  NextYearForecastNetSales2ndQuarter REAL,
  NextYearForecastOperatingProfit2ndQuarter REAL,
  NextYearForecastOrdinaryProfit2ndQuarter REAL,
  NextYearForecastProfit2ndQuarter REAL,
  NextYearForecastEarningsPerShare2ndQuarter REAL,
  ForecastNetSales REAL,
  ForecastOperatingProfit REAL,
  ForecastOrdinaryProfit REAL,
  ForecastProfit REAL,
  ForecastEarningsPerShare REAL,
  NextYearForecastNetSales REAL,
  NextYearForecastOperatingProfit REAL,
  NextYearForecastOrdinaryProfit REAL,
  NextYearForecastProfit REAL,
  NextYearForecastEarningsPerShare REAL,
  MaterialChangesInSubsidiaries TEXT,
  SignificantChangesInTheScopeOfConsolidation TEXT,
  ChangesBasedOnRevisionsOfAccountingStandard TEXT,
  ChangesOtherThanOnesBasedOnRevisionsOfAccountingStandard TEXT,
  ChangesInAccountingEstimates TEXT,
  RetrospectiveRestatement TEXT,
  NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock REAL,
  NumberOfTreasuryStockAtTheEndOfFiscalYear REAL,
  AverageNumberOfShares REAL,
  NonConsolidatedNetSales REAL,
  NonConsolidatedOperatingProfit REAL,
  NonConsolidatedOrdinaryProfit REAL,
  NonConsolidatedProfit REAL,
  NonConsolidatedEarningsPerShare REAL,
  NonConsolidatedTotalAssets REAL,
  NonConsolidatedEquity REAL,
  NonConsolidatedEquityToAssetRatio REAL,
  NonConsolidatedBookValuePerShare REAL,
  ForecastNonConsolidatedNetSales2ndQuarter REAL,
  ForecastNonConsolidatedOperatingProfit2ndQuarter REAL,
  ForecastNonConsolidatedOrdinaryProfit2ndQuarter REAL,
  ForecastNonConsolidatedProfit2ndQuarter REAL,
  ForecastNonConsolidatedEarningsPerShare2ndQuarter REAL,
  NextYearForecastNonConsolidatedNetSales2ndQuarter REAL,
  NextYearForecastNonConsolidatedOperatingProfit2ndQuarter REAL,
  NextYearForecastNonConsolidatedOrdinaryProfit2ndQuarter REAL,
  NextYearForecastNonConsolidatedProfit2ndQuarter REAL,
  NextYearForecastNonConsolidatedEarningsPerShare2ndQuarter REAL,
  ForecastNonConsolidatedNetSales REAL,
  ForecastNonConsolidatedOperatingProfit REAL,
  ForecastNonConsolidatedOrdinaryProfit REAL,
  ForecastNonConsolidatedProfit REAL,
  ForecastNonConsolidatedEarningsPerShare REAL,
  NextYearForecastNonConsolidatedNetSales REAL,
  NextYearForecastNonConsolidatedOperatingProfit REAL,
  NextYearForecastNonConsolidatedOrdinaryProfit REAL,
  NextYearForecastNonConsolidatedProfit REAL,
  NextYearForecastNonConsolidatedEarningsPerShare REAL,
  ingested_at TEXT DEFAULT (datetime('now')),
  PRIMARY KEY (DisclosedDate, LocalCode, DisclosureNumber)
);

-- /fins/announcement
CREATE TABLE IF NOT EXISTS fins_announcement (
  Date TEXT NOT NULL,
  Code TEXT NOT NULL,
  CompanyName TEXT,
  FiscalYear TEXT,
  SectorName TEXT,
  FiscalQuarter TEXT,
  Section TEXT,
  ingested_at TEXT DEFAULT (datetime('now')),
  PRIMARY KEY (Date, Code, FiscalYear, FiscalQuarter)
);

-- /indices 指数四本値
CREATE TABLE IF NOT EXISTS indices (
  Date  TEXT NOT NULL,   -- YYYY-MM-DD
  Code  TEXT NOT NULL,   -- 指数コード (e.g. '0000', '0028' etc.)
  Open  REAL,            -- 始値
  High  REAL,            -- 高値
  Low   REAL,            -- 安値
  Close REAL,            -- 終値
  ingested_at TEXT DEFAULT (datetime('now')),
  PRIMARY KEY (Date, Code)
);

-- /option/index_option
CREATE TABLE IF NOT EXISTS index_option (
  Date TEXT NOT NULL,              -- YYYY-MM-DD
  Code TEXT NOT NULL,              -- 銘柄コード（5桁）
  WholeDayOpen REAL,
  WholeDayHigh REAL,
  WholeDayLow REAL,
  WholeDayClose REAL,
  NightSessionOpen REAL,
  NightSessionHigh REAL,
  NightSessionLow REAL,
  NightSessionClose REAL,
  DaySessionOpen REAL,
  DaySessionHigh REAL,
  DaySessionLow REAL,
  DaySessionClose REAL,
  Volume REAL,
  OpenInterest REAL,
  TurnoverValue REAL,
  ContractMonth TEXT,              -- YYYY-MM
  StrikePrice REAL,
  Volume_OnlyAuction REAL,      -- [NOTE] Renamed
  EmergencyMarginTriggerDivision TEXT,
  PutCallDivision TEXT,
  LastTradingDay TEXT,             -- YYYY-MM-DD
  SpecialQuotationDay TEXT,        -- YYYY-MM-DD
  SettlementPrice REAL,
  TheoreticalPrice REAL,
  BaseVolatility REAL,
  UnderlyingPrice REAL,
  ImpliedVolatility REAL,
  InterestRate REAL,
  ingested_at TEXT DEFAULT (datetime('now')),
  PRIMARY KEY (Date, Code, EmergencyMarginTriggerDivision)
);

-- /markets/weekly_margin_interest 信用取引週末残高
CREATE TABLE IF NOT EXISTS markets_weekly_margin_interest (
  Date TEXT NOT NULL,   -- YYYY-MM-DD（申込日付＝基準日）
  Code TEXT NOT NULL,
  ShortMarginTradeVolume REAL,               -- 売合計信用取引週末残高
  LongMarginTradeVolume REAL,                -- 買合計信用取引週末残高
  ShortNegotiableMarginTradeVolume REAL,     -- 売一般信用取引週末残高
  LongNegotiableMarginTradeVolume REAL,      -- 買一般信用取引週末残高
  ShortStandardizedMarginTradeVolume REAL,   -- 売制度信用取引週末残高
  LongStandardizedMarginTradeVolume REAL,    -- 買制度信用取引週末残高
  IssueType TEXT,                            -- 1:信用, 2:貸借, 3:その他
  ingested_at TEXT DEFAULT (datetime('now')),
  PRIMARY KEY (Date, Code)
);

-- /markets/short_selling 業種別空売り比率
CREATE TABLE IF NOT EXISTS markets_short_selling (
  Date TEXT NOT NULL,          -- YYYY-MM-DD
  Sector33Code TEXT NOT NULL,  -- 33業種コード
  SellingExcludingShortSellingTurnoverValue REAL,      -- 実注文の売買代金
  ShortSellingWithRestrictionsTurnoverValue REAL,      -- 価格規制有りの空売り売買代金
  ShortSellingWithoutRestrictionsTurnoverValue REAL,   -- 価格規制無しの空売り売買代金
  ingested_at TEXT DEFAULT (datetime('now')),
  PRIMARY KEY (Date, Sector33Code)
);

-- /markets/short_selling_positions 空売り残高報告
CREATE TABLE IF NOT EXISTS markets_short_selling_positions (
  DisclosedDate TEXT NOT NULL,      -- 公表日 YYYY-MM-DD
  CalculatedDate TEXT NOT NULL,     -- 計算日 YYYY-MM-DD
  Code TEXT NOT NULL,               -- 5桁コード
  ShortSellerName TEXT,             -- 商号・名称・氏名
  ShortSellerAddress TEXT,
  DiscretionaryInvestmentContractorName TEXT,
  DiscretionaryInvestmentContractorAddress TEXT,
  InvestmentFundName TEXT,
  ShortPositionsToSharesOutstandingRatio REAL,   -- 残高割合
  ShortPositionsInSharesNumber REAL,             -- 残高数量
  ShortPositionsInTradingUnitsNumber REAL,       -- 残高売買単位数
  CalculationInPreviousReportingDate TEXT,       -- 直近計算日 YYYY-MM-DD
  ShortPositionsInPreviousReportingRatio REAL,   -- 直近残高割合
  Notes TEXT,
  ingested_at TEXT DEFAULT (datetime('now')),
  -- 報告単位は DisclosedDate × Code × ShortSellerName × CalculatedDate でほぼ一意と想定
  PRIMARY KEY (DisclosedDate, Code, ShortSellerName, CalculatedDate)
);

-- /markets/daily_margin_interest 日々公表信用取引残高
CREATE TABLE IF NOT EXISTS markets_daily_margin_interest (
  PublishedDate TEXT NOT NULL,   -- 公表日 YYYY-MM-DD
  Code TEXT NOT NULL,
  ApplicationDate TEXT NOT NULL, -- 申込日 YYYY-MM-DD（残高基準日）
  -- PublishReason は Map なので、必要に応じて JSON で丸ごと保存する案もあり。
  PublishReason_JSON TEXT,       -- オプション: 生 JSON を保存したい場合

  ShortMarginOutstanding REAL,
  DailyChangeShortMarginOutstanding TEXT,         -- Number/String -> TEXT で受ける
  ShortMarginOutstandingListedShareRatio TEXT,    -- Number/String or "*"
  LongMarginOutstanding REAL,
  DailyChangeLongMarginOutstanding TEXT,
  LongMarginOutstandingListedShareRatio TEXT,
  ShortLongRatio REAL,

  ShortNegotiableMarginOutstanding REAL,
  DailyChangeShortNegotiableMarginOutstanding TEXT,
  ShortStandardizedMarginOutstanding REAL,
  DailyChangeShortStandardizedMarginOutstanding TEXT,
  LongNegotiableMarginOutstanding REAL,
  DailyChangeLongNegotiableMarginOutstanding TEXT,
  LongStandardizedMarginOutstanding REAL,
  DailyChangeLongStandardizedMarginOutstanding TEXT,

  TSEMarginBorrowingAndLendingRegulationClassification TEXT,
  ingested_at TEXT DEFAULT (datetime('now')),
  PRIMARY KEY (PublishedDate, Code, ApplicationDate)
);


-- ======
-- [VIEW]
-- ======
CREATE VIEW IF NOT EXISTS DailyOHLCV AS 
  Select
      spt.Date,
      'spot' as Source,
      lst.ScaleCategory,
      lst.MarketCodeName,
      lst.Sector17CodeName,
      lst.Sector33CodeName,
      spt.Code,
      lst.CompanyName as Name,
      spt.AdjustmentOpen As Open,
      spt.AdjustmentHigh As High,
      spt.AdjustmentLow As Low,
      spt.AdjustmentClose As Close,
      spt.AdjustmentVolume As Volume,
      spt.TurnoverValue,
      spt.UpperLimit,
      spt.LowerLimit
  From prices_daily_quotes spt
  Left Join listed_info lst On spt.Date=lst.Date and spt.Code = lst.Code

  Union All

  Select
      idx.Date,
      'index' as Source,
      NULL as ScaleCategory,
      NULL as MarketCodeName,
      lst.Sector17CodeName,
      lst.Sector33CodeName,
      idx.Code,
      lst.IndexName as Name,
      idx.Open,
      idx.High,
      idx.Low,
      idx.Close,
      NULL as Volume,
      NULL as TurnoverValue,
      NULL as UpperLimit,
      NULL as LowerLimit
  From indices idx
  Left Join indices_info lst On idx.Code = lst.Code

  Union All

  Select 
      opt.Date,
      'index_opt' as Source,
      opt.PutCallDivision as ScaleCategory,
      opt.SpecialQuotationDay as MarketCodeName,
      opt.UnderlyingPrice as Sector17CodeName,
      opt.StrikePrice as Sector33CodeName,
      opt.Code,
      '日経225オプション' as Name,
      WholeDayOpen As Open,
      WholeDayHigh As High,
      WholeDayLow As Low,
      WholeDayClose As Close,
      opt.Volume,
      opt.TurnoverValue,
      opt.BaseVolatility as UpperLimit,
      opt.ImpliedVolatility as LowerLimit
  From index_option opt
  Left Join listed_info lst On opt.Code = lst.Code and opt.Date = lst.Date
  