PRAGMA journal_mode = DELETE;
PRAGMA page_size = 4096;
PRAGMA foreign_keys = ON;
VACUUM;

CREATE TABLE IF NOT EXISTS equities_master (
  Date TEXT NOT NULL,
  Code TEXT NOT NULL,
  CoName TEXT,
  CoNameEn TEXT,
  S17 TEXT,
  S17Nm TEXT,
  S33 TEXT,
  S33Nm TEXT,
  ScaleCat TEXT,
  Mkt TEXT,
  MktNm TEXT,
  Mrgn TEXT,
  MrgnNm TEXT,
  ingested_at TEXT DEFAULT (datetime('now')),
  PRIMARY KEY (Date, Code)
);

CREATE TABLE IF NOT EXISTS equities_bars_daily (
  Date TEXT NOT NULL,
  Code TEXT NOT NULL,
  O REAL,
  H REAL,
  L REAL,
  C REAL,
  UL TEXT,
  LL TEXT,
  Vo REAL,
  Va REAL,
  AdjFactor REAL,
  AdjO REAL,
  AdjH REAL,
  AdjL REAL,
  AdjC REAL,
  AdjVo REAL,
  ingested_at TEXT DEFAULT (datetime('now')),
  PRIMARY KEY (Date, Code)
);

CREATE TABLE IF NOT EXISTS equities_bars_minute (
  Date TEXT NOT NULL,
  Time TEXT NOT NULL,
  Code TEXT NOT NULL,
  O REAL,
  H REAL,
  L REAL,
  C REAL,
  Vo REAL,
  Va REAL,
  ingested_at TEXT DEFAULT (datetime('now')),
  PRIMARY KEY (Date, Code, Time)
);

CREATE TABLE IF NOT EXISTS equities_trades (
  Date TEXT NOT NULL,
  Code TEXT NOT NULL,
  Time TEXT NOT NULL,
  SessionDistinction TEXT,
  Price REAL,
  TradingVolume REAL,
  TransactionId TEXT NOT NULL,

  ingested_at TEXT DEFAULT (datetime('now')),
  PRIMARY KEY (Date, Code, TransactionId)
);

CREATE TABLE IF NOT EXISTS equities_earnings_calendar  (
  Date TEXT NOT NULL,
  Code TEXT NOT NULL,
  CoName TEXT,
  FY TEXT,
  SectorNm TEXT,
  FQ TEXT,
  Section TEXT,
  ingested_at TEXT DEFAULT (datetime('now')),
  PRIMARY KEY (Date, Code, FY, FQ)
);

CREATE TABLE IF NOT EXISTS fins_summary (
  DiscDate TEXT NOT NULL,
  DiscTime TEXT,
  Code TEXT NOT NULL,
  DiscNo TEXT NOT NULL,
  DocType TEXT,
  CurPerType TEXT,
  CurPerSt TEXT,
  CurPerEn TEXT,
  CurFYSt TEXT,
  CurFYEn TEXT,
  NxtFYSt TEXT,
  NxtFYEn TEXT,
  Sales REAL,
  OP REAL,
  OdP REAL,
  NP REAL,
  EPS REAL,
  DEPS REAL,
  TA REAL,
  Eq REAL,
  EqAR REAL,
  BPS REAL,
  CFO REAL,
  CFI REAL,
  CFF REAL,
  CashEq REAL,
  Div1Q REAL,
  Div2Q REAL,
  Div3Q REAL,
  DivFY REAL,
  DivAnn REAL,
  DivUnit REAL,
  DivTotalAnn REAL,
  PayoutRatioAnn REAL,
  FDiv1Q REAL,
  FDiv2Q REAL,
  FDiv3Q REAL,
  FDivFY REAL,
  FDivAnn REAL,
  FDivUnit REAL,
  FDivTotalAnn REAL,
  FPayoutRatioAnn REAL,
  NxFDiv1Q REAL,
  NxFDiv2Q REAL,
  NxFDiv3Q REAL,
  NxFDivFY REAL,
  NxFDivAnn REAL,
  NxFDivUnit REAL,
  NxFPayoutRatioAnn REAL,
  FSales2Q REAL,
  FOP2Q REAL,
  FOdP2Q REAL,
  FNP2Q REAL,
  FEPS2Q REAL,
  NxFSales2Q REAL,
  NxFOP2Q REAL,
  NxFOdP2Q REAL,
  NxFNp2Q REAL,
  NxFEPS2Q REAL,
  FSales REAL,
  FOP REAL,
  FOdP REAL,
  FNP REAL,
  FEPS REAL,
  NxFSales REAL,
  NxFOP REAL,
  NxFOdP REAL,
  NxFNp REAL,
  NxFEPS REAL,
  MatChgSub TEXT,
  SigChgInC TEXT,
  ChgByASRev TEXT,
  ChgNoASRev TEXT,
  ChgAcEst TEXT,
  RetroRst TEXT,
  ShOutFY REAL,
  TrShFY REAL,
  AvgSh REAL,
  NCSales REAL,
  NCOP REAL,
  NCOdP REAL,
  NCNP REAL,
  NCEPS REAL,
  NCTA REAL,
  NCEq REAL,
  NCEqAR REAL,
  NCBPS REAL,
  FNCSales2Q REAL,
  FNCOP2Q REAL,
  FNCOdP2Q REAL,
  FNCNP2Q REAL,
  FNCEPS2Q REAL,
  NxFNCSales2Q REAL,
  NxFNCOP2Q REAL,
  NxFNCOdP2Q REAL,
  NxFNCNP2Q REAL,
  NxFNCEPS2Q REAL,
  FNCSales REAL,
  FNCOP REAL,
  FNCOdP REAL,
  FNCNP REAL,
  FNCEPS REAL,
  NxFNCSales REAL,
  NxFNCOP REAL,
  NxFNCOdP REAL,
  NxFNCNP REAL,
  NxFNCEPS REAL,
  ingested_at TEXT DEFAULT (datetime('now')),
  PRIMARY KEY (DiscDate, Code, DiscNo)
);

CREATE TABLE IF NOT EXISTS indices_bars_daily (
  Date  TEXT NOT NULL,
  Code  TEXT NOT NULL,
  O  REAL,
  H  REAL,
  L   REAL,
  C REAL,
  ingested_at TEXT DEFAULT (datetime('now')),
  PRIMARY KEY (Date, Code)
);

CREATE TABLE IF NOT EXISTS indices_info (
  Code TEXT NOT NULL,
  IndexName TEXT NOT NULL,
  S17Nm TEXT,
  S33Nm TEXT,
  ingested_at TEXT DEFAULT (datetime('now')),
  PRIMARY KEY (Code, IndexName)
);

CREATE TABLE IF NOT EXISTS markets_calendar (
  Date TEXT NOT NULL,
  HolDiv TEXT NOT NULL,
  ingested_at TEXT DEFAULT (datetime('now')),
  PRIMARY KEY (Date, HolDiv)
);

CREATE TABLE IF NOT EXISTS index_option_N225 (
  Date TEXT NOT NULL,
  Code TEXT NOT NULL,
  O REAL,
  H REAL,
  L REAL,
  C REAL,
  EO REAL,
  EH REAL,
  EL REAL,
  EC REAL,
  AO REAL,
  AH REAL,
  AL REAL,
  AC REAL,
  Vo REAL,
  OI REAL,
  Va REAL,
  CM TEXT,
  Strike REAL,
  VoOA REAL,
  EmMrgnTrgDiv TEXT,
  PCDiv TEXT,
  LTD TEXT,
  SQD TEXT,
  Settle REAL,
  Theo REAL,
  BaseVol REAL,
  UnderPx REAL,
  IV REAL,
  IR REAL,
  ingested_at TEXT DEFAULT (datetime('now')),
  PRIMARY KEY (Date, Code, EmMrgnTrgDiv)
);

CREATE TABLE IF NOT EXISTS markets_margin_interest (
  Date TEXT NOT NULL,          -- YYYY-MM-DD（申込日付＝基準日）
  Code TEXT NOT NULL,
  ShrtVol REAL,                -- 売合計信用取引週末残高
  LongVol REAL,                -- 買合計信用取引週末残高
  ShrtNegVol REAL,             -- 売一般信用取引週末残高
  LongNegVol REAL,             -- 買一般信用取引週末残高
  ShrtStdVol REAL,             -- 売制度信用取引週末残高
  LongStdVol REAL,             -- 買制度信用取引週末残高
  IssType TEXT,                -- 1:信用, 2:貸借, 3:その他
  ingested_at TEXT DEFAULT (datetime('now')),
  PRIMARY KEY (Date, Code)
);

CREATE TABLE IF NOT EXISTS markets_short_ratio (
  Date TEXT NOT NULL,          -- YYYY-MM-DD
  S33 TEXT NOT NULL,  -- 33業種コード
  SellExShortVa REAL,      -- 実注文の売買代金
  ShrtWithResVa REAL,      -- 価格規制有りの空売り売買代金
  ShrtNoResVa REAL,   -- 価格規制無しの空売り売買代金
  ingested_at TEXT DEFAULT (datetime('now')),
  PRIMARY KEY (Date, S33)
);

CREATE TABLE IF NOT EXISTS markets_short_sale_report (
  DiscDate TEXT NOT NULL,       -- 公表日 YYYY-MM-DD
  CalcDate TEXT NOT NULL,       -- 計算日 YYYY-MM-DD
  Code TEXT NOT NULL,           -- 5桁コード
  SSName TEXT,                  -- 商号・名称・氏名
  SSAddr TEXT,
  DICName TEXT,
  DICAddr TEXT,
  FundName TEXT,
  ShrtPosToSO REAL,             -- 残高割合
  ShrtPosShares REAL,           -- 残高数量
  ShrtPosUnits REAL,            -- 残高売買単位数
  PrevRptDate TEXT,             -- 直近計算日 YYYY-MM-DD
  PrevRptRatio REAL,            -- 直近残高割合
  Notes TEXT,
  ingested_at TEXT DEFAULT (datetime('now')),
  -- 報告単位は DiscDate × Code × SSName × CalcDate でほぼ一意
  PRIMARY KEY (DiscDate, Code, SSName, CalcDate)
);

CREATE TABLE IF NOT EXISTS markets_margin_alert (
  PubDate TEXT NOT NULL,        -- 公表日 YYYY-MM-DD
  Code TEXT NOT NULL,
  AppDate TEXT NOT NULL,        -- 申込日 YYYY-MM-DD（残高基準日）
  PubReason TEXT,
  ShrtOut REAL,
  ShrtOutChg TEXT,              -- Number/String 混在 → TEXT
  ShrtOutRatio TEXT,            -- Number/String or "*"
  LongOut REAL,
  LongOutChg TEXT,
  LongOutRatio TEXT,
  SLRatio REAL,
  ShrtNegOut REAL,
  ShrtNegOutChg TEXT,
  ShrtStdOut REAL,
  ShrtStdOutChg TEXT,
  LongNegOut REAL,
  LongNegOutChg TEXT,
  LongStdOut REAL,
  LongStdOutChg TEXT,
  TSEMrgnRegCls TEXT,
  ingested_at TEXT DEFAULT (datetime('now')),
  PRIMARY KEY (PubDate, Code, AppDate)
);

-- ===========
--   [VIEW]
-- ==========
CREATE VIEW IF NOT EXISTS DailyOHLC AS 
 Select
      'spot' as Source,
      spt.Date,
      meta.MktNm,
      meta.ScaleCat,
      meta.S17Nm,
      meta.S33Nm,
      meta.MrgnNm,
      spt.Code,
      meta.CoName as Name,
      spt.AdjO As Open,
      spt.AdjH As High,
      spt.AdjL As Low,
      spt.AdjC As Close,
      spt.AdjVo As Volume,
      spt.Va As Turnover,
      spt.UL,
      spt.LL
  From equities_bars_daily spt
  Left Join equities_master meta On spt.Date=meta.Date and spt.Code = meta.Code

  Union All

  Select
      'index' as Source,
      idx.Date,
      NULL as MktNm,
      NULL as ScaleCat,
      lst.S17Nm,
      lst.S33Nm,
      idx.Code,
      NULL as MrgnNm,
      lst.IndexName as Name,
      idx.O as Open,
      idx.H as H,
      idx.L as L,
      idx.C as C,
      NULL as Volume,
      NULL as Turnover,
      NULL as UL,
      NULL as LL
  From indices_bars_daily idx
  Left Join indices_info lst On idx.Code = lst.Code

  Union All

  Select 
      'index_opt' as Source,
      opt.Date,
      opt.Strike as MktNm,
      CASE
        WHEN opt.PCDiv = '1' THEN 'P'
        WHEN opt.PCDiv = '2' THEN 'C'
        ELSE '-'
      END As ScaleCat,
      opt.LTD as S17Nm,
      opt.SQD as S33Nm,
      opt.OI as MrgnNm,
      opt.Code,
      '日経225オプション' as Name,
      O As Open,
      H As High,
      L As Low,
      C As Close,
      opt.Vo as Volume,
      opt.Va as Turnover,
      opt.BaseVol as UL,
      opt.IV as LL
  From index_option_N225 opt
  Left Join equities_master lst On opt.Code = lst.Code and opt.Date = lst.Date
  