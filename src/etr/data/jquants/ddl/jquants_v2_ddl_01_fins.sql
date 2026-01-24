-- INDEX設定
-- CREATE INDEX IF NOT EXISTS idx_fins_code_doc_discdate_discno
-- ON fins_summary(Code, DocType, DiscDate, DiscNo);

-- CREATE INDEX IF NOT EXISTS idx_bars_code_date
-- ON equities_bars_daily(Code, Date);

-- ユニバース定義 (date x code)
-- DROP VIEW IF EXISTS v_daily_universe;
-- CREATE VIEW v_daily_universe AS
-- SELECT
--   b.Date,
--   b.Code,
--   b.AdjC AS Price,
--   m.CoName AS Name,
--   m.MktNm,
--   m.ScaleCat,
--   m.S17Nm,
--   m.S33Nm,
--   m.MrgnNm
-- FROM equities_bars_daily b
-- LEFT JOIN equities_master m
--   ON m.Date = b.Date AND m.Code = b.Code;

-- 実績のみ as-of（その日までの最新開示キー）
-- DROP VIEW IF EXISTS v_fins_asof_daily;
-- CREATE VIEW v_fins_asof_daily AS
-- SELECT
--   d.Date,
--   d.Code,
--   (
--     SELECT fs.DiscDate
--     FROM fins_summary fs
--     WHERE fs.Code = d.Code
--       AND fs.DiscDate <= d.Date
--       AND fs.DocType LIKE '%FinancialStatements%'
--     ORDER BY fs.DiscDate DESC, fs.DiscNo DESC
--     LIMIT 1
--   ) AS DiscDate_AsOf,
--   (
--     SELECT fs.DiscNo
--     FROM fins_summary fs
--     WHERE fs.Code = d.Code
--       AND fs.DiscDate <= d.Date
--       AND fs.DocType LIKE '%FinancialStatements%'
--     ORDER BY fs.DiscDate DESC, fs.DiscNo DESC
--     LIMIT 1
--   ) AS DiscNo_AsOf
-- FROM v_daily_universe d;

-- as-of 開示の財務数値（実績のみ）
-- DROP VIEW IF EXISTS v_fins_asof_daily_values;
-- CREATE VIEW v_fins_asof_daily_values AS
-- SELECT
--   a.Date,
--   a.Code,
--   a.DiscDate_AsOf,
--   a.DiscNo_AsOf,

--   fs.DocType,
--   fs.CurPerType,
--   fs.CurPerSt,
--   fs.CurPerEn,
--   fs.CurFYSt,
--   fs.CurFYEn,

--   fs.Sales,
--   fs.OP,
--   fs.NP,
--   fs.EPS,
--   fs.TA,
--   fs.Eq,
--   fs.BPS,
--   fs.CFO,
--   fs.CFI,
--   fs.CFF,
--   fs.AvgSh,
--   fs.ShOutFY
-- FROM v_fins_asof_daily a
-- LEFT JOIN fins_summary fs
--   ON fs.Code = a.Code
--  AND fs.DiscDate = a.DiscDate_AsOf
--  AND fs.DiscNo  = a.DiscNo_AsOf
--  AND fs.DocType LIKE '%FinancialStatements%';

-- バックテスト用：日次 as-of 指標（実績のみ）
-- DROP VIEW IF EXISTS v_fundamentals_asof_daily;
-- CREATE VIEW v_fundamentals_asof_daily AS
-- SELECT
--   d.Date,
--   d.Code,
--   d.Price,

--   d.Name,
--   d.MktNm,
--   d.ScaleCat,
--   d.S17Nm,
--   d.S33Nm,
--   d.MrgnNm,

--   f.DiscDate_AsOf AS DiscDate,
--   f.DiscNo_AsOf   AS DiscNo,
--   f.DocType,
--   f.CurPerType,
--   f.CurFYEn,

--   -- Profitability
--   (f.NP / NULLIF(f.TA, 0.0))     AS ROA,
--   (f.NP / NULLIF(f.Eq, 0.0))     AS ROE,
--   (f.OP / NULLIF(f.Sales, 0.0))  AS OprMargin,

--   -- EBITDA（近似：減価償却が無いのでOP）
--   (f.OP) AS EBITDA_Apx,

--   -- Balance sheet
--   (f.Eq / NULLIF(f.TA, 0.0))         AS EquityRatio,
--   ((f.TA - f.Eq) / NULLIF(f.Eq, 0.0)) AS DERatio,

--   -- Cash flow
--   (f.CFO / NULLIF(f.Sales, 0.0))          AS SalesCF,
--   (f.CFO + f.CFI)                         AS FreeCF,
--   ((f.CFO + f.CFI) / NULLIF(f.Sales, 0.0)) AS CFmargin,

--   -- Valuation
--   (d.Price / NULLIF(f.EPS, 0.0)) AS PER,
--   (d.Price / NULLIF(f.BPS, 0.0)) AS PBR,
--   (d.Price * NULLIF(f.AvgSh, 0.0) / NULLIF(f.Sales, 0.0)) AS PSR,

--   -- Efficiency
--   (f.Sales / NULLIF(f.TA, 0.0)) AS AssetTurnover

-- FROM v_daily_universe d
-- LEFT JOIN v_fins_asof_daily_values f
--   ON f.Date = d.Date AND f.Code = d.Code;

-- 5-1) 成長率（実績のみ）
-- DROP VIEW IF EXISTS v_fins_growth_by_disclosure_actual;
-- CREATE VIEW v_fins_growth_by_disclosure_actual AS
-- WITH h AS (
--   SELECT
--     fs.DiscDate,
--     fs.DiscNo,
--     fs.Code,
--     fs.CurFYEn,
--     fs.Sales,
--     fs.EPS,
--     LAG(fs.Sales) OVER (PARTITION BY fs.Code ORDER BY fs.CurFYEn) AS Sales_Lag1,
--     LAG(fs.EPS)   OVER (PARTITION BY fs.Code ORDER BY fs.CurFYEn) AS EPS_Lag1
--   FROM fins_summary fs
--   WHERE fs.DocType LIKE '%FinancialStatements%'
--     AND fs.CurFYEn IS NOT NULL
-- )
-- SELECT
--   DiscDate,
--   DiscNo,
--   Code,
--   CurFYEn,
--   ((Sales / NULLIF(Sales_Lag1, 0.0)) - 1.0) AS SalesGrowth,
--   ((EPS   / NULLIF(EPS_Lag1,   0.0)) - 1.0) AS EpsGrowth
-- FROM h;

-- 成長率 + Snapshot ... これを使う
-- DROP VIEW IF EXISTS v_fundamentals_asof_daily_actual;
-- CREATE VIEW v_fundamentals_asof_daily_actual AS
-- SELECT
--   f.*,
--   g.SalesGrowth,
--   g.EpsGrowth
-- FROM v_fundamentals_asof_daily f
-- LEFT JOIN v_fins_growth_by_disclosure_actual g
--   ON g.Code = f.Code
--  AND g.DiscDate = f.DiscDate
--  AND g.DiscNo  = f.DiscNo;
