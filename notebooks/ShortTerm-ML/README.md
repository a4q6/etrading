# 分析概要
venue=GMO, BitFlyerでのHFT戦略への入力として短期の値動き方向・ボラティリティの予測シグナルが作れるとプライシングやポジション管理の際の判断軸として有用である.
これをmicrostrucuture featureを入力としたNNモデルを用いて実現したい.
モデルの実装と検証作業を行なって欲しい.

# Model/Datasource
- Input
    - Rate, MarketTrade, MarketBook, MarketLiquidity(=MarketBookから作成した特徴量) が各取引所・通貨ペアにおいて利用可能である. 
    - 上記から計算可能なmicrostructure featureの時系列(priceの動きや出来高、板の動きを定量化)したものを入力とする (外れ値や欠損値、リークに注意) ... `timestamp`を共通して参照すれば基本的にはOK.
- Horizon
    - 1min and/or 3min and/or 5min
    - *予測性能やトレーニング時間の制約とのバランスもみつつだが、複数のhorizonを同時に予測するモデルを構築しても良い
- Label
    - ボラティリティと方向性を同時予測する. 予測性能に悪影響がなければさらに複数の通貨ペアを同時に予測するモデルを作っても良い.
        - target1: direction (2-class)
        - target2: volatility (regression)
        - (Foreach taget ccypair)
    - (venue, sym)の優先順位は下記の通り:
        - Priority1: (venue, sym) = (bitflyer, FXBTCJPY), (gmo, BTCJPY), (gmo, ETHJPY)
        - Priority2: (venue, sym) = (gmo, others), (bitbank, BTCJPY), (bitbank, ETHJPY)
        - 複数の通貨ペアを同時に予測する際にはアーキテクチャ上の工夫を考慮して欲しい
- Architecture
    - 時系列を入力とするため、これに強いアーキテクチャがよい. (e.g. Attention)
    - ここは特に工夫が必要な箇所なのでプロフェッショナルであるあなたのインプットを必要とするところ

# その他
- ipynbのhtmlファイルの生成は不要, しかしnotebookは実行済みの状態で置いておいてほしい
