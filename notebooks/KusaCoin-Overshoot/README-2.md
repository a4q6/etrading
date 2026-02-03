# 分析概要
HyperLiquidのperpetualの1min candleデータを利用して短期的なpriceのovershootのtrend-followを狙う戦略構築のための検証を行いたい.
これを幅広いユニバースのトークンに対して行い取引数を稼ぐことで定量的な検証の土台にあげると共に、高いsharpeを狙う.
まずはトランザクションコストは考えない.
1. 幅広いユニバースのトークン(適宜filterはする)に対して大きな変動をモニター
2. 大きく動いた銘柄に対して、変動方向にエントリ(@close price)
3. N-min後にclose priceで決済

# 詳細
## データソース
Hyperliquidのcandle(1min)をソースとする. ただしcategory=perpに限る.
candleはresamplingして適当な時間軸に直した上でつかってもよい.
candle datasetは1min freqとは限らないのでresampleして1minにノーマライズしてから用いる.
サンプルは2025-12月から用いる. スリッページを減らすためにあまりに流動性の低い銘柄は避ける.

## 特徴量
参照する特徴量の候補としてはこちら (適宜追加して良い)
- OS_upper := abs(high - max(open, close)) / open
- OS_lower := abs(low - min(open, close)) / open
- ATR
- close2close historical return
- BTC,ETHなどのメジャーコイン、あるいは相関の高い複数トークンからの乖離 (...一時的な変動か否かのフィルターとして期待)
*特徴量は可読性のためになるべくbps単位で計算.

## ターゲット/シグナル
シンプルな戦略のためclose2closeのリターンを予測できればよい.
バックテストまで行い戦略のリターンを計算できると尚良い.

## モデル
まずはMLは用いずに特徴量をベースとしたシンプルなルールベースの戦略を試したい.
