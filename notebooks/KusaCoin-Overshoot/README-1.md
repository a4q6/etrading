# 分析概要
HyperLiquidのperpetualの1min candleデータを利用して短期的なpriceのovershootからのリバーサルを狙う戦略を構築のための検証を行いたい.
仮説としては、流動性の薄いトークンほどプライスのインパクトが大きく、非効率的な一時的な変動が大きくなり、そこからのリバーサルが起きるであろうこと.
いってみれば、"ヒゲキャッチ"のような戦略.
これを幅広いユニバースのトークンに対して行い取引数を稼ぐことで定量的な検証の土台にあげると共に、高いsharpeを狙う.
戦略の動き方としては下記のようなものを想定. mid frequencyでの複数tokenのmarket makingのようなイメージ.
1. 幅広いユニバースのトークン(適宜filterはする)に対して大きな変動を狙って遠くに両側指値
2. fillされた場合に反対指値を大変動前の水準でおく (相関の高いtokenの指値執行でproxy hedgeもあり) 
3. fillされない場合にはN-min後に強制決済

# 詳細
## データソース
Hyperliquidのcandle(1min)をソースとする. ただしcategory=perpに限る.
candleはresamplingして適当な時間軸に直した上でつかってもよい.

## 特徴量
参照する特徴量の候補としてはこちら (適宜追加して良い)
- OS_upper := abs(high - max(open, close)) / open
- OS_lower := abs(low - min(open, close)) / open
- ATR
- close2close historical return
- BTC,ETHなどのメジャーコイン、あるいは相関の高い複数トークンからの乖離 (...一時的な変動か否かのフィルターとして期待)
- ...etc.
*特徴量は可読性のためになるべくbps単位で計算.

## ターゲット/シグナル
指値での執行を想定しているため、次のN minのHigh/Lowが予測できると良い.
バックテストまで行い戦略のリターンを計算できると尚良い. この時のベースの執行戦略は、指値fillからNmin後のclose priceで決済.

## モデル
まずはMLは用いずに特徴量をベースとしたシンプルなルールベースの戦略を試したい.
