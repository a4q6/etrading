# 分析概要
bitflyer FXBTCJPYはtransaction fee=0bpsの通貨ペアであり高頻度戦略構築の余地がある.
1日あたりの取引回数>1000程度Market Making戦略を構築して安定した収益を狙いたい.
(*また、bitflyerは取引量に応じてFeeが低下するため、AvgPL=0でturnoverを稼げる戦略を開発しても良い)
まずは既存のレポジトリの戦略にはとらわれず、自由な発想で戦略構築、検証を行って欲しい.
backtestの実行方法については `@notebooks/Dev-MM-backtest.ipynb` が参考になる(が、これにとらわれる必要もない).

# データソース
bitflyer FXBTCJPYが取引対象. 適宜HDBに入っている他のデータも利用して良い.
