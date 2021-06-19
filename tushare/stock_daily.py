from base import *


class StockDaily(AbstractDataRetriever):
    def __init__(self):
        super().__init__('stock_daily')

    def _full(self, **kwargs):
        pass

    def _delta(self, **kwargs):
        pass