import time
from collections import deque

class RealtimeCounter:
    def __init__(self, window_sec: int):
        self.window = window_sec
        self.timestamps = deque()  # 各 increment 呼び出し時のタイムスタンプ

    def increment(self):
        now = time.time()
        self.timestamps.append(now)
        self._trim_old(now)

    @property
    def count(self) -> int:
        self._trim_old()
        return len(self.timestamps)

    def _trim_old(self, now: float = None):
        if now is None:
            now = time.time()
        threshold = now - self.window
        while self.timestamps and self.timestamps[0] < threshold:
            self.timestamps.popleft()