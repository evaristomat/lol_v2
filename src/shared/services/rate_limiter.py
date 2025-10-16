import asyncio
import time


class RateLimiter:
    def __init__(self, max_requests=3500, time_window=3600):
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = []
        self.lock = asyncio.Lock()

    async def acquire(self):
        async with self.lock:
            now = time.time()
            self.requests = [
                req_time
                for req_time in self.requests
                if now - req_time < self.time_window
            ]

            if len(self.requests) >= self.max_requests:
                oldest_req = min(self.requests)
                wait_time = self.time_window - (now - oldest_req)
                if wait_time > 0:
                    await asyncio.sleep(wait_time)
                    now = time.time()
                    self.requests = [
                        req_time
                        for req_time in self.requests
                        if now - req_time < self.time_window
                    ]

            self.requests.append(now)
