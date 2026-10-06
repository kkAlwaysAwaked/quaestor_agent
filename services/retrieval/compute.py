"""限制本地模型计算；协程取消后，计算完成前仍占用执行槽。"""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import partial


class BoundedCompute:
    # 作用：创建单线程模型执行器，保护共享模型并限制待执行计算数量。
    def __init__(self) -> None:
        self.executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="retrieval-model")
        self.slot = asyncio.Semaphore(1)
        self.closed = False

    # 作用：在真实计算结束后释放槽位并读取异常，防止取消请求造成并发失控。
    def _completed(self, future: asyncio.Future) -> None:
        if not future.cancelled():
            future.exception()
        self.slot.release()

    # 作用：把同步模型计算放入受限线程，取消等待时继续保护尚未结束的计算。
    async def run(self, function, *args, **kwargs):
        if self.closed:
            raise RuntimeError("模型执行器已关闭")
        await self.slot.acquire()
        try:
            if self.closed:
                raise RuntimeError("模型执行器已关闭")
            future = asyncio.get_running_loop().run_in_executor(
                self.executor, partial(function, *args, **kwargs)
            )
        except BaseException:
            self.slot.release()
            raise
        future.add_done_callback(self._completed)
        return await asyncio.shield(future)

    # 作用：退出时等待在途计算结束，再关闭线程池。
    async def close(self) -> None:
        self.closed = True
        await asyncio.to_thread(self.executor.shutdown, wait=True, cancel_futures=True)
