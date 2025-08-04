from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from threading import Lock, Thread
from typing import List, Tuple

import requests


@dataclass
class Task:
    id: str
    name: str
    url: str


class NonBlockingQueueWithBatchedExecution:
    """
    - Runs tasks concurrently in batches of pre-defined size.
    - Does *not* block enqueing new tasks while a batch runs by
      relying on a separate thread for receiving tasks
    """

    def __init__(
        self, threshold: int, concurrent_batch_size: int = 10
    ) -> NonBlockingQueueWithBatchedExecution:
        self.threshold = threshold
        self.concurrent_batch_size = concurrent_batch_size

        # While this works initially, this implies O(n) complexity whenever
        # updating pending or failed tasks.
        # If we make these be Dict keyed by task_id, we take updates down to
        # O(1) as long as we mutate and don't re-instantiate the dicts every time.
        self.tasks: List[Task] = []
        self.failed_tasks: List[Task] = []

        # dameon=True makes it so this runs separately form the main program thread
        self.enqueueing_thread = Thread(
            target=self.enqueue, name="enqueueing thread", daemon=True
        )
        self.pending_tasks_lock = Lock()
        self.failed_tasks_lock = Lock()

    def enqueue(self, task: Task) -> None:
        self.pending_tasks_lock.acquire()
        self.tasks.append(task)
        self.pending_tasks_lock.release()

        if len(self.tasks) >= self.threshold:
            self._flush()

    def _flush(self) -> None:
        with ThreadPoolExecutor(self.concurrent_batch_size) as p:
            results = p.map(self._run_task, self.tasks)
            print(f"Ran tasks: {results}")

    def _clear_task(self, task_to_clear: Task) -> None:
        self.pending_tasks_lock.acquire()
        self.tasks = [task for task in self.tasks if task.id != task_to_clear.id]
        self.pending_tasks_lock.release()

    def _fail_task(self, task_to_fail: Task) -> None:
        self.failed_tasks_lock.acquire()
        self.failed_tasks.append(task_to_fail)
        self.failed_tasks_lock.release()

    def _run_task(self, task: Task) -> Tuple[Task, bool]:
        try:
            print(f"Running task {task.id}")
            response = requests.get(task.url)
            response.raise_for_status()
            self._clear_task(task)
            return (task, True)
        except Exception as e:
            print(f"Task failed: {task.id} - {str(e)}")
            self._fail_task(task)
            self._clear_task(task)
            return (task, False)

    def report(self) -> dict:
        """Get current status of the queue."""
        return {
            "pending_tasks": len(self.tasks),
            "failed_tasks": len(self.failed_tasks),
            "threshold": self.threshold,
            "task_ids": [task.id for task in self.tasks],
            "failed_task_ids": [task.id for task in self.failed_tasks],
        }


def generate_task(id: int) -> Task:
    status = 200 if id % 2 == 0 else 404
    return Task(
        **{
            "id": id,
            "name": id,
            "url": f"http://httpbin.org/status/{status}",
        }
    )


task_count = 100
tasks = [generate_task(id) for id in range(task_count)]
queue = NonBlockingQueueWithBatchedExecution(threshold=100, concurrent_batch_size=100)

for task in tasks:
    queue.enqueue(task)


print(queue.report())
