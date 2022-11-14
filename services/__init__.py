from concurrent.futures import as_completed, ThreadPoolExecutor
from typing import Callable, Any

MAX_PARALLEL_NETWORK_REQUESTS = 5
PAGINATION_SIZE = 100


def paginate(
        total,
        paginate_fn: Callable[[int, int], Any],
        on_paginated: Callable[[Any], None],
        num_parallel_requests=MAX_PARALLEL_NETWORK_REQUESTS,
        pagination_size=PAGINATION_SIZE
):
    if total == 0:
        return

    batch_offset_increments = min(total, num_parallel_requests * pagination_size)

    for batch_offset in range(0, total, batch_offset_increments):
        with ThreadPoolExecutor(num_parallel_requests) as executor:
            futures = [executor.submit(paginate_fn, cur_offset, pagination_size)
                       for cur_offset in range(
                    batch_offset,
                    min(batch_offset + batch_offset_increments, total),
                    pagination_size
                )]

            for future in futures:
                on_paginated(future.result())
