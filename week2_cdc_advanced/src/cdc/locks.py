from filelock import FileLock


def acquire_file_lock():
    lock = FileLock("/tmp/sales_orders.lock")
    lock.acquire(timeout=10)
    return lock
