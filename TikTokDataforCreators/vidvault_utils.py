from functools import wraps
import time


def timeit(message='Elapsed time:', user=None, date=None):
    def decorator(func):
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            elapsed_time = time.time() - start_time
            total_time = round(elapsed_time, 4)
            if user:
                print(f'{message} for {user} | complete in {total_time:.4f} seconds')
            else:
                print(f'{message} | complete in {total_time:.4f} seconds')
            return result
        return wrapper
    return decorator




