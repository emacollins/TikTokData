from functools import wraps
import time
import re


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


def clean_user(user: str):
    # Remove any special characters like @ symbols and make it all lowercase
    
    # Extract the username if the user string includes a URL
    if '//' in user:
        if '?' in user:
            user = user.split('/')[-1].split('?')[0].strip('@')
        else:
            # Extract the username if the user string is just a username
            user = user.split('@')[-1]
    user = user.lower()
    return user

