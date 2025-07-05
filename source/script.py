import time

def timing(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        print(f"{func.__name__} ran in {time.time() - start:.4f} sec")
        return result
    return wrapper
