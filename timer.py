import polars
import contextlib
import time

@contextlib.contextmanager
def timeit(label):
    print(f"timing: `{label}`")
    start = time.perf_counter()
    try:
        yield
    finally:
        end = time.perf_counter()
        print(f"{label}: {round(end-start, 2)}s")