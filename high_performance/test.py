import numpy as np
import time
from numba import njit, prange

# Array size
N = 10**7

# Initialize a large numpy array
arr = np.arange(N)

# Normal Python function (without Numba)
def normal_loop(arr):
    for i in range(len(arr)):
        arr[i] = arr[i] ** 2

# Numba-optimized function
@njit(parallel=True)
def numba_loop(arr):
    for i in prange(len(arr)):
        arr[i] = arr[i] ** 2

# Copy of the array for testing
arr_normal = arr.copy()
arr_numba = arr.copy()

# Measure time for normal Python loop
start_time = time.time()
normal_loop(arr_normal)
end_time = time.time()
normal_time = (end_time - start_time) * 1000  # Convert to milliseconds

# # Run the Numba function once to compile it (ignore this time)
# numba_loop(arr_numba)

# # Copy the array again for a fresh run after compilation
# arr_numba = arr.copy()

# Measure time for Numba-optimized loop
start_time = time.time()
numba_loop(arr_numba)
end_time = time.time()
numba_time = (end_time - start_time) * 1000  # Convert to milliseconds

# Display results
print(f"Normal Python loop time: {normal_time:.2f} ms")
print(f"Numba-optimized loop time: {numba_time:.2f} ms")

# Ensure the results are correct and the same
assert np.array_equal(arr_normal, arr_numba), "The results are not the same!"
