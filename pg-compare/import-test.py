from importlib.resources import files
import ctypes
import os

# Use the os library to list files in the current directory
so_path = os.path.join(os.path.dirname(__file__), "pgcompare_darwin_arm64.so")
lib = ctypes.CDLL(str(so_path))
file_location = "config.json"
file_location_bytes = ctypes.c_char_p(file_location.encode("utf-8"))
lib.Run.argtypes = [ctypes.c_char_p]
lib.Run.restype = ctypes.c_char_p  # Set return type to c_char_p to capture string
result_ptr = lib.Run(file_location_bytes)
if result_ptr:
    result = result_ptr.decode("utf-8")
    print(result)
else:
    print("No result returned.")
