import os, ctypes, posix_ipc, sys, mmap
from subprocess import call
SHARED_MEMORY_NAME = "/shared_memory"



memory = posix_ipc.SharedMemory(SHARED_MEMORY_NAME, posix_ipc.O_CREX,
                                size=1024)

mapFile = mmap.mmap(memory.fd, memory.size)
memory.close_fd()


mapFile.seek(0)
mapFile.write("Hello world!\n".encode('utf-8'))
mapFile.seek(0)

print("FROM PYTHON MAIN PROCESS: ", mapFile.readline().decode('utf-8'))
mapFile.seek(0)



call([
    "node", "./test_mmap.js", SHARED_MEMORY_NAME
])





mapFile.close()


posix_ipc.unlink_shared_memory(SHARED_MEMORY_NAME)