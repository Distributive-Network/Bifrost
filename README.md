# Bifrost

![Testing Suite](https://github.com/Distributive-Network/Bifrost/actions/workflows/test_dcp.yaml/badge.svg)


An SHM/memmap based python and node communication/evaluation tool.

The idea is that you should be able to start a node process in the background byimporting this module and should be able to share and sync variables across node and python for a variety of purposes. 


Sharing variables is managed by serializing variables and writing them to a buffer that is memmap'ed to both the python process and the node process.

## Requirements

### Environment Requirements

- Node 14 or Node 16 LTS is a *mandatory* requirement. Any environment without Node 14 or 16 is not supported.

### Python Requirements

This should be handled by pip when installing Bifrost: 
```
cloudpickle<2.1
numpy
xxhash
posix_ipc ; os_name == 'posix' and python_version < '3.8'
```

## NOTE

This tool is highly experimental and requires much more work before being production ready. It currently 'works' for the majority of simple use cases, but more work could be done on making it more robust and better manage the pipes.
