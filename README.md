# Bifrost

![Testing Suite](https://github.com/Kings-Distributed-Systems/Bifrost/actions/workflows/tests_dcp.yaml/badge.svg)


An SHM/memmap based python and node communication/evaluation tool.

The idea is that you should be able to start a node process in the background byimporting this module and should be able to share and sync variables across node and python for a variety of purposes. 


Sharing variables is managed by serializing variables and writing them to a buffer that is memmap'ed to both the python process and the node process.

## NOTE

This tool is highly experimental and requires much more work before being production ready. It currently 'works' for the majority of simple use cases, but more work could be done on making it more robust and better manage the pipes.
