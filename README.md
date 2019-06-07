# dv-user-driver-rpc
Implementation of dv-user-driver API over tcp.

The purpose of this package is to allow development on the Host PC by providing the version of shared library with same user driver API and doing communication with real board over network.

# On the host

```bash
make libdmpdv_rps.so
sudo make install_lib
```
* It will copy `libdmpdv_rpc.so` to `/usr/local/lib/` and make a link to it as `/usr/local/lib/libdmpdv.so`.
* It will copy header files to `/usr/include/`.

Then set environment variable `DMPDV_RPC` to `HOST:PORT` for example at the end of the `.bashrc` add
```bash
export DMPDV_RPC=board-host-name-or-ip:5555
```

Now it'll be possible to compile and run applications using dv-user-driver API directly on the host PC.

# On the board

```bash
make dmpdv_server
sudo make install_server
```

* It will copy `dmpdv_server` binary to `/usr/local/bin`.

Then launch the server, for example:
```bash
dmpdv_server 5555
```
or create a starup script in your preferred way.

**Host must be 64-bit or Host and Device must be 32-bit.**

# Server implementation details

* Each client connection served in separate process within single thread.
* The client will exit immediately on connection loss.
* Server terminates all client connections on `SIGINT`.

# Client library implementation details

* The connection is per-context.
* All operations over connection executed in serial way under the mutex.
