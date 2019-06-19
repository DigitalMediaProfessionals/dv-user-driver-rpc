/*
 *  Copyright 2018 Digital Media Professionals Inc.

 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at

 *      http://www.apache.org/licenses/LICENSE-2.0

 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
/*
 * @brief Shared library exported functions implementation for working over network with dmpdv_server.
 */
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <dirent.h>
#include <signal.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pthread.h>
#include <netdb.h>
#include <sys/mman.h>

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <stdint.h>
#include <string.h>
#include <stdarg.h>
#include <errno.h>

#include <map>

#include "dmp_dv.h"

#include "rpc_cmd.h"


static char version_string[256] = {0};
extern "C" {
char s_last_error_message[256] = {0};
}


#define set_last_error_message dmp_dv_set_last_error_message


static inline const char *get_last_error_message() {
  return s_last_error_message;
}


#define SEND(buf, n, more) if (!Send(buf, n, more)) { return false; }
#define RECV(buf, n) if (!Recv(buf, n)) { return false; }


/// @brief Base class handling reference counting.
class Base {
 public:
  Base() {
    n_ref_ = 1;
  }

  virtual ~Base() {
    // Empty by design
  }

  inline int Retain() {
    return __sync_add_and_fetch(&n_ref_, 1);
  }

  inline int Release() {
    int n = __sync_add_and_fetch(&n_ref_, -1);
    if (!n) {
      delete this;
    }
    return n;
  }

 private:
  int n_ref_;
};


/// @brief Connection to dmpdv_server.
class Connection : public Base {
 public:
  Connection() : Base() {
    sc_ = -1;
    pthread_mutex_init(&mt_, NULL);
  }

  virtual ~Connection() {
    Disconnect();
    pthread_mutex_destroy(&mt_);
  }

  inline bool Connected() const {
    return (sc_ != -1);
  }

  bool Reconnect() {
    if (Connected()) {
      return true;
    }
    Lock();
    if (Connected()) {
      Unlock();
      return true;
    }
    bool res = ReconnectLocked();
    Unlock();
    return res;
  }

  void Disconnect() {
    if (Connected()) {
      DLOG("Closing the connection\n");
      close(sc_);
      sc_ = -1;
    }
  }

  inline void Lock() {
    pthread_mutex_lock(&mt_);
  }

  inline void Unlock() {
    pthread_mutex_unlock(&mt_);
  }

  bool get_version_string() {
    Lock();
    bool res = get_version_string_locked();
    if (!res) {
      Disconnect();
    }
    Unlock();
    return res;
  }

  uint64_t create_context() {
    Lock();
    uint64_t remote_handle = 0;
    bool res = create_context_locked(remote_handle);
    if (!res) {
      Disconnect();
    }
    Unlock();
    return remote_handle;
  }

  int release_context(uint64_t remote_handle) {
    Lock();
    int n_ref = -1;
    bool res = release_context_locked(remote_handle, n_ref);
    if (!res) {
      Disconnect();
    }
    Unlock();
    return n_ref;
  }

  const char *get_info_string(uint64_t remote_handle, char *buf, int size) {
    Lock();
    bool res = get_info_string_locked(remote_handle, buf, size);
    if (!res) {
      Disconnect();
    }
    Unlock();
    return buf;
  }

  int get_context_info(uint64_t remote_handle, struct dmp_dv_info *info) {
    if ((!info) || (info->size < sizeof(*info))) {
      return EINVAL;
    }
    Lock();
    int32_t retval = -1;
    bool res = get_context_info_locked(remote_handle, info, retval);
    if (!res) {
      Disconnect();
    }
    Unlock();
    return retval;
  }

  uint64_t mem_alloc(uint64_t remote_handle, size_t size) {
    Lock();
    uint64_t retval = 0;
    bool res = mem_alloc_locked(remote_handle, size, retval);
    if (!res) {
      Disconnect();
    }
    Unlock();
    return retval;
  }

  int mem_release(uint64_t remote_handle_mem) {
    Lock();
    int32_t retval = -1;
    bool res = mem_release_locked(remote_handle_mem, retval);
    if (!res) {
      Disconnect();
    }
    Unlock();
    return retval;
  }

  uint64_t mem_map(uint64_t remote_handle_mem) {
    Lock();
    uint64_t retval = 0;
    bool res = mem_map_locked(remote_handle_mem, retval);
    if (!res) {
      Disconnect();
    }
    Unlock();
    return retval;
  }

  void mem_unmap(uint64_t remote_handle_mem) {
    Lock();
    bool res = mem_unmap_locked(remote_handle_mem);
    if (!res) {
      Disconnect();
    }
    Unlock();
  }

  int mem_sync_start(uint64_t remote_handle_mem, uint64_t remote_ptr, uint64_t sz, uint8_t *local_ptr,
                     uint8_t& rdwr, int rd, int wr) {
    Lock();
    int32_t retval = -1;
    bool res = mem_sync_start_locked(remote_handle_mem, remote_ptr, sz, local_ptr, rdwr, rd, wr, retval);
    if (!res) {
      Disconnect();
    }
    Unlock();
    return retval;
  }

  int mem_sync_end(uint64_t remote_handle_mem, uint64_t remote_ptr, uint64_t sz, uint8_t *local_ptr,
                   uint8_t& rdwr) {
    Lock();
    int32_t retval = -1;
    bool res = mem_sync_end_locked(remote_handle_mem, remote_ptr, sz, local_ptr, rdwr, retval);
    if (!res) {
      Disconnect();
    }
    Unlock();
    return retval;
  }

  uint64_t cmdlist_create(uint64_t remote_handle) {
    Lock();
    uint64_t retval = 0;
    bool res = cmdlist_create_locked(remote_handle, retval);
    if (!res) {
      Disconnect();
    }
    Unlock();
    return retval;
  }

  int cmdlist_release(uint64_t remote_handle_cmdlist) {
    Lock();
    int32_t retval = -1;
    bool res = cmdlist_release_locked(remote_handle_cmdlist, retval);
    if (!res) {
      Disconnect();
    }
    Unlock();
    return retval;
  }

  int cmdlist_commit(uint64_t remote_handle_cmdlist) {
    Lock();
    int32_t retval = -1;
    bool res = cmdlist_commit_locked(remote_handle_cmdlist, retval);
    if (!res) {
      Disconnect();
    }
    Unlock();
    return retval;
  }

  int64_t cmdlist_exec(uint64_t remote_handle_cmdlist) {
    Lock();
    int64_t retval = -1;
    bool res = cmdlist_exec_locked(remote_handle_cmdlist, retval);
    if (!res) {
      Disconnect();
    }
    Unlock();
    return retval;
  }

  int cmdlist_wait(uint64_t remote_handle_cmdlist, int64_t exec_id, int64_t& last_exec_time) {
    Lock();
    int32_t retval = -1;
    bool res = cmdlist_wait_locked(remote_handle_cmdlist, exec_id, last_exec_time, retval);
    if (!res) {
      Disconnect();
    }
    Unlock();
    return retval;
  }

  int cmdlist_add_raw(uint64_t remote_handle_cmdlist, struct dmp_dv_cmdraw *cmd) {
    Lock();
    int32_t retval = -1;
    bool res = cmdlist_add_raw_locked(remote_handle_cmdlist, cmd, retval);
    if (!res) {
      Disconnect();
    }
    Unlock();
    return retval;
  }

  int mem_to_device(uint64_t remote_handle_mem, uint64_t remote_ptr, uint8_t *local_ptr,
                    uint64_t offs, uint64_t size, int flags) {
    Lock();
    int32_t retval = -1;
    bool res = mem_to_device_locked(remote_handle_mem, remote_ptr, local_ptr,
                                    offs, size, flags, retval);
    if (!res) {
      Disconnect();
    }
    Unlock();
    return retval;
  }

  int mem_to_cpu(uint64_t remote_handle_mem, uint64_t remote_ptr, uint8_t *local_ptr,
                 uint64_t offs, uint64_t size, int flags) {
    Lock();
    int32_t retval = -1;
    bool res = mem_to_cpu_locked(remote_handle_mem, remote_ptr, local_ptr,
                                 offs, size, flags, retval);
    if (!res) {
      Disconnect();
    }
    Unlock();
    return retval;
  }

  int device_exists(uint64_t remote_handle, int dev_type_id) {
    Lock();
    int32_t retval = -1;
    bool res = device_exists_locked(remote_handle, dev_type_id, retval);
    if (!res) {
      Disconnect();
    }
    Unlock();
    return retval;
  }

 protected:
  bool device_exists_locked(uint64_t remote_handle, int32_t dev_type_id, int32_t& retval) {
    if (!Connected()) {
      return false;
    }

    uint8_t cmd_id = k_dmp_dv_device_exists;
    SEND(&cmd_id, 1, true);
    SEND(&remote_handle, 8, true);
    SEND(&dev_type_id, 4, false);

    RECV(&retval, 4);
    if (retval < 0) {
      return get_last_error_message_locked();
    }
    return true;
  }

  bool mem_to_cpu_locked(uint64_t remote_handle_mem, uint64_t remote_ptr, uint8_t *local_ptr,
                         uint64_t offs, uint64_t size, int32_t flags, int32_t& retval) {
    if (!Connected()) {
      return false;
    }

    uint8_t cmd_id = k_dmp_dv_mem_to_cpu;
    SEND(&cmd_id, 1, true);
    SEND(&remote_handle_mem, 8, true);
    SEND(&remote_ptr, 8, true);
    SEND(&offs, 8, true);
    SEND(&size, 8, true);
    SEND(&flags, 4, false);

    RECV(&retval, 4);
    if (retval) {
      return get_last_error_message_locked();
    }
    RECV(local_ptr + offs, size);

    return true;
  }

  bool mem_to_device_locked(uint64_t remote_handle_mem, uint64_t remote_ptr, uint8_t *local_ptr,
                            uint64_t offs, uint64_t size, int32_t flags, int32_t& retval) {
    if (!Connected()) {
      return false;
    }

    uint8_t cmd_id = k_dmp_dv_mem_to_device;
    SEND(&cmd_id, 1, true);
    SEND(&remote_handle_mem, 8, true);
    SEND(&remote_ptr, 8, true);
    SEND(&offs, 8, true);
    SEND(&size, 8, true);
    SEND(&flags, 4, flags & DMP_DV_MEM_AS_DEV_OUTPUT ? false : true);
    if (!(flags & DMP_DV_MEM_AS_DEV_OUTPUT)) {
      SEND(local_ptr + offs, size, false);
    }

    RECV(&retval, 4);
    if (retval) {
      return get_last_error_message_locked();
    }
    return true;
  }

  int cmdlist_add_raw_locked(uint64_t remote_handle_cmdlist, struct dmp_dv_cmdraw *cmd, int32_t& retval) {
    if (!Connected()) {
      return false;
    }

    uint8_t cmd_id = k_dmp_dv_cmdlist_add_raw;
    SEND(&cmd_id, 1, true);
    SEND(&remote_handle_cmdlist, 8, true);
    SEND(cmd, cmd->size, false);

    RECV(&retval, 4);
    if (retval) {
      return get_last_error_message_locked();
    }
    return true;
  }

  bool cmdlist_wait_locked(uint64_t remote_handle_cmdlist, int64_t exec_id, int64_t& last_exec_time, int32_t& retval) {
    if (!Connected()) {
      return false;
    }

    uint8_t cmd_id = k_dmp_dv_cmdlist_wait;
    SEND(&cmd_id, 1, true);
    SEND(&remote_handle_cmdlist, 8, true);
    SEND(&exec_id, 8, false);

    RECV(&retval, 4);
    if (retval) {
      return get_last_error_message_locked();
    }
    RECV(&last_exec_time, 8);

    return true;
  }

  bool cmdlist_exec_locked(uint64_t remote_handle_cmdlist, int64_t& retval) {
    if (!Connected()) {
      return false;
    }

    uint8_t cmd_id = k_dmp_dv_cmdlist_exec;
    SEND(&cmd_id, 1, true);
    SEND(&remote_handle_cmdlist, 8, false);

    RECV(&retval, 8);
    if (retval < 0) {
      return get_last_error_message_locked();
    }
    return true;
  }

  bool cmdlist_commit_locked(uint64_t remote_handle_cmdlist, int32_t& retval) {
    if (!Connected()) {
      return false;
    }

    uint8_t cmd_id = k_dmp_dv_cmdlist_commit;
    SEND(&cmd_id, 1, true);
    SEND(&remote_handle_cmdlist, 8, false);

    RECV(&retval, 4);
    if (retval) {
      return get_last_error_message_locked();
    }
    return true;
  }

  bool cmdlist_release_locked(uint64_t remote_handle_cmdlist, int32_t& retval) {
    if (!Connected()) {
      return false;
    }

    uint8_t cmd_id = k_dmp_dv_cmdlist_release;
    SEND(&cmd_id, 1, true);
    SEND(&remote_handle_cmdlist, 8, false);

    RECV(&retval, 4);
    return true;
  }

  bool cmdlist_create_locked(uint64_t remote_handle, uint64_t& retval) {
    if (!Connected()) {
      return false;
    }

    uint8_t cmd_id = k_dmp_dv_cmdlist_create;
    SEND(&cmd_id, 1, true);
    SEND(&remote_handle, 8, false);

    RECV(&retval, 8);
    if (!retval) {
      return get_last_error_message_locked();
    }
    return true;
  }

  bool mem_sync_end_locked(uint64_t remote_handle_mem, uint64_t remote_ptr, uint64_t sz, uint8_t *local_ptr,
                           uint8_t& rdwr, int32_t& retval) {
    if (!Connected()) {
      return false;
    }

    uint8_t cmd_id = k_dmp_dv_mem_sync_end;
    SEND(&cmd_id, 1, true);
    SEND(&remote_handle_mem, 8, true);
    SEND(&remote_ptr, 8, true);
    SEND(&sz, 8, true);
    if (rdwr & 2) {
      SEND(&rdwr, 1, true);
      SEND(local_ptr, sz, false);
    }
    else {
      SEND(&rdwr, 1, false);
    }

    RECV(&retval, 4);
    if (!retval) {
      rdwr = 0;
    }
    else {
      return get_last_error_message_locked();
    }
    return true;
  }

  bool mem_sync_start_locked(uint64_t remote_handle_mem, uint64_t remote_ptr, uint64_t sz, uint8_t *local_ptr,
                             uint8_t& rdwr, int rd, int wr, int32_t& retval) {
    if (!Connected()) {
      return false;
    }

    uint8_t new_rdwr = (rd ? 1 : 0) | (wr ? 2 : 0);
    if (!new_rdwr) {
      retval = EINVAL;
      set_last_error_message("dmp_dv_mem_sync_start(): Invalid argument: rd or wr must be non-zero, got rd=%d wr=%d",
                             rd, wr);
      return true;
    }
    if (rdwr) {
      if ((rdwr | new_rdwr) == rdwr) {
        retval = 0;  // already in the requested sync
        return true;
      }
      else {
        Unlock();
        int n = dmp_dv_mem_sync_end((dmp_dv_mem)(size_t)remote_handle_mem);
        Lock();
        if (n) {
          retval = n;
          return true;
        }
      }
    }
    rdwr = new_rdwr;
    uint8_t cmd_id = k_dmp_dv_mem_sync_start;
    SEND(&cmd_id, 1, true);
    SEND(&remote_handle_mem, 8, true);
    SEND(&remote_ptr, 8, true);
    SEND(&sz, 8, true);
    SEND(&rdwr, 1, false);

    RECV(&retval, 4);
    if (retval) {
      return get_last_error_message_locked();
    }
    if (rdwr & 1) {
      RECV(local_ptr, sz);
    }
    retval = 0;
    return true;
  }

  bool mem_unmap_locked(uint64_t remote_handle_mem) {
    if (!Connected()) {
      return false;
    }

    uint8_t cmd_id = k_dmp_dv_mem_unmap;
    SEND(&cmd_id, 1, true);
    SEND(&remote_handle_mem, 8, false);
    uint8_t res = 0xFF;
    RECV(&res, 1);
    if (res) {
      set_last_error_message("dmp_dv_mem_unmap(): protocol error: res=%d", (int)res);
      ERR("%s\n", get_last_error_message());
      return false;
    }
    return true;
  }

  bool mem_map_locked(uint64_t remote_handle_mem, uint64_t& retval) {
    if (!Connected()) {
      return false;
    }

    uint8_t cmd_id = k_dmp_dv_mem_map;
    SEND(&cmd_id, 1, true);
    SEND(&remote_handle_mem, 8, false);

    RECV(&retval, 8);
    if (!retval) {
      return get_last_error_message_locked();
    }
    return true;
  }

  bool mem_release_locked(uint64_t remote_handle_mem, int32_t& retval) {
    if (!Connected()) {
      return false;
    }

    uint8_t cmd_id = k_dmp_dv_mem_release;
    SEND(&cmd_id, 1, true);
    SEND(&remote_handle_mem, 8, false);

    RECV(&retval, 4);
    return true;
  }

  bool mem_alloc_locked(uint64_t remote_handle, size_t size, uint64_t& retval) {
    if (!Connected()) {
      return false;
    }

    uint8_t cmd_id = k_dmp_dv_mem_alloc;
    SEND(&cmd_id, 1, true);
    SEND(&remote_handle, 8, true);
    uint64_t sz = size;
    if (sz & 4095) {
      sz += 4096 - (sz & 4095);
    }
    SEND(&sz, 8, false);

    RECV(&retval, 8);

    if (!retval) {
      return get_last_error_message_locked();
    }
    return true;
  }

  bool get_context_info_locked(uint64_t remote_handle, struct dmp_dv_info *info, int32_t& retval) {
    if (!Connected()) {
      return false;
    }

    uint8_t cmd_id = k_dmp_dv_context_get_info;
    SEND(&cmd_id, 1, true);
    SEND(&remote_handle, 8, true);
    SEND(info, sizeof(*info), 0);

    RECV(&retval, 4);
    if (!retval) {
      RECV(info, info->size);
    }
    else {
      return get_last_error_message_locked();
    }
    return true;
  }

  bool get_info_string_locked(uint64_t remote_handle, char *buf, int size) {
    if (!Connected()) {
      return false;
    }

    uint8_t cmd_id = k_dmp_dv_context_get_info_string;
    SEND(&cmd_id, 1, true);
    SEND(&remote_handle, 8, false);

    int32_t n = -1;
    RECV(&n, 4);
    if (n < 0) {
      buf[0] = 0;
      return false;
    }
    if (n) {
      if (n > size - 2) {
        set_last_error_message("Length of context info string is too long: %d", n);
        return false;
      }
      RECV(buf, n);
      buf[n + 1] = 0;
    }
    else {
      buf[0] = 0;
    }
    return true;
  }

  bool release_context_locked(uint64_t remote_handle, int& n_ref) {
    if (!Connected()) {
      return false;
    }

    uint8_t cmd_id = k_dmp_dv_context_release;
    SEND(&cmd_id, 1, true);
    SEND(&remote_handle, 8, false);
    RECV(&n_ref, 4);
    return true;
  }

  /// @brief Does RPC for context creation.
  /// @param remote_handle Result of the operation (handle of created context on remote side).
  /// @return true on communication success, false on network or protocol error.
  bool create_context_locked(uint64_t& remote_handle) {
    if (!Connected()) {
      return false;
    }

    uint8_t cmd_id = k_dmp_dv_context_create;
    SEND(&cmd_id, 1, false);
    RECV(&remote_handle, 8);
    if (!remote_handle) {
      return get_last_error_message_locked();
    }
    return true;
  }

  bool get_last_error_message_locked() {
    if (!Connected()) {
      return false;
    }

    uint8_t cmd_id = k_dmp_dv_get_last_error_message;
    SEND(&cmd_id, 1, false);
    int32_t n = -1;
    RECV(&n, 4);
    if (n < 0) {
      set_last_error_message("Got invalid length of last error message: %d bytes\n", n);
      return false;
    }
    if (n) {
      if (n > (int)sizeof(s_last_error_message) - 2) {
        set_last_error_message("Got unsupported length of last error message: %d bytes\n", n);
        return false;
      }
      RECV(s_last_error_message, n);
      s_last_error_message[n + 1] = 0;
    }
    else {
      s_last_error_message[0] = 0;
    }
    return true;
  }

  bool get_version_string_locked() {
    if (!Connected()) {
      return false;
    }

    uint8_t cmd_id = k_dmp_dv_get_version_string;
    SEND(&cmd_id, 1, false);
    int32_t n = -1;
    RECV(&n, 4);
    if (n < 0) {
      version_string[0] = 0;
      set_last_error_message("Got invalid length of version string: %d bytes\n", n);
      return false;
    }
    if (n) {
      if (n > (int)sizeof(version_string) - 2) {
        version_string[0] = 0;
        set_last_error_message("Got unsupported length of version string: %d bytes\n", n);
        return false;
      }
      RECV(version_string, n);
      version_string[n + 1] = 0;
    }
    else {
      version_string[0] = 0;
    }
    return true;
  }

  bool Recv(void *buf, size_t size) {
    ssize_t n = 0;
    for (size_t offs = 0; offs < size; offs += n) {
      n = recv(sc_, (uint8_t*)buf + offs, size - offs, MSG_WAITALL);
      if (n > 0) {
        continue;
      }
      if (!n) {
        set_last_error_message("recv() failed: connection closed by remote side");
        ERR("%s\n", get_last_error_message());
        close(sc_);
        sc_ = -1;
        return false;
      }
      switch (errno) {
        case EINTR:
          continue;
        default:
          set_last_error_message("recv() failed: errno=%d: %s\n", errno, strerror(errno));
          ERR("%s\n", get_last_error_message());
          close(sc_);
          sc_ = -1;
          return false;
      }
    }
    return true;
  }

  bool Send(const void *buf, size_t size, bool more) {
    ssize_t n = 0;
    for (size_t offs = 0; offs < size; offs += n) {
      n = send(sc_, (const uint8_t*)buf + offs, size - offs, more ? MSG_MORE : 0);
      if (n > 0) {
        continue;
      }
      if (!n) {
        set_last_error_message("send() failed: connection closed by remote side");
        ERR("%s\n", get_last_error_message());
        close(sc_);
        sc_ = -1;
        return false;
      }
      switch (errno) {
        case EINTR:
          continue;
        default:
          set_last_error_message("send() failed: errno=%d: %s\n", errno, strerror(errno));
          ERR("%s\n", get_last_error_message());
          close(sc_);
          sc_ = -1;
          return false;
      }
    }
    return true;
  }

  bool ReconnectLocked() {
    const char *addr = getenv("DMPDV_RPC");
    char s_host[256];
    char s_port[64];
    if (addr) {
      int i;
      for (i = 0; addr[i] && (addr[i] != ':'); ++i) {
        s_host[i] = addr[i];
      }
      s_host[i] = 0;
      if (addr[i]) {
        ++i;
      }
      int j;
      for (j = 0; addr[j + i]; ++j) {
        s_port[j] = addr[j + i];
      }
      s_port[j] = 0;
    }
    if ((!s_host[0]) || (!s_port[0])) {
      set_last_error_message("DMPDV_RPC env var must point to dmpdv_server in form of HOST:PORT, example: 'localhost:5555'");
      ERR("%s\n", get_last_error_message());
      return -1;
    }

    struct addrinfo hints;
    struct addrinfo *result;
    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    int n = getaddrinfo(s_host, s_port, &hints, &result);
    if (n) {
      set_last_error_message("getaddrinfo() failed with code %d: %s", n, gai_strerror(n));
      ERR("%s\n", get_last_error_message());
      return false;
    }
    for (struct addrinfo *rp = result; rp != NULL; rp = rp->ai_next) {
      sc_ = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
      if (sc_ == -1) {
        continue;
      }
      if (connect(sc_, rp->ai_addr, rp->ai_addrlen) != -1) {
        break;
      }
      close(sc_);
      sc_ = -1;
    }
    if (sc_ == -1) {
      set_last_error_message("Could not connect to %s:%s, errno=%d: %s", s_host, s_port, errno, strerror(errno));
      return false;
    }

    n = 1;
    setsockopt(sc_, SOL_SOCKET, SO_KEEPALIVE, &n, sizeof(n));
    n = 1;
    setsockopt(sc_, SOL_TCP, TCP_NODELAY, &n, sizeof(n));

    n = strlen(client_magic);
    if (n > MAGIC_MAX) {
      n = MAGIC_MAX;
    }
    SEND(client_magic, n, false);
    n = strlen(server_magic);
    if (n > MAGIC_MAX) {
      n = MAGIC_MAX;
    }
    char buf[MAGIC_MAX];
    RECV(buf, n);
    if (memcmp(buf, server_magic, n)) {
      ERR("Wrong magic packet received\n");
      close(sc_);
      sc_ = -1;
      return false;
    }
    return true;
  }

 private:
  int sc_;  // client socket
  pthread_mutex_t mt_;
};


/// @brief Wrapper for dmp_dv_context.
class Context : public Base {
 public:
  Context(Connection *conn) : Base() {
    conn_ = conn;
    remote_handle_ = 0;
    memset(info_string_, 0, sizeof(info_string_));
  }

  virtual ~Context() {
    if ((conn_->Connected()) && (remote_handle_)) {
      conn_->release_context(remote_handle_);
    }
    conn_->Release();
  }

  bool Create() {
    remote_handle_ = conn_->create_context();
    return (remote_handle_ != 0);
  }

  const char *GetInfoString() {
    return conn_->get_info_string(remote_handle_, info_string_, sizeof(info_string_));
  }

  int GetInfo(struct dmp_dv_info *info) {
    return conn_->get_context_info(remote_handle_, info);
  }

  dmp_dv_mem MemAlloc(size_t size);

  dmp_dv_cmdlist CreateCmdList();

  int DeviceExists(int dev_type_id) {
    return conn_->device_exists(remote_handle_, dev_type_id);
  }

 public:
  Connection *conn_;
  uint64_t remote_handle_;
  char info_string_[256];
};


/// @brief Wrapper for dmp_dv_mem.
class Memory : public Base {
 public:
  Memory(Context *ctx, uint64_t remote_handle, size_t sz) : Base() {
    ctx_ = ctx;
    remote_handle_ = remote_handle;
    sz_ = sz;
    remote_ptr_ = 0;
    local_ptr_ = NULL;
    rdwr_ = 0;
    ctx_->Retain();
  }

  virtual ~Memory() {
    if (remote_handle_) {
      pthread_mutex_lock(&mt_);
      mem_by_handle_.erase(remote_handle_);
      pthread_mutex_unlock(&mt_);
      ctx_->conn_->mem_release(remote_handle_);
    }
    if (local_ptr_) {
      munmap(local_ptr_, sz_);
      __sync_add_and_fetch(&Memory::total_size_, -(int64_t)sz_);
    }
    ctx_->Release();
  }

  bool Initialize() {
    local_ptr_ = (uint8_t*)mmap(NULL, sz_, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (!local_ptr_) {
      set_last_error_message("Failed to allocate %zu bytes of memory", sz_);
      return false;
    }
    __sync_add_and_fetch(&Memory::total_size_, (int64_t)sz_);

    pthread_mutex_lock(&mt_);
    mem_by_handle_.emplace(std::make_pair(remote_handle_, this));
    pthread_mutex_unlock(&mt_);

    return true;
  }

  uint8_t *MemMap() {
    remote_ptr_ = ctx_->conn_->mem_map(remote_handle_);
    if (!remote_ptr_) {
      return NULL;
    }
    return local_ptr_;
  }

  void MemUnmap() {
    ctx_->conn_->mem_unmap(remote_handle_);
    remote_ptr_ = 0;
  }

  int MemSyncStart(int rd, int wr) {
    return ctx_->conn_->mem_sync_start(remote_handle_, remote_ptr_, sz_, local_ptr_, rdwr_, rd, wr);
  }

  int MemSyncEnd() {
    return ctx_->conn_->mem_sync_end(remote_handle_, remote_ptr_, sz_, local_ptr_, rdwr_);
  }

  int MemToDevice(size_t offs, size_t size, int flags) {
    if (offs + size > sz_) {
      set_last_error_message("Invalid memory range specified: offs=%zu size=%zu while memory buffer size is %zu",
                             offs, size, sz_);
      return EINVAL;
    }
    if (!size) {
      return 0;
    }
    if (!remote_ptr_) {
      set_last_error_message("Memory must be mapped before starting synchronization");
      return EINVAL;
    }
    return ctx_->conn_->mem_to_device(remote_handle_, remote_ptr_, local_ptr_, offs, size, flags);
  }

  int MemToCPU(size_t offs, size_t size, int flags) {
    if (offs + size > sz_) {
      set_last_error_message("Invalid memory range specified: offs=%zu size=%zu while memory buffer size is %zu",
                             offs, size, sz_);
      return EINVAL;
    }
    if (!size) {
      return 0;
    }
    if (!remote_ptr_) {
      set_last_error_message("Memory must be mapped before starting synchronization");
      return EINVAL;
    }
    return ctx_->conn_->mem_to_cpu(remote_handle_, remote_ptr_, local_ptr_, offs, size, flags);
  }

  static Memory* get_by_handle(uint64_t handle) {
    pthread_mutex_lock(&mt_);
    auto it = mem_by_handle_.find(handle);
    Memory* obj = (it != mem_by_handle_.end()) ? it->second : NULL;
    pthread_mutex_unlock(&mt_);
    if (!obj) {
      set_last_error_message("Memory object was not found by handle %zu", handle);
      ERR("%s\n", get_last_error_message());
    }
    return obj;
  }

 public:
  Context *ctx_;
  uint64_t remote_handle_;
  uint64_t sz_;
  uint64_t remote_ptr_;
  uint8_t *local_ptr_;
  uint8_t rdwr_;

  static std::map<uint64_t, Memory*> mem_by_handle_;
  static pthread_mutex_t mt_;
  static int64_t total_size_;
};


std::map<uint64_t, Memory*> Memory::mem_by_handle_;
pthread_mutex_t Memory::mt_ = PTHREAD_MUTEX_INITIALIZER;
int64_t Memory::total_size_ = 0;


/// @brief Wrapper for dmp_dv_cmdlist.
class CmdList : public Base {
 public:
  CmdList(Context *ctx, uint64_t remote_handle) : Base() {
    ctx_ = ctx;
    remote_handle_ = remote_handle;
    ctx_->Retain();
    last_exec_time_ = 0;
  }

  virtual ~CmdList() {
    if (remote_handle_) {
      ctx_->conn_->cmdlist_release(remote_handle_);
    }
    ctx_->Release();
  }

  int Commit() {
    return ctx_->conn_->cmdlist_commit(remote_handle_);
  }

  int64_t Exec() {
    return ctx_->conn_->cmdlist_exec(remote_handle_);
  }

  int Wait(int64_t exec_id) {
    return ctx_->conn_->cmdlist_wait(remote_handle_, exec_id, last_exec_time_);
  }

  int AddRaw(struct dmp_dv_cmdraw *cmd) {
    return ctx_->conn_->cmdlist_add_raw(remote_handle_, cmd);
  }

  inline int64_t GetLastExecTime() const {
    return last_exec_time_;
  }

 public:
  Context *ctx_;
  uint64_t remote_handle_;

 private:
  int64_t last_exec_time_;
};


dmp_dv_mem Context::MemAlloc(size_t size) {
  uint64_t remote_handle_mem = conn_->mem_alloc(remote_handle_, size);
  if (!remote_handle_mem) {
    return 0;
  }

  Memory *mem = new Memory(this, remote_handle_mem, size);
  if (!mem) {
    conn_->mem_release(remote_handle_mem);
    set_last_error_message("Failed to allocate %zu bytes of host memory", sizeof(Memory));
    return NULL;
  }
  if (!mem->Initialize()) {
    mem->Release();
    return NULL;
  }

  return (dmp_dv_mem)(size_t)remote_handle_mem;
}


dmp_dv_cmdlist Context::CreateCmdList() {
  uint64_t remote_handle_cmdlist = conn_->cmdlist_create(remote_handle_);
  if (!remote_handle_cmdlist) {
    return 0;
  }

  CmdList *cmdlist = new CmdList(this, remote_handle_cmdlist);
  if (!cmdlist) {
    conn_->cmdlist_release(remote_handle_cmdlist);
    set_last_error_message("Failed to allocate %zu bytes of host memory", sizeof(CmdList));
    return NULL;
  }
  return (dmp_dv_cmdlist)cmdlist;
}


extern "C" {

const char *dmp_dv_get_version_string() {
  Connection *conn = new Connection();
  if (!conn->Reconnect()) {
    conn->Release();
    return version_string;
  }
  conn->get_version_string();
  conn->Release();
  return version_string;
}


void dmp_dv_set_last_error_message(const char *format, ...) {
  va_list args;
  va_start(args, format);
  vsnprintf(s_last_error_message, sizeof(s_last_error_message), format, args);
}


const char *dmp_dv_get_last_error_message() {
  return s_last_error_message;
}


dmp_dv_context dmp_dv_context_create() {
  Connection *conn = new Connection();
  if (!conn->Reconnect()) {
    conn->Release();
    return NULL;
  }
  Context *ctx = new Context(conn);
  if (!ctx->Create()) {
    ctx->Release();
    return NULL;
  }
  return (dmp_dv_context)ctx;
}


int dmp_dv_context_release(dmp_dv_context ctx) {
  if (!ctx) {
    return 0;
  }
  return ((Context*)ctx)->Release();
}


int dmp_dv_context_retain(dmp_dv_context ctx) {
  if (!ctx) {
    return 0;
  }
  return ((Context*)ctx)->Retain();
}


const char *dmp_dv_context_get_info_string(dmp_dv_context ctx) {
  if (!ctx) {
    return "";
  }
  return ((Context*)ctx)->GetInfoString();
}


int dmp_dv_context_get_info(dmp_dv_context ctx, struct dmp_dv_info *info) {
  if (!ctx) {
    return EINVAL;
  }
  return ((Context*)ctx)->GetInfo(info);
}


dmp_dv_mem dmp_dv_mem_alloc(dmp_dv_context ctx, size_t size) {
  if (!ctx) {
    return NULL;
  }
  return ((Context*)ctx)->MemAlloc(size);
}


int dmp_dv_mem_release(dmp_dv_mem remote_handle_mem) {
  if (!remote_handle_mem) {
    return 0;
  }
  Memory *obj = Memory::get_by_handle((uint64_t)remote_handle_mem);
  if (!obj) {
    return 0;
  }
  return obj->Release();
}


uint8_t *dmp_dv_mem_map(dmp_dv_mem remote_handle_mem) {
  if (!remote_handle_mem) {
    return NULL;
  }
  Memory *obj = Memory::get_by_handle((uint64_t)remote_handle_mem);
  if (!obj) {
    return NULL;
  }
  return obj->MemMap();
}


void dmp_dv_mem_unmap(dmp_dv_mem remote_handle_mem) {
  if (dmp_dv_mem_sync_end(remote_handle_mem)) {
    return;
  }
  if (!remote_handle_mem) {
    return;
  }
  Memory *obj = Memory::get_by_handle((uint64_t)remote_handle_mem);
  if (!obj) {
    return;
  }
  obj->MemUnmap();
}


int dmp_dv_mem_sync_start(dmp_dv_mem remote_handle_mem, int rd, int wr) {
  int res = dmp_dv_mem_sync_end(remote_handle_mem);
  if (res) {
    return res;
  }
  if (!remote_handle_mem) {
    return -1;
  }
  Memory *obj = Memory::get_by_handle((uint64_t)remote_handle_mem);
  if (!obj) {
    return -1;
  }
  return obj->MemSyncStart(rd, wr);
}


int dmp_dv_mem_sync_end(dmp_dv_mem remote_handle_mem) {
  if (!remote_handle_mem) {
    return -1;
  }
  Memory *obj = Memory::get_by_handle((uint64_t)remote_handle_mem);
  if (!obj) {
    return -1;
  }
  return obj->MemSyncEnd();
}


size_t dmp_dv_mem_get_size(dmp_dv_mem remote_handle_mem) {
  if (!remote_handle_mem) {
    return -1;
  }
  Memory *obj = Memory::get_by_handle((uint64_t)remote_handle_mem);
  if (!obj) {
    return -1;
  }
  return obj->sz_;
}


int64_t dmp_dv_mem_get_total_size() {
  return __sync_add_and_fetch(&Memory::total_size_, 0);
}


dmp_dv_cmdlist dmp_dv_cmdlist_create(dmp_dv_context ctx) {
  DLOG("dmp_dv_cmdlist_create(): ENTER\n");
  if (!ctx) {
    return NULL;
  }
  dmp_dv_cmdlist res = ((Context*)ctx)->CreateCmdList();
  DLOG("dmp_dv_cmdlist_create(): EXIT: res_local=%zu res_remote=%zu\n", (size_t)res, (size_t)(((CmdList*)res)->remote_handle_));
  return res;
}


int dmp_dv_cmdlist_release(dmp_dv_cmdlist cmdlist) {
  if (!cmdlist) {
    return 0;
  }
  return ((CmdList*)cmdlist)->Release();
}


int dmp_dv_cmdlist_retain(dmp_dv_cmdlist cmdlist) {
  if (!cmdlist) {
    return 0;
  }
  return ((CmdList*)cmdlist)->Retain();
}


int dmp_dv_cmdlist_commit(dmp_dv_cmdlist cmdlist) {
  if (!cmdlist) {
    set_last_error_message("dmp_dv_cmdlist_commit(): Invalid argument: cmdlist is NULL");
    return EINVAL;
  }
  return ((CmdList*)cmdlist)->Commit();
}


int64_t dmp_dv_cmdlist_exec(dmp_dv_cmdlist cmdlist) {
  if (!cmdlist) {
    set_last_error_message("dmp_dv_cmdlist_exec(): Invalid argument: cmdlist is NULL");
    return -1;
  }
  return ((CmdList*)cmdlist)->Exec();
}


int dmp_dv_cmdlist_wait(dmp_dv_cmdlist cmdlist, int64_t exec_id) {
  if (!cmdlist) {
    set_last_error_message("dmp_dv_cmdlist_wait(): Invalid argument: cmdlist is NULL");
    return -1;
  }
  return ((CmdList*)cmdlist)->Wait(exec_id);
}


int dmp_dv_cmdlist_add_raw(dmp_dv_cmdlist cmdlist, struct dmp_dv_cmdraw *cmd) {
  DLOG("dmp_dv_cmdlist_add_raw(): ENTER\n");
  if ((!cmdlist) || (!cmd) || (cmd->size < sizeof(struct dmp_dv_cmdraw))) {
    set_last_error_message("dmp_dv_cmdlist_add_raw(): EINVAL: cmdlist=%zu cmd=%zu cmd->size=%zu\n",
                           (size_t)cmdlist, (size_t)cmd, cmd ? (size_t)cmd->size : (size_t)0);
    return EINVAL;
  }
  int res = ((CmdList*)cmdlist)->AddRaw(cmd);
  DLOG("dmp_dv_cmdlist_add_raw(): EXIT: res=%d\n", res);
  return res;
}


int dmp_dv_mem_to_device(dmp_dv_mem remote_handle_mem, size_t offs, size_t size, int flags) {
  DLOG("dmp_dv_mem_to_device(): ENTER\n");
  if (!remote_handle_mem) {
    DLOG("dmp_dv_mem_to_device(): EXIT: !remote_handle_mem\n");
    return -1;
  }
  Memory *obj = Memory::get_by_handle((uint64_t)remote_handle_mem);
  if (!obj) {
    DLOG("dmp_dv_mem_to_device(): EXIT: !obj\n");
    return -1;
  }
  int res = obj->MemToDevice(offs, size, flags);
  DLOG("dmp_dv_mem_to_device(): EXIT: res=%d\n", res);
  return res;
}


int dmp_dv_mem_to_cpu(dmp_dv_mem remote_handle_mem, size_t offs, size_t size, int flags) {
  DLOG("dmp_dv_mem_to_cpu(): ENTER\n");
  if (!remote_handle_mem) {
    DLOG("dmp_dv_mem_to_cpu(): EXIT: !remote_handle_mem\n");
    return -1;
  }
  Memory *obj = Memory::get_by_handle((uint64_t)remote_handle_mem);
  if (!obj) {
    DLOG("dmp_dv_mem_to_cpu(): EXIT: !obj\n");
    return -1;
  }
  int res = obj->MemToCPU(offs, size, flags);
  DLOG("dmp_dv_mem_to_cpu(): EXIT: res=%d\n", res);
  return res;
}


int64_t dmp_dv_cmdlist_get_last_exec_time(dmp_dv_cmdlist cmdlist) {
  if (!cmdlist) {
    set_last_error_message("dmp_dv_cmdlist_wait(): Invalid argument: cmdlist is NULL");
    return -1;
  }
  return ((CmdList*)cmdlist)->GetLastExecTime();
}


int dmp_dv_device_exists(dmp_dv_context ctx, int dev_type_id) {
  if(!ctx) {
    set_last_error_message("Invalid argument: ctx is NULL");
    return -1;
  }
  return ((Context*)ctx)->DeviceExists(dev_type_id);
}


int dmp_dv_fpga_device_exists(dmp_dv_context ctx, int dev_type_id) {
  return dmp_dv_device_exists(ctx, dev_type_id);
}

}  // extern "C"
