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
 * @brief RPC server (TCP) for libdmpdv.so.
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

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <stdint.h>
#include <string.h>

#include <map>

#include "dmp_dv.h"
#include "dmp_dv_cmdraw_v1.h"

#include "rpc_cmd.h"


struct ClientInfo {
  ClientInfo(const struct sockaddr_in& sin) {
    memcpy(&this->sin, &sin, sizeof(sin));
  }

  struct sockaddr_in sin;
};


static volatile int sock_server = -1;
static volatile bool g_should_stop = false;
static std::map<pid_t, ClientInfo> *clients;
static bool separate_log = false;


static bool recv_all(int sc, void *buf, size_t size) {
  ssize_t n = 0;
  for (size_t offs = 0; offs < size; offs += n) {
    n = recv(sc, (uint8_t*)buf + offs, size - offs > MAX_CHUNK ? MAX_CHUNK : size - offs, 0/*MSG_WAITALL*/);
    if (n > 0) {
      continue;
    }
    if (!n) {
      LOG("%s: Client disconnected\n", format_time());
      return false;
    }
    switch (errno) {
      case EINTR:
        n = 0;
        continue;
      default:
        ERR("%s: Socket error: errno=%d\n", format_time(), errno);
        return false;
    }
  }
  return true;
}


static bool send_all(int sc, const void *buf, size_t size, bool more) {
  ssize_t n = 0;
  for (size_t offs = 0; offs < size; offs += n) {
    DLOG("send: offs=%zu size=%zu n=%zu flags=%s\n", offs, size, size - offs > MAX_CHUNK ? MAX_CHUNK : size - offs,
         ((more) || (size - offs > MAX_CHUNK)) ? "MSG_MORE" : "0");
    n = send(sc, (const uint8_t*)buf + offs, size - offs > MAX_CHUNK ? MAX_CHUNK : size - offs,
             ((more) || (size - offs > MAX_CHUNK)) ? MSG_MORE : 0);
    if (n > 0) {
      continue;
    }
    if (!n) {
      LOG("%s: Client disconnected\n", format_time());
      return false;
    }
    switch (errno) {
      case EINTR:
        n = 0;
        continue;
      default:
        ERR("%s: Socket error: errno=%d\n", format_time(), errno);
        return false;
    }
  }
  return true;
}


#define SEND(data, size, more) if (!send_all(sc, data, size, more)) { return false; }
#define RECV(data, size) if (!recv_all(sc, data, size)) { return false; }


static int get_n_fd() {
  int n_fd = 0;
  DIR *d;
  struct dirent *dir;
  d = opendir("/proc/self/fd");
  if (!d) {
    ERR("%s: Could not open \"/proc/self/fd\" folder\n", format_time());
    return -1;
  }
  while ((dir = readdir(d))) {
    char *fnme = dir->d_name;
    int num = 1;
    for (; *fnme; ++fnme) {
      if ((*fnme >= '0') && (*fnme <= '9')) {
        continue;
      }
      num = 0;
      break;
    }
    if (num) {
      ++n_fd;
    }
  }
  closedir(d);
  return n_fd;
}


bool process_command(int sc, struct sockaddr_in& sin) {
  struct cmd_header cmd_hdr;
  RECV(&cmd_hdr, sizeof(cmd_hdr));
  if (!CMD_OK(cmd_hdr)) {
    ERR("%s: Received invalid cmd header\n", format_time());
    return false;
  }
  switch (cmd_hdr.cmd_id) {
    case k_dmp_dv_get_version_string:
    {
      const char *msg = dmp_dv_get_version_string();
      int32_t n = strlen(msg);
      SEND(&n, 4, n ? true : false);
      if (n) {
        SEND(msg, n, false);
      }
      break;
    }
    case k_dmp_dv_get_last_error_message:
    {
      const char *msg = dmp_dv_get_last_error_message();
      int32_t n = strlen(msg);
      SEND(&n, 4, n ? true : false);
      if (n) {
        SEND(msg, n, false);
      }
      break;
    }
    case k_dmp_dv_context_create:
    {
      dmp_dv_context ctx = dmp_dv_context_create();
      uint64_t remote = (uint64_t)(size_t)ctx;
      SEND(&remote, 8, false);
      LOG("%s: dmp_dv_context_create(): res=%zu\n", format_time(), (size_t)ctx);
      break;
    }
    case k_dmp_dv_context_release:
    {
      uint64_t remote = 0;
      RECV(&remote, 8);
      int32_t res = dmp_dv_context_release((dmp_dv_context)(size_t)remote);
      LOG("%s: dmp_dv_context_release(): %d, n_fd=%d\n", format_time(), (int)res, get_n_fd());
      SEND(&res, 4, false);
      break;
    }
    case k_dmp_dv_context_get_info_string:
    {
      uint64_t remote = 0;
      RECV(&remote, 8);
      const char *msg = dmp_dv_context_get_info_string((dmp_dv_context)(size_t)remote);
      int32_t n = strlen(msg);
      SEND(&n, 4, n ? true : false);
      if (n) {
        SEND(msg, n, false);
      }
      break;
    }
    case k_dmp_dv_context_get_info: {
      uint64_t remote = 0;
      RECV(&remote, 8);
      struct dmp_dv_info_v0 info;
      memset(&info, 0, sizeof(info));
      RECV(&info.header, sizeof(info.header));
      int32_t res = -1;
      if (info.header.size != sizeof(info)) {
        SEND(&res, 4, false);
        break;
      }
      res = dmp_dv_context_get_info((dmp_dv_context)(size_t)remote, &info.header);
      SEND(&res, 4, true);
      SEND(&info.header, sizeof(info), false);
      break;
    }
    case k_dmp_dv_mem_alloc:
    {
      uint64_t ctx = 0;
      RECV(&ctx, 8);
      uint64_t sz = 0;
      RECV(&sz, 8);
      uint64_t mem = (uint64_t)(size_t)dmp_dv_mem_alloc((dmp_dv_context)(size_t)ctx, (size_t)sz);
      LOG("%s: dmp_dv_mem_alloc(%d) => %08llx total_mem=%zu\n",
          format_time(), (int)sz, (unsigned long long)mem, dmp_dv_mem_get_total_size());
      SEND(&mem, 8, false);
      break;
    }
    case k_dmp_dv_mem_release:
    {
      uint64_t mem = 0;
      RECV(&mem, 8);
      LOG("%s: dmp_dv_mem_release(%08llx): ", format_time(), (unsigned long long)mem);
      int32_t res = dmp_dv_mem_release((dmp_dv_mem)(size_t)mem);
      LOG("%d total_mem=%zu\n", (int)res, dmp_dv_mem_get_total_size());
      SEND(&res, 4, false);
      break;
    }
    case k_dmp_dv_mem_map:
    {
      uint64_t mem = 0;
      RECV(&mem, 8);
      uint64_t ptr = (uint64_t)(size_t)dmp_dv_mem_map((dmp_dv_mem)(size_t)mem);
      LOG("%s: dmp_dv_mem_map(%08llx) => %08llx\n", format_time(), (unsigned long long)mem, (unsigned long long)ptr);
      SEND(&ptr, 8, false);
      break;
    }
    case k_dmp_dv_mem_unmap:
    {
      uint64_t mem = 0;
      RECV(&mem, 8);
      dmp_dv_mem_unmap((dmp_dv_mem)(size_t)mem);
      uint8_t res = 0;
      SEND(&res, 1, false);
      break;
    }
    case k_dmp_dv_mem_sync_start:
    {
      uint64_t mem = 0;
      RECV(&mem, 8);
      uint64_t ptr = 0;
      RECV(&ptr, 8);
      uint64_t sz = 0;
      RECV(&sz, 8);
      uint8_t rdwr = 0;
      RECV(&rdwr, 1);
      int32_t res = dmp_dv_mem_sync_start((dmp_dv_mem)(size_t)mem, rdwr & 1, (rdwr >> 1) & 1);
      if (res) {
        SEND(&res, 4, false);
        break;
      }
      if (rdwr & 1) {
        SEND(&res, 4, true);
        SEND((uint8_t*)(size_t)ptr, sz, false);
      }
      else {
        SEND(&res, 4, false);
      }
      break;
    }
    case k_dmp_dv_mem_sync_end:
    {
      uint64_t mem = 0;
      RECV(&mem, 8);
      uint64_t ptr = 0;
      RECV(&ptr, 8);
      uint64_t sz = 0;
      RECV(&sz, 8);
      uint8_t rdwr = 0;
      RECV(&rdwr, 1);
      if (rdwr & 2) {
        RECV((uint8_t*)(size_t)ptr, sz);
      }
      int32_t res = dmp_dv_mem_sync_end((dmp_dv_mem)(size_t)mem);
      SEND(&res, 4, false);
      break;
    }
    case k_dmp_dv_mem_get_total_size:
    {
      int64_t res = dmp_dv_mem_get_total_size();
      SEND(&res, 8, false);
      break;
    }
    case k_dmp_dv_cmdlist_create:
    {
      uint64_t ctx = 0;
      RECV(&ctx, 8);
      uint64_t cmdlist = (uint64_t)(size_t)dmp_dv_cmdlist_create((dmp_dv_context)(size_t)ctx);
      LOG("%s: dmp_dv_cmdlist_create(): res=%zu\n", format_time(), (size_t)cmdlist);
      SEND(&cmdlist, 8, false);
      break;
    }
    case k_dmp_dv_cmdlist_release:
    {
      uint64_t cmdlist = 0;
      RECV(&cmdlist, 8);
      int32_t res = dmp_dv_cmdlist_release((dmp_dv_cmdlist)(size_t)cmdlist);
      LOG("%s: dmp_dv_cmdlist_release(): %d\n", format_time(), (int)res);
      SEND(&res, 4, false);
      break;
    }
    case k_dmp_dv_cmdlist_commit:
    {
      uint64_t cmdlist = 0;
      RECV(&cmdlist, 8);
      int32_t res = dmp_dv_cmdlist_commit((dmp_dv_cmdlist)(size_t)cmdlist);
      SEND(&res, 4, false);
      break;
    }
    case k_dmp_dv_cmdlist_exec:
    {
      uint64_t cmdlist = 0;
      RECV(&cmdlist, 8);
      int64_t res = dmp_dv_cmdlist_exec((dmp_dv_cmdlist)(size_t)cmdlist);
      SEND(&res, 8, false);
      break;
    }
    case k_dmp_dv_cmdlist_wait:
    {
      uint64_t cmdlist = 0;
      RECV(&cmdlist, 8);
      int64_t exec_id = -1;
      RECV(&exec_id, 8);
      int32_t res = dmp_dv_cmdlist_wait((dmp_dv_cmdlist)(size_t)cmdlist, exec_id);
      if (!res) {
        int64_t last_exec_time = dmp_dv_cmdlist_get_last_exec_time((dmp_dv_cmdlist)(size_t)cmdlist);
        SEND(&res, 4, true);
        SEND(&last_exec_time, 8, false);
      }
      else {
        SEND(&res, 4, false);
      }
      break;
    }
    case k_dmp_dv_cmdlist_add_raw:
    {
      uint64_t cmdlist = 0;
      RECV(&cmdlist, 8);
      union {
        struct dmp_dv_cmdraw header;
        struct dmp_dv_cmdraw_conv_v0 v0;
        struct dmp_dv_cmdraw_conv_v1 v1;
        uint8_t buf[4096];
      } cmd;
      RECV(&cmd.header, sizeof(cmd.header));
      if (cmd.header.size > sizeof(cmd)) {
        ERR("%s: Received command with too large size %u\n", format_time(), (unsigned)cmd.header.size);
        return false;
      }
      RECV((uint8_t*)&cmd.header + sizeof(cmd.header), cmd.header.size - sizeof(cmd.header));
      int32_t res = dmp_dv_cmdlist_add_raw((dmp_dv_cmdlist)(size_t)cmdlist, &cmd.header);
      LOG("%s: dmp_dv_cmdlist_add_raw(): size=%u res=%d\n", format_time(), (unsigned)cmd.header.size, (int)res);
      SEND(&res, 4, false);
      break;
    }
    case k_dmp_dv_mem_to_device:
    {
      uint64_t mem = 0;
      RECV(&mem, 8);
      uint64_t ptr = 0;
      RECV(&ptr, 8);
      uint64_t offs = 0;
      RECV(&offs, 8);
      uint64_t size = 0;
      RECV(&size, 8);
      int32_t flags = 0;
      RECV(&flags, 4);
      if (!(flags & DMP_DV_MEM_AS_DEV_OUTPUT)) {
        RECV((uint8_t*)((size_t)ptr + (size_t)offs), (size_t)size);
      }
      int32_t res = dmp_dv_mem_to_device((dmp_dv_mem)(size_t)mem, (size_t)offs, (size_t)size, flags);
      SEND(&res, 4, false);
      break;
    }
    case k_dmp_dv_mem_to_cpu:
    {
      uint64_t mem = 0;
      RECV(&mem, 8);
      uint64_t ptr = 0;
      RECV(&ptr, 8);
      uint64_t offs = 0;
      RECV(&offs, 8);
      uint64_t size = 0;
      RECV(&size, 8);
      uint8_t flags = 0;
      RECV(&flags, 4);
      DLOG("k_dmp_dv_mem_to_cpu: mem=%zu ptr=%zu offs=%zu size=%zu flags=%d\n",
           (size_t)mem, (size_t)ptr, (size_t)offs, (size_t)size, flags);
      int32_t res = dmp_dv_mem_to_cpu((dmp_dv_mem)(size_t)mem, (size_t)offs, (size_t)size, flags);
      DLOG(" => %d\n", (int)res);
      SEND(&res, 4, res ? false : true);
      if (!res) {
        DLOG(" => sending %zu bytes\n", (size_t)size);
        SEND((uint8_t*)((size_t)ptr + (size_t)offs), (size_t)size, false);
        DLOG(" => sent %zu bytes\n", (size_t)size);
      }
      break;
    }
    case k_dmp_dv_device_exists:
    {
      uint64_t ctx = 0;
      RECV(&ctx, 8);
      uint32_t dev_type_id = 0;
      RECV(&dev_type_id, 4);
      int32_t res = dmp_dv_device_exists((dmp_dv_context)(size_t)ctx, dev_type_id);
      SEND(&res, 4, 0);
      break;
    }
    default:
    {
      ERR("%s: Received invalid cmd_id %d\n", format_time(), (int)cmd_hdr.cmd_id);
      return false;
    }
  }
  return true;
}


bool authorize(int sc, struct sockaddr_in& sin) {
  char buf[MAGIC_MAX];
  int n = strlen(client_magic);
  if (n > MAGIC_MAX) {
    n = MAGIC_MAX;
  }
  RECV(buf, n);
  if (memcmp(buf, client_magic, n)) {
    ERR("%s: Wrong magic packet\n", format_time());
    return false;
  }
  n = strlen(server_magic);
  if (n > MAGIC_MAX) {
    n = MAGIC_MAX;
  }
  SEND(server_magic, n, false);
  return true;
}


int communicate(int sc, struct sockaddr_in& sin) {
  if (separate_log) {
    char fnme[64];
    snprintf(fnme, sizeof(fnme), "%d.%d.%d.%d.%d.log",
             (int)(sin.sin_addr.s_addr & 0xFF), (int)((sin.sin_addr.s_addr >> 8) & 0xFF),
             (int)((sin.sin_addr.s_addr >> 16) & 0xFF), (int)((sin.sin_addr.s_addr >> 24) & 0xFF),
             (int)ntohs(sin.sin_port));
    int fout = open(fnme, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fout > 2) {
      close(2);
      close(1);
      dup2(fout, 1);
      dup2(fout, 2);
      close(fout);
    }
  }

  LOG("%s: Client connected: PID=%d ADDR=%d.%d.%d.%d:%d\n",
      format_time(), (int)getpid(),
      (int)(sin.sin_addr.s_addr & 0xFF), (int)((sin.sin_addr.s_addr >> 8) & 0xFF),
      (int)((sin.sin_addr.s_addr >> 16) & 0xFF), (int)((sin.sin_addr.s_addr >> 24) & 0xFF),
      (int)ntohs(sin.sin_port));

  int n = 1;
  setsockopt(sc, SOL_SOCKET, SO_KEEPALIVE, &n, sizeof(n));
  n = 1;
  setsockopt(sc, SOL_TCP, TCP_NODELAY, &n, sizeof(n));

  if (!authorize(sc, sin)) {
    close(sc);
    _exit(-1);
    return -1;
  }

  while (process_command(sc, sin)) {
    // Empty by design
  }

  LOG("%s: Client will now exit: PID=%d ADDR=%d.%d.%d.%d:%d\n",
      format_time(), (int)getpid(),
      (int)(sin.sin_addr.s_addr & 0xFF), (int)((sin.sin_addr.s_addr >> 8) & 0xFF),
      (int)((sin.sin_addr.s_addr >> 16) & 0xFF), (int)((sin.sin_addr.s_addr >> 24) & 0xFF),
      (int)ntohs(sin.sin_port));
  close(sc);
  _exit(0);
  return 0;
}


void on_sigint(int s) {
  g_should_stop = true;
  if (sock_server != -1) {
    int sock = sock_server;
    sock_server = -1;
    close(sock);
  }
}


void on_sigchld(int s) {
  if (g_should_stop) {
    return;
  }

  pid_t pid;
  int status;

  while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
    clients->erase(pid);
  }
}


int main(int argc, char **argv) {
  if (argc < 2) {
    ERR("USAGE: ./dmpdv_server PORT [IP]\n"
        "Examples: ./dmpdv_server 5555\n"
        "env SEPARATE_LOG=1 ./dmpdv_server 5555 0.0.0.0\n");
    _exit(EINVAL);
    return EINVAL;
  }
  const char *s_log = getenv("SEPARATE_LOG");
  separate_log = s_log ? (atoi(s_log) == 1) : false;
  const int port = atoi(argv[1]);
  const char *ip = argc > 2 ? argv[2] : "0.0.0.0";

  if ((port <= 0) || (port > 65535)) {
    ERR("%s: Invalid PORT specified: %d\n", format_time(), port);
    _exit(EINVAL);
    return EINVAL;
  }

  struct sockaddr_in sin;
  memset(&sin, 0, sizeof(sin));
  if (!inet_aton(ip, &sin.sin_addr)) {
    ERR("%s: Invalid IP address specified: %s\n", format_time(), ip);
    _exit(EINVAL);
    return EINVAL;
  }
  sin.sin_family = AF_INET;
  sin.sin_port = htons(port);

  clients = new std::map<pid_t, ClientInfo>();

  signal(SIGCHLD, on_sigchld);
  signal(SIGINT, on_sigint);

  sock_server = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (sock_server < 0) {
    ERR("%s: socket() failed\n", format_time());
    _exit(-1);
    return -1;
  }
  int n = 1;
  if (setsockopt(sock_server, SOL_SOCKET, SO_REUSEADDR, &n, sizeof(n)) < 0) {
    ERR("%s: setsockopt() failed\n", format_time());
    _exit(-1);
    return -1;
  }
  if (bind(sock_server, (struct sockaddr*)&sin, sizeof(sin)) < 0) {
    ERR("%s: bind() failed\n", format_time());
    _exit(-1);
    return -1;
  }
  if (listen(sock_server, 5) < 0) {
    ERR("%s: listen() failed\n", format_time());
    _exit(-1);
    return -1;
  }

  LOG("%s: Started server at %s:%d\n", format_time(), ip, port);

  // Simple single-threaded implementation
  for (; !g_should_stop;) {
    struct sockaddr_in sin;
    memset(&sin, 0, sizeof(sin));
    sin.sin_family = AF_INET;
    n = sizeof(sin);
    int sc = accept(sock_server, (struct sockaddr*)&sin, (unsigned*)&n);
    if (sc >= 0) {
      LOG("%s: Accepted connection from %d.%d.%d.%d:%d\n",
          format_time(),
          (int)(sin.sin_addr.s_addr & 0xFF), (int)((sin.sin_addr.s_addr >> 8) & 0xFF),
          (int)((sin.sin_addr.s_addr >> 16) & 0xFF), (int)((sin.sin_addr.s_addr >> 24) & 0xFF),
          (int)ntohs(sin.sin_port));
      pid_t pid = fork();
      if (pid < 0) {
        ERR("%s: fork() failed with code %d: %s\n", format_time(), errno, strerror(errno));
        ERR("%s: Will retry in 1.0 sec\n", format_time());
        close(sc);
        usleep(1000000);
      }
      if (pid) {  // parent
        clients->emplace(std::make_pair(pid, sin));
        close(sc);  // close client socket
        continue;
      }
      delete clients;
      close(sock_server);
      return communicate(sc, sin);
    }
    if (g_should_stop) {
      break;
    }
    switch (errno) {
      case EINTR:
        continue;
      case EMFILE:
      case ENFILE:
      case ENOBUFS:
      case ENOMEM:
        ERR("%s: accept() failed with code %d: %s\n", format_time(), errno, strerror(errno));
        ERR("%s: Will retry in 0.5 sec\n", format_time());
        usleep(500000);
        continue;
      case EPROTO:
      case EPERM:
        ERR("%s: accept() failed with code %d: %s\n", format_time(), errno, strerror(errno));
        ERR("%s: Will retry in 0.1 sec\n", format_time());
        usleep(100000);
        continue;
      default:
        ERR("%s: accept() failed with code %d: %s\n", format_time(), errno, strerror(errno));
        break;
    }
    break;
  }

  LOG("%s: Server will now prepare for exit\n", format_time());

  g_should_stop = true;
  if (sock_server != -1) {
    int sock = sock_server;
    sock_server = -1;
    close(sock);
  }

  LOG("%s: Killing %zu children...\n", format_time(), clients->size());
  for (auto it = clients->begin(); it != clients->end(); ++it) {
    int status;
    pid_t pid = it->first;
    if (waitpid(pid, &status, WNOHANG) > 0) {
      continue;
    }
    struct sockaddr_in& sin = it->second.sin;
    LOG("%s: Killing client %d.%d.%d.%d:%d\n",
        format_time(),
        (int)(sin.sin_addr.s_addr & 0xFF), (int)((sin.sin_addr.s_addr >> 8) & 0xFF),
        (int)((sin.sin_addr.s_addr >> 16) & 0xFF), (int)((sin.sin_addr.s_addr >> 24) & 0xFF),
        (int)ntohs(sin.sin_port));
    kill(it->first, SIGKILL);
    bool exited = false;
    for (int k = 0; k < 20; ++k) {
      usleep(10000);
      if (waitpid(pid, &status, WNOHANG) > 0) {
        exited = true;
        break;
      }
    }
    if (!exited) {
      LOG("%s: Terminating client %d.%d.%d.%d:%d\n",
          format_time(),
          (int)(sin.sin_addr.s_addr & 0xFF), (int)((sin.sin_addr.s_addr >> 8) & 0xFF),
          (int)((sin.sin_addr.s_addr >> 16) & 0xFF), (int)((sin.sin_addr.s_addr >> 24) & 0xFF),
          (int)ntohs(sin.sin_port));
      kill(it->first, SIGTERM);
      waitpid(pid, &status, 0);
    }
  }

  LOG("%s: Server will now exit\n", format_time());
  _exit(0);
  return 0;
}
