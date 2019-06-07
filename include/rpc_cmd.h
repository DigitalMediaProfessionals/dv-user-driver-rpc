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
 * @brief Command ids for RPC.
 */
#pragma once

enum cmd_id_e {
  k_dmp_dv_get_version_string = 1,
  k_dmp_dv_get_last_error_message,
  k_dmp_dv_context_create,
  k_dmp_dv_context_release,
  k_dmp_dv_context_get_info_string,
  k_dmp_dv_context_get_info,
  k_dmp_dv_mem_alloc,
  k_dmp_dv_mem_release,
  k_dmp_dv_mem_map,
  k_dmp_dv_mem_unmap,
  k_dmp_dv_mem_sync_start,
  k_dmp_dv_mem_sync_end,
  k_dmp_dv_mem_get_total_size,
  k_dmp_dv_cmdlist_create,
  k_dmp_dv_cmdlist_release,
  k_dmp_dv_cmdlist_commit,
  k_dmp_dv_cmdlist_exec,
  k_dmp_dv_cmdlist_wait,
  k_dmp_dv_cmdlist_add_raw,
  k_dmp_dv_mem_to_device,
  k_dmp_dv_mem_to_cpu,
  k_dmp_dv_device_exists
};

#define LOG(...) fprintf(stdout, __VA_ARGS__); fflush(stdout)
#define ERR(...) fprintf(stderr, __VA_ARGS__); fflush(stderr)

#if 0
#define DLOG LOG
#else
#define DLOG(...)
#endif

#define MAGIC_MAX 256
static const char *client_magic = "4ITTzFdXFtr4u777p5kkXTx87G63cGjtsEe8loSpOPJz4ir5ZgvQ43a69ZudBi6D";
static const char *server_magic = "zCHyB7Lp1A4LheWbe+UhBWfXA9FkY0ZR6VmaVcp4V2eLY/fZ0xBlVGEr/+JmoJQB";


static char s_time[64];

static inline const char *format_time() {
  time_t t = time(NULL);
  strftime(s_time, sizeof(s_time), "%Y%m%d%H%M%S", localtime(&t));
  return s_time;
}
