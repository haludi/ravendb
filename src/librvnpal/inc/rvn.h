#ifndef RVN_H
#define RVN_H

#include <stdint.h>

#ifdef __APPLE__
#define EXPORT __attribute__((visibility("default")))	
#elif _WIN32
#define EXPORT _declspec(dllexport) 
#else
#define EXPORT 
#endif

typedef int32_t bool;
#define true 1
#define false 0

EXPORT int32_t
rvn_get_error_string(uint32_t error, char* buf, int32_t buf_size, int32_t* special_errno_flags);

EXPORT uint64_t
rvn_get_current_thread_id(void);

EXPORT int32_t
rvn_write_header(const char *path, void *header, int32_t size, uint32_t *detailed_error_code);

EXPORT int32_t
sync_directory_for (const char *file_path, uint32_t * detailed_error_code);

int32_t /* internal use */
flush_file (int32_t fd);

int32_t /* internal use */
sync_directory_allowed (int dir_fd);

#define JOURNAL_MODE_DANGER 1

EXPORT int32_t
open_journal(char* file_name, int32_t flags, int64_t file_size, void** handle, uint32_t* error_code);

EXPORT int32_t 
close_journal(void* handle, uint32_t* error_code);

EXPORT int32_t
write_journal(void* handle, char* buffer, uint64_t size, int64_t offset, uint32_t* error_code);

#endif
