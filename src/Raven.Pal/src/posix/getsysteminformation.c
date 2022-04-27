#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <unistd.h>
#include <errno.h>
#include <sys/statvfs.h>
#include <sys/stat.h>
#include <stdio.h>
#include <string.h>


#ifdef __APPLE__
#include <sys/types.h>
#else
#include <sys/sysmacros.h>
#endif

#include "rvn.h"
#include "status_codes.h"
#include "internal_posix.h"

#define MAX_PF_NAME 1024
#define __DEV_BLOCK "dev/block"
#define S_STAT "stat"
#define SLASH_SYS "/sys"

EXPORT int32_t
rvn_get_system_information(struct SYSTEM_INFORMATION *sys_info,
                           int32_t *detailed_error_code)
{
    int64_t page_size = sysconf(_SC_PAGE_SIZE);
    if (page_size == -1)
        goto error;

    sys_info->page_size = page_size;
    sys_info->prefetch_status = true;

    return SUCCESS;
    
error:
    *detailed_error_code = errno;
    return FAIL;
}


EXPORT int32_t
rvn_get_path_disk_space(const char* path, uint64_t* total_free_bytes, uint64_t* total_size_bytes, int32_t* detailed_error_code)
{
    int rc;
    struct statvfs buffer;
    *detailed_error_code = 0;

    rc = statvfs(path, &buffer);

    if (rc != 0) {
        *detailed_error_code = errno;
        return FAIL_STAT_FILE;
    }

    *total_free_bytes = (uint64_t)buffer.f_bsize * (uint64_t)buffer.f_bavail;
    *total_size_bytes = (uint64_t)buffer.f_bsize * (uint64_t)buffer.f_blocks;

    return SUCCESS;
}

EXPORT int32_t
rvn_get_path_disk_stats(const char * path, struct IO_STATS* io_stats, int32_t* detailed_error_code)
{
    struct stat stats;
	*detailed_error_code = 0;
    int minorValue, majorValue;
    char dfile[MAX_PF_NAME];

    /* stat() returns 0 on successful operation,
    otherwise returns -1 if unable to get file properties.*/
    if (stat(path, &stats) != 0)
        goto error;

    majorValue = major(stats.st_dev);
    minorValue = minor(stats.st_dev);

    /* Read stats for device */
    snprintf(dfile, sizeof(dfile), "%s/%s/%d:%d/%s", SLASH_SYS, __DEV_BLOCK, majorValue, minorValue, S_STAT);
    dfile[sizeof(dfile) - 1] = '\0';

    if(_read_sysfs_file_stat(dfile, io_stats) == -1){
        return FAIL_UNKNOWN_STAT_FILE_FORMAT;
    }

    return SUCCESS;

error:
    *detailed_error_code = errno;
    return FAIL;
}
