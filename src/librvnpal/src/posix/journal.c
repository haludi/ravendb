#if defined(__unix__) && !defined(__APPLE__)

#define __USE_FILE_OFFSET64

#include <stdint.h>
#include <rvn.h>
#include <status_codes.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

int32_t
pwrite_with_retries(...)
{
    
}

int32_t
ensure_file_size(int fd, int64_t file_size, uint32_t* error_code)
{
    int rc = EINVAL;

#ifndef __APPLE__

    rc = posix_fallocate64(fd, 0, file_size);

#endif

    switch (rc)
    {
    case EINVAL:
    case EFBIG:
        // can occure on >4GB allocation on fs such as ntfs-3g, W95 FAT32, etc.
        // fallocate is not supported, we'll use lseek instead
        char zero = 0;
        rc = pwrite_with_retries(fd, &zero, 1, file_size - 1);


        break;

    default:
        break;
    }

}

int32_t
open_journal(char* file_name, int32_t flags, int64_t file_size, void** handle, uint32_t* error_code)
{
    int rc;
    struct stat fs;
    int flags = O_DSYNC | O_DIRECT;
    if (flags & JOURNAL_MODE_DANGER)
        flags = 0;

    if (sizeof(void*) == 4) // 32 bits
        flags |= O_LARGEFILE;

    int fd = open(file_name, flags | O_WRONLY | O_CREAT | S_IWUSR | S_IRUSR);
    if (fd == -1)
    {
        rc = FAIL_OPEN_FILE;
        goto error_cleanup;
    }

    *handle = fd;

#if __APPLE__
    // mac doesn't support O_DIRECT, we fcntl instead:
    if (!fcntl(fd, F_NOCACHE, 1))
    {
        rc = FAIL_SYNC_FILE;
        goto error_cleanup;
    }
#endif

    if (-1 == fstat(fd, &fs))
    {
        rc = FAIL_SEEK_FILE;
        goto error_cleanup;
    }

    if (fs.st_size >= file_size)
        return SUCCSS;

error_cleanup:
    *error_code = errno;
    if (fd != -1)
        close(fd);
    }
}

int32_t
close_journal(void* handle, uint32_t* error_code)
{
    int fd = *handle;
    *error_code = close(fd);

    if (*error_code)
        return FAIL_CLOSE_FILE;
    return SUCCESS;
}

EXPORT int32_t
write_journal(void* handle, char* buffer, uint64_t size, int64_t offset, uint32_t* error_code);

#endif
