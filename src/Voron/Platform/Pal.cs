using System;
using System.IO;
using System.Runtime.InteropServices;
using static Voron.Platform.PalDefinitions;

namespace Voron.Platform
{
    public static unsafe class Pal
    {
        public static SystemInformation SysInfo;

        static Pal()
        {
            var toFilename = LIBRVNPAL;
            string fromFilename;
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                if (RuntimeInformation.ProcessArchitecture != Architecture.Arm &&
                    RuntimeInformation.ProcessArchitecture != Architecture.Arm64)
                {
                    fromFilename = Environment.Is64BitProcess ? $"{toFilename}.linux.x64.so" : $"{toFilename}.linux.x86.so";
                    toFilename += ".so";
                }
                else
                {
                    fromFilename = Environment.Is64BitProcess ? $"{toFilename}.arm.64.so" : $"{toFilename}.arm.32.so";
                    toFilename += ".so";
                }
            }
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
            {
                fromFilename = Environment.Is64BitProcess ? $"{toFilename}.mac.x64.dylib" : $"{toFilename}.mac.x86.dylib";
                // in mac we are not : `toFilename += ".so";` as DllImport doesn't assume .so nor .dylib by default
            }
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                fromFilename = Environment.Is64BitProcess ? $"{toFilename}.win.x64.dll" : $"{toFilename}.win.x86.dll";
                toFilename += ".dll";
            }
            else
            {
                throw new NotSupportedException("Not supported platform - no Linux/OSX/Windows is detected ");
            }

            try
            {
                if (File.Exists(toFilename) == false)
                    File.Move(fromFilename, toFilename);
            }
            catch (IOException e)
            {
                throw new IOException(
                    $"Cannot copy {fromFilename} to {toFilename}, make sure appropriate {toFilename} to your platform architecture exists in Raven.Server executable folder",
                    e);
            }

            if (rvn_get_system_information(out SysInfo, out var errorCode) != 0)
                PalHelper.ThrowLastError(errorCode, "Cannot get system information");
        }

        private const string LIBRVNPAL = "librvnpal";

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern Int32 rvn_write_header(
            string filename,
            void* header,
            Int32 size,
            out Int32 errorCode);

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern Int32 rvn_get_error_string(
            Int32 errorCode,
            void* sb,
            Int32 capacity,
            out Int32 specialErrnoCodes);

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern Int32 rvn_create_and_mmap64_file(
            string filename,
            Int64 initialFileSize,
            PalFlags.MmapOptions flags,
            out void* handle,
            out void* baseAddress,
            out Int64 actualFileSize,
            out Int32 errorCode);

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern Int32 rvn_prefetch_virtual_memory(
            void *virtualAddress,
            Int64 length,
            out Int32 errorCode);

        [DllImport(LIBRVNPAL, SetLastError = true)]
        private static extern Int32 rvn_get_system_information(
            out SystemInformation systemInformation,
            out Int32 errorCode);

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern Int32 rvn_memory_sync(
            void *address,
            Int64 size,
            out Int32 errorCode);

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern Int32 rvn_dispose_handle(
            string filepath,
            void* handle,
            PalFlags.FileCloseFlags closeFlag,
            out Int32 errorCode);

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern Int32 rvn_unmap(
            void* address,
            Int64 size,
            PalFlags.FileCloseFlags closeFlag,
            out Int32 errorCode);

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern Int32 rvn_prefetch_ranges(
            PrefetchRanges* list,
            Int32 count,
            out Int32 errorCode);

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern Int32 rvn_protect_range(
            void* start,
            Int64 size,
            PalFlags.ProtectRange protection,
            out Int32 errorCode);

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern Int32 rvn_allocate_more_space(
            string fileNameFullPath,
            Int64 newLengthAfterAdjustment,
            void* handle,
            PalFlags.MmapOptions mmapOptions,
            out void* newAddress,
            out Int32 errorCode);

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern Int32 rvn_open_journal(
            string fileName,
            Int32 mode,
            Int64 requiredSize,
            out IntPtr handle,
            out Int64 actualSize,
            out Int32 errorCode);

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern Int32 rvn_close_journal(
            IntPtr handle,
            out Int32 errorCode
        );


        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern Int32 rvn_write_journal(
            IntPtr handle,
            IntPtr buffer,
            UInt64 size,
            Int64 offset,
            out Int32 errorCode
        );

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern Int32 rvn_read_journal(
            string fileName,
            ref IntPtr handle,
            byte* buffer,
            UInt64 requiredSize,
            Int64 offset,
            out UInt64 actualSize,
            out Int32 errorCode
        );

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern Int32 rvn_truncate_journal(
            string fileName,
            IntPtr handle,
            UInt64 size,
            out Int32 errorCode
            );
    }
}
