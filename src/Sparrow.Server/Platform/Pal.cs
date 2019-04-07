using System;
using System.IO;
using System.Runtime.InteropServices;

namespace Sparrow.Server.Platform
{
    public static unsafe class Pal
    {
        public static PalDefinitions.SystemInformation SysInfo;
        public const int PAL_VER = 42004; // Should match auto generated rc from rvn_get_pal_ver() @ src/rvngetpalver.c

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
                var toTime = DateTime.MinValue.Ticks;
                if (File.Exists(toFilename))
                    toTime = new FileInfo(toFilename).CreationTime.Ticks;

                if (File.Exists(fromFilename) &&
                    new FileInfo(fromFilename).CreationTime.Ticks > toTime)
                    File.Copy(fromFilename, toFilename, overwrite: true);
            }
            catch (IOException e)
            {
                throw new IOException(
                    $"Cannot copy {fromFilename} to {toFilename}, make sure appropriate {toFilename} to your platform architecture exists in Raven.Server executable folder",
                    e);
            }

            PalFlags.FailCodes rc = PalFlags.FailCodes.None;
            int errorCode;
            try
            {
                var palver = rvn_get_pal_ver();
                if (palver != 0 && palver != PAL_VER)
                {
                    throw new IncorrectDllException(
                        $"{LIBRVNPAL} version '{palver}' mismatches this RavenDB instance version (set to '{PAL_VER}'). Either use correct {fromFilename}, or a new one returning zero in 'rvn_get_pal_ver()'");
                }

                rc = rvn_get_system_information(out SysInfo, out errorCode);
            }
            catch (IncorrectDllException)
            {
                throw;
            }
            catch (Exception ex)
            {
                var ErrString =
                    "'Microsoft Visual C++ 2015 Redistributable Package' (or newer). It can be downloaded from https://support.microsoft.com/en-us/help/2977003/the-latest-supported-visual-c-downloads";
                throw new IncorrectDllException(
                    $"{LIBRVNPAL} version might be invalid or not usable on current platform. Initialization error could also be caused by missing {ErrString}",
                    ex);
            }

            if (rc != PalFlags.FailCodes.Success)
                PalHelper.ThrowLastError(rc, errorCode, "Cannot get system information");
        }

        private const string LIBRVNPAL = "librvnpal";

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern PalFlags.FailCodes rvn_write_header(
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
        public static extern PalFlags.FailCodes rvn_create_and_mmap64_file(
            string filename,
            Int64 initialFileSize,
            PalFlags.MmapOptions flags,
            out SafeMmapHandle handle,
            out void* baseAddress,
            out Int64 actualFileSize,
            out Int32 errorCode);

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern PalFlags.FailCodes rvn_prefetch_virtual_memory(
            void *virtualAddress,
            Int64 length,
            out Int32 errorCode);

        [DllImport(LIBRVNPAL, SetLastError = true)]
        private static extern PalFlags.FailCodes rvn_get_system_information(
            out PalDefinitions.SystemInformation systemInformation,
            out Int32 errorCode);

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern PalFlags.FailCodes rvn_memory_sync(
            void *address,
            Int64 size,
            out Int32 errorCode);

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern PalFlags.FailCodes rvn_mmap_dispose_handle(
            IntPtr handle,
            out Int32 errorCode);

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern PalFlags.FailCodes rvn_unmap(
            PalFlags.MmapOptions flags,
            void* address,
            Int64 size,
            out Int32 errorCode);

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern PalFlags.FailCodes rvn_prefetch_ranges(
            PalDefinitions.PrefetchRanges* list,
            Int32 count,
            out Int32 errorCode);

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern PalFlags.FailCodes rvn_protect_range(
            void* start,
            Int64 size,
            PalFlags.ProtectRange protection,
            out Int32 errorCode);

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern PalFlags.FailCodes rvn_allocate_more_space(
            Int64 newLengthAfterAdjustment,
            SafeMmapHandle handle,
            out void* newAddress,
            out Int32 errorCode);

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern PalFlags.FailCodes rvn_open_journal_for_writes(
            string fileName,
            PalFlags.JournalMode mode,
            Int64 requiredSize,
            PalFlags.DurabilityMode supportDurability,
            out SafeJournalHandle handle,
            out Int64 actualSize,
            out Int32 errorCode);

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern PalFlags.FailCodes rvn_close_journal(
            IntPtr handle,
            out Int32 errorCode
        );


        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern PalFlags.FailCodes rvn_write_journal(
            SafeJournalHandle handle,
            void* buffer,
            Int64 size,
            Int64 offset,
            out Int32 errorCode
        );

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern PalFlags.FailCodes rvn_read_journal(
            SafeJournalHandle handle,
            void* buffer,
            Int64 requiredSize,
            Int64 offset,
            out Int64 actualSize,
            out Int32 errorCode
        );

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern PalFlags.FailCodes rvn_truncate_journal(
            SafeJournalHandle handle,
            Int64 size,
            out Int32 errorCode
            );

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern PalFlags.FailCodes rvn_open_journal_for_reads(
            string fileNameFullPath,
            out SafeJournalHandle  handle,
            out Int32 errorCode
        );

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern Int32 rvn_discard_virtual_memory(
            void* address,
            Int64 size,
            out Int32 errorCode);

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern PalFlags.FailCodes rvn_test_storage_durability(
            string tempFilename,
            out Int32 errorCode);

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern Int32 rvn_get_pal_ver();

        [DllImport(LIBRVNPAL, SetLastError = true)]
        public static extern Int64 rvn_get_current_thread_id();
    }
}
