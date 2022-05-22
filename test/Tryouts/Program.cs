using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using FastTests.Blittable;
using FastTests.Client;
using SlowTests.Client.TimeSeries.Replication;
using SlowTests.Issues;
using SlowTests.MailingList;
using SlowTests.Rolling;
using SlowTests.Server.Documents.ETL.Raven;
using Sparrow.Server.LowMemory;
using Sparrow.Server.Platform.Posix;
using StressTests.Issues;
using Tests.Infrastructure;

namespace Tryouts
{
    public static class Program
    {
        static Program()
        {
            XunitLogging.RedirectStreams = false;
        }

        public static async Task Main(string[] args)
        {
            // var b = CheckPageFileOnHdd.LoadLibrary("libc");
            // var b1 = CheckPageFileOnHdd.GetProcAddress(b, "stat");
            // var b2 = CheckPageFileOnHdd.GetProcAddress(b, "stat64");
            
            const string path = "/home/haludi-work/work/ravendb/RavenDB-17473/coreutils";
            if(Syscall.statx(0, path, 0, 0x00000fffU,out var buf) != 0)
                throw new InvalidOperationException($"Could not get statx of {path} - {Marshal.GetLastWin32Error()}");

            var statPath = $"/sys/dev/block/{buf.stx_dev_major}:{buf.stx_dev_minor}/stat";
            // using (var file = File.OpenRead(statPath))

            await using (var reader = File.OpenRead(statPath))
            {
                var buffer = new byte[4098];
                var k = await reader.ReadAsync(buffer, 0, buffer.Length);
                NewMethod(k, buffer);
            }
        }

        private static void NewMethod(int k, byte[] buffer)
        {
            Span<char> charBuf = stackalloc char[19];

            for (int j = 0; j < k; j++)
            {
                var c1 = (char)buffer[j];
                if (char.IsWhiteSpace(c1))
                    continue;

                var index = 0;
                while (j < k)
                {
                    charBuf[index++] = c1;
                    c1 = (char)buffer[++j];
                    
                    if (char.IsWhiteSpace(c1))
                        break;
                }

                if (long.TryParse(charBuf[..index], out var value) == false)
                    throw new Exception();

                Console.WriteLine(value);
            }

            var str2 = "";
            for (int j = 0; j < k; j++)
            {
                var c1 = (char)buffer[j];
                str2 += c1;
            }

            Console.WriteLine(str2);
        }
    }
}
