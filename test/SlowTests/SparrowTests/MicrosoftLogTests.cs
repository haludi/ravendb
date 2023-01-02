using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.SparrowTests;

public class MicrosoftLogTests : RavenTestBase
{
    public MicrosoftLogTests(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task TestCase()
    {
        var mainLogPath = NewDataPath();
        try
        {
            Directory.Delete(mainLogPath, true);
        }
        catch (Exception e)
        {
        }
        
        var options = new ServerCreationOptions();
        options.CustomSettings = new Dictionary<string, string>
        {
            {"Logs.Microsoft.Disable","false"},
            {"Logs.Path",mainLogPath}
        };
        _ = GetNewServer(options);

        string combine = Path.Combine(mainLogPath, "MicrosoftLogs");
        await AssertWaitForTrueAsync(() => Task.FromResult(Directory.GetFiles(combine).Any()));
    }
}
