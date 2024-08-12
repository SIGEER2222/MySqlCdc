using MySqlCdc.Constants;
using MySqlCdc.Events;
using Serilog;
using System.Reactive.Linq;

namespace MySqlCdc.Sample;
public static class MomClientExampleRx
{
    static ILogger log = Log.Logger = new LoggerConfiguration()
        .MinimumLevel.Debug()
        .WriteTo.Console()
        .WriteTo.Debug()
        .WriteTo.File("logs/MySqlCdcSample.log", rollingInterval: RollingInterval.Day)
        .CreateLogger();

    static List<string> IgnoreTables = new()
    {
        "MOM_BUSINESSRULE",
    };

    public static void Start()
    {
        var client = new BinlogClientRx(options =>
        {
            options.Port = 3306;
            options.Username = "root";
            options.Password = "ZZQ159357";
            options.SslMode = SslMode.Disabled;
            options.HeartbeatInterval = TimeSpan.FromSeconds(30);
            options.Blocking = true;
            options.Binlog = BinlogOptions.FromEnd();
        });

        var cancellationTokenSource = new CancellationTokenSource();
        var observable = client.Replicate(cancellationTokenSource.Token);
        observable
            .Select(x => x.Item2)
            .OfType<RowsQueryEvent>()
            .Select(x => x.Query)
            .Where(x => !x.ContainsAny(IgnoreTables))
            .Subscribe(query =>
            {
                log.Information(query);
            });
    }
}

public static class StringExtensions
{
    public static bool ContainsAny(this string source, List<string> values)
    {
        if (string.IsNullOrEmpty(source) || values == null || values.Count == 0)
        {
            return false;
        }

        foreach (var value in values)
        {
            if (source.Contains(value))
            {
                return true;
            }
        }

        return false;
    }
}