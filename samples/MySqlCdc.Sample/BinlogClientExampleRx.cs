using MySqlCdc.Constants;
using MySqlCdc.Events;
using MySqlCdc.Providers.MySql;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Serilog;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Reactive.Linq;

namespace MySqlCdc.Sample;
public class BinlogClientExampleRx
{
    private static readonly ConcurrentDictionary<long, string> TableMap = new ConcurrentDictionary<long, string>();
    static ILogger log = Log.Logger = new LoggerConfiguration()
        .MinimumLevel.Debug()
        .WriteTo.Console()
        .WriteTo.Debug()
        .WriteTo.File("logs/MySqlCdcSample.log", rollingInterval: RollingInterval.Day)
        .CreateLogger();
    public static void Start()
    {
        var client = new BinlogClientRx(options =>
        {
            options.Port = 3306;
            options.Username = "root";
            options.Password = "ZZQ159357";

            // options.Port = 31001;
            // options.Username = "mom";
            // options.Password = "mom";
            // options.Hostname = "10.10.10.106";
            options.SslMode = SslMode.Disabled;
            options.HeartbeatInterval = TimeSpan.FromSeconds(30);
            options.Blocking = true;
            options.Binlog = BinlogOptions.FromEnd();
            // options.Binlog = BinlogOptions.FromPosition("MySQL.000006", 1);
        });

        var cancellationTokenSource = new CancellationTokenSource();
        var observable = client.Replicate(cancellationTokenSource.Token);

        observable.Subscribe(
        async x =>
        {
            var (header, binlogEvent) = x;
            Console.WriteLine(header.EventType);
            var state = client.State;

            if (binlogEvent is TableMapEvent tableMap)
            {
                Console.WriteLine($"{tableMap.TableId} {tableMap.DatabaseName}.{tableMap.TableName}");
                TableMap[tableMap.TableId] = $"{tableMap.DatabaseName}.{tableMap.TableName}";
                await HandleTableMapEvent(tableMap);
            }

            else if (binlogEvent is WriteRowsEvent writeRows)
            {
                await HandleWriteRowsEvent(writeRows);
            }
            else if (binlogEvent is UpdateRowsEvent updateRows)
            {
                await HandleUpdateRowsEvent(updateRows);
            }
            else if (binlogEvent is DeleteRowsEvent deleteRows)
            {
                await HandleDeleteRowsEvent(deleteRows);
            }
            else if (binlogEvent is FormatDescriptionEvent formatDescriptionEvent)
            {
                Console.WriteLine(formatDescriptionEvent);
            }
            else if (binlogEvent is QueryEvent rotateEvent)
            {
                Console.WriteLine(rotateEvent);
            }
            else if (binlogEvent is RowsQueryEvent rowsQueryEvent)
            {
                var query = rowsQueryEvent.Query;
                if (query.Contains("MOM_BUSINESSRULE")) return;
                log.Information(rowsQueryEvent.Query);
            }
            else
            {
                Console.WriteLine("Unhandled event");
                await PrintEventAsync(binlogEvent);
            }
            Console.WriteLine(new string('-', 100));
        },
        ex =>
        {
            if (Debugger.IsAttached)
            {
                Debugger.Break();
            }
            Console.Error.WriteLine($"Error: {ex.Message}");
        },
        () => Console.WriteLine("Replication completed.")
        );
    }

    private static async Task PrintEventAsync(IBinlogEvent binlogEvent)
    {
        var json = JsonConvert.SerializeObject(binlogEvent, Formatting.Indented,
        new JsonSerializerSettings()
        {
            Converters = new List<JsonConverter> { new StringEnumConverter() }
        });
        await Console.Out.WriteLineAsync(json);
    }

    private static async Task HandleTableMapEvent(TableMapEvent tableMap)
    {
        Console.WriteLine($"Processing {tableMap.DatabaseName}.{tableMap.TableName}");
        await PrintEventAsync(tableMap);
    }

    private static async Task HandleWriteRowsEvent(WriteRowsEvent writeRows)
    {
        if (TableMap.TryGetValue(writeRows.TableId, out var tableName))
        {
            Console.WriteLine($"Table: {tableName}");
        }
        Console.WriteLine($"{writeRows.Rows.Count} rows were written");
        await PrintEventAsync(writeRows);

        foreach (var row in writeRows.Rows)
        {
            var cells = row.Cells.Where(cell => cell is object).Select(cell => cell?.ToString());
            Console.WriteLine(cells.Aggregate((a, b) => $"{a}, {b}"));
        }
    }

    private static Task HandleUpdateRowsEvent(UpdateRowsEvent updatedRows)
    {
        if (TableMap.TryGetValue(updatedRows.TableId, out var tableName))
        {
            Console.WriteLine($"Table: {tableName}");
        }
        Console.WriteLine($"{updatedRows.Rows.Count} rows were updated");
        // await PrintEventAsync(updatedRows);

        foreach (var row in updatedRows.Rows)
        {
            var rowBeforeUpdate = row.BeforeUpdate;
            var rowAfterUpdate = row.AfterUpdate;

            var diff = new Dictionary<string, (object? OldValue, object? NewValue)>();

            for (int i = 0; i < rowBeforeUpdate.Cells.Count; i++)
            {
                var oldValue = rowBeforeUpdate.Cells[i];
                var newValue = rowAfterUpdate.Cells[i];

                if (!Equals(oldValue, newValue))
                {
                    diff.Add($"Column{i}", (OldValue: oldValue, NewValue: newValue));
                }
            }

            Console.WriteLine("Updated fields:");
            foreach (var kvp in diff)
            {
                Console.WriteLine($"{kvp.Key}: Old Value = {kvp.Value.OldValue}, New Value = {kvp.Value.NewValue}");
            }
        }
        return Task.CompletedTask;
    }

    private static async Task HandleDeleteRowsEvent(DeleteRowsEvent deleteRows)
    {
        if (TableMap.TryGetValue(deleteRows.TableId, out var tableName))
        {
            Console.WriteLine($"Table: {tableName}");
        }
        Console.WriteLine($"{deleteRows.Rows.Count} rows were deleted");
        await PrintEventAsync(deleteRows);
        foreach (var row in deleteRows.Rows)
        {
            var cells = row.Cells.Where(cell => cell is object).Select(cell => cell?.ToString());
            Console.WriteLine(cells.Aggregate((a, b) => $"{a}, {b}"));
        }
        Console.WriteLine(new string('-', 100));
    }
}