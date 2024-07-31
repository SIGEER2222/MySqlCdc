using MySqlCdc.Constants;
using MySqlCdc.Events;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace MySqlCdc.Sample;

class BinlogClientExample
{
    internal static async Task Start()
    {
        var client = new BinlogClient(options =>
        {
            options.Port = 3306;
            options.Username = "root";
            options.Password = "ZZQ159357";
            options.SslMode = SslMode.Disabled;
            options.HeartbeatInterval = TimeSpan.FromSeconds(30);
            options.Blocking = true;

            // // Start replication from MariaDB GTID
            // options.Binlog = BinlogOptions.FromGtid(GtidList.Parse("0-1-270"));

            // // Start replication from MySQL GTID
            // var gtidSet = "d4c17f0c-4f11-11ea-93e3-325d3e1cd1c8:1-107, f442510a-2881-11ea-b1dd-27916133dbb2:1-7";
            // options.Binlog = BinlogOptions.FromGtid(GtidSet.Parse(gtidSet));

            // // Start replication from the position
            options.Binlog = BinlogOptions.FromPosition("MySQL.000006", 195);

            // Start replication from last master position.
            // Useful when you are only interested in new changes.
            options.Binlog = BinlogOptions.FromEnd();

            // // Start replication from first event of first available master binlog.
            // // Note that binlog files by default have expiration time and deleted.
            // options.Binlog = BinlogOptions.FromStart();
        });

        await foreach (var (header, binlogEvent) in client.Replicate())
        {
            var state = client.State;
            System.Console.WriteLine(binlogEvent.GetType());

            if (binlogEvent is TableMapEvent tableMap)
            {
                System.Console.WriteLine($"{tableMap.TableId} {tableMap.DatabaseName}.{tableMap.TableName}");
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
                System.Console.WriteLine(formatDescriptionEvent);
            }
            else if (binlogEvent is MySqlCdc.Events.QueryEvent rotateEvent)
            {
                System.Console.WriteLine(rotateEvent);
            }
            else
            {
                System.Console.WriteLine("Unhandled event");
                await PrintEventAsync(binlogEvent);
            }
            System.Console.WriteLine(new string('-', 100));
            _ = 1;
        }
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
        Console.WriteLine($"{writeRows.Rows.Count} rows were written");
        await PrintEventAsync(writeRows);

        foreach (var row in writeRows.Rows)
        {
            var cells = row.Cells.Where(cell => cell is object).Select(cell => cell?.ToString());
            System.Console.WriteLine(cells.Aggregate((a, b) => $"{a}, {b}"));
        }
    }

    private static async Task HandleUpdateRowsEvent(UpdateRowsEvent updatedRows)
    {
        Console.WriteLine($"{updatedRows.Rows.Count} rows were updated");
        await PrintEventAsync(updatedRows);

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
    }

    private static async Task HandleDeleteRowsEvent(DeleteRowsEvent deleteRows)
    {
        Console.WriteLine($"{deleteRows.Rows.Count} rows were deleted");
        await PrintEventAsync(deleteRows);
        foreach (var row in deleteRows.Rows)
        {
            var cells = row.Cells.Where(cell => cell is object).Select(cell => cell?.ToString());
            System.Console.WriteLine(cells.Aggregate((a, b) => $"{a}, {b}"));
        }
        System.Console.WriteLine(new string('-', 100));
    }
}