using System.Diagnostics;
using MySqlCdc.Events;
using MySqlCdc.Providers.MariaDb;
using MySqlCdc.Providers.MySql;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace MySqlCdc.Sample;

class BinlogReaderExample
{
    internal static async Task Start(bool mariadb)
    {
        using (Stream stream = File.OpenRead(@"D:\桌面\MySqlCdc\samples\MySqlCdc.Sample\mysql-bin.000001"))
        {
            EventDeserializer deserializer = mariadb
                ? new MariaDbEventDeserializer()
                : new MySqlEventDeserializer();

            var reader = new BinlogReader(deserializer, stream);

            await foreach (var (header, binlogEvent) in reader.ReadEvents())
            {
                if (binlogEvent is TableMapEvent tableMap)
                {
                    Debug.WriteLine(new string('-', 100));
                    System.Console.WriteLine();
                    await PrintEventAsync(binlogEvent);
                }
                else if (binlogEvent is WriteRowsEvent writeRows)
                {
                    Debug.WriteLine(new string('-', 100));
                    System.Console.WriteLine();
                    await PrintEventAsync(binlogEvent);
                }
                else if (binlogEvent is UpdateRowsEvent updateRows)
                {
                    Debug.WriteLine(new string('-', 100));
                    System.Console.WriteLine(); await PrintEventAsync(binlogEvent);
                }
                else if (binlogEvent is DeleteRowsEvent deleteRows)
                {
                    Debug.WriteLine(new string('-', 100));
                    System.Console.WriteLine(); await PrintEventAsync(binlogEvent);
                }
                else await PrintEventAsync(binlogEvent);
            }
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
}