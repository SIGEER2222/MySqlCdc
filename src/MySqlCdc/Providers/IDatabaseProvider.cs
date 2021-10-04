using System.Threading;
using System.Threading.Tasks;
using MySqlCdc.Events;
using MySqlCdc.Network;

namespace MySqlCdc.Providers;

internal interface IDatabaseProvider
{
    Task DumpBinlogAsync(Connection channel, ConnectionOptions options, CancellationToken cancellationToken = default);

    EventDeserializer Deserializer { get; }
}