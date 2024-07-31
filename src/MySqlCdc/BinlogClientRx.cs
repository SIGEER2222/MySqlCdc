
using System.Reactive.Linq;
using MySqlCdc.Constants;
using MySqlCdc.Events;
using MySqlCdc.Network;
using MySqlCdc.Packets;

namespace MySqlCdc;
/// <summary>
/// MySql replication client streaming binlog events in real-time.
/// </summary>
public class BinlogClientRx
{
    private readonly ReplicaOptions _options = new();

    private IGtid? _gtid;
    private bool _transaction;

    /// <summary>
    /// Creates a new <see cref="BinlogClientRx"/>.
    /// </summary>
    /// <param name="configureOptions">The configure callback</param>
    public BinlogClientRx(Action<ReplicaOptions> configureOptions)
    {
        configureOptions(_options);

        if (_options.SslMode == SslMode.RequireVerifyCa || _options.SslMode == SslMode.RequireVerifyFull)
            throw new NotSupportedException($"{nameof(SslMode.RequireVerifyCa)} and {nameof(SslMode.RequireVerifyFull)} ssl modes are not supported");
    }

    /// <summary>
    /// Gets current replication state.
    /// </summary>
    public BinlogOptions State => _options.Binlog;

    /// <summary>
    /// Replicates binlog events from the server using Rx
    /// </summary>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>IObservable of binlog events</returns>
    public IObservable<(EventHeader, IBinlogEvent)> Replicate(CancellationToken cancellationToken = default)
    {
        return Observable.Create<(EventHeader, IBinlogEvent)>(async observer =>
        {
            var (connection, databaseProvider) = await new Connector(_options).ConnectAsync(cancellationToken);

            // Clear on reconnect
            _gtid = null;
            _transaction = false;

            var configurator = new Configurator(_options, connection, databaseProvider);
            await configurator.AdjustStartingPosition(cancellationToken);
            await configurator.SetMasterHeartbeat(cancellationToken);
            await configurator.SetMasterBinlogChecksum(cancellationToken);
            await databaseProvider.DumpBinlogAsync(connection, _options, cancellationToken);

            var eventStreamReader = new EventStreamReader(databaseProvider.Deserializer);
            var channel = new EventStreamChannel(eventStreamReader, connection.Stream);
            var timeout = _options.HeartbeatInterval.Add(TimeSpan.FromMilliseconds(TimeoutConstants.Delta));

            await foreach (var packet in channel.ReadPacketAsync(timeout, cancellationToken).WithCancellation(cancellationToken))
            {
                if (packet is HeaderWithEvent binlogEvent)
                {
                    observer.OnNext((binlogEvent.Header, binlogEvent.Event));

                    // Commit replication state if there is no exception.
                    UpdateGtidPosition(binlogEvent.Event);
                    UpdateBinlogPosition(binlogEvent.Header, binlogEvent.Event);
                }
                else if (packet is EndOfFilePacket && !_options.Blocking)
                {
                    observer.OnCompleted();
                    break;
                }
                else if (packet is ErrorPacket error)
                {
                    observer.OnError(new InvalidOperationException($"Event stream error. {error.ToString()}"));
                    break;
                }
                else
                {
                    observer.OnError(new InvalidOperationException("Event stream unexpected error."));
                    break;
                }
            }

            return () => { /* Cleanup code if needed */ };
        });
    }

    private void UpdateGtidPosition(IBinlogEvent binlogEvent)
    {
        if (_options.Binlog.StartingStrategy != StartingStrategy.FromGtid)
            return;

        if (binlogEvent is GtidEvent gtidEvent)
        {
            _gtid = gtidEvent.Gtid;
        }
        else if (binlogEvent is XidEvent)
        {
            CommitGtid();
        }
        else if (binlogEvent is QueryEvent queryEvent)
        {
            if (string.IsNullOrWhiteSpace(queryEvent.SqlStatement))
                return;

            if (queryEvent.SqlStatement == "BEGIN")
            {
                _transaction = true;
            }
            else if (queryEvent.SqlStatement == "COMMIT" || queryEvent.SqlStatement == "ROLLBACK")
            {
                CommitGtid();
            }
            else if (!_transaction)
            {
                // Auto-commit query like DDL
                CommitGtid();
            }
        }
    }

    private void CommitGtid()
    {
        _transaction = false;
        if (_gtid != null)
            _options.Binlog.GtidState!.AddGtid(_gtid);
    }

    private void UpdateBinlogPosition(EventHeader header, IBinlogEvent binlogEvent)
    {
        // Rows event depends on preceding TableMapEvent & we change the position
        // after we read them atomically to prevent missing mapping on reconnect.
        // Figure out something better as TableMapEvent can be followed by several row events.
        if (binlogEvent is TableMapEvent)
            return;

        if (binlogEvent is RotateEvent rotateEvent)
        {
            _options.Binlog.Filename = rotateEvent.BinlogFilename;
            _options.Binlog.Position = rotateEvent.BinlogPosition;
        }
        else if (header.NextEventPosition > 0)
        {
            _options.Binlog.Position = header.NextEventPosition;
        }
    }
}
