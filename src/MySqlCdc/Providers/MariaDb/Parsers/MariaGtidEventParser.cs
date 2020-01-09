using MySqlCdc.Events;
using MySqlCdc.Parsers;
using MySqlCdc.Protocol;

namespace MySqlCdc.Providers.MariaDb
{
    public class MariaGtidEventParser : IEventParser
    {
        public IBinlogEvent ParseEvent(EventHeader header, ref PacketReader reader)
        {
            var sequence = reader.ReadLong(8);
            var domainId = reader.ReadLong(4);
            var gtid = $"{domainId}-{header.ServerId}-{sequence}";

            var flags = reader.ReadInt(1);

            return new GtidEvent(header, gtid, flags);
        }
    }
}
