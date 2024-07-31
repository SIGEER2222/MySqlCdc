using MySqlCdc.Sample;

await BinlogReaderExample.Start(mariadb: false);
BinlogClientExampleRx.Start();
Console.ReadLine();