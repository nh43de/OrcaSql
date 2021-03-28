using System;
using System.ComponentModel.DataAnnotations;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using DataPowerTools.Extensions;
using NUnit.Framework;
using OrcaSql.Core.Engine;
using OrcaSql.Core.Tests.SqlServerVersion;
using OrcaSql.Framework;

namespace OrcaSql.Core.Tests.Features.BaseTables
{
	public class SmokeTestsBase : SqlServerSystemTestBase
	{
        [Test]
        public void GetTestDb()
        {
            var d = new Database(new[] { @"F:\mssql\DATA\TestDateTime.mdf",
                @"F:\mssql\DATA\TestDateTime_log.ldf" });
            //var rr = d.Dmvs.Indexes.Select(p => p.Name).ToArray();

            var my = d.Dmvs.Indexes.Where(p => p.Name == "PK__TestTabl__3BB02EDE1841AE52").ToArray();

            var ss = new DataScanner(d);

            var rrr = ss.ScanTable("TestTable").Take(10).ToArray();

            var scanner = new IndexScanner(d);

            var rr = scanner.ScanIndex("TestTable", "PK__TestTabl__3BB02EDE1841AE52").Take(10).ToArray();
            
        }

        [SqlServerTest]
		public void Sysobjvalues(DatabaseVersion version)
		{
			RunDatabaseTest(version, db => {
				var row = db.BaseTables.SysObjValues.First();
				TestHelper.GetAllPublicProperties(row);
			});
		}

		[SqlServerTest]
		public void Sysowners(DatabaseVersion version)
		{
			RunDatabaseTest(version, db => {
				var row = db.BaseTables.SysOwners.First();
				TestHelper.GetAllPublicProperties(row);
			});
		}

		protected override void RunSetupQueries(SqlConnection conn, DatabaseVersion version)
		{
			RunQuery(@"
				CREATE TABLE TestA (A int, PRIMARY KEY CLUSTERED (A));
				CREATE TABLE TestB (B int, FOREIGN KEY (B) REFERENCES TestA(A));
			", conn);

			RunQuery(@"
				CREATE PROCEDURE TestC AS SELECT 1 AS A;
			", conn);
		}
	}


    public class MarketData
    {
        public DateTime RecordDateTimeUtc { get; set; }

        [MaxLength(28)]
        public string Exchange { get; set; }

        [MaxLength(24)]
        public string Ticker { get; set; }

        [MaxLength(24)]
        public string NormalizedTicker { get; set; }


        public double BaseVolume { get; set; }
        public double QuoteVolume { get; set; }

        public double Ask { get; set; }
        public double Bid { get; set; }

        //   exchange, price, ask, bid, spread, volume]
    }
}
