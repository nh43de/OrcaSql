﻿using System.Data.SqlClient;
using System.Linq;
using NUnit.Framework;
using OrcaSql.Core.Engine;
using OrcaSql.Core.Tests.SqlServerVersion;

namespace OrcaSql.Core.Tests.Features.DataTypes
{
	public class DecimalTestsBase : SqlServerSystemTestBase
	{
		[SqlServerTest]
		public void DecimalTest(DatabaseVersion version)
		{
			RunDatabaseTest(version, db =>
			{
				var scanner = new DataScanner(db);
				var rows = scanner.ScanTable("DecimalTest").ToList();

				Assert.AreEqual(12345m, rows[0].Field<decimal>("A"));
				Assert.AreEqual(39201.230m, rows[0].Field<decimal>("B"));
				Assert.AreEqual(-4892384.382090m, rows[0].Field<decimal>("C"));
				Assert.AreEqual(1328783742987.29m, rows[0].Field<decimal>("D"));
				Assert.AreEqual(2940382040198493029.235m, rows[0].Field<decimal>("E"));
			});
		}

		protected override void RunSetupQueries(SqlConnection conn, DatabaseVersion version)
		{
			RunQuery(@"
				CREATE TABLE DecimalTest
				(
					A decimal (5, 0),
					B decimal (9, 3),
					C decimal (14, 6),
					D decimal (17, 2),
					E decimal (22, 3)
				)
				INSERT INTO DecimalTest VALUES (12345, 39201.230, -4892384.38209, 1328783742987.29, 2940382040198493029.23456)", conn);
		}
	}
}