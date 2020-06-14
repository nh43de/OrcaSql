﻿using System.Linq;
using System.Text;
using NUnit.Framework;
using OrcaSql.Framework;
using OrcaSql.RawCore.Records;
using OrcaSql.RawCore.Utilities.SQL2012;

namespace OrcaSql.RawCore.Tests.Records
{
	public class RawPrimaryRecordTests : BaseFixture
	{
		[TestCase(AW2005Path, 408, 0x80, "Mr.", TestName = "2005")]
		[TestCase(AW2008Path, 448, 0x00, "Mr.", TestName = "2008")]
		[TestCase(AW2008R2Path, 417, 0x00, "Ms.", TestName = "2008R2")]
		[TestCase(AW2012Path, 448, 0x00, "Mr.", TestName = "2012")]
		public void Parse(string dbPath, int pageID, byte secondNullBitmapByte, string title)
		{
			var db = new RawDataFile(dbPath);
			var page = db.GetPage(pageID);
			var record = (RawPrimaryRecord)page.Records.First();

			Assert.AreEqual(true, record.HasNullBitmap);
			Assert.AreEqual(true, record.HasVariableLengthColumns);
			Assert.AreEqual(false, record.HasVersioningInformation);
			Assert.AreEqual(false, record.IsGhostForwardedRecord);
			Assert.AreEqual(0x30, record.RawStatusByteA);
			Assert.AreEqual(0x00, record.RawStatusByteB);
			Assert.AreEqual(false, record.Version);
			Assert.AreEqual(29, record.FixedLengthData.Count);
			Assert.AreEqual(15, record.NullBitmapColumnCount);
			Assert.AreEqual(new[] { 0x40, secondNullBitmapByte }, record.NullBitmapRawBytes.ToArray());
			Assert.AreEqual(11, record.NumberOfVariableLengthOffsetArrayEntries);
			Assert.AreEqual(11, record.VariableLengthOffsetValues.Count());
			Assert.AreEqual(6, record.VariableLengthOffsetValues.ToArray()[0].Count);
			Assert.AreEqual(title, Encoding.Unicode.GetString(record.VariableLengthOffsetValues.ToArray()[0].ToArray()));
		}

		/// <summary>
		/// This record has no variable length offset section, yet it has a non-null variable length value
		/// </summary>
		[Test]
		public void Regression_EmptyVariableLengthValuePerceivedAsNull()
		{
			var rawBytes = TestHelper.GetBytesFromByteString(@"
				10002d00 d637b35f 01000000 0000e7e7 0000001e
				00000008 d0003400 02000000 00000000 00000000
				00000000 00100000 80
			");

			var record = new RawPrimaryRecord(new ArrayDelimiter<byte>(rawBytes));
			var row = RawColumnParser.Parse(record, SQL2012Syscolpars.Schema);

			Assert.IsNotNull(row.name);
		}
	}
}