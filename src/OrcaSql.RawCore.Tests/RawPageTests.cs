﻿using System.Linq;
using NUnit.Framework;

namespace OrcaSql.RawCore.Tests
{
	public class RawPageTests : BaseFixture
	{
		[TestCase(AW2005Path, 420, TestName = "2005")]
		[TestCase(AW2008Path, 460, TestName = "2008")]
		[TestCase(AW2008R2Path, 520, TestName = "2008R2")]
		[TestCase(AW2012Path, 460, TestName = "2012")]
		public void Parse_Page(string dbPath, int pageID)
		{
			var db = new RawDataFile(dbPath);
			var page = db.GetPage(pageID);

			Assert.AreEqual(pageID, page.PageID);
			Assert.AreEqual(8192, page.RawBytes.Length);
			Assert.AreEqual(25, page.SlotArray.Count());
			Assert.AreEqual(25, page.Records.Count());
		}
	}
}