﻿using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using OrcaSql.RawCore.Records;
using OrcaSql.RawCore.Types;

namespace OrcaSql.RawCore.Tests
{
	public class RawColumnParserTests : BaseFixture
	{
		[TestCase(AW2005Path, 118, "9.06.04.26.00", "2006-04-26", TestName = "2005")]
		[TestCase(AW2008Path, 187, "10.00.80404.00", "2008-04-04", TestName = "2008")]
		[TestCase(AW2008R2Path, 184, "10.00.80404.00", "2008-04-04", TestName = "2008R2")]
		[TestCase(AW2012Path, 187, "11.0.2100.60", "2012-03-15", TestName = "2012")]
		public void Parse_BuildVersion(string dbPath, int pageID, string databaseVersion, string versionDate)
		{
			var db = new RawDataFile(dbPath);
			var page = db.GetPage(pageID);
			var record = page.Records.First() as RawPrimaryRecord;

			var result = RawColumnParser.Parse(record, new IRawType[] {
				RawType.TinyInt("SystemInformationID"),
				RawType.NVarchar("Database Version"),
				RawType.DateTime("VersionDate"),
				RawType.DateTime("ModifiedDate")
			});

			Assert.AreEqual(4,  ((Dictionary<string, object>)result).Count);
			Assert.AreEqual(1, result.SystemInformationID);
			Assert.AreEqual(databaseVersion, ((Dictionary<string, object>)result)["Database Version"]);
			Assert.AreEqual(Convert.ToDateTime(versionDate), result.VersionDate);
			Assert.AreEqual(Convert.ToDateTime(versionDate), result.ModifiedDate);
		}

		[TestCase(AW2005Path, 356, TestName = "2005")]
		[TestCase(AW2008Path, 405, TestName = "2008")]
		[TestCase(AW2008R2Path, 197, TestName = "2008R2")]
		[TestCase(AW2012Path, 405, TestName = "2012")]
		public void Parse_Address(string dbPath, int pageID)
		{
			var db = new RawDataFile(dbPath);
			var page = db.GetPage(pageID);
			var record = page.Records.First() as RawPrimaryRecord;

			var result = RawColumnParser.Parse(record, new IRawType[] {
				RawType.Int("AddressID"),
				RawType.NVarchar("AddressLine1"),
				RawType.NVarchar("AddressLine2"),
				RawType.NVarchar("City"),
				RawType.NVarchar("StateProvince"),
				RawType.NVarchar("CountryRegion"),
				RawType.NVarchar("PostalCode"),
				RawType.UniqueIdentifier("rowguid"),
				RawType.DateTime("ModifiedDate")
			});

			Assert.AreEqual(9, ((Dictionary<string, object>)result).Count);
			Assert.AreEqual(9, result.AddressID);
			Assert.AreEqual("8713 Yosemite Ct.", result.AddressLine1);
			Assert.AreEqual(null, result.AddressLine2);
			Assert.AreEqual("Bothell", result.City);
			Assert.AreEqual("Washington", result.StateProvince);
			Assert.AreEqual("United States", result.CountryRegion);
			Assert.AreEqual("98011", result.PostalCode);
			Assert.AreEqual(new Guid("268af621-76d7-4c78-9441-144fd139821a"), result.rowguid);
			Assert.AreEqual(new DateTime(2002, 07, 01), result.ModifiedDate);
		}

		[TestCase(AW2005Path, 408, "2004-10-13 11:15:07.263", TestName = "2005")]
		[TestCase(AW2008Path, 448, "2001-08-01", TestName = "2008")]
		[TestCase(AW2008R2Path, 546, "2001-08-01", TestName = "2008R2")]
		[TestCase(AW2012Path, 448, "2001-08-01", TestName = "2012")]
		public void Parse_Customer(string dbPath, int pageID, string modifiedDate)
		{
			var db = new RawDataFile(dbPath);
			var page = db.GetPage(pageID);
			var record = page.Records.First() as RawPrimaryRecord;

			var result = RawColumnParser.Parse(record, new IRawType[] {
				RawType.Int("CustomerID"),
				RawType.Bit("NameStyle"),
				RawType.NVarchar("Title"),
				RawType.NVarchar("FirstName"),
				RawType.NVarchar("MiddleName"),
				RawType.NVarchar("LastName"),
				RawType.NVarchar("Suffix"),
				RawType.NVarchar("CompanyName"),
				RawType.NVarchar("SalesPerson"),
				RawType.NVarchar("EmailAddress"),
				RawType.NVarchar("Phone"),
				RawType.Varchar("PasswordHash"),
				RawType.Varchar("PasswordSalt"),
				RawType.UniqueIdentifier("rowguid"),
				RawType.DateTime("ModifiedDate")
			});

			Assert.AreEqual(15, ((Dictionary<string, object>)result).Count);
			Assert.AreEqual(1, result.CustomerID);
			Assert.AreEqual(false, result.NameStyle);
			Assert.AreEqual("Mr.", result.Title);
			Assert.AreEqual("Orlando", result.FirstName);
			Assert.AreEqual("N.", result.MiddleName);
			Assert.AreEqual("Gee", result.LastName);
			Assert.AreEqual(null, result.Suffix);
			Assert.AreEqual("A Bike Store", result.CompanyName);
			Assert.AreEqual(@"adventure-works\pamela0", result.SalesPerson);
			Assert.AreEqual("orlando0@adventure-works.com", result.EmailAddress);
			Assert.AreEqual("245-555-0173", result.Phone);
			Assert.AreEqual("L/Rlwxzp4w7RWmEgXX+/A7cXaePEPcp+KwQhl2fJL7w=", result.PasswordHash);
			Assert.AreEqual("1KjXYs4=", result.PasswordSalt);
			Assert.AreEqual(new Guid("3f5ae95e-b87d-4aed-95b4-c3797afcb74f"), result.rowguid);
			Assert.AreEqual(Convert.ToDateTime(modifiedDate), result.ModifiedDate);
		}

		[TestCase(AW2005Path, 90, 1, 832, "314f2574-1f75-457f-9bd1-74d1ce53daa5", "2001-08-01", TestName = "2005")]
		[TestCase(AW2008Path, 178, 29485, 1086, "16765338-dbe4-4421-b5e9-3836b9278e63", "2003-09-01", TestName = "2008")]
		[TestCase(AW2008R2Path, 109, 29485, 1086, "16765338-dbe4-4421-b5e9-3836b9278e63", "2003-09-01", TestName = "2008R2")]
		[TestCase(AW2012Path, 178, 29485, 1086, "16765338-dbe4-4421-b5e9-3836b9278e63", "2003-09-01", TestName = "2012")]
		public void Parse_CustomerAddress(string dbPath, int pageID, int customerID, int addressID, string rowguid, string modifiedDate)
		{
			var db = new RawDataFile(dbPath);
			var page = db.GetPage(pageID);
			var record = page.Records.First() as RawPrimaryRecord;

			var result = RawColumnParser.Parse(record, new IRawType[] {
				RawType.Int("CustomerID"),
				RawType.Int("AddressID"),
				RawType.NVarchar("AddressType"),
				RawType.UniqueIdentifier("rowguid"),
				RawType.DateTime("ModifiedDate")
			});

			Assert.AreEqual(5,  ((Dictionary<string, object>)result).Count);
			Assert.AreEqual(customerID, result.CustomerID);
			Assert.AreEqual(addressID, result.AddressID);
			Assert.AreEqual("Main Office", result.AddressType);
			Assert.AreEqual(new Guid(rowguid), result.rowguid);
			Assert.AreEqual(Convert.ToDateTime(modifiedDate), result.ModifiedDate);
		}

		[TestCase(AW2005Path, 448, TestName = "2005")]
		[TestCase(AW2008Path, 520, TestName = "2008")]
		[TestCase(AW2008R2Path, 517, TestName = "2008R2")]
		[TestCase(AW2012Path, 520, TestName = "2012")]
		public void Parse_Product(string dbPath, int pageID)
		{
			var db = new RawDataFile(dbPath);
			var page = db.GetPage(pageID);
			var record = page.Records.First() as RawPrimaryRecord;

			var result = RawColumnParser.Parse(record, new IRawType[] {
				RawType.Int("ProductID"),
				RawType.NVarchar("Name"),
				RawType.NVarchar("ProductNumber"),
				RawType.NVarchar("Color"),
				RawType.Money("StandardCost"),
				RawType.Money("ListPrice"),
				RawType.NVarchar("Size"),
				RawType.Decimal("Weight", 8, 2),
				RawType.Int("ProductCategoryID"),
				RawType.Int("ProductModelID"),
				RawType.DateTime("SellStartDate"),
				RawType.DateTime("SellEndDate"),
				RawType.DateTime("DiscontinuedDate"),
				RawType.VarBinary("ThumbNailPhoto"),
				RawType.NVarchar("ThumbnailPhotoFileName"),
				RawType.UniqueIdentifier("rowguid"),
				RawType.DateTime("ModifiedDate")
			});

			Assert.AreEqual(17,  ((Dictionary<string, object>)result).Count);
			Assert.AreEqual(680, result.ProductID);
			Assert.AreEqual("HL Road Frame - Black, 58", result.Name);
			Assert.AreEqual("FR-R92B-58", result.ProductNumber);
			Assert.AreEqual("Black", result.Color);
			Assert.AreEqual(1059.31, result.StandardCost);
			Assert.AreEqual(1431.50, result.ListPrice);
			Assert.AreEqual("58", result.Size);
			Assert.AreEqual(1016.04, result.Weight);
			Assert.AreEqual(18, result.ProductCategoryID);
			Assert.AreEqual(6, result.ProductModelID);
			Assert.AreEqual(Convert.ToDateTime("1998-06-01"), result.SellStartDate);
			Assert.AreEqual(null, result.SellEndDate);
			Assert.AreEqual(null, result.DiscontinuedDate);
			Assert.AreEqual(1077, result.ThumbNailPhoto.Length);
			Assert.AreEqual("no_image_available_small.gif", result.ThumbnailPhotoFileName);
			Assert.AreEqual(new Guid("43dd68d6-14a4-461f-9069-55309d90ea7e"), result.rowguid);
			Assert.AreEqual(Convert.ToDateTime("2004-03-11 10:01:36.827"), result.ModifiedDate);
		}

		[TestCase(AW2005Path, 93, TestName = "2005")]
		[TestCase(AW2008Path, 186, TestName = "2008")]
		[TestCase(AW2008R2Path, 212, TestName = "2008R2")]
		[TestCase(AW2012Path, 186, TestName = "2012")]
		public void Parse_ProductCategory(string dbPath, int pageID)
		{
			var db = new RawDataFile(dbPath);
			var page = db.GetPage(pageID);
			var record = page.Records.First() as RawPrimaryRecord;

			var result = RawColumnParser.Parse(record, new IRawType[] {
				RawType.Int("ProductCategoryID"),
				RawType.Int("ParentProductCategoryID"),
				RawType.NVarchar("Name"),
				RawType.UniqueIdentifier("rowguid"),
				RawType.DateTime("ModifiedDate")
			});

			Assert.AreEqual(5,  ((Dictionary<string, object>)result).Count);
			Assert.AreEqual(1, result.ProductCategoryID);
			Assert.AreEqual(null, result.ParentProductCategoryID);
			Assert.AreEqual("Bikes", result.Name);
			Assert.AreEqual(new Guid("cfbda25c-df71-47a7-b81b-64ee161aa37c"), result.rowguid);
			Assert.AreEqual(Convert.ToDateTime("1998-06-01"), result.ModifiedDate);
		}

		[TestCase(AW2005Path, 592, TestName = "2005")]
		[TestCase(AW2008Path, 216, TestName = "2008")]
		[TestCase(AW2008R2Path, 410, TestName = "2008R2")]
		[TestCase(AW2012Path, 216, TestName = "2012")]
		public void Parse_ProductDescription(string dbPath, int pageID)
		{
			var db = new RawDataFile(dbPath);
			var page = db.GetPage(pageID);
			var record = page.Records.First() as RawPrimaryRecord;

			var result = RawColumnParser.Parse(record, new IRawType[] {
				RawType.Int("ProductDescriptionID"),
				RawType.NVarchar("Description"),
				RawType.UniqueIdentifier("rowguid"),
				RawType.DateTime("ModifiedDate")
			});

			Assert.AreEqual(4,  ((Dictionary<string, object>)result).Count);
			Assert.AreEqual(3, result.ProductDescriptionID);
			Assert.AreEqual("Chromoly steel.", result.Description);
			Assert.AreEqual(new Guid("301eed3a-1a82-4855-99cb-2afe8290d641"), result.rowguid);
			Assert.AreEqual(Convert.ToDateTime("2003-06-01"), result.ModifiedDate);
		}

		[TestCase(AW2005Path, 632, TestName = "2005")]
		[TestCase(AW2008Path, 272, TestName = "2008")]
		[TestCase(AW2008R2Path, 386, TestName = "2008R2")]
		[TestCase(AW2012Path, 272, TestName = "2012")]
		public void Parse_ProductModel(string dbPath, int pageID)
		{
			var db = new RawDataFile(dbPath);
			var page = db.GetPage(pageID);
			var record = page.Records.First() as RawPrimaryRecord;

			var result = RawColumnParser.Parse(record, new IRawType[] {
				RawType.Int("ProductModelID"),
				RawType.NVarchar("Name"),
				RawType.Xml("CatalogDescription"),
				RawType.UniqueIdentifier("rowguid"),
				RawType.DateTime("ModifiedDate")
			});

			Assert.AreEqual(5,  ((Dictionary<string, object>)result).Count);
			Assert.AreEqual(1, result.ProductModelID);
			Assert.AreEqual("Classic Vest", result.Name);
			Assert.AreEqual(null, result.CatalogDescription);
			Assert.AreEqual(new Guid("29321d47-1e4c-4aac-887c-19634328c25e"), result.rowguid);
			Assert.AreEqual(Convert.ToDateTime("2003-06-01"), result.ModifiedDate);
		}

		[TestCase(AW2005Path, 79, "2fc8f1dd-098d-4126-a0d8-0d112b118142", TestName = "2005")]
		[TestCase(AW2008Path, 362, "9fcbccbf-56cc-48e5-9bf9-42fc99af0968", TestName = "2008")]
		[TestCase(AW2008R2Path, 120, "4d00b649-027a-4f99-a380-f22a46ec8638", TestName = "2008R2")]
		[TestCase(AW2012Path, 362, "9fcbccbf-56cc-48e5-9bf9-42fc99af0968", TestName = "2012")]
		public void Parse_ProductModelProductDescription(string dbPath, int pageID, string rowguid)
		{
			var db = new RawDataFile(dbPath);
			var page = db.GetPage(pageID);
			var record = page.Records.First() as RawPrimaryRecord;

			var result = RawColumnParser.Parse(record, new IRawType[] {
				RawType.Int("ProductModelID"),
				RawType.Int("ProductDescriptionID"),
				RawType.NChar("Culture", 6),
				RawType.UniqueIdentifier("rowguid"),
				RawType.DateTime("ModifiedDate")
			});

			Assert.AreEqual(5,  ((Dictionary<string, object>)result).Count);
			Assert.AreEqual(1, result.ProductModelID);
			Assert.AreEqual(1199, result.ProductDescriptionID);
			Assert.AreEqual("en".PadRight(6, ' '), result.Culture);
			Assert.AreEqual(new Guid(rowguid), result.rowguid);
			Assert.AreEqual(Convert.ToDateTime("2003-06-01"), result.ModifiedDate);
		}

		[TestCase(AW2005Path, 337, TestName = "2005")]
		[TestCase(AW2008Path, 386, TestName = "2008")]
		[TestCase(AW2008R2Path, 175, TestName = "2008R2")]
		[TestCase(AW2012Path, 386, TestName = "2012")]
		public void Parse_SalesOrderDetail(string dbPath, int pageID)
		{
			var db = new RawDataFile(dbPath);
			var page = db.GetPage(pageID);
			var record = page.Records.First() as RawPrimaryRecord;

			var result = RawColumnParser.Parse(record, new IRawType[] {
				RawType.Int("SalesOrderID"),
				RawType.Int("SalesOrderDetailID"),
				RawType.SmallInt("OrderQty"),
				RawType.Int("ProductID"),
				RawType.Money("UnitPrice"),
				RawType.Money("UnitPriceDiscount"),
				RawType.UniqueIdentifier("rowguid"),
				RawType.DateTime("ModifiedDate")
			});

			Assert.AreEqual(8,  ((Dictionary<string, object>)result).Count);
			Assert.AreEqual(71774, result.SalesOrderID);
			Assert.AreEqual(110562, result.SalesOrderDetailID);
			Assert.AreEqual(1, result.OrderQty);
			Assert.AreEqual(836, result.ProductID);
			Assert.AreEqual(356.898, result.UnitPrice);
			Assert.AreEqual(0.00, result.UnitPriceDiscount);
			Assert.AreEqual(new Guid("e3a1994c-7a68-4ce8-96a3-77fdd3bbd730"), result.rowguid);
			Assert.AreEqual(Convert.ToDateTime("2004-06-01"), result.ModifiedDate);
		}

		[TestCase(AW2005Path, 316, 609, TestName = "2005")]
		[TestCase(AW2008Path, 393, 29847, TestName = "2008")]
		[TestCase(AW2008R2Path, 221, 29847, TestName = "2008R2")]
		[TestCase(AW2012Path, 393, 29847, TestName = "2012")]
		public void Parse_SalesOrderHeader(string dbPath, int pageID, int customerID)
		{
			var db = new RawDataFile(dbPath);
			var page = db.GetPage(pageID);
			var record = page.Records.First() as RawPrimaryRecord;

			var result = RawColumnParser.Parse(record, new IRawType[] {
				RawType.Int("SalesOrderID"),
				RawType.TinyInt("RevisionNumber"),
				RawType.DateTime("OrderDate"),
				RawType.DateTime("DueDate"),
				RawType.DateTime("ShipDate"),
				RawType.TinyInt("Status"),
				RawType.Bit("OnlineOrderFlag"),
				RawType.NVarchar("PurchaseOrderNumber"),
				RawType.NVarchar("AccountNumber"),
				RawType.Int("CustomerID"),
				RawType.Int("ShipToAddressID"),
				RawType.Int("BillToAddressID"),
				RawType.NVarchar("ShipMethod"),
				RawType.Varchar("CreditCardApprovalCode"),
				RawType.Money("SubTotal"),
				RawType.Money("TaxAmt"),
				RawType.Money("Freight"),
				RawType.NVarchar("Comment"),
				RawType.UniqueIdentifier("rowguid"),
				RawType.DateTime("ModifiedDate")
			});

			Assert.AreEqual(20,  ((Dictionary<string, object>)result).Count);
			Assert.AreEqual(71774, result.SalesOrderID);
			Assert.AreEqual(1, result.RevisionNumber);
			Assert.AreEqual(Convert.ToDateTime("2004-06-01"), result.OrderDate);
			Assert.AreEqual(Convert.ToDateTime("2004-06-13"), result.DueDate);
			Assert.AreEqual(Convert.ToDateTime("2004-06-08"), result.ShipDate);
			Assert.AreEqual(5, result.Status);
			Assert.AreEqual(false, result.OnlineOrderFlag);
			Assert.AreEqual("PO348186287", result.PurchaseOrderNumber);
			Assert.AreEqual("10-4020-000609", result.AccountNumber);
			Assert.AreEqual(customerID, result.CustomerID);
			Assert.AreEqual(1092, result.ShipToAddressID);
			Assert.AreEqual(1092, result.BillToAddressID);
			Assert.AreEqual("CARGO TRANSPORT 5", result.ShipMethod);
			Assert.AreEqual(null, result.CreditCardApprovalCode);
			Assert.AreEqual(880.3484, result.SubTotal);
			Assert.AreEqual(70.4279, result.TaxAmt);
			Assert.AreEqual(22.0087, result.Freight);
			Assert.AreEqual(null, result.Comment);
			Assert.AreEqual(new Guid("89e42cdc-8506-48a2-b89b-eb3e64e3554e"), result.rowguid);
			Assert.AreEqual(Convert.ToDateTime("2004-06-08"), result.ModifiedDate);
		}
	}
}