﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace OrcaSql.Framework
{
	public static class Corruptor
	{
		/// <summary>
		/// Corrups an MDF file by overwriting random pages with garbage
		/// </summary>
		/// <param name="path">The path of the file to corrupt</param>
		/// <param name="corruptionPercentage">To percentage of the pages to corrupt. 0.1 = 10%</param>
		/// <returns>A list of the page IDs that were corrupted</returns>
		public static IEnumerable<int> CorruptFileUsingGarbage(string path, double corruptionPercentage, bool onlyBody)
		{
			var rnd = new Random();

			if (corruptionPercentage > 1)
				throw new ArgumentException("Corruption percentage can't be more than 100%");

			if (corruptionPercentage <= 0)
				throw new ArgumentException("Corruption percentage must be positive.");

			using (var file = File.OpenWrite(path))
			{
				int pageCount = (int)(file.Length / 8192);
				int pageCountToCorrupt = (int)(pageCount * corruptionPercentage);

				IEnumerable<int> pageIDsToCorrupt = Enumerable
					.Range(0, pageCount)
					.OrderBy(x => rnd.Next())
					.Take(pageCountToCorrupt)
					.ToList();

				foreach (int pageID in pageIDsToCorrupt)
				{
					byte[] garbage = new byte[8192];
					rnd.NextBytes(garbage);

					if (onlyBody)
					{
						file.Position = pageID * 8192 + 96;
						file.Write(garbage, 0, 8060);
					}
					else
					{
						file.Position = pageID * 8192;
						file.Write(garbage, 0, 8192);
					}
				}

				return pageIDsToCorrupt;
			}
		}

		/// <summary>
		/// Corrups an MDF file by overwriting pages with all zeros in random locations
		/// </summary>
		/// <param name="path">The path of the file to corrupt</param>
		/// <param name="corruptionPercentage">To percentage of the pages to corrupt. 0.1 = 10%</param>
		/// <returns>A list of the page IDs that were corrupted</returns>
		public static IEnumerable<int> CorruptFileUsingZeros(string path, double corruptionPercentage)
		{
			if (corruptionPercentage > 1)
				throw new ArgumentException("Corruption percentage can't be more than 100%");

			if (corruptionPercentage <= 0)
				throw new ArgumentException("Corruption percentage must be positive.");

			using (var file = File.OpenWrite(path))
			{
				var rnd = new Random();
				byte[] zeros = new byte[8192];

				int pageCount = (int)(file.Length / 8192);
				int pageCountToCorrupt = (int)(pageCount * corruptionPercentage);
				
				IEnumerable<int> pageIDsToCorrupt = Enumerable
					.Range(0, pageCount)
					.OrderBy(x => rnd.Next())
					.Take(pageCountToCorrupt)
					.ToList();

				foreach (int pageID in pageIDsToCorrupt)
				{
					file.Position = pageID * 8192;
					file.Write(zeros, 0, 8192);
				}

				return pageIDsToCorrupt;
			}
		}

		/// <summary>
		/// Overwrites a single page with all zeros
		/// </summary>
		/// <param name="path">The MDF file to corrupt</param>
		/// <param name="pageID">The ID of the page to zero out</param>
		public static void CorruptPageUsingZeros(string path, int pageID)
		{
			CorruptFileUsingZeros(path, 1, pageID, pageID);
		}

		/// <summary>
		/// Corrups an MDF file by overwriting pages with all zeros in random locations
		/// </summary>
		/// <param name="path">The path of the file to corrupt</param>
		/// <param name="pagesToCorrupt">The number of pages to corrupt</param>
		/// <param name="startPageID">The inclusive lower bound page ID that may be corrupted</param>
		/// <param name="endPageID">The inclusive upper bound page ID that may be corrupted</param>
		/// <returns>A list of the page IDs that were corrupted</returns>
		public static IEnumerable<int> CorruptFileUsingZeros(string path, int pagesToCorrupt, int startPageID, int endPageID)
		{
			if (startPageID > endPageID)
				throw new ArgumentException("startPageID must be lower than or equal to endPageID.");

			if (pagesToCorrupt > (endPageID - startPageID + 1))
				throw new ArgumentException("Can't corrupt more pages than are available between startPageID and endPageID");

			using (var file = File.OpenWrite(path))
			{
				long numPagesInFile = file.Length / 8192;
				endPageID = (int)Math.Min(endPageID, numPagesInFile);

				var rnd = new Random();
				byte[] zeros = new byte[8192];

				IEnumerable<int> pageIDsToCorrupt = Enumerable
					.Range(startPageID, (endPageID - startPageID + 1))
					.OrderBy(x => rnd.Next())
					.Take(pagesToCorrupt)
					.ToList();

				foreach (int pageID in pageIDsToCorrupt)
				{
					file.Position = pageID * 8192;
					file.Write(zeros, 0, 8192);
				}

				return pageIDsToCorrupt;
			}
		}
	}
}