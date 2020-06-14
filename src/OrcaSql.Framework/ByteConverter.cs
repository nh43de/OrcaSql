﻿using System;

namespace OrcaSql.Framework
{
	/// <summary>
	/// Generic endian & offset aware byte converter pendant to BitConverter. Work in progress.
	/// </summary>
	public static class ByteConverter
	{
		public static short ToInt16(byte[] input)
		{
			return ToInt16(input, 0);
		}

		public static short ToInt16(byte[] input, int index)
		{
			return ToInt16(input, index, Endian.Little, Offset.Zero, false);
		}

		public static unsafe short ToInt16(byte[] input, int index, Endian endian, Offset offset, bool autoPad)
		{
			if (index >= input.Length)
				throw new ArgumentOutOfRangeException("index");

			// Check there's either enough input bytes, or we're allowed to pad
			if (input.Length - index < 2 && !autoPad)
				throw new ArgumentException("Not enough bytes.");

			switch (input.Length - index)
			{
				case 1:
					if (offset == Offset.Zero)
						return input[index];
					else
						return (short)(-128 + input[index]);

				default:
					if (endian == Endian.Little)
					{
						fixed (byte* ptr = &input[index])
						{
							if (offset == Offset.Zero)
								return *(short*)ptr;
							
							return (short)(-32768 + *(short*)ptr);
						}
					}
					else
					{
						if (offset == Offset.Zero)
							return (short)(input[index] << 8);
						
						return (short)(-32768 + (short)(input[index] << 8 | input[index + 1]));
					}
			}
		}
	}
}