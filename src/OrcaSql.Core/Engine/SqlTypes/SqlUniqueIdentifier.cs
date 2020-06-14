using System;
using OrcaSql.Core.MetaData.DMVs;

namespace OrcaSql.Core.Engine.SqlTypes
{
	public class SqlUniqueIdentifier : SqlTypeBase
	{
		public SqlUniqueIdentifier(CompressionContext compression)
			: base(compression)
		{ }

		public override bool IsVariableLength
		{
			get { return false; }
		}

		public override short? FixedLength
		{
			get { return 16; }
		}

		public override object GetValue(byte[] value)
		{
			// Uniqueidentifier is always 16 bytes
			if (value.Length != 16)
				throw new ArgumentException("Invalid value length: " + value.Length);

			return new Guid(value);
		}

        public override object GetDefaultValue(SysDefaultConstraint columnConstraint)
        {
            return Guid.TryParse(columnConstraint.Definition.Trim('(', ')'), out var parsedResult) ? parsedResult : (object)null;
        }
    }
}