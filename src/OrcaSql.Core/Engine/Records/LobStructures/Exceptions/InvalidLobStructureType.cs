using System;

namespace OrcaSql.Core.Engine.Records.LobStructures.Exceptions
{
	public class InvalidLobStructureType : Exception
	{
		public InvalidLobStructureType(short type)
			: base("Unsupported LOB structure type: " + type)
		{ }
	}
}