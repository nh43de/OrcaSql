using System;
using System.ComponentModel.DataAnnotations;

namespace DataToolChain
{
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