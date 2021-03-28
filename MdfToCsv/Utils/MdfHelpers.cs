using System;
using System.Linq;
using DataPowerTools.Extensions;
using OrcaSql.Core.Engine;

namespace DataToolChain
{
    public class MdfHelpers
    {
        private readonly string[] files;

        public MdfHelpers(string[] files)
        {
            this.files = files;

        }

        public void Upload(IProgress<string> progress, string outputFile, string table, string index)
        {
            var d = new Database(files);

            var i = 0;

            if (string.IsNullOrWhiteSpace(index))
            {

                var ss = new DataScanner(d);

                

                //var rr = scanner.ScanIndex("MarketData", "PK__MarketDa__3214EC0757C4568C").Take(10).ToArray();
                var rrr = ss.ScanTable("MarketData", null, false).Select(p =>
                {
                    var r = new MarketData()
                    {
                        Ask = (double) p["Ask"],
                        BaseVolume = (double) p["BaseVolume"],
                        Bid = (double) p["Bid"],
                        Exchange = p["Exchange"]?.ToString(),
                        NormalizedTicker = p["NormalizedTicker"]?.ToString(),
                        QuoteVolume = (double) p["QuoteVolume"],
                        RecordDateTimeUtc = (DateTime) p["RecordDateTimeUtc"],
                        Ticker = p["Ticker"]?.ToString()
                    };
                    
                    i++;

                    if(i % 4096 == 0)
                        progress.Report(@$"Output {i} rows");

                    return r;
                });

                rrr.WriteCsv(outputFile);
                
                progress.Report(@$"Finished: output total of {i} rows");
            }
            else
            {
                //not yet supported
                //var scanner = new IndexScanner(d);
                var my = d.Dmvs.Indexes.Where(p => p.Name == index).ToArray();

                //var rr = scanner.ScanIndex("MarketData", "PK__MarketDa__3214EC0757C4568C").Take(10).ToArray();
            }

        }
        

    }
}