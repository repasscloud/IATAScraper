using System.Text;
using HtmlAgilityPack;

namespace IATAScraper;

class Program
{
    static async Task Main(string[] args)
    {
        var basePath = "https://en.wikipedia.org/wiki/List_of_airline_codes_";
        var csvPath = "airline_codes_all.csv";
        var outputDir = Path.Combine(Directory.GetCurrentDirectory(), "airline_vectors");
        var baseUrl = "https://images.trvl-media.com/media/content/expus/graphics/static_content/fusion/v0.1b/images/airlines/vector/s/";

        // Create output directory first
        if (!Directory.Exists(outputDir)) Directory.CreateDirectory(outputDir);

        var suffixes = new List<string> { "0%E2%80%939" }; // Encoded 0–9
        suffixes.AddRange(Enumerable.Range('A', 26).Select(i => ((char)i).ToString())); // A–Z

        var sb = new StringBuilder();
        bool headerWritten = false;

        foreach (var suffix in suffixes)
        {
            var url = $"{basePath}({suffix})";
            Console.WriteLine($"Fetching: {url}");

            var web = new HtmlWeb();
            HtmlDocument doc;
            try
            {
                doc = web.Load(url);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to load {url}: {ex.Message}");
                continue;
            }

            var table = doc.DocumentNode.SelectSingleNode("//table[contains(@class, 'wikitable')]");
            if (table == null)
            {
                Console.WriteLine("No table found on this page.");
                continue;
            }

            var rows = table.SelectNodes(".//tr");
            if (rows == null || rows.Count == 0)
            {
                Console.WriteLine("No rows found.");
                continue;
            }

            // Header
            if (!headerWritten)
            {
                var headerCells = rows[0].SelectNodes("th|td");
                if (headerCells != null)
                {
                    var header = string.Join(",", headerCells.Select(cell => Quote(cell.InnerText.Trim())));
                    sb.AppendLine(header);
                    headerWritten = true;
                }
            }

            // Data rows
            foreach (var row in rows.Skip(1))
            {
                var cells = row.SelectNodes("td");
                if (cells == null) continue;

                var values = cells.Select(cell => Quote(cell.InnerText.Trim()));
                sb.AppendLine(string.Join(",", values));
            }
        }

        // Write final file
        File.WriteAllText(csvPath, sb.ToString(), Encoding.UTF8);
        Console.WriteLine($"✅ Done! Combined CSV saved to: {csvPath}");

        
        Console.WriteLine("🔍 Checking and downloading airline SVG logos...");

        // Read all lines
        var lines = File.ReadAllLines(csvPath);

        // Skip if no rows
        if (lines.Length <= 1)
        {
            Console.WriteLine("No data rows found.");
            return;
        }

        // Parse header to find IATA index
        var headers = lines[0].Split(',');
        int iataIndex = Array.FindIndex(headers, h => h.Replace("\"", "").Trim().ToUpper() == "IATA");

        if (iataIndex == -1)
        {
            Console.WriteLine("IATA column not found.");
            return;
        }

        // Setup HTTP client
        using var httpClient = new HttpClient();

        // Loop over data rows
        foreach (var line in lines.Skip(1))
        {
            var cols = SplitCsvLine(line);
            if (iataIndex >= cols.Count) continue;

            var iata = cols[iataIndex].Trim().Trim('"');
            if (string.IsNullOrWhiteSpace(iata)) continue;

            var svgUrl = $"{baseUrl}{iata}_sq.svg";
            var localFile = Path.Combine(outputDir, $"{iata}.svg");

            try
            {
                using var response = await httpClient.GetAsync(svgUrl);
                if (response.IsSuccessStatusCode)
                {
                    var bytes = await response.Content.ReadAsByteArrayAsync();
                    await File.WriteAllBytesAsync(localFile, bytes);
                    Console.WriteLine($"✅ Downloaded {iata}.svg");
                }
                else
                {
                    using var response2 = await httpClient.GetAsync("https://raw.githubusercontent.com/googlefonts/noto-emoji/main/svg/emoji_u2708.svg");
                    if (response2.IsSuccessStatusCode)
                    {
                        Console.WriteLine($"⛔ {iata}: Not found (HTTP {(int)response.StatusCode})");
                        var bytes = await response2.Content.ReadAsByteArrayAsync();
                        await File.WriteAllBytesAsync(localFile, bytes);
                        Console.WriteLine($"✅ Downloaded {iata}.svg as u2708 emoji instead.");
                    }
                    else
                    {
                        Console.WriteLine($"⛔ {iata}: Not found (HTTP {(int)response2.StatusCode})");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"⚠️  {iata}: Error - {ex.Message}");
            }
        }

        Console.WriteLine("🏁 Done!");
    }

    // CSV escape helper
    static string Quote(string input)
    {
        return "\"" + input.Replace("\"", "\"\"") + "\"";
    }

    // --- CSV parser (basic, handles quoted strings) ---
    static List<string> SplitCsvLine(string line)
    {
        var values = new List<string>();
        var sb = new StringBuilder();
        bool inQuotes = false;

        foreach (char c in line)
        {
            if (c == '"' && inQuotes)
            {
                inQuotes = false;
            }
            else if (c == '"' && !inQuotes)
            {
                inQuotes = true;
            }
            else if (c == ',' && !inQuotes)
            {
                values.Add(sb.ToString());
                sb.Clear();
            }
            else
            {
                sb.Append(c);
            }
        }

        values.Add(sb.ToString());
        return values;
    }
}
