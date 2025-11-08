use anyhow::{Context, Result};
use csv::{ReaderBuilder, WriterBuilder};
use futures::{stream, StreamExt};
use reqwest::Client;
use scraper::{Html, Selector};
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

const BASE_WIKI: &str = "https://en.wikipedia.org/wiki/List_of_airline_codes_";
const CSV_PATH: &str = "airline_codes_all.csv";
const OUT_DIR: &str = "airline_bitmaps";
const UA: &str = "Mozilla/5.0 (compatible; iata-scraper/0.3; rust)";

#[tokio::main]
async fn main() -> Result<()> {
    let base_logo = std::env::args()
        .nth(1)
        .expect("usage: iata-scraper <base_logo_url/>\nexample: iata-scraper https://cdn.example.com/logos/");
    let base_logo = ensure_trailing_slash(&base_logo);

    let client = Client::builder().user_agent(UA).build().context("http client")?;
    fs::create_dir_all(OUT_DIR).context("mkdir output")?;

    // "0–9" plus A..Z
    let mut suffixes = vec!["0%E2%80%939".to_string()];
    suffixes.extend(('A'..='Z').map(|c| c.to_string()));

    let (header, rows) = scrape_all(&client, &suffixes).await?;
    write_csv_normalized(&header, &rows)?;

    download_logos(&client, CSV_PATH, OUT_DIR, &base_logo).await?;
    println!("Done.");
    Ok(())
}

fn ensure_trailing_slash(s: &str) -> String {
    if s.ends_with('/') { s.to_string() } else { format!("{s}/") }
}

async fn scrape_all(client: &Client, suffixes: &[String]) -> Result<(Vec<String>, Vec<Vec<String>>)> {
    let mut header: Option<Vec<String>> = None;
    let mut rows_all: Vec<Vec<String>> = Vec::new();

    for s in suffixes {
        let url = format!("{BASE_WIKI}({s})");
        println!("Fetching: {url}");
        match fetch_iata_table(client, &url).await {
            Ok(Some((h, rows))) => {
                if header.is_none() {
                    header = Some(h);
                }
                rows_all.extend(rows);
            }
            Ok(None) => eprintln!("warn: {url}: no wikitable with IATA header"),
            Err(e) => eprintln!("warn: {url}: {e}"),
        }
    }

    let header = header.context("no pages yielded an IATA table")?;
    Ok((header, rows_all))
}

/// Fetch the first wikitable whose header contains "IATA".
async fn fetch_iata_table(client: &Client, url: &str) -> Result<Option<(Vec<String>, Vec<Vec<String>>)>> {
    let body = client
        .get(url)
        .send()
        .await?
        .error_for_status()
        .with_context(|| format!("GET {url}"))?
        .text()
        .await?;

    let doc = Html::parse_document(&body);

    let table_sel = Selector::parse("table.wikitable").unwrap();
    let tr_sel = Selector::parse("tr").unwrap();
    let thtd_sel = Selector::parse("th, td").unwrap();
    let td_sel = Selector::parse("td").unwrap();

    for table in doc.select(&table_sel) {
        let mut rows = table.select(&tr_sel);

        let header_cells = match rows.next() {
            Some(h) => h.select(&thtd_sel).map(extract_text).collect::<Vec<_>>(),
            None => continue,
        };
        if !header_has_iata(&header_cells) {
            continue;
        }

        let mut data: Vec<Vec<String>> = Vec::new();
        for tr in rows {
            let row = tr.select(&td_sel).map(extract_text).collect::<Vec<_>>();
            if !row.is_empty() {
                data.push(row);
            }
        }
        return Ok(Some((header_cells, data)));
    }

    Ok(None)
}

fn header_has_iata(header: &[String]) -> bool {
    header.iter().any(|h| h.trim().eq_ignore_ascii_case("IATA"))
}

fn extract_text(node: scraper::ElementRef<'_>) -> String {
    let raw: String = node.text().collect::<Vec<_>>().join(" ");
    raw.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Normalize every row to the header width to avoid ragged CSV.
fn write_csv_normalized(header: &[String], rows: &[Vec<String>]) -> Result<()> {
    let mut wtr = WriterBuilder::new().has_headers(true).from_path(CSV_PATH)?;
    let hlen = header.len();

    wtr.write_record(header)?;
    for r in rows {
        if r.len() == hlen {
            wtr.write_record(r)?;
        } else if r.len() > hlen {
            wtr.write_record(r.iter().take(hlen))?;
        } else {
            let mut tmp = Vec::with_capacity(hlen);
            tmp.extend_from_slice(r);
            tmp.resize(hlen, String::new());
            wtr.write_record(&tmp)?;
        }
    }
    wtr.flush()?;
    println!("CSV written: {CSV_PATH} ({} columns)", hlen);
    Ok(())
}

async fn download_logos(client: &Client, csv_path: &str, out_dir: &str, base_logo_url: &str) -> Result<()> {
    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_path(csv_path)?;

    let headers = rdr.headers()?.clone();
    let iata_index = headers
        .iter()
        .position(|h| h.trim().eq_ignore_ascii_case("IATA"))
        .context("IATA column not found")?;

    let mut unique: HashSet<String> = HashSet::new();
    for rec in rdr.records() {
        let rec = rec?;
        if let Some(val) = rec.get(iata_index) {
            let code = val.trim().to_uppercase();
            if code.len() == 2 && code.chars().all(|c| c.is_ascii_alphanumeric()) {
                unique.insert(code);
            }
        }
    }

    fs::create_dir_all(out_dir).ok();

    let tasks = unique.into_iter().map(|iata| {
        let out_dir = out_dir.to_string();
        let base = base_logo_url.to_string();
        async move {
            // Adjust extension if needed. Here: PNG.
            let url = format!("{base}{iata}.png");
            let path = Path::new(&out_dir).join(format!("{iata}.png"));

            match try_download(client, &url, &path).await {
                Ok(true) => {
                    println!("ok   {iata}");
                    Ok::<(), anyhow::Error>(())
                }
                Ok(false) => {
                    println!("skip {iata} (not found)");
                    Ok::<(), anyhow::Error>(())
                }
                Err(e) => {
                    eprintln!("err  {iata}: {e}");
                    Ok::<(), anyhow::Error>(())
                }
            }
        }
    });

    stream::iter(tasks).buffer_unordered(12).collect::<Vec<_>>().await;
    Ok(())
}

async fn try_download(client: &Client, url: &str, dst: &PathBuf) -> Result<bool> {
    let resp = client.get(url).send().await?;
    let status = resp.status();

    if status.as_u16() == 404 || status.as_u16() == 410 {
        return Ok(false); // skip
    }
    if !status.is_success() {
        anyhow::bail!("http {}", status);
    }

    let bytes = resp.bytes().await?;
    fs::write(dst, &bytes)?;
    Ok(true)
}
