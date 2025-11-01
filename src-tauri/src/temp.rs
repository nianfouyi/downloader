use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};
use std::sync::Arc;
use anyhow::{anyhow, bail, Context, Result};
use aes::Aes128;
use cbc::cipher::{block_padding::Pkcs7, BlockDecryptMut, KeyIvInit};
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use url::Url;
use tempfile::Builder as TempDirBuilder;

#[derive(Debug, Parser)]
#[command(name = "m3u8_download", version, about = "Download HLS (m3u8) to MP4 with faststart")]
struct Args {
    /// M3U8 URL or path to text file containing URLs
    url: String,

    /// Title of the output video (output file will be <title>.mp4)
    title: String,

    /// Directory to save output and temporary cache
    #[arg(short = 'd', long = "dir", default_value = ".")]
    save_dir: PathBuf,

    /// Concurrent downloads (default 15)
    #[arg(short = 'c', long = "concurrency", default_value_t = 15)]
    concurrency: usize,

    /// Keep temporary segment files (do not auto-clean)
    #[arg(long = "keep-segments")]
    keep_segments: bool,

    /// Path to ffmpeg (default: search in PATH)
    #[arg(long = "ffmpeg")]
    ffmpeg: Option<PathBuf>,

    /// Optional HTTP Referer header
    #[arg(long = "referer")]
    referer: Option<String>,

    /// Optional HTTP User-Agent header
    #[arg(long = "user-agent")]
    user_agent: Option<String>,

    /// Retry times per segment
    #[arg(long = "retries", default_value_t = 3)]
    retries: u32,
}

#[derive(Clone, Debug)]
struct SegmentKey {
    method: String,
    uri: Url,
    iv: Option<[u8; 16]>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
struct Segment {
    index: usize,
    seq: u64,
    duration: Option<f32>,
    uri: Url,
    key: Option<SegmentKey>,
    discontinuity: bool,
}

#[allow(dead_code)]
#[derive(Debug)]
struct MediaPlaylist {
    media_sequence: u64,
    segments: Vec<Segment>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let start = Instant::now();
    let args = Args::parse();

    let ffmpeg_path = find_ffmpeg(args.ffmpeg.as_deref())
        .context("ffmpeg not found; please install ffmpeg or pass --ffmpeg")?;

    // Ensure save directory exists
    fs::create_dir_all(&args.save_dir).context("create save dir")?;

    // Create a temporary working directory under save_dir for caching segments
    let tempdir = TempDirBuilder::new()
        .prefix("m3u8_")
        .tempdir_in(&args.save_dir)
        .context("create temp working dir under save dir")?;
    let segments_dir = tempdir.path().join("segments");
    fs::create_dir_all(&segments_dir).context("create segments dir")?;

    let client = build_client(args.user_agent.clone(), args.referer.clone())?;

    // Resolve URL input (either direct URL or file path)
    let m3u8_url = resolve_url_input(&args.url)?;

    // Resolve to media playlist (handle master playlists by choosing highest bandwidth)
    let (media_url, media_text) = resolve_media_playlist(&client, m3u8_url.clone()).await?;

    let playlist = parse_media_playlist(&media_text, &media_url)?;
    if playlist.segments.is_empty() {
        bail!("no segments found in playlist");
    }

    // Download segments concurrently
    let total = playlist.segments.len();
    let pb = ProgressBar::new(total as u64);
    pb.set_style(
        ProgressStyle::with_template(
            "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} segments",
        )
        .unwrap()
        .progress_chars("##-"),
    );

    let semaphore = Arc::new(Semaphore::new(args.concurrency.max(1)));
    let key_cache = Arc::new(tokio::sync::Mutex::new(HashMap::<String, Arc<Vec<u8>>>::new()));

    let mut set = JoinSet::new();
    for seg in playlist.segments.clone() {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let client = client.clone();
        let seg_path = segments_dir.join(format!("{:06}.ts", seg.index));
        let key_cache = key_cache.clone();
        let retries = args.retries;
        

        set.spawn(async move {
            let _permit = permit; // keep until task ends
            // download (with retries)
            let mut last_err: Option<anyhow::Error> = None;
            for attempt in 0..=retries {
                match download_and_save_segment(
                    &client,
                    &seg,
                    &seg_path,
                    &key_cache,
                )
                .await
                {
                    Ok(_) => return Ok(seg.index),
                    Err(e) => {
                        last_err = Some(e);
                        let delay = std::time::Duration::from_millis(300 * (attempt as u64 + 1));
                        tokio::time::sleep(delay).await;
                    }
                }
            }
            Err(last_err.unwrap())
        });
    }

    // Drive tasks to completion
    let mut failed: Vec<(usize, String)> = Vec::new();
    while let Some(res) = set.join_next().await {
        match res {
            Ok(Ok(_idx)) => {
                pb.inc(1);
                // optionally: pb.println(format!("Downloaded segment #{idx}"));
            }
            Ok(Err(e)) => {
                pb.println(format!("Segment failed: {e}"));
                failed.push((0, e.to_string()));
            }
            Err(join_err) => {
                pb.println(format!("Task join error: {join_err}"));
            }
        }
    }
    pb.finish_and_clear();

    if !failed.is_empty() {
        bail!("some segments failed to download: {}", failed.len());
    }

    // Create concat list
    let concat_list = tempdir.path().join("segments.txt");
    let mut writer = BufWriter::new(File::create(&concat_list)?);
    for i in 0..total {
        let p = segments_dir.join(format!("{:06}.ts", i));
        let abs = fs::canonicalize(&p).unwrap_or(p);
        let s = abs.to_string_lossy().replace("'", "'\\''");
        writeln!(writer, "file '{}'", s)?;
    }
    writer.flush()?;

    // Merge with ffmpeg to MP4 using faststart for seekability
    let title = sanitize_filename(&args.title);
    let output = args.save_dir.join(format!("{}.mp4", title));
    if output.exists() { fs::remove_file(&output).ok(); }

    let status = Command::new(&ffmpeg_path)
        .args([
            "-hide_banner",
            "-loglevel",
            "error",
            "-stats",
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
        ])
        .arg(&concat_list)
        .args(["-c", "copy", "-bsf:a", "aac_adtstoasc", "-movflags", "+faststart"])
        .arg(&output)
        .status()
        .context("failed to run ffmpeg")?;

    if !status.success() {
        bail!("ffmpeg failed with status: {status}");
    }

    let elapsed = start.elapsed();
    if args.keep_segments {
        // Persist tempdir by preventing automatic deletion
        let kept = tempdir.keep();
        println!(
            "Saved: {}\nSegments kept at: {}\nElapsed: {}",
            output.display(),
            kept.display(),
            fmt_duration(elapsed)
        );
    } else {
        // TempDir will auto clean on drop
        println!(
            "Saved: {}\nElapsed: {}",
            output.display(),
            fmt_duration(elapsed)
        );
    }
    Ok(())
}

fn find_ffmpeg(provided: Option<&Path>) -> Result<PathBuf> {
    if let Some(p) = provided {
        if p.exists() {
            return Ok(p.to_path_buf());
        } else {
            bail!("ffmpeg not found at {}", p.display());
        }
    }
    // Fallback to PATH
    which::which("ffmpeg").map_err(|_| anyhow!("ffmpeg not found in PATH"))
}

fn resolve_url_input(input: &str) -> Result<Url> {
    // Remove potential backslash escapes from shell
    let cleaned_input = input.replace(r"\=", "=")
                             .replace(r"\&", "&")
                             .replace(r"\?", "?");

    // First try to parse as URL
    if let Ok(url) = Url::parse(&cleaned_input) {
        return Ok(url);
    }

    // If not a valid URL, treat as file path
    let path = Path::new(input);
    if !path.exists() {
        bail!("Invalid URL and file does not exist: {}", input);
    }

    // Read first non-empty line from file
    let file = File::open(path).context("open URL file")?;
    let reader = BufReader::new(file);

    for line in reader.lines() {
        let line = line?;
        let trimmed = line.trim();
        if !trimmed.is_empty() && !trimmed.starts_with('#') {
            // Remove backslash escapes that might be in the URL
            let unescaped = trimmed.replace(r"\=", "=")
                                   .replace(r"\&", "&")
                                   .replace(r"\?", "?");
            return Url::parse(&unescaped)
                .with_context(|| format!("parse URL from file: {}", unescaped));
        }
    }

    bail!("No valid URL found in file: {}", input)
}

fn build_client(user_agent: Option<String>, referer: Option<String>) -> Result<reqwest::Client> {
    let mut headers = reqwest::header::HeaderMap::new();
    if let Some(ua) = user_agent {
        headers.insert(
            reqwest::header::USER_AGENT,
            reqwest::header::HeaderValue::from_str(&ua)?,
        );
    } else {
        headers.insert(
            reqwest::header::USER_AGENT,
            reqwest::header::HeaderValue::from_static(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 \
                 (KHTML, like Gecko) Chrome/119.0 Safari/537.36",
            ),
        );
    }
    if let Some(r) = referer {
        headers.insert(
            reqwest::header::REFERER,
            reqwest::header::HeaderValue::from_str(&r)?,
        );
    }

    let client = reqwest::Client::builder()
        .default_headers(headers)
        .redirect(reqwest::redirect::Policy::limited(10))
        .build()?;
    Ok(client)
}

async fn resolve_media_playlist(
    client: &reqwest::Client,
    initial_url: Url,
) -> Result<(Url, String)> {
    let mut url = initial_url.clone();
    let mut depth = 0;
    const MAX_DEPTH: u32 = 10; // Prevent infinite recursion

    loop {
        if depth >= MAX_DEPTH {
            bail!("Too many nested playlists (max depth: {})", MAX_DEPTH);
        }

        println!("Fetching playlist: {}", url);
        let text = http_get_text(client, url.clone()).await?;

        if is_master_playlist(&text) {
            println!("  -> Detected master playlist, selecting highest bandwidth variant...");
            let base = base_url(&url);
            let variant_url = select_highest_bandwidth_variant(&text, &base)?;
            println!("  -> Selected variant: {}", variant_url);
            url = variant_url;
            depth += 1;
        } else {
            println!("  -> Detected media playlist with segments");
            return Ok((url, text));
        }
    }
}

async fn http_get_text(client: &reqwest::Client, url: Url) -> Result<String> {
    let resp = client.get(url.clone()).send().await?;
    let resp = resp.error_for_status()?;
    let bytes = resp.bytes().await?;
    let text = String::from_utf8_lossy(&bytes).to_string();
    Ok(text)
}

fn is_master_playlist(text: &str) -> bool {
    text.lines().any(|l| l.trim_start().starts_with("#EXT-X-STREAM-INF"))
}

fn base_url(url: &Url) -> Url {
    // Strip path to directory by joining with '.'
    // Url::join handles directory semantics
    url.join(".").unwrap_or_else(|_| url.clone())
}

fn select_highest_bandwidth_variant(text: &str, base: &Url) -> Result<Url> {
    let mut best_bw: u64 = 0;
    let mut best_uri: Option<String> = None;
    let mut lines = text.lines().peekable();
    while let Some(line) = lines.next() {
        let line = line.trim();
        if line.starts_with("#EXT-X-STREAM-INF") {
            // Parse attributes like BANDWIDTH=123456,RESOLUTION=...
            let attrs = line.split_once(':').map(|(_, v)| v).unwrap_or("");
            let mut bw: u64 = 0;
            for part in attrs.split(',') {
                let part = part.trim();
                if let Some(v) = part.strip_prefix("BANDWIDTH=") {
                    if let Ok(n) = v.parse::<u64>() {
                        bw = n;
                    }
                }
            }
            // Next non-empty, non-comment line is the URI
            let mut next_uri: Option<String> = None;
            while let Some(nl) = lines.next() {
                let t = nl.trim();
                if t.is_empty() || t.starts_with('#') {
                    continue;
                }
                next_uri = Some(t.to_string());
                break;
            }
            if let Some(u) = next_uri {
                if bw >= best_bw {
                    best_bw = bw;
                    best_uri = Some(u);
                }
            }
        }
    }
    let rel = best_uri.ok_or_else(|| anyhow!("no variant URI found in master playlist"))?;
    let abs = base.join(&rel).context("failed to join variant URI")?;
    Ok(abs)
}

fn parse_media_playlist(text: &str, playlist_url: &Url) -> Result<MediaPlaylist> {
    let mut media_sequence: u64 = 0;
    let mut segments: Vec<Segment> = Vec::new();
    let mut current_key: Option<SegmentKey> = None;
    let mut pending_duration: Option<f32> = None;
    let mut discontinuity_next = false;

    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() { continue; }
        if line.starts_with("#") {
            if let Some(v) = line.strip_prefix("#EXT-X-MEDIA-SEQUENCE:") {
                media_sequence = v.trim().parse().unwrap_or(0);
            } else if let Some(v) = line.strip_prefix("#EXTINF:") {
                // format: <duration>[,<title>]
                let dur = v.split(',').next().unwrap_or("").trim();
                pending_duration = dur.parse::<f32>().ok();
            } else if line.starts_with("#EXT-X-KEY:") {
                // parse attributes METHOD,URI,IV
                let attrs = line.split_once(':').map(|(_, v)| v).unwrap_or("");
                let mut method: Option<String> = None;
                let mut uri: Option<Url> = None;
                let mut iv: Option<[u8;16]> = None;
                for part in attrs.split(',') {
                    let part = part.trim();
                    if let Some(v) = part.strip_prefix("METHOD=") {
                        method = Some(v.trim_matches('"').to_string());
                    } else if let Some(v) = part.strip_prefix("URI=") {
                        let raw = v.trim().trim_matches('"');
                        let joined = base_url(playlist_url).join(raw).context("join key URI")?;
                        uri = Some(joined);
                    } else if let Some(v) = part.strip_prefix("IV=") {
                        let raw = v.trim();
                        let hexstr = raw.strip_prefix("0x").unwrap_or(raw);
                        let bytes = hex::decode(hexstr).context("parse IV hex")?;
                        if bytes.len() != 16 { bail!("IV must be 16 bytes"); }
                        let mut iv_arr = [0u8;16];
                        iv_arr.copy_from_slice(&bytes);
                        iv = Some(iv_arr);
                    }
                }
                if let Some(m) = method {
                    if m.eq_ignore_ascii_case("NONE") {
                        current_key = None;
                    } else if m.eq_ignore_ascii_case("AES-128") {
                        if let Some(u) = uri {
                            current_key = Some(SegmentKey { method: m, uri: u, iv });
                        } else {
                            bail!("AES-128 specified without URI");
                        }
                    } else {
                        bail!("Unsupported encryption method: {m}");
                    }
                }
            } else if line.starts_with("#EXT-X-DISCONTINUITY") {
                discontinuity_next = true;
            }
            continue;
        }
        // Non-comment line is a segment URI
        let uri = base_url(playlist_url).join(line).context("join segment URI")?;
        let index = segments.len();
        let seq = media_sequence + index as u64;
        segments.push(Segment{
            index,
            seq,
            duration: pending_duration.take(),
            uri,
            key: current_key.clone(),
            discontinuity: std::mem::take(&mut discontinuity_next),
        });
    }

    Ok(MediaPlaylist{ media_sequence, segments })
}

async fn download_and_save_segment(
    client: &reqwest::Client,
    seg: &Segment,
    path: &Path,
    key_cache: &tokio::sync::Mutex<HashMap<String, Arc<Vec<u8>>>>,
) -> Result<()> {
    // Fetch bytes
    let mut resp = client
        .get(seg.uri.clone())
        .send()
        .await
        .with_context(|| format!("GET {}", seg.uri))?;
    resp = resp.error_for_status()?;
    let mut bytes = resp.bytes().await?.to_vec();

    // Decrypt if needed
    if let Some(ref keyinfo) = seg.key {
        if keyinfo.method.eq_ignore_ascii_case("AES-128") {
            let key_bytes = {
                let mut cache = key_cache.lock().await;
                if let Some(k) = cache.get(keyinfo.uri.as_str()) {
                    k.clone()
                } else {
                    let kb = download_key(client, &keyinfo.uri).await?;
                    let k = Arc::new(kb);
                    cache.insert(keyinfo.uri.to_string(), k.clone());
                    k
                }
            };
            let iv = match keyinfo.iv {
                Some(iv) => iv,
                None => iv_from_seq(seg.seq),
            };
            bytes = decrypt_aes128_cbc_pkcs7(&mut bytes, &key_bytes, &iv)
                .with_context(|| format!("decrypt segment {}", seg.index))?;
        }
    }

    // Write to file atomically
    let tmp = path.with_extension("part");
    {
        let mut f = BufWriter::new(File::create(&tmp)?);
        f.write_all(&bytes)?;
        f.flush()?;
    }
    fs::rename(tmp, path).context("atomic rename")?;
    Ok(())
}

async fn download_key(client: &reqwest::Client, url: &Url) -> Result<Vec<u8>> {
    let resp = client.get(url.clone()).send().await?;
    let resp = resp.error_for_status()?;
    let key = resp.bytes().await?.to_vec();
    if key.len() != 16 {
        bail!("AES-128 key length must be 16 bytes, got {}", key.len());
    }
    Ok(key)
}

fn decrypt_aes128_cbc_pkcs7(buf: &mut Vec<u8>, key: &[u8], iv: &[u8;16]) -> Result<Vec<u8>> {
    if key.len() != 16 { bail!("AES-128 key must be 16 bytes"); }
    let mut data = buf.clone();
    let dec: cbc::Decryptor<Aes128> = cbc::Decryptor::<Aes128>::new_from_slices(key, iv)
        .map_err(|e| anyhow!("cipher init: {e:?}"))?;
    let out = dec
        .decrypt_padded_mut::<Pkcs7>(&mut data)
        .map_err(|_| anyhow!("AES-128-CBC decrypt failed"))?;
    Ok(out.to_vec())
}

fn iv_from_seq(seq: u64) -> [u8; 16] {
    let mut iv = [0u8; 16];
    iv[8..].copy_from_slice(&seq.to_be_bytes());
    iv
}

fn sanitize_filename(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            '\\' | '/' | ':' | '*' | '?' | '"' | '<' | '>' | '|' | '\0' => out.push('_'),
            ch if ch.is_control() => out.push('_'),
            _ => out.push(ch),
        }
    }
    let out = out.trim();
    let out = out.trim_matches('.');
    if out.is_empty() { "video".to_string() } else { out.to_string() }
}

fn fmt_duration(d: Duration) -> String {
    let secs = d.as_secs();
    let ms = d.subsec_millis();
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    if h > 0 {
        format!("{:02}:{:02}:{:02}.{:03}", h, m, s, ms)
    } else {
        format!("{:02}:{:02}.{:03}", m, s, ms)
    }
}
