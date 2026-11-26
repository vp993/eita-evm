//! Streaming block source for Eita over Avalanche C-Chain `block_diffs` (local cache + R2 fallback)
//! and `replay_store` parquet archives.
//!
//! Zero disk writes for R2 objects: missing `block_diffs` parquet objects are fetched directly into
//! memory with background prefetching, merged with local `replay_store` transactions/receipts, and
//! streamed to Eita over stdin/stdout as length-prefixed JSON:
//!   stdin:  `B <block_number>\n` | `H <block_number>\n` | `Q\n`
//!   stdout: `<byte_len>\n<utf8 json bytes>` (or `0\n` if absent)

use arrow::array::{Array as _, BinaryArray, Int64Array, LargeBinaryArray, UInt64Array};
use bytes::Bytes;
use eyre::{eyre, Result, WrapErr};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rusty_s3::{Bucket, Credentials, S3Action as _, UrlStyle};
use serde::Deserialize;
use serde_json::{Map, Value};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

const INDEX_SPAN: u64 = 1000;
const PREFETCH_OBJECTS: usize = 4;
const MAX_HEADER_CACHE_OBJECTS: usize = 6;
const MAX_TX_CACHE_FILES: usize = 4;

#[derive(Deserialize)]
struct IndexBucket {
    blocks: BTreeMap<String, IndexEntry>,
}

#[derive(Deserialize, Clone)]
struct IndexEntry {
    object_key: String,
    #[allow(dead_code)]
    block_hash: String,
}

#[derive(Deserialize)]
struct BlockDiffHeaderOnly {
    block: Value,
}

#[derive(Deserialize)]
struct ReplayBlockWire {
    transactions: Vec<ReplayTxWire>,
}

#[derive(Deserialize)]
struct ReplayTxWire {
    transaction: Value,
    receipt: Value,
}

struct R2Client {
    bucket: Bucket,
    credentials: Credentials,
    http: reqwest::Client,
}

impl R2Client {
    fn from_env(bucket_name: &str) -> Result<Option<Self>> {
        let (Ok(endpoint), Ok(ak), Ok(sk)) = (
            std::env::var("R2_ENDPOINT"),
            std::env::var("R2_ACCESS_KEY"),
            std::env::var("R2_SECRET_KEY"),
        ) else {
            return Ok(None);
        };
        let url: url::Url = endpoint.parse().wrap_err("invalid R2_ENDPOINT")?;
        let bucket = Bucket::new(url, UrlStyle::Path, bucket_name.to_owned(), "auto")
            .map_err(|e| eyre!("invalid R2 bucket: {e}"))?;
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(120))
            .build()?;
        Ok(Some(Self {
            bucket,
            credentials: Credentials::new(ak, sk),
            http,
        }))
    }

    async fn get(&self, key: &str) -> Result<Bytes> {
        let mut last_err = None;
        for attempt in 0..8u32 {
            if attempt > 0 {
                let delay_ms = (300u64 << (attempt - 1).min(5)).min(15_000);
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
            let url = self
                .bucket
                .get_object(Some(&self.credentials), key)
                .sign(Duration::from_secs(900));
            match self.http.get(url).send().await {
                Ok(resp) if resp.status().is_success() => match resp.bytes().await {
                    Ok(b) => return Ok(b),
                    Err(e) => last_err = Some(eyre!("R2 body {key}: {e}")),
                },
                Ok(resp) => {
                    let st = resp.status();
                    if st.as_u16() == 404 {
                        return Err(eyre!("R2 404: {key}"));
                    }
                    last_err = Some(eyre!("R2 HTTP {st} for {key}"));
                }
                Err(e) => last_err = Some(eyre!("R2 GET {key}: {}", e.without_url())),
            }
        }
        Err(last_err.unwrap_or_else(|| eyre!("R2 GET failed: {key}")))
    }
}

struct Store {
    journal_cache: PathBuf,
    hawkeye_id: String,
    r2: Option<Arc<R2Client>>,
    replay_files: Vec<(u64, u64, PathBuf)>,
    ancestors: HashMap<u64, String>,
    // Index bucket cache: partition_start -> HashMap<block_num, object_key>
    index_cache: Mutex<HashMap<u64, Arc<HashMap<u64, String>>>>,
    index_order: Mutex<VecDeque<u64>>,
    // Decoded block_diffs header cache: object_key -> Arc<HashMap<block_num, Value>>
    header_cache: Mutex<HashMap<String, Arc<HashMap<u64, Value>>>>,
    header_order: Mutex<VecDeque<String>>,
    // In-flight R2 prefetch handles: object_key -> JoinHandle
    inflight: Mutex<HashMap<String, tokio::task::JoinHandle<Result<Arc<HashMap<u64, Value>>>>>>,
    // Decoded replay_store cache: file_idx -> Arc<HashMap<block_num, (Vec<Value>, Vec<Value>)>>
    tx_cache: Mutex<HashMap<usize, Arc<HashMap<u64, (Vec<Value>, Vec<Value>)>>>>,
    tx_order: Mutex<VecDeque<usize>>,
    // Stats
    r2_fetches: std::sync::atomic::AtomicU64,
    local_hits: std::sync::atomic::AtomicU64,
}

fn parse_span(name: &str) -> Option<(u64, u64)> {
    let rest = name.strip_prefix("block_number_")?;
    let (span, _) = rest.split_once('_')?;
    let (a, b) = span.split_once('-')?;
    Some((a.parse().ok()?, b.parse().ok()?))
}

/// Fast zero-copy slice of the top-level `"block": { ... }` object inside a `block_diffs` JSON
/// payload, skipping the giant `"transactions": [...]` prestateTracer diff array completely.
fn extract_block_object(payload: &[u8]) -> Option<&[u8]> {
    let needle = b"\"block\"";
    let mut i = 0;
    while i + needle.len() < payload.len() {
        if &payload[i..i + needle.len()] == needle {
            let mut j = i + needle.len();
            while j < payload.len() && payload[j].is_ascii_whitespace() {
                j += 1;
            }
            if j < payload.len() && payload[j] == b':' {
                j += 1;
                while j < payload.len() && payload[j].is_ascii_whitespace() {
                    j += 1;
                }
                if j < payload.len() && payload[j] == b'{' {
                    let start = j;
                    let mut depth = 0usize;
                    let mut in_str = false;
                    let mut esc = false;
                    while j < payload.len() {
                        let c = payload[j];
                        if in_str {
                            if esc {
                                esc = false;
                            } else if c == b'\\' {
                                esc = true;
                            } else if c == b'"' {
                                in_str = false;
                            }
                        } else {
                            match c {
                                b'"' => in_str = true,
                                b'{' => depth += 1,
                                b'}' => {
                                    depth -= 1;
                                    if depth == 0 {
                                        return Some(&payload[start..=j]);
                                    }
                                }
                                _ => {}
                            }
                        }
                        j += 1;
                    }
                    return None;
                }
            }
        }
        i += 1;
    }
    None
}

fn decode_block_diffs_parquet(bytes: Bytes) -> Result<HashMap<u64, Value>> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)?;
    let reader = builder.with_batch_size(64).build()?;
    let mut out = HashMap::new();
    for batch in reader {
        let batch = batch?;
        let nums = batch
            .column_by_name("block_number")
            .ok_or_else(|| eyre!("block_diffs parquet missing block_number"))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| eyre!("block_number not UInt64"))?;
        let data_col = batch
            .column_by_name("data")
            .ok_or_else(|| eyre!("block_diffs parquet missing data"))?;
        for row in 0..batch.num_rows() {
            let n = nums.value(row);
            let payload: &[u8] = if let Some(a) = data_col.as_any().downcast_ref::<BinaryArray>() {
                a.value(row)
            } else if let Some(a) = data_col.as_any().downcast_ref::<LargeBinaryArray>() {
                a.value(row)
            } else {
                return Err(eyre!("unexpected data column type {:?}", data_col.data_type()));
            };
            let block_val: Value = if let Some(slice) = extract_block_object(payload) {
                serde_json::from_slice(slice).wrap_err_with(|| format!("decode fast header {n}"))?
            } else {
                let parsed: BlockDiffHeaderOnly =
                    serde_json::from_slice(payload).wrap_err_with(|| format!("decode header {n}"))?;
                parsed.block
            };
            out.insert(n, block_val);
        }
    }
    Ok(out)
}

fn decode_replay_store_parquet(path: &Path) -> Result<HashMap<u64, (Vec<Value>, Vec<Value>)>> {
    let file = File::open(path).wrap_err_with(|| format!("open {}", path.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.with_batch_size(256).build()?;
    let mut out = HashMap::new();
    for batch in reader {
        let batch = batch?;
        let nums = batch
            .column_by_name("block_number")
            .ok_or_else(|| eyre!("replay_store missing block_number"))?;
        let data_col = batch
            .column_by_name("data")
            .ok_or_else(|| eyre!("replay_store missing data"))?;
        for row in 0..batch.num_rows() {
            let n = if let Some(a) = nums.as_any().downcast_ref::<UInt64Array>() {
                a.value(row)
            } else if let Some(a) = nums.as_any().downcast_ref::<Int64Array>() {
                a.value(row) as u64
            } else {
                return Err(eyre!("unexpected replay_store block_number type"));
            };
            let payload: &[u8] = if let Some(a) = data_col.as_any().downcast_ref::<BinaryArray>() {
                a.value(row)
            } else if let Some(a) = data_col.as_any().downcast_ref::<LargeBinaryArray>() {
                a.value(row)
            } else {
                return Err(eyre!("unexpected replay_store data type"));
            };
            let wire: ReplayBlockWire =
                serde_json::from_slice(payload).wrap_err_with(|| format!("decode replay tx {n}"))?;
            let mut txs = Vec::with_capacity(wire.transactions.len());
            let mut rcpts = Vec::with_capacity(wire.transactions.len());
            for t in wire.transactions {
                txs.push(t.transaction);
                rcpts.push(t.receipt);
            }
            out.insert(n, (txs, rcpts));
        }
    }
    Ok(out)
}

impl Store {
    async fn load_index_bucket(self: &Arc<Self>, part: u64) -> Result<Arc<HashMap<u64, String>>> {
        {
            let cache = self.index_cache.lock().await;
            if let Some(m) = cache.get(&part) {
                return Ok(Arc::clone(m));
            }
        }
        let key = format!(
            "_block_diff_index/v2/{}/blocks_{}-{}.json",
            self.hawkeye_id,
            part,
            part + INDEX_SPAN - 1
        );
        let local_path = self.journal_cache.join(&key);
        let bytes = if local_path.exists() {
            Bytes::from(std::fs::read(&local_path)?)
        } else if let Some(ref r2) = self.r2 {
            r2.get(&key).await?
        } else {
            return Err(eyre!("missing index bucket {key} and R2 disabled"));
        };
        let bucket: IndexBucket = serde_json::from_slice(&bytes).wrap_err_with(|| format!("parse {key}"))?;
        let mut map = HashMap::with_capacity(bucket.blocks.len());
        for (k, v) in bucket.blocks {
            if let Ok(n) = k.parse::<u64>() {
                map.insert(n, v.object_key);
            }
        }
        let arc = Arc::new(map);
        let mut cache = self.index_cache.lock().await;
        let mut order = self.index_order.lock().await;
        if !cache.contains_key(&part) {
            cache.insert(part, Arc::clone(&arc));
            order.push_back(part);
            while order.len() > 8 {
                if let Some(old) = order.pop_front() {
                    cache.remove(&old);
                }
            }
        }
        Ok(arc)
    }

    async fn object_key_for(self: &Arc<Self>, n: u64) -> Result<String> {
        let part = (n / INDEX_SPAN) * INDEX_SPAN;
        let bucket = self.load_index_bucket(part).await?;
        bucket
            .get(&n)
            .cloned()
            .ok_or_else(|| eyre!("block {n} absent from index bucket {part}"))
    }

    fn spawn_load_headers(self: &Arc<Self>, key: String) -> tokio::task::JoinHandle<Result<Arc<HashMap<u64, Value>>>> {
        let me = Arc::clone(self);
        tokio::spawn(async move {
            let local_path = me.journal_cache.join(&key);
            let bytes = if local_path.exists() {
                me.local_hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let p = local_path.clone();
                tokio::task::spawn_blocking(move || std::fs::read(&p)).await??.into()
            } else if let Some(ref r2) = me.r2 {
                me.r2_fetches.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                r2.get(&key).await?
            } else {
                return Err(eyre!("missing local object {key} and R2 disabled"));
            };
            let decoded = tokio::task::spawn_blocking(move || decode_block_diffs_parquet(bytes)).await??;
            Ok(Arc::new(decoded))
        })
    }

    async fn prefetch_ahead(self: &Arc<Self>, start_block: u64) {
        let mut probe = start_block;
        let mut seen = std::collections::HashSet::new();
        for _ in 0..PREFETCH_OBJECTS {
            if self.inflight.lock().await.len() >= PREFETCH_OBJECTS {
                break;
            }
            let Ok(key) = self.object_key_for(probe).await else {
                break;
            };
            let end_block = Path::new(&key)
                .file_name()
                .and_then(|s| s.to_str())
                .and_then(parse_span)
                .map(|(_, b)| b)
                .unwrap_or(probe);
            if seen.insert(key.clone()) {
                let cached = self.header_cache.lock().await.contains_key(&key);
                if !cached {
                    let mut inf = self.inflight.lock().await;
                    if inf.len() < PREFETCH_OBJECTS && !inf.contains_key(&key) {
                        let h = self.spawn_load_headers(key.clone());
                        inf.insert(key, h);
                    }
                }
            }
            probe = end_block + 1;
        }
    }

    async fn get_header(self: &Arc<Self>, n: u64) -> Result<Value> {
        let key = self.object_key_for(n).await?;
        {
            let cache = self.header_cache.lock().await;
            if let Some(m) = cache.get(&key) {
                if let Some(v) = m.get(&n) {
                    return Ok(v.clone());
                }
            }
        }
        // Check if already inflight, else spawn now
        let handle = {
            let mut inf = self.inflight.lock().await;
            if let Some(h) = inf.remove(&key) {
                h
            } else {
                self.spawn_load_headers(key.clone())
            }
        };
        // Kick off background prefetch for subsequent objects while awaiting this one
        let end_block = Path::new(&key)
            .file_name()
            .and_then(|s| s.to_str())
            .and_then(parse_span)
            .map(|(_, b)| b)
            .unwrap_or(n);
        let me = Arc::clone(self);
        tokio::spawn(async move {
            me.prefetch_ahead(end_block + 1).await;
        });

        let decoded = handle.await??;
        let val = decoded
            .get(&n)
            .cloned()
            .ok_or_else(|| eyre!("block {n} missing inside {key}"))?;
        let mut cache = self.header_cache.lock().await;
        let mut order = self.header_order.lock().await;
        if !cache.contains_key(&key) {
            cache.insert(key.clone(), decoded);
            order.push_back(key);
            while order.len() > MAX_HEADER_CACHE_OBJECTS {
                if let Some(old) = order.pop_front() {
                    cache.remove(&old);
                }
            }
        }
        Ok(val)
    }

    async fn get_txs_and_receipts(self: &Arc<Self>, n: u64) -> Result<(Vec<Value>, Vec<Value>)> {
        let end_pos = self.replay_files.partition_point(|&(a, _, _)| a <= n);
        if end_pos == 0 {
            return Err(eyre!("block {n} below replay_store range"));
        }
        // Scan backwards from the last interval with start <= n to handle overlapping
        // sub-range parquet files (e.g. a narrow backfill file inside a wider span).
        let mut candidates = Vec::new();
        let mut i = end_pos;
        while i > 0 {
            i -= 1;
            let (a, b, _) = self.replay_files[i];
            if a <= n && n <= b {
                candidates.push(i);
            }
            if n.saturating_sub(a) > 10_000 {
                break;
            }
        }
        if candidates.is_empty() {
            let (a, b, _) = self.replay_files[end_pos - 1];
            return Err(eyre!("block {n} not in replay_store (nearest {a}..={b})"));
        }
        for idx in candidates {
            let (_, _, ref path) = self.replay_files[idx];
            let decoded = {
                let cache = self.tx_cache.lock().await;
                cache.get(&idx).cloned()
            };
            let decoded = match decoded {
                Some(m) => m,
                None => {
                    let p = path.clone();
                    let m = Arc::new(
                        tokio::task::spawn_blocking(move || decode_replay_store_parquet(&p)).await??,
                    );
                    let mut cache = self.tx_cache.lock().await;
                    let mut order = self.tx_order.lock().await;
                    if !cache.contains_key(&idx) {
                        cache.insert(idx, Arc::clone(&m));
                        order.push_back(idx);
                        while order.len() > MAX_TX_CACHE_FILES {
                            if let Some(old) = order.pop_front() {
                                cache.remove(&old);
                            }
                        }
                    }
                    m
                }
            };
            if let Some(pair) = decoded.get(&n) {
                return Ok(pair.clone());
            }
        }
        Err(eyre!("block {n} missing inside all covering replay_store files"))
    }

    async fn get_full_block_json(self: &Arc<Self>, n: u64) -> Result<Vec<u8>> {
        let (hdr, (txs, rcpts)) = tokio::try_join!(self.get_header(n), self.get_txs_and_receipts(n))?;
        let mut hdr_obj = match hdr {
            Value::Object(m) => m,
            _ => return Err(eyre!("header {n} is not JSON object")),
        };
        hdr_obj.insert("transactions".to_owned(), Value::Array(txs));
        let mut wrapper = Map::with_capacity(2);
        wrapper.insert("block".to_owned(), Value::Object(hdr_obj));
        wrapper.insert("receipts".to_owned(), Value::Array(rcpts));
        Ok(serde_json::to_vec(&Value::Object(wrapper))?)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let mut journal_cache = PathBuf::from("/root/backtests/avax-1003/journal_cache");
    let mut replay_store = PathBuf::from("/root/backtests/avax-1003/inputs/replay_store");
    let mut ancestors_path: Option<PathBuf> = None;
    let mut bucket = "evm-backtest-curated-data".to_owned();
    let mut hawkeye_id = "avax-hawkeye-fra".to_owned();

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--journal-cache" => { journal_cache = PathBuf::from(&args[i + 1]); i += 2; }
            "--replay-store" => { replay_store = PathBuf::from(&args[i + 1]); i += 2; }
            "--ancestors" => { ancestors_path = Some(PathBuf::from(&args[i + 1])); i += 2; }
            "--r2-bucket" => { bucket = args[i + 1].clone(); i += 2; }
            "--hawkeye-id" => { hawkeye_id = args[i + 1].clone(); i += 2; }
            other => return Err(eyre!("unknown arg: {other}")),
        }
    }

    let mut replay_files = Vec::new();
    for entry in std::fs::read_dir(&replay_store)? {
        let entry = entry?;
        let name = entry.file_name();
        if let Some(s) = name.to_str() {
            if let Some((a, b)) = parse_span(s) {
                replay_files.push((a, b, entry.path()));
            }
        }
    }
    replay_files.sort_by_key(|&(a, b, _)| (a, b));

    let mut ancestors = HashMap::new();
    if let Some(ref p) = ancestors_path {
        let raw: HashMap<String, String> = serde_json::from_slice(&std::fs::read(p)?)?;
        for (k, v) in raw {
            if let Ok(n) = k.parse::<u64>() {
                ancestors.insert(n, v);
            }
        }
    }

    let r2 = R2Client::from_env(&bucket)?.map(Arc::new);
    eprintln!(
        "[avax-parquet-source] indexed {} replay_store files ({}..={}), {} ancestors, r2={}",
        replay_files.len(),
        replay_files.first().map(|x| x.0).unwrap_or(0),
        replay_files.last().map(|x| x.1).unwrap_or(0),
        ancestors.len(),
        r2.is_some()
    );

    let store = Arc::new(Store {
        journal_cache,
        hawkeye_id,
        r2,
        replay_files,
        ancestors,
        index_cache: Mutex::new(HashMap::new()),
        index_order: Mutex::new(VecDeque::new()),
        header_cache: Mutex::new(HashMap::new()),
        header_order: Mutex::new(VecDeque::new()),
        inflight: Mutex::new(HashMap::new()),
        tx_cache: Mutex::new(HashMap::new()),
        tx_order: Mutex::new(VecDeque::new()),
        r2_fetches: std::sync::atomic::AtomicU64::new(0),
        local_hits: std::sync::atomic::AtomicU64::new(0),
    });

    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut reader = BufReader::new(stdin.lock());
    let mut writer = BufWriter::new(stdout.lock());
    let mut line = String::new();

    loop {
        line.clear();
        let nread = reader.read_line(&mut line)?;
        if nread == 0 {
            break;
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let mut parts = trimmed.split_whitespace();
        match parts.next() {
            Some("Q") => break,
            Some("H") => {
                let n: u64 = parts.next().ok_or_else(|| eyre!("H missing n"))?.parse()?;
                if let Some(h) = store.ancestors.get(&n) {
                    writeln!(writer, "{}", h.len())?;
                    writer.write_all(h.as_bytes())?;
                } else {
                    writeln!(writer, "0")?;
                }
                writer.flush()?;
            }
            Some("B") => {
                let n: u64 = parts.next().ok_or_else(|| eyre!("B missing n"))?.parse()?;
                match store.get_full_block_json(n).await {
                    Ok(bytes) => {
                        writeln!(writer, "{}", bytes.len())?;
                        writer.write_all(&bytes)?;
                        writer.flush()?;
                    }
                    Err(e) => {
                        eprintln!("[avax-parquet-source] error block {n}: {e:#}");
                        writeln!(writer, "0")?;
                        writer.flush()?;
                    }
                }
            }
            Some("P") => {
                let lo: u64 = parts.next().ok_or_else(|| eyre!("P missing lo"))?.parse()?;
                let me = Arc::clone(&store);
                tokio::spawn(async move { me.prefetch_ahead(lo).await; });
                writeln!(writer, "0")?;
                writer.flush()?;
            }
            Some("S") => {
                let r2_n = store.r2_fetches.load(std::sync::atomic::Ordering::Relaxed);
                let loc_n = store.local_hits.load(std::sync::atomic::Ordering::Relaxed);
                let msg = format!("local_objects={loc_n} r2_objects={r2_n}");
                writeln!(writer, "{}", msg.len())?;
                writer.write_all(msg.as_bytes())?;
                writer.flush()?;
            }
            _ => {
                writeln!(writer, "0")?;
                writer.flush()?;
            }
        }
    }
    Ok(())
}
