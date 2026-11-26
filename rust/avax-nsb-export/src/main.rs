//! eita-avax-nsb-export: streams a verified Avalanche C-Chain `NSBFLAT2` full snapshot
//! into Eita's `EITARETH` v1 hashed-key record stream for `eita import-reth`.
//!
//! Both formats are sorted in exact `(keccak(addr), hslot)` / `codehash` order across 256
//! shards (`00..ff`), so this runs in a single streaming pass with ~4 MiB RAM and zero sorting.
//! Coreth's `multi_coin` account flag maps to Eita v2's `FLAG_MULTI_COIN = 8` (bit 3).

use eyre::{bail, ensure, eyre, Result, WrapErr};
use serde::Deserialize;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

const NSB_MAGIC: &[u8; 8] = b"NSBFLAT2";
const NSB_HEADER_BYTES: usize = 64;
const NSB_HAS_ADDRESS: u8 = 1;
const NSB_HAS_MULTI_COIN: u8 = 2;
const NSB_MULTI_COIN: u8 = 4;

const EITA_FLAG_EXISTS: u32 = 1;
const EITA_FLAG_MULTI_COIN: u32 = 8;
const KECCAK256_EMPTY: [u8; 32] = [
    0xc5, 0xd2, 0x46, 0x01, 0x86, 0xf7, 0x23, 0x3c, 0x92, 0x7e, 0x7d, 0xb2, 0xdc, 0xc7, 0x03, 0xc0,
    0xe5, 0x00, 0xb6, 0x53, 0xca, 0x82, 0x27, 0x3b, 0x7b, 0xfa, 0xd8, 0x04, 0x5d, 0x85, 0xa4, 0x70,
];

#[derive(Deserialize)]
struct Manifest {
    chain_id: u64,
    block: ManifestBlock,
    content: ManifestContent,
    verified_state_root: Option<String>,
    files: Vec<ManifestFile>,
}

#[derive(Deserialize)]
struct ManifestBlock {
    number: u64,
    hash: String,
    state_root: String,
}

#[derive(Deserialize)]
struct ManifestContent {
    accounts: u64,
    slots: u64,
}

#[derive(Deserialize)]
struct ManifestFile {
    role: String,
    name: String,
    rows: u64,
}

struct BlockEntry {
    offset: u64,
    compressed: u32,
    records: u32,
}

struct FlatReader {
    file: File,
    record_bytes: usize,
    blocks: Vec<BlockEntry>,
    block_idx: usize,
    buf: Vec<u8>,
    rec_idx: usize,
    rec_count: usize,
}

impl FlatReader {
    fn open(path: &Path, expected_tag: u8, record_bytes: usize, key_bytes: usize, expected_rows: u64) -> Result<Self> {
        let file = File::open(path).wrap_err_with(|| format!("open {}", path.display()))?;
        let file_len = file.metadata()?.len();
        let mut hdr = [0u8; NSB_HEADER_BYTES];
        file.read_exact_at(&mut hdr, 0)?;
        ensure!(&hdr[0..8] == NSB_MAGIC && hdr[8] == expected_tag, "{} bad NSBFLAT2 header", path.display());
        let u64_at = |o: usize| u64::from_le_bytes(hdr[o..o + 8].try_into().unwrap());
        let (held, nblocks, index_at, end_len) = (u64_at(16), u64_at(24), u64_at(32), u64_at(40));
        ensure!(held == expected_rows, "{} rows mismatch: {held} vs {expected_rows}", path.display());
        let entry_bytes = key_bytes + 16;
        let index_bytes = nblocks * entry_bytes as u64;
        ensure!(index_at + index_bytes == file_len && end_len == file_len, "{} truncated index", path.display());
        let mut index = vec![0u8; index_bytes as usize];
        file.read_exact_at(&mut index, index_at)?;
        let mut blocks = Vec::with_capacity(nblocks as usize);
        for chunk in index.chunks_exact(entry_bytes) {
            let offset = u64::from_le_bytes(chunk[key_bytes..key_bytes + 8].try_into().unwrap());
            let compressed = u32::from_le_bytes(chunk[key_bytes + 8..key_bytes + 12].try_into().unwrap());
            let records = u32::from_le_bytes(chunk[key_bytes + 12..key_bytes + 16].try_into().unwrap());
            blocks.push(BlockEntry { offset, compressed, records });
        }
        Ok(Self { file, record_bytes, blocks, block_idx: 0, buf: Vec::new(), rec_idx: 0, rec_count: 0 })
    }

    fn next_record(&mut self) -> Result<Option<&[u8]>> {
        if self.rec_idx >= self.rec_count {
            if self.block_idx >= self.blocks.len() {
                return Ok(None);
            }
            let b = &self.blocks[self.block_idx];
            self.block_idx += 1;
            let mut comp = vec![0u8; b.compressed as usize];
            self.file.read_exact_at(&mut comp, b.offset)?;
            let expected = b.records as usize * self.record_bytes;
            self.buf = zstd::bulk::decompress(&comp, expected + 64)?;
            ensure!(self.buf.len() == expected, "decompressed {} != {expected}", self.buf.len());
            self.rec_idx = 0;
            self.rec_count = b.records as usize;
        }
        let start = self.rec_idx * self.record_bytes;
        self.rec_idx += 1;
        Ok(Some(&self.buf[start..start + self.record_bytes]))
    }
}

struct MultiFlatReader {
    files: Vec<(PathBuf, u64)>,
    file_idx: usize,
    tag: u8,
    record_bytes: usize,
    key_bytes: usize,
    cur: Option<FlatReader>,
    peeked: Option<Vec<u8>>,
}

impl MultiFlatReader {
    fn new(files: Vec<(PathBuf, u64)>, tag: u8, record_bytes: usize, key_bytes: usize) -> Self {
        Self { files, file_idx: 0, tag, record_bytes, key_bytes, cur: None, peeked: None }
    }

    fn pull(&mut self) -> Result<Option<Vec<u8>>> {
        loop {
            if let Some(ref mut r) = self.cur {
                if let Some(rec) = r.next_record()? {
                    return Ok(Some(rec.to_vec()));
                }
                self.cur = None;
            }
            if self.file_idx >= self.files.len() {
                return Ok(None);
            }
            let (ref path, rows) = self.files[self.file_idx];
            self.file_idx += 1;
            self.cur = Some(FlatReader::open(path, self.tag, self.record_bytes, self.key_bytes, rows)?);
        }
    }

    fn peek(&mut self) -> Result<Option<&[u8]>> {
        if self.peeked.is_none() {
            self.peeked = self.pull()?;
        }
        Ok(self.peeked.as_deref())
    }

    fn take(&mut self) -> Result<Option<Vec<u8>>> {
        if let Some(v) = self.peeked.take() {
            return Ok(Some(v));
        }
        self.pull()
    }
}

fn parse_hex32(s: &str) -> Result<[u8; 32]> {
    let hex = s.strip_prefix("0x").unwrap_or(s);
    ensure!(hex.len() == 64, "expected 32-byte hex, got len {}", hex.len());
    let mut out = [0u8; 32];
    for i in 0..32 {
        out[i] = u8::from_str_radix(&hex[2 * i..2 * i + 2], 16)?;
    }
    Ok(out)
}

fn stream_merged_codes<W: Write>(
    code_files: &[(PathBuf, u64)],
    out: &mut W,
) -> Result<u64> {
    // Each NSBFLAT2 code_XX shard is sorted by codehash within itself, but partitioned by
    // account-hash byte XX, so different shards can contain overlapping codehashes in any
    // global range. Perform a k-way min-heap merge across all shards and deduplicate.
    let mut readers: Vec<(FlatReader, File)> = Vec::with_capacity(code_files.len());
    let mut heap: BinaryHeap<Reverse<([u8; 32], usize, u64, u64)>> = BinaryHeap::new();
    for (idx, (path, rows)) in code_files.iter().enumerate() {
        let mut r = FlatReader::open(path, 3, 48, 32, *rows)?;
        let raw_file = File::open(path)?;
        if let Some(c) = r.next_record()? {
            let mut h = [0u8; 32];
            h.copy_from_slice(&c[0..32]);
            let off = u64::from_le_bytes(c[32..40].try_into().unwrap());
            let len = u64::from_le_bytes(c[40..48].try_into().unwrap());
            heap.push(Reverse((h, idx, off, len)));
        }
        readers.push((r, raw_file));
    }

    let mut wc = 0u64;
    let mut last_hash: Option<[u8; 32]> = None;
    let mut code_buf = Vec::new();
    while let Some(Reverse((codehash, idx, off, len))) = heap.pop() {
        // Advance this shard before emitting
        if let Some(c) = readers[idx].0.next_record()? {
            let mut nh = [0u8; 32];
            nh.copy_from_slice(&c[0..32]);
            let noff = u64::from_le_bytes(c[32..40].try_into().unwrap());
            let nlen = u64::from_le_bytes(c[40..48].try_into().unwrap());
            heap.push(Reverse((nh, idx, noff, nlen)));
        }
        if codehash == KECCAK256_EMPTY || last_hash == Some(codehash) {
            continue;
        }
        last_hash = Some(codehash);
        let len_usize = len as usize;
        code_buf.resize(len_usize, 0);
        readers[idx]
            .1
            .read_exact_at(&mut code_buf, off)
            .map_err(|e| eyre!("read code at {off}: {e}"))?;
        out.write_all(b"C")?;
        out.write_all(&codehash)?;
        out.write_all(&(len_usize as u32).to_be_bytes())?;
        out.write_all(&code_buf)?;
        wc += 1;
    }
    Ok(wc)
}

fn main() -> Result<()> {
    let mut manifest_path = None;
    let mut flat_dir = None;
    let mut codes_only = false;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--manifest" => manifest_path = args.next().map(PathBuf::from),
            "--flat-dir" => flat_dir = args.next().map(PathBuf::from),
            "--codes-only" => codes_only = true,
            _ => bail!("usage: eita-avax-nsb-export --manifest FILE --flat-dir DIR [--codes-only]"),
        }
    }
    let (Some(manifest_path), Some(flat_dir)) = (manifest_path, flat_dir) else {
        bail!("usage: eita-avax-nsb-export --manifest FILE --flat-dir DIR [--codes-only]");
    };

    let manifest: Manifest = serde_json::from_reader(File::open(&manifest_path)?)?;
    let block_hash = parse_hex32(&manifest.block.hash)?;
    let root_hex = manifest.verified_state_root.as_deref().unwrap_or(&manifest.block.state_root);
    let state_root = parse_hex32(root_hex)?;

    let mut acct_files: Vec<(PathBuf, u64)> = Vec::new();
    let mut slot_files: Vec<(PathBuf, u64)> = Vec::new();
    let mut code_files: Vec<(PathBuf, u64)> = Vec::new();
    for f in &manifest.files {
        let p = flat_dir.join(format!("{}.flat", f.name));
        match f.role.as_str() {
            "accounts" => acct_files.push((p, f.rows)),
            "storage" => slot_files.push((p, f.rows)),
            "code" => code_files.push((p, f.rows)),
            other => bail!("unknown manifest file role {other}"),
        }
    }
    acct_files.sort_by(|a, b| a.0.cmp(&b.0));
    slot_files.sort_by(|a, b| a.0.cmp(&b.0));
    code_files.sort_by(|a, b| a.0.cmp(&b.0));

    let mut out = BufWriter::with_capacity(1 << 22, std::io::stdout().lock());
    if codes_only {
        let wc = stream_merged_codes(&code_files, &mut out)?;
        out.write_all(b"E")?;
        out.flush()?;
        eprintln!("avax-nsb-export (--codes-only): wrote {wc} unique codes");
        return Ok(());
    }

    eprintln!(
        "avax-nsb-export: chain {} block {} hash {} stateRoot {}; {} accounts, {} slots ({} acct / {} slot / {} code flat files)",
        manifest.chain_id, manifest.block.number, manifest.block.hash, root_hex,
        manifest.content.accounts, manifest.content.slots,
        acct_files.len(), slot_files.len(), code_files.len()
    );

    let provenance = format!("imported avalanche NSBFLAT2 @ block {} ({})", manifest.block.number, manifest.block.hash);
    out.write_all(b"EITARETH")?;
    out.write_all(&1u32.to_be_bytes())?;
    out.write_all(&manifest.chain_id.to_be_bytes())?;
    out.write_all(&manifest.block.number.to_be_bytes())?;
    out.write_all(&block_hash)?;
    out.write_all(&state_root)?;
    out.write_all(&manifest.content.accounts.to_be_bytes())?;
    out.write_all(&manifest.content.slots.to_be_bytes())?;
    out.write_all(&(provenance.len() as u16).to_be_bytes())?;
    out.write_all(provenance.as_bytes())?;

    let mut accts = MultiFlatReader::new(acct_files, 1, 160, 32);
    let mut slots = MultiFlatReader::new(slot_files, 2, 96, 64);
    let (mut wa, mut ws, mut mc_count) = (0u64, 0u64, 0u64);
    let mut rec = [0u8; 128];

    while let Some(a) = accts.take()? {
        let hkey = &a[0..32];
        while let Some(s) = slots.peek()? {
            let haddr = &s[0..32];
            if haddr > hkey {
                break;
            }
            if haddr < hkey {
                bail!("storage without matching account (S5)");
            }
            let s = slots.take()?.unwrap();
            if s[64..96].iter().all(|&b| b == 0) {
                continue;
            }
            out.write_all(b"S")?;
            out.write_all(&s[0..96])?;
            ws += 1;
        }
        rec.fill(0);
        rec[0..32].copy_from_slice(hkey);
        let nsb_flags = a[40];
        if nsb_flags & NSB_HAS_ADDRESS != 0 {
            rec[32..52].copy_from_slice(&a[44..64]);
        }
        let nonce = u64::from_le_bytes(a[32..40].try_into().unwrap());
        let balance_zero = a[64..96].iter().all(|&b| b == 0);
        let code_empty = &a[96..128] == &KECCAK256_EMPTY;
        let empty = nonce == 0 && balance_zero && code_empty;
        let mut eita_flags = if empty { EITA_FLAG_EXISTS } else { 0 };
        if (nsb_flags & NSB_HAS_MULTI_COIN != 0) && (nsb_flags & NSB_MULTI_COIN != 0) {
            eita_flags |= EITA_FLAG_MULTI_COIN;
            mc_count += 1;
        }
        rec[52..56].copy_from_slice(&eita_flags.to_be_bytes());
        rec[56..64].copy_from_slice(&nonce.to_be_bytes());
        rec[64..96].copy_from_slice(&a[64..96]);
        rec[96..128].copy_from_slice(&a[96..128]);
        out.write_all(b"A")?;
        out.write_all(&rec)?;
        wa += 1;
        if wa % 10_000_000 == 0 {
            eprintln!("avax-nsb-export: {wa} accounts ({mc_count} multiCoin), {ws} slots");
        }
    }
    if slots.peek()?.is_some() {
        bail!("trailing storage slots after last account (S5)");
    }

    let wc = stream_merged_codes(&code_files, &mut out)?;
    out.write_all(b"E")?;
    out.write_all(&wa.to_be_bytes())?;
    out.write_all(&ws.to_be_bytes())?;
    out.write_all(&wc.to_be_bytes())?;
    out.flush()?;
    eprintln!("avax-nsb-export: wrote {wa} accounts ({mc_count} multiCoin), {ws} slots, {wc} unique codes");
    Ok(())
}
