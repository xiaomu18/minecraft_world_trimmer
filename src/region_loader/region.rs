use crate::region_loader::chunk_loader::chunk::Chunk;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use thiserror::Error;

const BLINEAR_SUPER_BLOCK: i64 = -0x200812250269;
const BLINEAR_V2_VERSION: u8 = 0x02;
const BLINEAR_V3_VERSION: u8 = 0x03;
const DEFAULT_HASH_SEED: u32 = 0x0721;
const REGION_CHUNK_COUNT: usize = 1024;
const BUCKET_SHIFT: usize = 6;
const BUCKET_SIZE: usize = 1 << BUCKET_SHIFT;
const BUCKET_COUNT: usize = REGION_CHUNK_COUNT / BUCKET_SIZE;
const V2_HEADER_SIZE: usize = 18;
const V3_HEADER_SIZE: usize = 14;
const V3_POSITION_TABLE_SIZE: usize = BUCKET_COUNT * std::mem::size_of::<u64>();
const V3_DATA_AREA_OFFSET: usize = V3_HEADER_SIZE + V3_POSITION_TABLE_SIZE;

#[derive(PartialEq, Debug, Clone, Copy)]
enum BLinearFormat {
    V2,
    V3,
}

#[derive(PartialEq, Debug)]
pub struct Region {
    chunks: Vec<Chunk>,
    is_modified: bool,
    format: BLinearFormat,
    hash_seed: u32,
}

#[derive(Error, Debug)]
pub enum ParseRegionError {
    #[error("error while reading the file")]
    ReadError,
    #[error("cannot read header of region file")]
    HeaderError,
}

impl Region {
    pub fn to_bytes_blinear(&self, timestamp: i64, compression_level: u8) -> Vec<u8> {
        match self.format {
            BLinearFormat::V2 => self.to_bytes_blinear_v2(timestamp, compression_level),
            BLinearFormat::V3 => self.to_bytes_blinear_v3(compression_level),
        }
    }

    pub fn from_bytes_blinear(bytes: &[u8]) -> Result<Self, ParseRegionError> {
        if bytes.len() < 9 {
            return Err(ParseRegionError::HeaderError);
        }

        let file_head = i64::from_be_bytes(bytes[0..8].try_into().unwrap());
        if file_head != BLINEAR_SUPER_BLOCK {
            return Err(ParseRegionError::HeaderError);
        }

        match bytes[8] {
            BLINEAR_V2_VERSION => Self::from_bytes_blinear_v2(bytes),
            BLINEAR_V3_VERSION => Self::from_bytes_blinear_v3(bytes),
            _ => Err(ParseRegionError::HeaderError),
        }
    }

    pub fn from_file_name(file_name: &PathBuf) -> Result<Self, ParseRegionError> {
        let bytes = try_read_bytes(file_name).map_err(|_| ParseRegionError::ReadError)?;
        Region::from_bytes_blinear(&bytes)
    }

    pub fn get_chunks(&self) -> &Vec<Chunk> {
        &self.chunks
    }

    pub fn get_chunk_count(&self) -> usize {
        self.chunks.len()
    }

    pub fn remove_chunk_by_index(&mut self, index: usize) {
        self.chunks.remove(index);
        if !self.is_modified {
            self.is_modified = true;
        }
    }

    pub fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }

    pub fn is_modified(&self) -> bool {
        self.is_modified
    }

    fn from_bytes_blinear_v2(bytes: &[u8]) -> Result<Self, ParseRegionError> {
        if bytes.len() < V2_HEADER_SIZE {
            return Err(ParseRegionError::HeaderError);
        }

        let decompressed_region_sections_data =
            zstd::decode_all(&bytes[V2_HEADER_SIZE..]).map_err(|_| ParseRegionError::ReadError)?;

        let chunks = Self::parse_v2_chunk_sections(&decompressed_region_sections_data)?;

        Ok(Self {
            chunks,
            is_modified: false,
            format: BLinearFormat::V2,
            hash_seed: DEFAULT_HASH_SEED,
        })
    }

    fn from_bytes_blinear_v3(bytes: &[u8]) -> Result<Self, ParseRegionError> {
        if bytes.len() < V3_DATA_AREA_OFFSET {
            return Err(ParseRegionError::HeaderError);
        }

        let hash_seed = u32::from_be_bytes(bytes[10..14].try_into().unwrap());
        let mut chunks = Vec::with_capacity(REGION_CHUNK_COUNT);

        for bucket_index in 0..BUCKET_COUNT {
            let offset_pos = V3_HEADER_SIZE + bucket_index * std::mem::size_of::<u64>();
            let bucket_offset = u64::from_be_bytes(
                read_slice(bytes, offset_pos, std::mem::size_of::<u64>())?
                    .try_into()
                    .unwrap(),
            ) as usize;

            if bucket_offset == 0 {
                continue;
            }

            let original_len = read_i32_at(bytes, bucket_offset)?;
            let compressed_len = read_i32_at(bytes, bucket_offset + 4)?;
            if original_len < 0 || compressed_len < 0 {
                return Err(ParseRegionError::ReadError);
            }

            let compressed_start = bucket_offset + 8;
            let compressed_end = compressed_start + compressed_len as usize;
            let compressed_data = read_slice(bytes, compressed_start, compressed_len as usize)?;
            let decompressed_bucket =
                zstd::decode_all(compressed_data).map_err(|_| ParseRegionError::ReadError)?;

            if decompressed_bucket.len() != original_len as usize {
                return Err(ParseRegionError::ReadError);
            }

            let mut buffer_pointer = 0usize;
            let base_chunk_index = bucket_index << BUCKET_SHIFT;

            for chunk_offset in 0..BUCKET_SIZE {
                let section_len = read_i32_at(&decompressed_bucket, buffer_pointer)?;
                buffer_pointer += 4;

                if section_len <= 0 {
                    continue;
                }

                let section_len = section_len as usize;
                let section_data = read_slice(&decompressed_bucket, buffer_pointer, section_len)?;
                buffer_pointer += section_len;

                if let Ok(chunk) = Chunk::from_blinear_section(
                    (base_chunk_index + chunk_offset) as i32,
                    section_data,
                ) {
                    chunks.push(chunk);
                }
            }
        }

        Ok(Self {
            chunks,
            is_modified: false,
            format: BLinearFormat::V3,
            hash_seed,
        })
    }

    fn to_bytes_blinear_v2(&self, timestamp: i64, compression_level: u8) -> Vec<u8> {
        let mut result = Vec::new();
        let mut file_header = [0_u8; V2_HEADER_SIZE];

        file_header[0..8].copy_from_slice(&BLINEAR_SUPER_BLOCK.to_be_bytes());
        file_header[8] = BLINEAR_V2_VERSION;
        file_header[9..17].copy_from_slice(&timestamp.to_be_bytes());
        file_header[17] = compression_level;
        result.extend_from_slice(&file_header);

        let chunk_lookup = self.build_chunk_lookup();
        let mut region_data = Vec::new();

        for chunk in chunk_lookup {
            if let Some(chunk) = chunk {
                let section_data = chunk.to_blinear_section_bytes(self.hash_seed);
                region_data.extend_from_slice(&(section_data.len() as i32).to_be_bytes());
                region_data.extend_from_slice(&section_data);
            } else {
                region_data.extend_from_slice(&0i32.to_be_bytes());
            }
        }

        if let Ok(compressed) = zstd::encode_all(region_data.as_slice(), compression_level as i32) {
            result.extend_from_slice(&compressed);
        }

        result
    }

    fn to_bytes_blinear_v3(&self, compression_level: u8) -> Vec<u8> {
        let mut result = Vec::new();
        let mut position_table = [0u64; BUCKET_COUNT];
        let mut bucket_payloads = Vec::with_capacity(BUCKET_COUNT);
        let chunk_lookup = self.build_chunk_lookup();
        let mut data_offset = V3_DATA_AREA_OFFSET as u64;

        for bucket_index in 0..BUCKET_COUNT {
            let base_chunk_index = bucket_index << BUCKET_SHIFT;
            let mut raw_bucket = Vec::new();
            let mut has_any_chunk = false;

            for chunk_offset in 0..BUCKET_SIZE {
                if let Some(chunk) = chunk_lookup[base_chunk_index + chunk_offset] {
                    let section_data = chunk.to_blinear_section_bytes(self.hash_seed);
                    raw_bucket.extend_from_slice(&(section_data.len() as i32).to_be_bytes());
                    raw_bucket.extend_from_slice(&section_data);
                    has_any_chunk = true;
                } else {
                    raw_bucket.extend_from_slice(&0i32.to_be_bytes());
                }
            }

            if has_any_chunk {
                if let Ok(compressed_bucket) =
                    zstd::encode_all(raw_bucket.as_slice(), compression_level as i32)
                {
                    let mut bucket_payload = Vec::with_capacity(8 + compressed_bucket.len());
                    bucket_payload.extend_from_slice(&(raw_bucket.len() as i32).to_be_bytes());
                    bucket_payload
                        .extend_from_slice(&(compressed_bucket.len() as i32).to_be_bytes());
                    bucket_payload.extend_from_slice(&compressed_bucket);

                    position_table[bucket_index] = data_offset;
                    data_offset += bucket_payload.len() as u64;
                    bucket_payloads.push(Some(bucket_payload));
                } else {
                    bucket_payloads.push(None);
                }
            } else {
                bucket_payloads.push(None);
            }
        }

        result.extend_from_slice(&BLINEAR_SUPER_BLOCK.to_be_bytes());
        result.push(BLINEAR_V3_VERSION);
        result.push(compression_level);
        result.extend_from_slice(&self.hash_seed.to_be_bytes());

        for position in position_table {
            result.extend_from_slice(&position.to_be_bytes());
        }

        for bucket_payload in bucket_payloads.into_iter().flatten() {
            result.extend_from_slice(&bucket_payload);
        }

        result
    }

    fn build_chunk_lookup(&self) -> Vec<Option<&Chunk>> {
        let mut chunk_lookup = vec![None; REGION_CHUNK_COUNT];

        for chunk in &self.chunks {
            let sector_index = chunk.location.get_sector_index() as usize;
            if sector_index < REGION_CHUNK_COUNT {
                chunk_lookup[sector_index] = Some(chunk);
            }
        }

        chunk_lookup
    }

    fn parse_v2_chunk_sections(
        decompressed_region_sections_data: &[u8],
    ) -> Result<Vec<Chunk>, ParseRegionError> {
        let mut chunks = Vec::with_capacity(REGION_CHUNK_COUNT);
        let mut buffer_pointer = 0usize;

        for sector_index in 0..REGION_CHUNK_COUNT {
            let section_len = read_i32_at(decompressed_region_sections_data, buffer_pointer)?;
            buffer_pointer += 4;

            if section_len <= 0 {
                continue;
            }

            let section_len = section_len as usize;
            let section_data = read_slice(
                decompressed_region_sections_data,
                buffer_pointer,
                section_len,
            )?;
            buffer_pointer += section_len;

            if let Ok(chunk) = Chunk::from_blinear_section(sector_index as i32, section_data) {
                chunks.push(chunk);
            }
        }

        Ok(chunks)
    }
}

fn align_vec_size(vec: &mut Vec<u8>) {
    let aligned_size = vec.len().div_ceil(4096) * 4096;
    vec.resize(aligned_size, 0);
}

fn try_read_bytes(file_path: &PathBuf) -> std::io::Result<Vec<u8>> {
    let mut buf = Vec::<u8>::new();
    File::open(file_path).and_then(|mut file| file.read_to_end(&mut buf))?;
    Ok(buf)
}

fn read_slice(bytes: &[u8], start: usize, len: usize) -> Result<&[u8], ParseRegionError> {
    bytes
        .get(start..start + len)
        .ok_or(ParseRegionError::ReadError)
}

fn read_i32_at(bytes: &[u8], start: usize) -> Result<i32, ParseRegionError> {
    Ok(i32::from_be_bytes(
        read_slice(bytes, start, std::mem::size_of::<i32>())?
            .try_into()
            .unwrap(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Local;

    #[test]
    fn test_align_vec_size() {
        let mut vec_500 = vec![0; 500];
        align_vec_size(&mut vec_500);
        assert_eq!(4096, vec_500.len());

        let mut vec_4096 = vec![0; 4096];
        align_vec_size(&mut vec_4096);
        assert_eq!(4096, vec_4096.len());

        let mut vec_4097 = vec![0; 4097];
        align_vec_size(&mut vec_4097);
        assert_eq!(8192, vec_4097.len());
    }

    #[test]
    fn test_small_region_v2_roundtrip() {
        let original_bytes = include_bytes!("../../test_files/r.0.0.b_linear");
        let original_region = Region::from_bytes_blinear(original_bytes).unwrap();
        let serialized_bytes = original_region.to_bytes_blinear(Local::now().timestamp_millis(), 6);
        let parsed_again = Region::from_bytes_blinear(&serialized_bytes).unwrap();

        assert_eq!(parsed_again.format, BLinearFormat::V2);
        assert_same_chunks(&original_region, &parsed_again);
    }

    #[test]
    fn test_small_region_v3_roundtrip() {
        let original_bytes = include_bytes!("../../test_files/r.0.0.b_linear");
        let mut original_region = Region::from_bytes_blinear(original_bytes).unwrap();
        original_region.format = BLinearFormat::V3;

        let serialized_bytes = original_region.to_bytes_blinear(Local::now().timestamp_millis(), 6);
        let parsed_again = Region::from_bytes_blinear(&serialized_bytes).unwrap();

        assert_eq!(parsed_again.format, BLinearFormat::V3);
        assert_same_chunks(&original_region, &parsed_again);
    }

    fn assert_same_chunks(left: &Region, right: &Region) {
        let left_chunks = left.get_chunks();
        let right_chunks = right.get_chunks();
        assert_eq!(left_chunks.len(), right_chunks.len());

        for (left_chunk, right_chunk) in left_chunks.iter().zip(right_chunks.iter()) {
            assert_eq!(left_chunk.location, right_chunk.location);
            assert_eq!(left_chunk.nbt, right_chunk.nbt);
        }
    }
}
