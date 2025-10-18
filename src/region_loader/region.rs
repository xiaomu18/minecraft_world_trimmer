use crate::region_loader::chunk_loader::chunk::Chunk;
use std::fs::File;
use std::hash::Hasher;
use std::io::Read;
use std::path::PathBuf;
use thiserror::Error;
use twox_hash::XxHash32;

#[derive(PartialEq, Debug)]
pub struct Region {
    chunks: Vec<Chunk>,
    is_modified: bool,
}

#[derive(Error, Debug)]
pub enum ParseRegionError {
    #[error("error while reading the file")]
    ReadError,
    #[error("cannot read header of region file")]
    HeaderError,
}

impl Region {
    pub fn to_bytes_blinear(&self, timestamp: i64, compression_level: u8) -> Vec<u8>{
        let mut result = Vec::new();

        let file_head = -0x200812250269i64;
        let version = 0x02u8;
        let hash_seed = 0x0721i32 as u32;

        // whole file head part
        // 8 + 1 + 8 + 1
        let mut file_header = [0_u8; 18];

        file_header[0..8].copy_from_slice(&file_head.to_be_bytes()); // superblock
        file_header[8..9].copy_from_slice(&version.to_be_bytes()); // version
        file_header[9..17].copy_from_slice(&timestamp.to_be_bytes()); // master file timestamp
        file_header[17..18].copy_from_slice(&compression_level.to_be_bytes()); // compression level

        result.extend_from_slice(&file_header); // append file head

        let mut region_data = Vec::new();

        for index in 0..1024 {
            let mut target_chunk = None;

            for chunk in &self.chunks {
                if (chunk.location.get_sector_index() as i32) == index {
                    target_chunk = Some(chunk);
                    break
                }
            }

            if target_chunk.is_none() {
                region_data.extend_from_slice(&0i32.to_be_bytes());
                continue;
            }

            let mut hasher = XxHash32::with_seed(hash_seed);

            let chunk_data = target_chunk.unwrap().to_raw_bytes(); // 3
            let length_of_chunk_data = (chunk_data.len() as i32).to_be_bytes(); // 0
            let timestamp_of_chunk = (target_chunk.unwrap().location.get_timestamp() as i64).to_be_bytes(); // 1

            hasher.write(&chunk_data);
            let xxhash32_of_chunk_data = (hasher.finish() as i32).to_be_bytes(); // 2

            let mut local_temp_buffer = Vec::new();

            local_temp_buffer.extend_from_slice(&length_of_chunk_data); // len
            local_temp_buffer.extend_from_slice(&timestamp_of_chunk); // timestamp of chunk
            local_temp_buffer.extend_from_slice(&xxhash32_of_chunk_data); // xxhash32 of chunk data
            local_temp_buffer.extend_from_slice(&chunk_data); // chunk data

            region_data.extend_from_slice(&(local_temp_buffer.len() as i32).to_be_bytes());
            region_data.extend_from_slice(local_temp_buffer.as_slice());
        }

        if let Ok(compressed) = zstd::encode_all(region_data.as_slice(), compression_level as i32) {
            result.extend_from_slice(&compressed);
        }

        result
    }

    pub fn from_bytes_blinear(bytes: &[u8]) -> Result<Self, ParseRegionError> {
        let mut chunk_sections = Vec::with_capacity(1024);

        // 8 + 1 + 8 + 1
        let file_head = i64::from_be_bytes(bytes[0..8].try_into().unwrap());
        let version = &bytes[8..9];

        // incorrect file
        if file_head != -0x200812250269 || version[0] != 0x02 {
            return Err(ParseRegionError::HeaderError);
        }

        let _timestamp_of_master_file = i64::from_be_bytes(bytes[9..17].try_into().unwrap());
        let _compression_level = &bytes[17..18];

        let decompressed_region_sections_data = zstd::decode_all(&bytes[18..bytes.len()])
            .map_err(|_| ParseRegionError::ReadError)?;

        let mut buffer_pointer = 0;
        for sector_index in 0..1024 {
            let sector_len = i32::from_be_bytes(decompressed_region_sections_data[buffer_pointer..buffer_pointer + 4].try_into().unwrap()) as usize;
            buffer_pointer += 4;

            if sector_len <= 0 {
                continue;
            }

            let section_data_this_section = &decompressed_region_sections_data[buffer_pointer..buffer_pointer + sector_len];
            buffer_pointer += sector_len;


            let _length_of_chunk = i32::from_be_bytes(section_data_this_section[0..4].try_into().unwrap()); // unused
            let timestamp_of_chunk = i64::from_be_bytes(section_data_this_section[4..12].try_into().unwrap());
            let _xxhash32_of_chunk = i32::from_be_bytes(section_data_this_section[12..16].try_into().unwrap()); // unused

            let data_of_chunk = &section_data_this_section[16..section_data_this_section.len()];

            if let Ok(chunk) = Chunk::from_sector(sector_index, timestamp_of_chunk, data_of_chunk) {
                chunk_sections.push(chunk);
            }
        }

        Ok(Self{
            chunks: chunk_sections,
            is_modified: false
        })
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
}

fn align_vec_size(vec: &mut Vec<u8>) {
    let aligned_size = ((vec.len() + 4095) / 4096) * 4096;
    vec.resize(aligned_size, 0);
}

fn try_read_bytes(file_path: &PathBuf) -> std::io::Result<Vec<u8>> {
    let mut buf = Vec::<u8>::new();
    File::open(file_path).and_then(|mut file| file.read_to_end(&mut buf))?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Local;
    use std::io::{BufWriter, Write};
    use std::path::Path;

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
    fn test_small_region() {
        let original_bytes = include_bytes!("../../test_files/r.0.0.b_linear");

        // Parse the region file
        let original_parsed_region_file = Region::from_bytes_blinear(original_bytes).unwrap();
        let serialized_bytes = original_parsed_region_file.to_bytes_blinear(Local::now().timestamp_millis(), 6);

        // Wa cannot validate the header as the compression and chunk order in the payload may differ
        // resulting in a modification of the offset bytes, so as long as the re-parsed region file is
        // the same as the parsed original, we should be fine

        // Try parsing again the serialized region file and check if both still have the same chunk data
        let parsed_again = Region::from_bytes_blinear(&serialized_bytes).unwrap();

        let original_chunks = original_parsed_region_file.get_chunks();
        let parsed_chunks = parsed_again.get_chunks();
        assert_eq!(parsed_chunks.len(), original_chunks.len());

        if let Ok (file) = File::open(Path::new("../../test_files/r.0.0.b_linear")){
            let mut writer = BufWriter::new(file);

            writer.write(&serialized_bytes).expect("io err");
            writer.flush().expect("io err");

            let parsed_again_again =  Region::from_bytes_blinear(&serialized_bytes).unwrap();
            let parsed_chunks = parsed_again_again.get_chunks();
            assert_eq!(parsed_chunks.len(), original_chunks.len());
        }


        // Assert the chunk data is unchanged
        for i in 0..original_chunks.len() {
            let original_chunk = &original_chunks[i];
            let parsed_chunk = &parsed_chunks[i];
            // We cannot check for equality on the location since it might have different offset and size
            assert_eq!(original_chunk.nbt, parsed_chunk.nbt);
        }
    }
}
