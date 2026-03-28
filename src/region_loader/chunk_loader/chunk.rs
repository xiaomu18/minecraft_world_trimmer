use crate::nbt::binary_reader::BinaryReader;
use crate::nbt::parse::parse_tag;
use crate::nbt::tag::Tag;
use crate::region_loader::sector_info::SectorInfo;
use std::hash::Hasher;
use twox_hash::XxHash32;

#[derive(PartialEq, Debug, Clone)]
pub struct Chunk {
    pub nbt: Tag,
    pub location: SectorInfo,
}

impl Chunk {
    const STATUS_FULL: &'static str = "minecraft:full";
    const BLINEAR_SECTION_META_SIZE: usize = 16;

    pub fn from_sector(
        sector_index: i32,
        timestamp: i64,
        data: &[u8],
    ) -> Result<Self, &'static str> {
        let nbt = parse_tag(&mut BinaryReader::new(&data));
        let location_blinear = SectorInfo::from_blinear_sector_idx(sector_index, timestamp);

        Ok(Self {
            nbt,
            location: location_blinear,
        })
    }

    pub fn from_blinear_section(
        sector_index: i32,
        section_data: &[u8],
    ) -> Result<Self, &'static str> {
        if section_data.len() < Self::BLINEAR_SECTION_META_SIZE {
            return Err("chunk section metadata is incomplete");
        }

        let chunk_len = i32::from_be_bytes(section_data[0..4].try_into().unwrap());
        if chunk_len < 0 {
            return Err("chunk section length is invalid");
        }

        let chunk_len = chunk_len as usize;
        let data_end = Self::BLINEAR_SECTION_META_SIZE + chunk_len;
        if section_data.len() < data_end {
            return Err("chunk section payload is incomplete");
        }

        let timestamp = i64::from_be_bytes(section_data[4..12].try_into().unwrap());
        let chunk_data = &section_data[Self::BLINEAR_SECTION_META_SIZE..data_end];

        Self::from_sector(sector_index, timestamp, chunk_data)
    }

    pub fn to_raw_bytes(&self) -> Vec<u8> {
        self.nbt.to_bytes()
    }

    pub fn to_blinear_section_bytes(&self, hash_seed: u32) -> Vec<u8> {
        let chunk_data = self.to_raw_bytes();
        let mut hasher = XxHash32::with_seed(hash_seed);
        hasher.write(&chunk_data);

        let mut section_bytes =
            Vec::with_capacity(Self::BLINEAR_SECTION_META_SIZE + chunk_data.len());
        section_bytes.extend_from_slice(&(chunk_data.len() as i32).to_be_bytes());
        section_bytes.extend_from_slice(&self.location.get_timestamp().to_be_bytes());
        section_bytes.extend_from_slice(&(hasher.finish() as i32).to_be_bytes());
        section_bytes.extend_from_slice(&chunk_data);
        section_bytes
    }

    /// Checks if a chunk is not fully generated or if it has never been inhabited
    pub fn should_delete(&self) -> bool {
        !self.is_fully_generated() || !self.has_been_inhabited()
    }

    fn is_fully_generated(&self) -> bool {
        self.nbt
            .find_tag("Status")
            .and_then(|tag| tag.get_string())
            .map(|status| status == Chunk::STATUS_FULL)
            .unwrap_or(false) // if the tag is not present, we can assume that the chunk is not fully generated
    }

    fn has_been_inhabited(&self) -> bool {
        // The InhabitedTime value seems to be incremented for all 8 chunks around a player (including the one the player is standing in)
        let inhabited_time = self
            .nbt
            .find_tag("InhabitedTime")
            .and_then(|tag| tag.get_long())
            .copied()
            .unwrap_or(0); // If the tag is not present, we can assume that the chunk has never been inhabited

        inhabited_time > 0
    }
}
