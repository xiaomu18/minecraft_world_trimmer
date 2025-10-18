use crate::nbt::binary_reader::BinaryReader;
use crate::nbt::parse::parse_tag;
use crate::nbt::tag::Tag;
use crate::region_loader::location::SectorInfo;

#[derive(PartialEq, Debug, Clone)]
pub struct Chunk {
    pub nbt: Tag,
    pub location: SectorInfo,
}

impl Chunk {
    const STATUS_FULL: &'static str = "minecraft:full";

    pub fn from_sector(sector_index: i32, timestamp: i64, data: &[u8]) -> Result<Self, &'static str> {
        let nbt = parse_tag(&mut BinaryReader::new(&data));
        let location_blinear = SectorInfo::from_blinear_sector_idx(sector_index, timestamp);

        Ok(Self {
            nbt,
            location: location_blinear
        })
    }

    pub fn to_raw_bytes(&self) -> Vec<u8> {
        self.nbt.to_bytes()
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
