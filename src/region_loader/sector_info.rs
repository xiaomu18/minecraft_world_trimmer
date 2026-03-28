#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SectorInfo {
    sector_index: u32,
    timestamp: i64,
}

impl SectorInfo {
    pub fn from_blinear_sector_idx(sector_index: i32, timestamp: i64) -> Self {
        Self {
            sector_index: sector_index as u32,
            timestamp,
        }
    }

    pub fn get_sector_index(&self) -> u32 {
        self.sector_index
    }

    pub fn get_timestamp(&self) -> i64 {
        self.timestamp
    }
}
