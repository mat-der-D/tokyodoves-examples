use super::hashutil::*;
use tokyodoves::*;

// --- 10 ---
// level in 0..=1
pub fn make_win_filter_10(level: u32) -> impl Fn(&u64) -> bool + Clone {
    move |hash| {
        let mask = 0b1010101010;
        let masked = (hash >> 48) & mask;
        let right_aligned = {
            let mut cursor = 1;
            let mut masked = masked;
            let mut aligned = 0;
            for _ in 0..5 {
                masked >>= 1;
                aligned |= masked & cursor;
                cursor <<= 1;
            }
            aligned
        };
        right_aligned.trailing_ones() / 3 == level
    }
}

// level in 0..=1
pub fn make_target_filter_10(level: u32) -> impl Fn(&u64) -> bool + Clone {
    move |hash| {
        let mask = 0b0101010101;
        let masked = (hash >> 48) & mask;
        let right_aligned = {
            let mut cursor = 1;
            let mut masked = masked;
            let mut aligned = 0;
            for _ in 0..5 {
                aligned |= masked & cursor;
                masked >>= 1;
                cursor <<= 1;
            }
            aligned
        };
        right_aligned.trailing_ones() / 3 == level
    }
}

// --- 11 ---
// level in 0..=1
pub fn make_win_filter_11(level: u64) -> impl Fn(&u64) -> bool + Clone {
    move |hash| (distance_a(*hash, Color::Red) + 1) / 4 == level
}

// level in 0..=1
pub fn make_target_filter_11(level: u64) -> impl Fn(&u64) -> bool + Clone {
    move |hash| (distance_a(*hash, Color::Green) + 1) / 4 == level
}
