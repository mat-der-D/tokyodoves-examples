use tokyodoves::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OnOff {
    onoff: u64,
}

impl std::fmt::Display for OnOff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "OnOff({:0>12b})", self.onoff >> 48)
    }
}

impl OnOff {
    pub fn new(hash: u64) -> Self {
        Self {
            onoff: hash & (0xfff << 48),
        }
    }

    pub fn contains(&self, color: Color, dove: Dove) -> bool {
        use Color::*;
        let icolor = match color {
            Red => 1,
            Green => 0,
        };
        use Dove::*;
        let idove = match dove {
            B => 58,
            A => 56,
            Y => 54,
            M => 52,
            T => 50,
            H => 48,
        };
        self.onoff & (1 << (icolor + idove)) != 0
    }
}

impl std::ops::Not for OnOff {
    type Output = Self;
    fn not(self) -> Self::Output {
        let red = 0xaaa << 48;
        let green = 0x555 << 48;
        Self {
            onoff: ((self.onoff & red) >> 1) | ((self.onoff & green) << 1),
        }
    }
}

fn coordinate_index(hash: u64, color: Color, dove: Dove) -> u64 {
    use Color::*;
    let icolor = match color {
        Red => 4,
        Green => 0,
    };
    use Dove::*;
    let idove = match dove {
        B => 40,
        A => 32,
        Y => 24,
        M => 16,
        T => 8,
        H => 0,
    };
    (hash >> (icolor + idove)) & 0xf
}

pub fn distance_a(hash: u64, color: Color) -> u64 {
    use Dove::*;
    if !OnOff::new(hash).contains(color, A) {
        0
    } else {
        let boss = coordinate_index(hash, color, B);
        let aniki = coordinate_index(hash, color, A);
        (boss % 4).abs_diff(aniki % 4) + (boss / 4).abs_diff(aniki / 4)
    }
}
