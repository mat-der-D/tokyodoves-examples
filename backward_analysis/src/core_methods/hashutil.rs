use tokyodoves::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OnOff {
    onoff: u64,
}

impl OnOff {
    pub const FULL: OnOff = OnOff { onoff: 0xfff << 48 };

    pub fn new(hash: u64) -> Self {
        Self {
            onoff: hash & (0xfff << 48),
        }
    }

    pub fn project_on(self, color: Color) -> Self {
        use Color::*;
        let projected = match color {
            Red => self.onoff & (0xaaa << 48),
            Green => self.onoff & (0x555 << 48),
        };
        Self { onoff: projected }
    }

    pub fn count_doves(&self) -> u32 {
        self.onoff.count_ones()
    }
}

impl std::ops::Not for OnOff {
    type Output = Self;
    fn not(self) -> Self::Output {
        let red = 0xaaa;
        let green = 0x555;
        Self {
            onoff: ((self.onoff & red) >> 1) | ((self.onoff & green) << 1),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PossibleAction {
    Put(Color, Dove),
    Move,
    Remove(Color, Dove),
}

impl PossibleAction {
    pub fn matches(&self, action: &Action) -> bool {
        use PossibleAction::*;
        match self {
            Put(color, dove) => {
                matches!(action, Action::Put(c, d, _) if *c == *color && *d == *dove)
            }
            Move => {
                matches!(action, Action::Move(..))
            }
            Remove(color, dove) => {
                matches!(action, Action::Remove(c, d) if *c == *color && *d == *dove)
            }
        }
    }
}

pub fn possible_action(from: OnOff, to: OnOff) -> Option<PossibleAction> {
    use PossibleAction::*;
    let diff = from.onoff ^ to.onoff;
    match diff.count_ones() {
        0 => return Some(Move),
        1 => (),
        _ => return None,
    }
    let num_zeros = diff.trailing_zeros();

    use Color::*;
    let color = match num_zeros % 2 {
        0 => Green,
        1 => Red,
        _ => unreachable!(),
    };

    use Dove::*;
    let dove = match num_zeros / 2 {
        29 => B,
        28 => A,
        27 => Y,
        26 => M,
        25 => T,
        24 => H,
        _ => return None,
    };

    if from.onoff.count_ones() < to.onoff.count_ones() {
        Some(Put(color, dove))
    } else {
        Some(Remove(color, dove))
    }
}

pub fn aniki_boss_distance(hash: u64, color: Color) -> u64 {
    use Color::*;
    let (aniki, boss) = match color {
        Red => ((hash >> 36) & 0xf, (hash >> 44) & 0xf),
        Green => ((hash >> 32) & 0xf, (hash >> 40) & 0xf),
    };
    (aniki % 4).abs_diff(boss % 4) + (aniki / 4).abs_diff(boss / 4)
}
