use itertools::{self, Itertools};
use std::collections::HashSet;
use std::sync::Arc;
use std::thread;
use tokyodoves::{analysis::*, collections::*, game::*, *};

// ****************************************************************
//  Constants
// ****************************************************************
const NOT_BOSS: [(Color, Dove); 10] = {
    use Color::*;
    use Dove::*;
    [
        (Red, A),
        (Red, Y),
        (Red, M),
        (Red, T),
        (Red, H),
        (Green, A),
        (Green, Y),
        (Green, M),
        (Green, T),
        (Green, H),
    ]
};

// ****************************************************************
//  Bit Utility
// ****************************************************************
struct HotBitIter<T> {
    bits: T,
}

impl<T> HotBitIter<T> {
    fn new(bits: T) -> Self {
        Self { bits }
    }
}

macro_rules! impl_iterator {
    ($($ty:ty),*) => {
        $(
        impl Iterator for HotBitIter<$ty> {
            type Item = $ty;
            fn next(&mut self) -> Option<Self::Item> {
                if self.bits != 0 {
                    let unit = 1 << self.bits.trailing_zeros();
                    self.bits &= !unit;
                    Some(unit)
                } else {
                    None
                }
            }
        }
        )*
    };
}

impl_iterator!(u16);

fn calc_adjacents(bits: u16) -> u16 {
    let r = 0xeeee;
    let l = 0x7777;
    let mut adj = 0;
    adj |= (bits & r) >> 5;
    adj |= bits >> 4;
    adj |= (bits & l) >> 3;
    adj |= (bits & r) >> 1;
    adj |= (bits & l) << 1;
    adj |= (bits & r) << 3;
    adj |= bits << 4;
    adj |= (bits & l) << 5;
    adj
}

fn is_isolated(bits: u16) -> bool {
    let adj = calc_adjacents(bits);
    bits & adj != bits
}

fn get_shape(nums: &[usize]) -> (usize, usize, usize, usize) {
    let (mut hmin, mut hmax, mut vmin, mut vmax) = (3, 0, 3, 0);
    for n in nums {
        let (x, y) = (n % 4, n / 4);
        hmin = hmin.min(x);
        hmax = hmax.max(x);
        vmin = vmin.min(y);
        vmax = vmax.max(y);
    }
    (hmin, hmax, vmin, vmax)
}

fn nums_to_bits(nums: &[usize]) -> u16 {
    let mut bits = 0;
    for n in nums {
        bits |= 1_u16 << n;
    }
    bits
}

fn decompose(bits: u16) -> (u16, u16) {
    let edge_e = 0x1111;
    let edge_w = 0x8888;
    let edge_n = 0xf000;
    let edge_s = 0x000f;
    let is_wall_ew = (bits & edge_e) != 0 && (bits & edge_w) != 0;
    let is_wall_ns = (bits & edge_n) != 0 && (bits & edge_s) != 0;

    let bits_e = {
        let mut b = (bits & !edge_e) >> 1;
        if is_wall_ew {
            b |= edge_w;
        }
        b
    };
    let bits_w = {
        let mut b = (bits & !edge_w) << 1;
        if is_wall_ew {
            b |= edge_e;
        }
        b
    };
    let bits_n = {
        let mut b = (bits & !edge_n) << 4;
        if is_wall_ns {
            b |= edge_s;
        }
        b
    };
    let bits_s = {
        let mut b = (bits & !edge_s) >> 4;
        if is_wall_ns {
            b |= edge_n;
        }
        b
    };
    let surrounded = bits & bits_n & bits_e & bits_w & bits_s;
    let not_surrounded = bits & !surrounded;
    (surrounded, not_surrounded)
}

fn pack_lose2(pool: &mut BoardSet, bits: u16, both_is_win: bool) {
    fn _color_to_index(color: Color) -> usize {
        match color {
            Color::Red => 0,
            Color::Green => 1,
        }
    }

    fn _dove_to_index(dove: Dove) -> usize {
        match dove {
            Dove::B => 0,
            Dove::A => 1,
            Dove::Y => 2,
            Dove::M => 3,
            Dove::T => 4,
            Dove::H => 5,
        }
    }

    let judgement = if both_is_win {
        Judge::LastWins
    } else {
        Judge::NextWins
    };
    let rule = GameRule::new(true).with_suicide_atk_judge(judgement);

    let (_, not_surrounded) = decompose(bits);
    let num_res = bits.count_ones() as usize - 2;
    for bosses in HotBitIter::new(not_surrounded).permutations(2) {
        let rb = bosses[0];
        let gb = bosses[1];
        let positions = [[rb, 0, 0, 0, 0, 0], [gb, 0, 0, 0, 0, 0]];
        let others = HotBitIter::new(bits & !(rb | gb)).collect_vec();

        let mut first_check = true;
        for cd in NOT_BOSS.into_iter().permutations(num_res) {
            let mut positions = positions;
            for ((c, d), &pos) in cd.into_iter().zip(others.iter()) {
                let ic = _color_to_index(c);
                let id = _dove_to_index(d);
                positions[ic][id] = pos;
            }
            let board = BoardBuilder::from_u16_bits(positions).build_unchecked();
            if first_check {
                if boss_may_die(&board, Color::Red, rule) {
                    first_check = false;
                } else {
                    break;
                }
            }

            if matches!(
                compare_board_value(board, BoardValue::lose(2).unwrap(), Color::Red, rule),
                Ok(std::cmp::Ordering::Equal)
            ) {
                pool.raw_mut().insert(board.to_invariant_u64(Color::Red));
            }
        }
    }
}

fn boss_may_die(board: &Board, player: Color, rule: GameRule) -> bool {
    for action in board.legal_actions(player, true, true, *rule.is_remove_accepted()) {
        if !matches!(action, Action::Move(_, Dove::B, _)) {
            continue;
        }
        let b = board.perform_unchecked_copied(action);
        if b.liberty_of_boss(player) >= 2 {
            return false;
        }
    }
    true
}

fn get_canonical_bits(nums: &[usize]) -> u16 {
    let (hmin, hmax, vmin, vmax) = get_shape(nums);
    let idx_shift = hmin + 4 * vmin;
    let aligned: Vec<usize> = nums.iter().map(|n| n - idx_shift).collect();
    let (hsize, vsize) = (hmax - hmin + 1, vmax - vmin + 1);

    let mapper = PositionMapper::try_create(vsize, hsize).unwrap();
    let mut bits = u16::MAX;
    for idx in 0..8 {
        let congruent: Vec<usize> = aligned.iter().map(|n| mapper.map(idx, *n)).collect();
        bits = bits.min(nums_to_bits(&congruent[..]));
    }
    bits
}

fn find_all_bits(num_doves: usize) -> HashSet<u16> {
    let mut all_bits = HashSet::new();
    for v_idx in (0..16).combinations(num_doves) {
        let bits = get_canonical_bits(&v_idx);
        if is_isolated(bits) {
            continue;
        }
        all_bits.insert(bits);
    }
    all_bits
}

fn find_all_lose2(num_doves: usize, both_is_win: bool, num_cores: usize) -> BoardSet {
    println!("[Thread Main] #doves={}, #cores={}", num_doves, num_cores);

    let all_bits = Arc::new(find_all_bits(num_doves).into_iter().collect_vec());
    let len_pack = (all_bits.len() + num_cores - 1) / num_cores;

    let mut handlers = Vec::with_capacity(num_cores);
    for i in 0..num_cores {
        let all_bits = Arc::clone(&all_bits);
        handlers.push(thread::spawn(move || {
            let mut pool = BoardSet::new();
            let begin = all_bits.len().min(i * len_pack);
            let end = all_bits.len().min((i + 1) * len_pack);
            let num_total = end - begin + 1;

            println!("[Thread {i}] started! Total={num_total}");

            for (count, bits) in all_bits[begin..end].iter().enumerate() {
                if (count + 1) % 10 == 0 {
                    println!(
                        "[Thread {i}] {} from {num_total} ({}%)",
                        count + 1,
                        ((count + 1) as f32) / (num_total as f32) * 100_f32
                    );
                }
                pack_lose2(&mut pool, *bits, both_is_win);
            }
            println!("[Thread {i}] finished!");
            pool
        }))
    }

    let results = handlers
        .into_iter()
        .map(|x| x.join().unwrap())
        .collect_vec();

    let mut capacity = Capacity::new();
    for set in results.iter() {
        capacity += set.capacity();
    }

    let mut lose2 = BoardSet::with_capacity(capacity);
    for result in results {
        lose2.absorb(result);
    }
    println!("[Thread Main] Concatenated");
    println!("Total={}", lose2.len());
    lose2
}

// ****************************************************************
//  Building Blocks
// ****************************************************************
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 4 {
        panic!("give more than 3 arguments");
    }
    let num_doves: usize = args[1].parse()?;
    let num_cores: usize = args[2].parse()?;
    let path = std::path::Path::new(args[3].as_str());
    let both_is_win = false;

    let fs = std::fs::File::create(path)?;

    let lose2 = find_all_lose2(num_doves, both_is_win, num_cores);

    // *** SAVE ***
    lose2.save(fs)?;
    println!("Saved to {:?}", path);
    Ok(())
}
