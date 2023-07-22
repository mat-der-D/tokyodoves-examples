use itertools::{self, iproduct, Itertools};
use std::collections::HashSet;
use std::sync::Arc;
use std::thread;
use tokyodoves::strum::IntoEnumIterator;
use tokyodoves::{analysis::*, collections::*, game::*, *};

struct HotBitIter<T> {
    bits: T,
}

impl<T> HotBitIter<T> {
    fn new(bits: T) -> Self {
        Self { bits }
    }
}

impl Iterator for HotBitIter<u16> {
    type Item = u16;
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

fn decompose_surrounded(bits: u16) -> (u16, u16) {
    let edge_e = 0x1111;
    let edge_w = 0x8888;
    let edge_n = 0xf000;
    let edge_s = 0x000f;
    let is_wall_ew = (bits & edge_e) != 0 && (bits & edge_w) != 0;
    let is_wall_ns = (bits & edge_n) != 0 && (bits & edge_s) != 0;

    macro_rules! shift {
        ($edge:expr, $is_wall:expr, $rot:ident, $rot_num:expr) => {{
            if $is_wall {
                bits | $edge
            } else {
                bits & !$edge
            }
            .$rot($rot_num)
        }};
    }

    let bits_e = shift!(edge_e, is_wall_ew, rotate_right, 1);
    let bits_w = shift!(edge_w, is_wall_ew, rotate_left, 1);
    let bits_n = shift!(edge_n, is_wall_ns, rotate_left, 4);
    let bits_s = shift!(edge_s, is_wall_ns, rotate_right, 4);

    let surrounded = bits & bits_n & bits_e & bits_w & bits_s;
    let not_surrounded = bits & !surrounded;
    (surrounded, not_surrounded)
}

fn boss_may_die(board: &Board, player: Color) -> bool {
    for action in board.legal_actions(player, false, true, false) {
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

fn pack_lose2(pool: &mut BoardSet, bits: u16, rule: GameRule) {
    fn _color_to_index(color: Color) -> usize {
        use Color::*;
        match color {
            Red => 0,
            Green => 1,
        }
    }

    fn _dove_to_index(dove: Dove) -> usize {
        use Dove::*;
        match dove {
            B => 0,
            A => 1,
            Y => 2,
            M => 3,
            T => 4,
            H => 5,
        }
    }

    let lose2 = BoardValue::lose(2).unwrap();
    let (_, not_surrounded) = decompose_surrounded(bits);
    let num_res = bits.count_ones() as usize - 2;
    for bosses in HotBitIter::new(not_surrounded).permutations(2) {
        let rb = bosses[0];
        let gb = bosses[1];
        let positions_base = [[rb, 0, 0, 0, 0, 0], [gb, 0, 0, 0, 0, 0]];
        let others: Vec<u16> = HotBitIter::new(bits & !(rb | gb)).collect();

        let mut first_check = true;
        for cd in iproduct!(Color::iter(), Dove::iter().skip(1)).permutations(num_res) {
            let mut positions = positions_base;
            for ((c, d), &pos) in cd.into_iter().zip(others.iter()) {
                let ic = _color_to_index(c);
                let id = _dove_to_index(d);
                positions[ic][id] = pos;
            }

            let board = BoardBuilder::from_u16_bits(positions).build_unchecked();
            if first_check {
                if boss_may_die(&board, Color::Red) {
                    first_check = false;
                } else {
                    break;
                }
            }

            if matches!(
                compare_board_value(board, lose2, Color::Red, rule),
                Ok(std::cmp::Ordering::Equal)
            ) {
                pool.raw_mut().insert(board.to_invariant_u64(Color::Red));
            }
        }
    }
}

fn get_canonical_bits(nums: &[usize]) -> u16 {
    fn _get_shape(nums: &[usize]) -> (usize, usize, usize, usize) {
        let (mut hmin, mut hmax, mut vmin, mut vmax) = (3, 0, 3, 0);
        for n in nums {
            let (h, v) = (n % 4, n / 4);
            hmin = hmin.min(h);
            hmax = hmax.max(h);
            vmin = vmin.min(v);
            vmax = vmax.max(v);
        }
        (hmin, hmax, vmin, vmax)
    }

    let (hmin, hmax, vmin, vmax) = _get_shape(nums);
    let idx_shift = hmin + 4 * vmin;
    let aligned: Vec<usize> = nums.iter().map(|n| n - idx_shift).collect();
    let (hsize, vsize) = (hmax - hmin + 1, vmax - vmin + 1);

    fn _nums_to_bits(nums: impl Iterator<Item = usize>) -> u16 {
        let mut bits = 0;
        for n in nums {
            bits |= 1_u16 << n;
        }
        bits
    }

    let mapper = PositionMapper::try_create(vsize, hsize).unwrap();
    (0..8)
        .map(|idx| _nums_to_bits(aligned.iter().map(|n| mapper.map(idx, *n))))
        .min()
        .unwrap()
}

fn find_all_bits(num_doves: usize) -> HashSet<u16> {
    fn _calc_adjacents(bits: u16) -> u16 {
        let mut adj = (bits << 4) | (bits >> 4);
        let center = adj | bits;
        adj |= (center & 0xeeee) >> 1;
        adj |= (center & 0x7777) << 1;
        adj
    }

    fn _is_isolated(bits: u16) -> bool {
        bits & _calc_adjacents(bits) != bits
    }

    let mut all_bits = HashSet::new();
    for v_idx in (0..16).combinations(num_doves) {
        let bits = get_canonical_bits(&v_idx);
        if _is_isolated(bits) {
            continue;
        }
        all_bits.insert(bits);
    }
    all_bits
}

fn find_all_lose2(num_doves: usize, rule: GameRule, num_thread: usize) -> BoardSet {
    println!("[Thread Main] #doves={}, #cores={}", num_doves, num_thread);

    let all_bits = Arc::new(find_all_bits(num_doves).into_iter().collect_vec());
    let len_pack = (all_bits.len() + num_thread - 1) / num_thread;

    let mut handlers = Vec::with_capacity(num_thread);
    for i in 0..num_thread {
        let all_bits = Arc::clone(&all_bits);
        handlers.push(thread::spawn(move || {
            let mut pool = BoardSet::new();
            let begin = all_bits.len().min(i * len_pack);
            let end = all_bits.len().min((i + 1) * len_pack);
            let num_total = end - begin + 1;

            println!("[Thread {i}] started! Total={num_total}");

            for (count, bits) in all_bits[begin..end].iter().enumerate() {
                pack_lose2(&mut pool, *bits, rule);
                if (count + 1) % 10 == 0 || count + 1 == num_total {
                    println!(
                        "[Thread {i}] {} from {num_total} ({}%)",
                        count + 1,
                        ((count + 1) as f32) / (num_total as f32) * 100_f32
                    );
                }
            }
            println!("[Thread {i}] finished!");
            pool
        }))
    }

    let results: Vec<BoardSet> = handlers.into_iter().map(|x| x.join().unwrap()).collect();

    let mut capacity = Capacity::new();
    for set in results.iter() {
        capacity += set.capacity();
    }

    let mut lose2_set = BoardSet::with_capacity(capacity);
    for result in results {
        lose2_set.absorb(result);
    }
    println!("[Thread Main] Concatenated");
    println!("Total={}", lose2_set.len());
    lose2_set
}

// ****************************************************************
//  Main
// ****************************************************************
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 4 {
        panic!("give more than 3 arguments");
    }
    let num_doves: usize = args[1].parse()?;
    let num_thread: usize = args[2].parse()?;
    let path = std::path::Path::new(args[3].as_str());
    let rule = GameRule::new(true);

    let fs = std::fs::File::create(path)?;

    let lose2_set = find_all_lose2(num_doves, rule, num_thread);

    // *** SAVE ***
    lose2_set.save(fs)?;
    println!("Saved to {:?}", path);
    Ok(())
}
