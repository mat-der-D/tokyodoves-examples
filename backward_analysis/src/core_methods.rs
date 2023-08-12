pub(crate) mod filter_maker;
pub(crate) mod hashutil;

use std::{collections::HashMap, ffi::OsString, path::PathBuf, sync::Arc};

use filter_maker::*;
use hashutil::*;
use tokyodoves::{analysis::*, collections::*, game::*, *};

use crate::{distributed_path, dove_dir};

fn load_win_filter<F>(
    win_paths: &[impl AsRef<std::path::Path>],
    filter: F,
) -> std::io::Result<BoardSet>
where
    F: Fn(&u64) -> bool,
{
    let mut capacity = Capacity::new();
    for path in win_paths.iter() {
        capacity += BoardSet::required_capacity_filter(std::fs::File::open(path)?, &filter);
    }
    let mut set = BoardSet::with_capacity(capacity);
    for path in win_paths.iter() {
        set.load_filter(std::fs::File::open(path)?, &filter)?;
    }
    Ok(set)
}

pub fn trim_simply(
    src_dir: impl AsRef<std::path::Path>,
    dst_dir: impl AsRef<std::path::Path>,
    win_paths: Vec<PathBuf>,
    num_processes: usize,
) -> anyhow::Result<()> {
    let src_dir = Arc::new(src_dir.as_ref().to_owned());
    let dst_dir = Arc::new(dst_dir.as_ref().to_owned());
    let win_paths = Arc::new(win_paths);

    let mut handlers = Vec::new();
    for i in 0..num_processes {
        let src_dir = src_dir.clone();
        let dst_dir = dst_dir.clone();
        let win_paths = win_paths.clone();
        handlers.push(std::thread::spawn(move || {
            println!("[Thread {i}] started");
            let src_path = distributed_path(src_dir.as_ref(), i);
            let dst_path = distributed_path(dst_dir.as_ref(), i);

            let original = BoardSet::new_from_file(&src_path).expect("new from file error");
            let trimmed = thin_out_set_no_action(original, win_paths.as_ref()).expect("trim error");
            trimmed
                .save(std::fs::File::create(dst_path).expect("create error"))
                .expect("save error");
            println!("[Thread {i}] finished");
        }))
    }
    handlers.into_iter().for_each(|x| x.join().unwrap());
    Ok(())
}

fn thin_out_set_no_action(
    target: BoardSet,
    win_paths: &[impl AsRef<std::path::Path>],
) -> anyhow::Result<BoardSet> {
    let mut trimmed = target;
    for path in win_paths.iter() {
        for hash in LazyRawBoardLoader::new(std::fs::File::open(path)?) {
            trimmed.raw_mut().remove(&hash);
        }
    }
    Ok(trimmed)
}

pub fn trim_on_action(
    src_dir: impl AsRef<std::path::Path>,
    dst_dir: impl AsRef<std::path::Path>,
    num_doves_from: usize,
    num_doves_to: usize,
    win_paths: Vec<PathBuf>,
    num_processes: usize,
) -> anyhow::Result<()> {
    let src_dir = Arc::new(src_dir.as_ref().to_owned());
    let dst_dir = Arc::new(dst_dir.as_ref().to_owned());
    let win_paths = Arc::new(win_paths);

    let mut handlers = Vec::new();
    for i in 0..num_processes {
        let src_dir = src_dir.clone();
        let dst_dir = dst_dir.clone();
        let win_paths = win_paths.clone();
        handlers.push(std::thread::spawn(move || {
            println!("[Thread {i}] started");
            let src_path = distributed_path(src_dir.as_ref(), i);
            let dst_path = distributed_path(dst_dir.as_ref(), i);

            let original = BoardSet::new_from_file(&src_path).expect("new from file error");
            let trimmed = thin_out_set(original, win_paths.as_ref(), num_doves_from, num_doves_to)
                .expect("trim error");
            trimmed
                .save(std::fs::File::create(dst_path).expect("create error"))
                .expect("save error");
            println!("[Thread {i}] finished");
        }))
    }
    handlers.into_iter().for_each(|x| x.join().unwrap());

    Ok(())
}

pub fn thin_out_set(
    target: BoardSet,
    win_paths: &[impl AsRef<std::path::Path>],
    num_doves_from: usize,
    num_doves_to: usize,
) -> anyhow::Result<BoardSet> {
    if !(2..=12).contains(&num_doves_from)
        || !(2..=12).contains(&num_doves_to)
        || num_doves_from.abs_diff(num_doves_to) >= 2
    {
        return Err(anyhow::anyhow!("invalid argument"));
    }

    let contains_put = num_doves_from < num_doves_to;
    let contains_move = num_doves_from == num_doves_to;
    let contains_remove = num_doves_from > num_doves_to;

    if num_doves_to <= 8 {
        println!("* [{num_doves_to}] ...");
        return Ok(thin_out_set_core(
            target,
            win_paths,
            |_| true,
            |_, _| true,
            |_| true,
            contains_put,
            contains_move,
            contains_remove,
        )?);
    }
    let mut trimmed = target;
    match num_doves_to {
        9 => {
            for num_doves in 3..=6 {
                println!("* [9] num_doves = {num_doves}");
                trimmed = thin_out_set_core(
                    trimmed,
                    win_paths,
                    make_target_filter_9(num_doves),
                    make_action_filter_9(num_doves_from, num_doves_to),
                    make_win_filter_9(num_doves),
                    contains_put,
                    contains_move,
                    contains_remove,
                )?;
            }
        }
        10 => {
            const BASE: u64 = 0b10_10_10_10_10_10 << 48;
            const fn mask(shift: usize) -> u64 {
                0b11 << (shift * 2 + 48)
            }

            // {}     => BASE
            // {0}    => BASE & !mask(0)
            // {0, 1} => BASE & !mask(0) & !mask(1)
            macro_rules! masked_base_array {
                ($({ $($num:expr),* }),*) => {
                    [
                        $( BASE $(& !mask($num))* ),*
                    ]
                };
            }

            const WIN_ONOFFS: [u64; 16] = masked_base_array![
                {}, {0}, {1}, {2}, {3}, {4},
                {0, 1}, {0, 2}, {0, 3}, {0, 4},
                {1, 2}, {1, 3}, {1, 4},
                {2, 3}, {2, 4},
                {3, 4}
            ];

            for win_onoff in WIN_ONOFFS.into_iter().map(OnOff::new) {
                println!("* [10] onoff = {win_onoff}");
                trimmed = thin_out_set_core(
                    trimmed,
                    win_paths,
                    make_target_filter_10(win_onoff),
                    make_action_filter_10(num_doves_from, num_doves_to),
                    make_win_filter_10(win_onoff),
                    contains_put,
                    contains_move,
                    contains_remove,
                )?;
            }
        }
        11 => {
            for n in 0..10 {
                let win_onoff = OnOff::new((0xfff ^ (1 << n)) << 48);
                println!("* [11] onoff = {win_onoff}");
                trimmed = thin_out_set_core(
                    trimmed,
                    win_paths,
                    make_target_filter_11(win_onoff),
                    make_action_filter_11(win_onoff),
                    make_win_filter_11(win_onoff),
                    contains_put,
                    contains_move,
                    contains_remove,
                )?;
            }
        }
        12 => {
            for dist in 1..=6 {
                println!("* [12] dist = {dist}");
                trimmed = thin_out_set_core(
                    trimmed,
                    win_paths,
                    make_target_filter_12(dist),
                    make_action_filter_12(dist),
                    make_win_filter_12(dist),
                    contains_put,
                    contains_move,
                    contains_remove,
                )?;
            }
        }
        _ => unreachable!(),
    }
    Ok(trimmed)
}

fn thin_out_set_core<FT, FA, FW>(
    target: BoardSet,
    win_paths: &[impl AsRef<std::path::Path>],
    target_filter: FT,
    action_filter: FA,
    win_filter: FW,
    contains_put: bool,
    contains_move: bool,
    contains_remove: bool,
) -> std::io::Result<BoardSet>
where
    FT: Fn(&u64) -> bool,
    FA: Fn(&Action, &u64) -> bool,
    FW: Fn(&u64) -> bool,
{
    let mut trimmed = BoardSet::with_capacity(target.capacity());
    let wins = load_win_filter(win_paths, win_filter)?;
    for h0 in target.into_raw() {
        if !target_filter(&h0) {
            trimmed.raw_mut().insert(h0);
        }
        let mut is_good_board = true;
        let b0 = BoardBuilder::from_u64(h0).build_unchecked();
        use Color::*;
        for a1 in b0.legal_actions(Red, contains_put, contains_move, contains_remove) {
            if !action_filter(&a1, &h0) {
                continue;
            }
            let b1 = b0.perform_unchecked_copied(a1);
            if !wins.raw().contains(&b1.to_invariant_u64(Green)) {
                is_good_board = false;
                break;
            }
        }
        if is_good_board {
            trimmed.raw_mut().insert(h0);
        }
    }
    trimmed.shrink_to_fit();
    Ok(trimmed)
}

fn count_doves_in_file(path: impl AsRef<std::path::Path>) -> std::io::Result<usize> {
    Ok(LazyBoardLoader::new(std::fs::File::open(path)?).count())
}

fn count_doves_in_dir(root: impl AsRef<std::path::Path>) -> std::io::Result<usize> {
    let mut count = 0;
    for entry in std::fs::read_dir(root)? {
        let path = entry?.path();
        if path.extension() != Some(&OsString::from("tdl")) {
            continue;
        }
        println!("Loading {path:?}");
        let count_local = count_doves_in_file(path)?;
        println!("Count = {count_local}");
        count += count_local;
    }
    Ok(count)
}

/// Gather all boards in files at `src_dir` and redistribute into `num_result_files` files
/// in `dst_dir`.
pub fn redistribute(
    src_dir: impl AsRef<std::path::Path>,
    dst_dir: impl AsRef<std::path::Path>,
    num_result_files: usize,
) -> anyhow::Result<()> {
    let total = count_doves_in_dir(&src_dir)?;
    let chunk = (total + num_result_files - 1) / num_result_files;
    println!("total = {total}");
    println!("chunk = {chunk}");

    let mut file_idx = 0;
    let mut set = BoardSet::new();

    macro_rules! save_set {
        () => {
            let dst_path = distributed_path(dst_dir.as_ref(), file_idx);
            println!("Saving to {dst_path:?} ...");
            set.save(std::fs::File::create(&dst_path)?)?;
            println!("Saved to {dst_path:?}");
            set.clear();
            file_idx += 1;
        };
    }

    for entry in std::fs::read_dir(&src_dir)? {
        let path = entry?.path();
        if path.extension() != Some(&OsString::from("tdl")) {
            continue;
        }

        println!("Loading {path:?} ...");
        let mut full_set = BoardSet::new_from_file(&path)?;
        println!("Loaded {path:?}");

        while !full_set.is_empty() {
            let tmp_set: BoardSet;
            (tmp_set, full_set) = full_set.split(chunk - set.len());
            set.reserve(tmp_set.capacity());
            set.absorb(tmp_set);
            if set.len() >= chunk {
                save_set!();
            }
        }
    }

    while file_idx < num_result_files {
        save_set!();
    }

    Ok(())
}

fn split_set_into(mut set: BoardSet, num: usize) -> Vec<BoardSet> {
    let chunk = (set.len() + num - 1) / num;
    let mut set_vec = Vec::with_capacity(num);
    for _ in 0..num {
        let tmp: BoardSet;
        (tmp, set) = set.split(chunk);
        set_vec.push(tmp);
    }
    set_vec
}

pub fn backstep(
    src_path: impl AsRef<std::path::Path>,
    dst_dir: impl AsRef<std::path::Path>,
    num_doves: usize,
    num_processes: usize,
    max_chunk_size: usize,
) -> anyhow::Result<()> {
    println!("Loading {:?} ...", src_path.as_ref());
    let mut full_set = BoardSet::new_from_file(&src_path)?;
    println!("Loaded {:?}", src_path.as_ref());

    let mut idx_chunk = 0;
    let mut load_next = true;
    while load_next {
        println!("*** idx_chunk = {idx_chunk} ***");
        let set: BoardSet;
        (set, full_set) = full_set.split(max_chunk_size);
        if set.len() < max_chunk_size {
            load_next = false;
        }

        let set_vec = Arc::new(split_set_into(set, num_processes));
        let mut handlers = Vec::new();
        for i in 0..num_processes {
            let set_vec = set_vec.clone();
            handlers.push(std::thread::spawn(move || {
                println!("[Thread {i}] started");
                let num_to_set = backstep_core(set_vec[i].iter(), num_doves);
                println!("[Thread {i}] finished");
                num_to_set
            }));
        }

        let vec_of_num_to_set: Vec<HashMap<usize, BoardSet>> =
            handlers.into_iter().map(|x| x.join().unwrap()).collect();

        println!("[Thread Main] calculating capacity ...");
        let mut capacity_map = HashMap::new();
        for num_to_set in vec_of_num_to_set.iter() {
            for (num, set) in num_to_set.iter() {
                *capacity_map.entry(*num).or_insert_with(Capacity::new) += set.capacity();
            }
        }
        println!("[Thread Main] calculated capacity");

        let mut num_to_set_all = HashMap::new();
        for (i, num_to_set) in vec_of_num_to_set.into_iter().enumerate() {
            for (num, set) in num_to_set {
                num_to_set_all
                    .entry(num)
                    .or_insert_with(|| BoardSet::with_capacity(capacity_map[&num].clone()))
                    .absorb(set);
            }
            println!("[Thread Main] concatenated {i}");
        }
        println!("[Thread Main] concatenated all");

        for (num, set) in num_to_set_all {
            let dst_path = dove_dir(dst_dir.as_ref(), num)
                .join(format!("from_{num_doves:0>2}_{idx_chunk:0>4}.tdl"));
            set.save(std::fs::File::create(dst_path)?)?;
        }
        idx_chunk += 1;
    }
    Ok(())
}

fn backstep_core(
    original: impl Iterator<Item = Board>,
    num_doves: usize,
) -> HashMap<usize, BoardSet> {
    use Color::*;
    let rule = GameRule::new(true);
    let mut num_to_set = HashMap::new();
    for n in (num_doves - 1).max(2)..=(num_doves + 1).min(12) {
        num_to_set.insert(n, BoardSet::new());
    }

    for b0 in original {
        for a1 in b0.legal_actions_bwd(Green, true, true, true) {
            let b1 = b0.perform_unchecked_copied(a1);
            if !matches!(
                compare_board_value(b1, BoardValue::MAX, Green, rule),
                Ok(std::cmp::Ordering::Less)
            ) {
                continue;
            }
            let n1 = b1.count_doves_on_field();
            let h1 = b1.to_invariant_u64(Green);
            num_to_set.get_mut(&n1).unwrap().raw_mut().insert(h1);
        }
    }
    num_to_set
}

pub fn gather(
    src_dir: impl AsRef<std::path::Path>,
    dst_path: impl AsRef<std::path::Path>,
) -> std::io::Result<()> {
    let entries = std::fs::read_dir(src_dir)?;
    let mut paths = Vec::new();
    for entry in entries {
        let path = entry?.path();
        if path.extension() != Some(&OsString::from("tdl")) {
            continue;
        }
        paths.push(path);
    }
    let mut capacity = Capacity::new();
    for path in paths.iter() {
        capacity += BoardSet::required_capacity(std::fs::File::open(path)?);
    }
    let mut set = BoardSet::with_capacity(capacity);
    for path in paths {
        set.load(std::fs::File::open(path)?)?;
    }
    set.save(std::fs::File::create(dst_path)?)?;
    Ok(())
}
