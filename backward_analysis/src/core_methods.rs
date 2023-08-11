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

            let capacity =
                BoardSet::required_capacity(std::fs::File::open(&src_path).expect("open error"));
            let mut original = BoardSet::with_capacity(capacity);
            original
                .load(std::fs::File::open(&src_path).expect("open error"))
                .expect("load error");
            let trimmed = thin_out_set_no_action(original, win_paths.as_ref()).expect("trim error");
            trimmed
                .save(std::fs::File::create(dst_path).expect("create error"))
                .expect("save error");
            println!("[Thread {i}] finished");
        }))
    }

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

            let capacity =
                BoardSet::required_capacity(std::fs::File::open(&src_path).expect("open error"));
            let mut original = BoardSet::with_capacity(capacity);
            original
                .load(std::fs::File::open(&src_path).expect("open error"))
                .expect("load error");
            let trimmed = thin_out_set(original, win_paths.as_ref(), num_doves_from, num_doves_to)
                .expect("trim error");
            trimmed
                .save(std::fs::File::create(dst_path).expect("create error"))
                .expect("save error");
            println!("[Thread {i}] finished");
        }))
    }

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

    if num_doves_to <= 8 {
        return Ok(thin_out_set_core(
            target,
            win_paths,
            |_| true,
            |_, _| true,
            |_| true,
        )?);
    }
    let mut trimmed = target;
    match num_doves_to {
        9 => {
            for num_doves in 3..=6 {
                trimmed = thin_out_set_core(
                    trimmed,
                    win_paths,
                    make_target_filter_9(num_doves),
                    make_action_filter_9(num_doves_from, num_doves_to),
                    make_win_filter_9(num_doves),
                )?;
            }
        }
        10 => {
            macro_rules! onoff_except {
                ($($num:expr),*) => {
                    (0b10_10_10_10_10_10_u64 << 48) $(& !(1 << (2 * $num + 49)) )*
                }
            }

            macro_rules! onoff_except_array {
                ($({$($num:expr),*}),*) => {
                    [
                        $(onoff_except!($($num),*)),*
                    ]
                };
            }

            let win_onoffs = onoff_except_array![
                {}, {0}, {1}, {2}, {3}, {4},
                {0, 1}, {0, 2}, {0, 3}, {0, 4},
                {1, 2}, {1, 3}, {1, 4},
                {2, 3}, {2, 4},
                {3, 4}
            ];

            for win_onoff in win_onoffs.into_iter().map(OnOff::new) {
                trimmed = thin_out_set_core(
                    trimmed,
                    win_paths,
                    make_target_filter_10(win_onoff),
                    make_action_filter_10(num_doves_from, num_doves_to),
                    make_win_filter_10(win_onoff),
                )?;
            }
        }
        11 => {
            for n in 0..10 {
                let win_onoff = OnOff::new((0xfff ^ (1 << n)) << 48);
                trimmed = thin_out_set_core(
                    trimmed,
                    win_paths,
                    make_target_filter_11(win_onoff),
                    make_action_filter_11(win_onoff),
                    make_win_filter_11(win_onoff),
                )?;
            }
        }
        12 => {
            for dist in 1..=6 {
                trimmed = thin_out_set_core(
                    trimmed,
                    win_paths,
                    make_target_filter_12(dist),
                    make_action_filter_12(dist),
                    make_win_filter_12(dist),
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
        for a1 in b0.legal_actions(Red, true, true, true) {
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

fn count_doves_in_dir(root: impl AsRef<std::path::Path>) -> std::io::Result<usize> {
    let mut count = 0;
    for entry in std::fs::read_dir(root)? {
        let path = entry?.path();
        if path.extension() != Some(&OsString::from("tdl")) {
            continue;
        }
        println!("Loading {path:?}");
        let count_local = LazyBoardLoader::new(std::fs::File::open(path)?).count();
        println!("Count = '{count_local}'");
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
            set.save(std::fs::File::create(&dst_path)?)?;
            println!("Saved to {dst_path:?}");
        };
    }

    for entry in std::fs::read_dir(&src_dir)? {
        let path = entry?.path();
        if path.extension() != Some(&OsString::from("tdl")) {
            continue;
        }
        for hash in LazyRawBoardLoader::new(std::fs::File::open(path)?) {
            set.raw_mut().insert(hash);

            if set.len() >= chunk {
                save_set!();
                set.clear();
                file_idx += 1;
            }
        }
    }
    if !set.is_empty() {
        save_set!();
    }
    Ok(())
}

fn split_set_into(set: BoardSet, num: usize) -> Vec<BoardSet> {
    let chunk = (set.len() + num - 1) / num;
    let mut iter = set.into_iter();
    (0..num)
        .map(|_| (&mut iter).take(chunk).collect())
        .collect()
}

pub fn backstep(
    src_path: impl AsRef<std::path::Path>,
    dst_dir: impl AsRef<std::path::Path>,
    num_doves: usize,
    num_processes: usize,
    max_chunk_size: usize, // 500_000_000 is recommended
) -> anyhow::Result<()> {
    let mut loader = LazyBoardLoader::new(std::fs::File::open(src_path)?);

    let mut idx_chunk = 0;
    let mut load_next = true;
    while load_next {
        println!("*** idx_chunk = {idx_chunk} ***");
        let set: BoardSet = (&mut loader).take(max_chunk_size).collect();
        match set.len() {
            0 => break,
            n if n < max_chunk_size => load_next = false,
            _ => (),
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

        let vec_of_num_to_set = handlers
            .into_iter()
            .map(|x| x.join().unwrap())
            .collect::<Vec<_>>();

        let mut capacity_map = HashMap::new();
        for num_to_set in vec_of_num_to_set.iter() {
            for (num, set) in num_to_set.iter() {
                *capacity_map.entry(*num).or_insert_with(Capacity::new) += set.capacity();
            }
        }
        let mut num_to_set_all = HashMap::new();
        for num_to_set in vec_of_num_to_set {
            for (num, set) in num_to_set {
                num_to_set_all
                    .entry(num)
                    .or_insert_with(|| BoardSet::with_capacity(capacity_map[&num].clone()))
                    .absorb(set);
            }
        }
        println!("[Thread Main] concatenated");

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
    for n in (num_doves - 1)..=(num_doves + 1) {
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
