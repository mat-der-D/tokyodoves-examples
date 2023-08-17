pub(crate) mod filter_maker;
pub(crate) mod hashutil;

use std::{
    collections::HashMap,
    ffi::OsString,
    path::{Path, PathBuf},
    sync::Arc,
};

use filter_maker::*;
use tokyodoves::{collections::*, *};

use crate::path_factory::*;

// =====================================================================
//  Helper Functions
// =====================================================================
fn load_files(paths: &[impl AsRef<std::path::Path>]) -> std::io::Result<BoardSet> {
    println!("Estimating required capacity ...");
    let mut capacity = Capacity::new();
    for path in paths.iter() {
        println!("Searching win at {:?} ...", path.as_ref());
        capacity += BoardSet::required_capacity(std::fs::File::open(path)?);
        println!("Searched win at {:?}", path.as_ref());
    }
    let mut set = BoardSet::with_capacity(capacity);
    println!("Prepared a set with required capacity");

    for path in paths.iter() {
        println!("Loading win at {:?} ...", path.as_ref());
        set.load(std::fs::File::open(path).expect("open error!"))?;
        println!("Loaded win at {:?}", path.as_ref());
    }
    Ok(set)
}

fn load_files_with_filter<F>(
    paths: &[impl AsRef<std::path::Path>],
    filter: F,
) -> std::io::Result<BoardSet>
where
    F: Fn(&u64) -> bool,
{
    println!("Estimating required capacity ...");
    let mut capacity = Capacity::new();
    for path in paths.iter() {
        println!("Searching win at {:?} ...", path.as_ref());
        capacity += BoardSet::required_capacity_filter(std::fs::File::open(path)?, &filter);
        println!("Searched win at {:?}", path.as_ref());
    }
    let mut set = BoardSet::with_capacity(capacity);
    println!("Prepared a set with required capacity");

    for path in paths.iter() {
        println!("Loading win at {:?} ...", path.as_ref());
        set.load_filter(std::fs::File::open(path)?, &filter)?;
        println!("Loaded win at {:?}", path.as_ref());
    }
    Ok(set)
}

fn is_win1_or_finished(board: Board, player: Color) -> bool {
    if !matches!(board.surrounded_status(), SurroundedStatus::None) {
        return true;
    }
    board
        .legal_actions(player, true, true, true)
        .into_iter()
        .map(|a1| board.perform_unchecked_copied(a1))
        .any(|b1| matches!(b1.surrounded_status(), SurroundedStatus::OneSide(p) if p != player))
}

// =====================================================================
//  Backstep
// =====================================================================
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
        if full_set.is_empty() {
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
    let mut num_to_set = HashMap::new();
    for n in (num_doves - 1).max(2)..=(num_doves + 1).min(12) {
        num_to_set.insert(n, BoardSet::new());
    }

    for b0 in original {
        for a1 in b0.legal_actions_bwd(Green, true, true, true) {
            let b1 = b0.perform_unchecked_copied(a1);
            if is_win1_or_finished(b1, Green) {
                continue;
            }
            let n1 = b1.count_doves_on_field();
            let h1 = b1.to_invariant_u64(Green);
            num_to_set.get_mut(&n1).unwrap().raw_mut().insert(h1);
        }
    }
    num_to_set
}

// =====================================================================
//  Redistribute
// =====================================================================
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

// =====================================================================
//  Trim Simply
// =====================================================================
pub fn trim_simply(
    src_dir: impl AsRef<std::path::Path>,
    dst_dir: impl AsRef<std::path::Path>,
    win_paths: Vec<PathBuf>,
    num_processes: usize,
    parallel_chunk: usize,
) -> anyhow::Result<()> {
    let src_dir = Arc::new(src_dir.as_ref().to_owned());
    let dst_dir = Arc::new(dst_dir.as_ref().to_owned());
    let win_paths = Arc::new(win_paths);

    for i0 in (0..num_processes).step_by(parallel_chunk) {
        let mut handlers = Vec::new();
        for i_add in 0..parallel_chunk {
            let i = i0 + i_add;
            let src_dir = src_dir.clone();
            let dst_dir = dst_dir.clone();
            let win_paths = win_paths.clone();
            handlers.push(std::thread::spawn(move || {
                println!("[Thread {i}] started");
                let src_path = distributed_path(src_dir.as_ref(), i);
                let dst_path = distributed_path(dst_dir.as_ref(), i);

                let mut target = BoardSet::new_from_file(src_path).expect("new from file error");
                thin_out_set(&mut target, win_paths.as_ref()).expect("trim error");
                target.shrink_to_fit();
                target
                    .save(std::fs::File::create(dst_path).expect("create error"))
                    .expect("save error");
                println!("[Thread {i}] finished");
            }));
        }
        handlers.into_iter().for_each(|x| x.join().unwrap());
    }

    Ok(())
}

fn thin_out_set(
    target: &mut BoardSet,
    win_paths: &[impl AsRef<std::path::Path>],
) -> anyhow::Result<()> {
    for path in win_paths.iter() {
        for hash in LazyRawBoardLoader::new(std::fs::File::open(path)?) {
            target.raw_mut().remove(&hash);
        }
    }
    Ok(())
}

// =====================================================================
//  Trim On Action
// =====================================================================
pub fn trim_on_action<P>(
    num_doves_win: usize,
    num_step_to: usize,
    factory: &PathFactory<P>,
    num_processes: usize,
    nums_doves_to_split_win_if_possible: &[usize],
) -> anyhow::Result<()>
where
    P: AsRef<Path>,
{
    if !(2..=12).contains(&num_doves_win) {
        return Err(anyhow::anyhow!("invalid num_doves_win"));
    }

    std::fs::create_dir_all(factory.trimmed_remove(num_step_to))?;
    std::fs::create_dir_all(factory.trimmed_move(num_step_to))?;
    std::fs::create_dir_all(factory.trimmed_put(num_step_to))?;

    let (sets_array, dst_dirs) = match num_doves_win {
        x if !nums_doves_to_split_win_if_possible.contains(&x) || matches!(x, 2..=9 | 12) => {
            let win_paths = factory.win_paths(num_step_to - 1, num_doves_win);
            let wins = load_files(&win_paths)?;
            create_three_thinned_sets(
                factory,
                |_| true,
                wins,
                num_doves_win,
                num_step_to,
                num_processes,
            )?
        }
        10 => {
            let win_paths = factory.win_paths(num_step_to - 1, num_doves_win);
            let mut sets_array_tmp = {
                let mut array: [Vec<BoardSet>; 3] = Default::default();
                for elem in array.iter_mut() {
                    *elem = (0..num_processes).map(|_| BoardSet::new()).collect()
                }
                array
            };
            let mut dst_dirs_tmp: [PathBuf; 3] = Default::default();
            for level in 0..=1 {
                let wins = load_files_with_filter(&win_paths, make_win_filter_10(level))?;
                let (sets_array_new, dst_dirs) = create_three_thinned_sets(
                    factory,
                    make_target_filter_10(level),
                    wins,
                    num_doves_win,
                    num_step_to,
                    num_processes,
                )?;
                for (tmp, new) in sets_array_tmp.iter_mut().zip(sets_array_new.into_iter()) {
                    for (tmp_elem, new_elem) in tmp.iter_mut().zip(new.into_iter()) {
                        tmp_elem.absorb(new_elem);
                    }
                }
                dst_dirs_tmp = dst_dirs;
            }
            (sets_array_tmp, dst_dirs_tmp)
        }
        11 => {
            let win_paths = factory.win_paths(num_step_to - 1, num_doves_win);
            let mut sets_array_tmp: [Vec<BoardSet>; 3] = {
                let mut array: [Vec<BoardSet>; 3] = Default::default();
                for elem in array.iter_mut() {
                    *elem = (0..num_processes).map(|_| BoardSet::new()).collect()
                }
                array
            };
            let mut dst_dirs_tmp: [PathBuf; 3] = Default::default();
            for level in 0..=1 {
                let wins = load_files_with_filter(&win_paths, make_win_filter_11(level))?;
                let (sets_array_new, dst_dirs) = create_three_thinned_sets(
                    factory,
                    make_target_filter_11(level),
                    wins,
                    num_doves_win,
                    num_step_to,
                    num_processes,
                )?;
                for (tmp, new) in sets_array_tmp.iter_mut().zip(sets_array_new.into_iter()) {
                    for (tmp_elem, new_elem) in tmp.iter_mut().zip(new.into_iter()) {
                        tmp_elem.absorb(new_elem);
                    }
                }
                dst_dirs_tmp = dst_dirs;
            }
            (sets_array_tmp, dst_dirs_tmp)
        }
        _ => unreachable!(),
    };

    println!("Saving ...");
    for (n, (sets, dst_dir)) in sets_array.into_iter().zip(dst_dirs.into_iter()).enumerate() {
        match (n, num_doves_win) {
            (0, 2) => {
                println!("Skipped");
                continue;
            }
            (2, 12) => {
                let src_dir = dove_dir(factory.trimmed_move(num_step_to), 12);
                let dst_dir = dove_dir(factory.trimmed_put(num_step_to), 12);
                std::fs::create_dir_all(&dst_dir)?;

                for i in 0..num_processes {
                    let src_path = distributed_path(&src_dir, i);
                    let dst_path = distributed_path(&dst_dir, i);
                    std::fs::copy(src_path, dst_path)?;
                }
                continue;
            }
            _ => {
                std::fs::create_dir_all(&dst_dir)?;
                for (i, set) in sets.into_iter().enumerate() {
                    let dst_path = distributed_path(&dst_dir, i);
                    println!("Saving to {dst_path:?} ...");
                    set.save(std::fs::File::create(&dst_path)?)?;
                    println!("Saved to {dst_path:?}");
                }
            }
        }
    }
    println!("Saved all");
    Ok(())
}

fn create_three_thinned_sets<P, FT>(
    factory: &PathFactory<P>,
    target_filter: FT,
    wins: BoardSet,
    num_doves_win: usize,
    num_target_step: usize,
    num_processes: usize,
) -> std::io::Result<([Vec<BoardSet>; 3], [PathBuf; 3])>
where
    P: AsRef<Path>,
    FT: Fn(&u64) -> bool + Send + Sync + 'static + Clone,
{
    let wins = Arc::new(wins);

    // move -> put
    println!("*** move -> put ***");
    let (sets0, dst_dir0) = if num_doves_win == 2 {
        // copy 2 as start -> remove
        let src_dir = dove_dir(factory.trimmed_simply(num_target_step), num_doves_win);
        let dst_dir = dove_dir(factory.trimmed_remove(num_target_step), num_doves_win);
        std::fs::create_dir_all(&dst_dir)?;

        for i in 0..num_processes {
            let src_path = distributed_path(&src_dir, i);
            let dst_path = distributed_path(&dst_dir, i);
            std::fs::copy(src_path, dst_path)?;
        }

        (Vec::new(), PathBuf::default()) // dummy
    } else {
        let num_doves_target = num_doves_win - 1;
        let src_dir = dove_dir(factory.trimmed_move(num_target_step), num_doves_target);
        let dst_dir = dove_dir(factory.trimmed_put(num_target_step), num_doves_target);

        (
            create_thinned_set_parallel(
                &src_dir,
                target_filter.clone(),
                wins.clone(),
                true,
                false,
                false,
                num_processes,
            )?,
            dst_dir,
        )
    };

    // remove -> move
    println!("*** remove -> move ***");
    let (sets1, dst_dir1) = {
        let num_doves_target = num_doves_win;
        let src_dir = dove_dir(factory.trimmed_remove(num_target_step), num_doves_target);
        let dst_dir = dove_dir(factory.trimmed_move(num_target_step), num_doves_target);

        (
            create_thinned_set_parallel(
                &src_dir,
                target_filter.clone(),
                wins.clone(),
                false,
                true,
                false,
                num_processes,
            )?,
            dst_dir,
        )
    };

    // start -> remove
    println!("*** start -> remove ***");
    let (sets2, dst_dir2) = if num_doves_win == 12 {
        (Vec::new(), PathBuf::default()) // dummy
    } else {
        let num_doves_target = num_doves_win + 1;
        let src_dir = dove_dir(factory.trimmed_simply(num_target_step), num_doves_target);
        let dst_dir = dove_dir(factory.trimmed_remove(num_target_step), num_doves_target);

        (
            create_thinned_set_parallel(
                &src_dir,
                target_filter,
                wins.clone(),
                false,
                false,
                true,
                num_processes,
            )?,
            dst_dir,
        )
    };

    Ok(([sets0, sets1, sets2], [dst_dir0, dst_dir1, dst_dir2]))
}

fn create_thinned_set_parallel<FT>(
    src_dir: impl AsRef<Path>,
    target_filter: FT,
    wins: Arc<BoardSet>,
    contains_put: bool,
    contains_move: bool,
    contains_remove: bool,
    num_processes: usize,
) -> std::io::Result<Vec<BoardSet>>
where
    FT: Fn(&u64) -> bool + Send + Sync + 'static + Clone,
{
    fn parallel_run(
        target: &mut Vec<BoardSet>,
        src_paths: &[impl AsRef<std::path::Path>],
        core_process: impl Fn(std::path::PathBuf) -> BoardSet + Send + Sync + 'static,
    ) {
        let core_process = Arc::new(core_process);
        let mut handlers = Vec::new();
        for (i, src_path) in src_paths.iter().enumerate() {
            let core_process = core_process.clone();
            let src_path = src_path.as_ref().to_owned();
            handlers.push(std::thread::spawn(move || {
                println!("[Thread {i}] start");
                let result = core_process(src_path);
                println!("[Thread {i}] finish");
                result
            }));
        }
        let new_sets = handlers.into_iter().map(|x| x.join().unwrap());
        for (n, (s, new)) in target.iter_mut().zip(new_sets).enumerate() {
            println!("[Thread Main] Absorbing {n} ...");
            s.reserve(new.capacity());
            s.absorb(new);
            println!("[Thread Main] Absorbed {n}");
        }
    }

    let mut trimmed_sets: Vec<BoardSet> = (0..num_processes).map(|_| BoardSet::new()).collect();
    let src_paths: Vec<PathBuf> = (0..num_processes)
        .map(|i| distributed_path(src_dir.as_ref(), i))
        .collect();

    parallel_run(&mut trimmed_sets, &src_paths, move |src_path| {
        create_thinned_set_core(
            src_path,
            target_filter.clone(),
            &wins,
            contains_put,
            contains_move,
            contains_remove,
        )
        .expect("thinning error")
    });
    Ok(trimmed_sets)
}

fn create_thinned_set_core<FT>(
    src_path: impl AsRef<std::path::Path>,
    target_filter: FT,
    wins: &BoardSet,
    contains_put: bool,
    contains_move: bool,
    contains_remove: bool,
) -> std::io::Result<BoardSet>
where
    FT: Fn(&u64) -> bool,
{
    let filter = |&h0: &u64| {
        if !target_filter(&h0) {
            return false;
        }
        let b0 = BoardBuilder::from_u64(h0).build_unchecked();
        use Color::*;
        for a1 in b0.legal_actions(Red, contains_put, contains_move, contains_remove) {
            let b1 = b0.perform_unchecked_copied(a1);
            if is_win1_or_finished(b1, Green) {
                continue;
            }

            if !wins.raw().contains(&b1.to_invariant_u64(Green)) {
                return false;
            }
        }
        true
    };
    let mut set = BoardSet::new();
    set.raw_mut()
        .load_filter(std::fs::File::open(src_path)?, filter)?;
    Ok(set)
}

// =====================================================================
//  Gather
// =====================================================================
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
        println!("Loading {path:?} ...");
        set.load(std::fs::File::open(&path)?)?;
        println!("Loaded {path:?}");
    }
    println!("Saving to {:?} ...", dst_path.as_ref());
    set.save(std::fs::File::create(&dst_path)?)?;
    println!("Saved to {:?}", dst_path.as_ref());
    Ok(())
}
