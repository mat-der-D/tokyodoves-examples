pub(crate) mod core_methods;
pub(crate) mod path_factory;

use clap::Parser;
use path_factory::*;
use std::path::{Path, PathBuf};

use crate::core_methods::trim_on_action;

// **********************************************************
//  Building Blocks
// **********************************************************
fn x_to_x_common<P>(
    factory: &PathFactory<P>,
    num_from: usize,
    num_processes: usize,
    del_tmp_files: bool,
) -> anyhow::Result<()>
where
    P: AsRef<Path>,
{
    let num_to = num_from + 1;

    // --- BackStep ---
    println!("### PHASE: BACKSTEP ###");
    for num_doves in 2..=12 {
        std::fs::create_dir_all(dove_dir(factory.backstepped(num_to), num_doves))?;
    }

    for num_doves in 2..=12 {
        println!("=== num_doves={num_doves} ===");
        let src_path = factory
            .num_dir(num_from)
            .join(format!("{num_doves:0>2}.tdl"));
        let dst_dir = factory.backstepped(num_to);
        core_methods::backstep(src_path, dst_dir, num_doves, num_processes, 400_000_000)?;
    }

    // --- Redistribute ---
    println!("### PHASE: REDISTRIBUTE ###");
    for num_doves in 2..=12 {
        println!("=== num_doves={num_doves} ===");
        let src_dir = dove_dir(factory.backstepped(num_to), num_doves);
        let dst_dir = dove_dir(factory.redistributed(num_to), num_doves);
        std::fs::create_dir_all(&dst_dir)?;
        core_methods::redistribute(&src_dir, dst_dir, num_processes)?;
        if del_tmp_files {
            std::fs::remove_dir_all(src_dir)?;
        }
    }
    if del_tmp_files {
        std::fs::remove_dir_all(factory.backstepped(num_to))?;
    }

    // --- Trim Simple ---
    println!("### PHASE: TRIM SIMPLE ###");
    for num_doves in 2..=12 {
        println!("=== num_doves={num_doves} ===");
        let src_dir = dove_dir(factory.redistributed(num_to), num_doves);
        let dst_dir = dove_dir(factory.trimmed_simply(num_to), num_doves);
        std::fs::create_dir_all(&dst_dir)?;
        let win_paths = factory.win_paths(num_from, num_doves);
        core_methods::trim_simply(&src_dir, dst_dir, win_paths, num_processes, num_processes)?;
        if del_tmp_files {
            std::fs::remove_dir_all(src_dir)?;
        }
    }

    if del_tmp_files {
        std::fs::remove_dir_all(factory.redistributed(num_to))?;
    }
    Ok(())
}

fn lose_to_win<P>(
    factory: &PathFactory<P>,
    num_from: usize,
    num_processes: usize,
    del_tmp_files: bool,
) -> anyhow::Result<()>
where
    P: AsRef<Path>,
{
    let num_to = num_from + 1;

    // x_to_x_common
    x_to_x_common(factory, num_from, num_processes, del_tmp_files)?;

    // Gather
    println!("### PHASE: GATHER ###");
    std::fs::create_dir_all(factory.num_dir(num_to))?;
    for num_doves in 2..=12 {
        println!("=== num_doves={num_doves} ===");
        core_methods::gather(
            dove_dir(factory.trimmed_simply(num_to), num_doves),
            factory.num_dir(num_to).join(format!("{num_doves:0>2}.tdl")),
        )?;
    }

    if del_tmp_files {
        std::fs::remove_dir_all(factory.num_tmp_dir(num_to))?;
    }
    Ok(())
}

fn win_to_lose<P>(
    factory: &PathFactory<P>,
    num_from: usize,
    num_processes: usize,
    del_tmp_files: bool,
    nums_doves_to_split_win_if_possible: &[usize],
) -> anyhow::Result<()>
where
    P: AsRef<Path>,
{
    let num_to = num_from + 1;

    // x_to_x_common
    x_to_x_common(factory, num_from, num_processes, del_tmp_files)?;

    println!("### PHASE: TRIM ON ACTION ###");
    for num_doves_of_wins in 2..=12 {
        println!("[num_doves_of_wins = {num_doves_of_wins}]");
        trim_on_action(
            num_doves_of_wins,
            num_to,
            factory,
            num_processes,
            nums_doves_to_split_win_if_possible,
        )?;
    }
    if del_tmp_files {
        std::fs::remove_dir_all(factory.trimmed_simply(num_to))?;
        std::fs::remove_dir_all(factory.trimmed_remove(num_to))?;
        std::fs::remove_dir_all(factory.trimmed_move(num_to))?;
    }

    // Gather
    println!("### PHASE: GATHER ###");
    std::fs::create_dir_all(factory.num_dir(num_to))?;
    for num_doves in 2..=12 {
        println!("=== num_doves={num_doves} ===");
        core_methods::gather(
            dove_dir(factory.trimmed_put(num_to), num_doves),
            factory.num_dir(num_to).join(format!("{num_doves:0>2}.tdl")),
        )?;
    }
    if del_tmp_files {
        std::fs::remove_dir_all(factory.num_tmp_dir(num_to))?;
    }
    Ok(())
}

// **********************************************************
//  Main Part
// **********************************************************
fn advance_one_step(
    root: impl AsRef<Path>,
    num_from: usize,
    num_processes: usize,
    del_tmp_files: bool,
    nums_doves_to_split_win_if_possible: &[usize],
) -> anyhow::Result<()> {
    let factory = PathFactory::new(root);
    match num_from {
        0 | 1 => return Err(anyhow::anyhow!("invalid num_from")),
        n => match n % 2 {
            0 => lose_to_win(&factory, num_from, num_processes, del_tmp_files)?,
            1 => win_to_lose(
                &factory,
                num_from,
                num_processes,
                del_tmp_files,
                nums_doves_to_split_win_if_possible,
            )?,
            _ => unreachable!(),
        },
    }
    println!("Finished all process!");
    Ok(())
}

#[derive(clap::Parser)]
#[clap(
    name = "Tokyodoves Backward Analyzer",
    author = "Smooth Pudding",
    version = "v0.1.0",
    about = "Analyze the Tokyodoves Boards"
)]
struct Args {
    #[clap(short = 's', long)]
    src_dir: Option<String>,

    #[clap(short = 'n', long)]
    num_doves: usize,

    #[clap(short = 'p', long)]
    num_processes: usize,

    #[clap(long = "split", num_args = 0..=11)]
    split_nums_doves: Vec<usize>,

    #[clap(long = "del_tmp_files")]
    del_tmp_files: Option<bool>,
}

fn main() -> anyhow::Result<()> {
    let arg: Args = Args::parse();
    let root = PathBuf::from(
        arg.src_dir
            .unwrap_or(r"C:\Users\t_ish\Documents\dev\github\TokyoDovesData".to_owned()),
    );
    let num_from = arg.num_doves;
    let num_processes = arg.num_processes;
    let split = arg.split_nums_doves;
    let del_tmp_files = arg.del_tmp_files.unwrap_or(true);
    advance_one_step(root, num_from, num_processes, del_tmp_files, &split)?;
    Ok(())
}
