use std::{
    ffi::OsString,
    path::{Path, PathBuf},
};
pub mod core_methods;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct PathFactory<P: AsRef<Path>> {
    root: P,
}

impl<P> PathFactory<P>
where
    P: AsRef<Path>,
{
    fn new(root: P) -> Self {
        Self { root }
    }

    fn num_dir(&self, num_step: usize) -> PathBuf {
        self.root.as_ref().join(format!("{num_step:0>4}"))
    }

    fn num_tmp_dir(&self, num_step: usize) -> PathBuf {
        self.root.as_ref().join(format!("{num_step:0>4}_tmp"))
    }

    fn backstepped(&self, num_step: usize) -> PathBuf {
        self.num_tmp_dir(num_step).join("backstepped")
    }

    fn redistributed(&self, num_step: usize) -> PathBuf {
        self.num_tmp_dir(num_step).join("redistributed")
    }

    fn trimmed_simply(&self, num_step: usize) -> PathBuf {
        self.num_tmp_dir(num_step).join("trimmed_simply")
    }

    fn trimmed_move(&self, num_step: usize) -> PathBuf {
        self.num_tmp_dir(num_step).join("trimmed_move")
    }

    fn trimmed_put(&self, num_step: usize) -> PathBuf {
        self.num_tmp_dir(num_step).join("trimmed_put")
    }

    fn trimmed_remove(&self, num_step: usize) -> PathBuf {
        self.num_tmp_dir(num_step).join("trimmed_remove")
    }

    fn win_paths(&self, num_step_ceil: usize, num_doves: usize) -> Vec<PathBuf> {
        (3..=num_step_ceil)
            .step_by(2)
            .map(|n| self.num_dir(n).join(format!("{num_doves:0>2}.tdl")))
            .collect()
    }
}

fn dove_dir(parent: impl AsRef<Path>, num_doves: usize) -> PathBuf {
    parent.as_ref().join(format!("{num_doves:0>2}"))
}

fn distributed_path(parent: impl AsRef<Path>, file_idx: usize) -> PathBuf {
    parent.as_ref().join(format!("{file_idx:0>4}.tdl"))
}

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
    // x_to_x_common(factory, num_from, num_processes, del_tmp_files)?;

    // Trim Move
    println!("### PHASE: TRIM MOVE ###");
    // for num_doves in 2..=12 {
    //     println!("=== num_doves={num_doves} ===");
    //     let src_dir = dove_dir(factory.trimmed_simply(num_to), num_doves);
    //     let dst_dir = dove_dir(factory.trimmed_move(num_to), num_doves);
    //     std::fs::create_dir_all(&dst_dir)?;
    //     let win_paths = factory.win_paths(num_to, num_doves);
    //     core_methods::trim_on_action(
    //         src_dir,
    //         dst_dir,
    //         num_doves,
    //         num_doves,
    //         &win_paths,
    //         num_processes,
    //         nums_doves_to_split_win_if_possible.contains(&num_doves),
    //     )?;
    // }
    // if del_tmp_files {
    //     std::fs::remove_dir_all(factory.trimmed_simply(num_to))?;
    // }

    // Trim Put
    println!("### PHASE: TRIM PUT ###");
    for num_doves in 2..=12 {
        println!("=== num_doves={num_doves} ===");
        if num_doves <= 7 {
            println!("Skipped");
            continue;
        }
        let src_dir = dove_dir(factory.trimmed_move(num_to), num_doves);
        let dst_dir = dove_dir(factory.trimmed_put(num_to), num_doves);
        std::fs::create_dir_all(&dst_dir)?;

        if num_doves == 12 {
            for entry in std::fs::read_dir(src_dir)? {
                let src_path = entry?.path();
                if src_path.extension() != Some(&OsString::from("tdl")) {
                    continue;
                }
                let dst_path = dst_dir.join(src_path.file_name().unwrap());
                std::fs::copy(src_path, dst_path)?;
            }
            continue;
        }

        let win_paths = factory.win_paths(num_to, num_doves + 1);
        core_methods::trim_on_action(
            src_dir,
            dst_dir,
            num_doves,
            num_doves + 1,
            &win_paths,
            num_processes,
            nums_doves_to_split_win_if_possible.contains(&(num_doves + 1)),
        )?;
    }
    if del_tmp_files {
        std::fs::remove_dir_all(factory.trimmed_move(num_to))?;
    }

    // Trim Remove
    println!("### PHASE: TRIM REMOVE ###");
    for num_doves in 2..=12 {
        println!("=== num_doves={num_doves} ===");
        let src_dir = dove_dir(factory.trimmed_put(num_to), num_doves);
        let dst_dir = dove_dir(factory.trimmed_remove(num_to), num_doves);
        std::fs::create_dir_all(&dst_dir)?;

        if num_doves == 2 {
            for entry in std::fs::read_dir(src_dir)? {
                let src_path = entry?.path();
                if src_path.extension() != Some(&OsString::from("tdl")) {
                    continue;
                }
                let dst_path = dst_dir.join(src_path.file_name().unwrap());
                std::fs::copy(src_path, dst_path)?;
            }
            continue;
        }

        let win_paths = factory.win_paths(num_to, num_doves - 1);
        core_methods::trim_on_action(
            src_dir,
            dst_dir,
            num_doves,
            num_doves - 1,
            &win_paths,
            num_processes,
            nums_doves_to_split_win_if_possible.contains(&(num_doves - 1)),
        )?;
    }
    if del_tmp_files {
        std::fs::remove_dir_all(factory.trimmed_put(num_to))?;
    }

    // Gather
    println!("### PHASE: GATHER ###");
    std::fs::create_dir_all(factory.num_dir(num_to))?;
    for num_doves in 2..=12 {
        println!("=== num_doves={num_doves} ===");
        core_methods::gather(
            dove_dir(factory.trimmed_remove(num_to), num_doves),
            factory.num_dir(num_to).join(format!("{num_doves:0>2}.tdl")),
        )?;
    }
    if del_tmp_files {
        std::fs::remove_dir_all(factory.num_tmp_dir(num_to))?;
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
    use clap::Parser;

    let arg: Args = Args::parse();
    let root = PathBuf::from(arg.src_dir.unwrap_or(r"...".to_owned()));
    let num_from = arg.num_doves;
    let num_processes = arg.num_processes;
    let split = arg.split_nums_doves;
    let del_tmp_files = arg.del_tmp_files.unwrap_or(true);
    advance_one_step(root, num_from, num_processes, del_tmp_files, &split)?;
    Ok(())
}
