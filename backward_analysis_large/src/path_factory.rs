use std::path::{Path, PathBuf};

// **********************************************************
//  Path Rules
// **********************************************************
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PathFactory<P: AsRef<Path>> {
    root: P,
}

impl<P> PathFactory<P>
where
    P: AsRef<Path>,
{
    pub fn new(root: P) -> Self {
        Self { root }
    }

    pub fn num_dir(&self, num_step: usize) -> PathBuf {
        self.root.as_ref().join(format!("{num_step:0>4}"))
    }

    pub fn num_tmp_dir(&self, num_step: usize) -> PathBuf {
        self.root.as_ref().join(format!("{num_step:0>4}_tmp"))
    }

    pub fn backstepped(&self, num_step: usize) -> PathBuf {
        self.num_tmp_dir(num_step).join("backstepped")
    }

    pub fn redistributed(&self, num_step: usize) -> PathBuf {
        self.num_tmp_dir(num_step).join("redistributed")
    }

    pub fn trimmed_simply(&self, num_step: usize) -> PathBuf {
        self.num_tmp_dir(num_step).join("trimmed_simply")
    }

    pub fn trimmed_move(&self, num_step: usize) -> PathBuf {
        self.num_tmp_dir(num_step).join("trimmed_move")
    }

    pub fn trimmed_put(&self, num_step: usize) -> PathBuf {
        self.num_tmp_dir(num_step).join("trimmed_put")
    }

    pub fn trimmed_remove(&self, num_step: usize) -> PathBuf {
        self.num_tmp_dir(num_step).join("trimmed_remove")
    }

    pub fn win_paths(&self, num_step_ceil: usize, num_doves: usize) -> Vec<PathBuf> {
        (3..=num_step_ceil)
            .step_by(2)
            .map(|n| self.num_dir(n).join(format!("{num_doves:0>2}.tdl")))
            .collect()
    }
}

pub fn dove_dir(parent: impl AsRef<Path>, num_doves: usize) -> PathBuf {
    parent.as_ref().join(format!("{num_doves:0>2}"))
}

pub fn distributed_path(parent: impl AsRef<Path>, file_idx: usize) -> PathBuf {
    parent.as_ref().join(format!("{file_idx:0>4}.tdl"))
}
