use super::hashutil::*;
use tokyodoves::*;

// --- 9 ---
pub fn make_win_filter_9(num_doves: u32) -> impl Fn(&u64) -> bool {
    move |hash| OnOff::new(*hash).project_on(Color::Red).count_doves() == num_doves
}

pub fn make_target_filter_9(num_doves: u32) -> impl Fn(&u64) -> bool {
    move |hash| OnOff::new(*hash).project_on(Color::Green).count_doves() == num_doves
}

pub fn make_action_filter_9(num_from: usize, num_to: usize) -> impl Fn(&Action, &u64) -> bool {
    move |action, _| {
        use Action::*;
        match action {
            Put(..) => num_from < num_to,
            Move(..) => num_from == num_to,
            Remove(..) => num_from > num_to,
        }
    }
}

// --- 10 ---
pub fn make_win_filter_10(win_onoff: OnOff) -> impl Fn(&u64) -> bool {
    move |hash| OnOff::new(*hash).project_on(Color::Red) == win_onoff
}

pub fn make_target_filter_10(win_onoff: OnOff) -> impl Fn(&u64) -> bool {
    move |hash| OnOff::new(*hash).project_on(Color::Green) == !win_onoff
}

pub fn make_action_filter_10(num_from: usize, num_to: usize) -> impl Fn(&Action, &u64) -> bool {
    move |action, _| -> bool {
        use Action::*;
        match action {
            Put(..) => num_from > num_to,
            Move(..) => num_from == num_to,
            Remove(..) => num_from < num_to,
        }
    }
}

// --- 11 ---
pub fn make_win_filter_11(win_onoff: OnOff) -> impl Fn(&u64) -> bool {
    move |hash: &u64| -> bool { OnOff::new(*hash) == win_onoff }
}

pub fn make_target_filter_11(win_onoff: OnOff) -> impl Fn(&u64) -> bool {
    move |hash: &u64| -> bool { OnOff::new(*hash) == !win_onoff }
}

pub fn make_action_filter_11(win_onoff: OnOff) -> impl Fn(&Action, &u64) -> bool {
    move |action, hash| match possible_action(OnOff::new(*hash), !win_onoff) {
        Some(pos) => pos.matches(action),
        None => false,
    }
}

// --- 12 ---
pub fn make_target_filter_12(dist: u64) -> impl Fn(&u64) -> bool {
    move |hash: &u64| -> bool { aniki_boss_distance(*hash, Color::Green) == dist }
}

pub fn make_win_filter_12(dist: u64) -> impl Fn(&u64) -> bool {
    move |hash: &u64| -> bool { aniki_boss_distance(*hash, Color::Red) == dist }
}

pub fn make_action_filter_12(dist: u64) -> impl Fn(&Action, &u64) -> bool {
    move |action: &Action, hash: &u64| -> bool {
        if aniki_boss_distance(*hash, Color::Green) != dist {
            return false;
        }
        let onoff = OnOff::new(*hash);
        match possible_action(onoff, OnOff::FULL) {
            Some(pos) => pos.matches(action),
            None => false,
        }
    }
}
