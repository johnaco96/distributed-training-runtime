#![forbid(unsafe_code)]

//! Rendezvous barrier for torch.distributed process-group formation.
//!
//! Workers call `JoinRendezvous` after registering; the coordinator collects
//! participants into a *round*.  A round completes when the target world size
//! is reached (fixed-size runs) or when the elastic completion rule fires
//! (`min_world_size` reached and the settle window elapsed, or
//! `max_world_size` reached).  On completion every participant is assigned a
//! rank — workers that held a rank in the previous round keep it when
//! possible, replacement workers fill the gaps — and the rank-0 participant's
//! host/port becomes the c10d TCPStore endpoint that all workers pass to
//! `torch.distributed.init_process_group`.
//!
//! Membership changes (a worker fails, a new worker joins after completion)
//! invalidate the completed round: the round number is bumped and all workers
//! must re-join.  Workers observe the bump through the `rendezvous_round`
//! field on heartbeat responses and re-enter the barrier.
//!
//! Rendezvous state is deliberately in-memory only.  Round numbers are seeded
//! from the wall clock so they remain monotonic across coordinator restarts
//! and HA failovers; after a failover every worker re-joins and forms a fresh
//! round.

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Conventional torch.distributed store port used when a worker does not
/// propose one.
pub const DEFAULT_STORE_PORT: u32 = 29500;

/// Default settle window for elastic rounds: how long the coordinator waits
/// for additional workers after `min_world_size` is reached.
pub const DEFAULT_SETTLE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RendezvousConfig {
    /// Fixed world size, used when the run is not elastic.
    pub world_size: u32,
    /// Elastic lower bound; 0 disables elasticity.
    pub min_world_size: u32,
    /// Elastic upper bound; ignored unless `min_world_size > 0`.
    pub max_world_size: u32,
    pub settle_timeout: Duration,
}

impl RendezvousConfig {
    pub fn fixed(world_size: u32) -> Self {
        Self {
            world_size,
            min_world_size: 0,
            max_world_size: 0,
            settle_timeout: DEFAULT_SETTLE_TIMEOUT,
        }
    }

    pub fn is_elastic(&self) -> bool {
        self.min_world_size > 0
    }

    fn min(&self) -> u32 {
        if self.is_elastic() {
            self.min_world_size
        } else {
            self.world_size.max(1)
        }
    }

    fn max(&self) -> u32 {
        if self.is_elastic() {
            self.max_world_size.max(self.min_world_size)
        } else {
            self.world_size.max(1)
        }
    }
}

#[derive(Clone, Debug)]
struct Participant {
    worker_id: String,
    host: String,
    store_port: u32,
    join_order: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CompletedRound {
    pub round: u64,
    pub world_size: u32,
    /// worker_id → rank for every ranked participant of the round.
    pub ranks: HashMap<String, u32>,
    pub store_addr: String,
    pub store_port: u32,
}

/// Snapshot returned to RPC handlers.
#[derive(Clone, Debug, PartialEq)]
pub struct RendezvousSnapshot {
    pub round: u64,
    pub complete: bool,
    /// Completed world size, or the current participant count while waiting.
    pub world_size: u32,
    /// Rank of the requesting worker in the completed round, if any.
    pub rank: Option<u32>,
    pub store_addr: String,
    pub store_port: u32,
}

pub struct RunRendezvous {
    config: RendezvousConfig,
    round: u64,
    participants: Vec<Participant>,
    completed: Option<CompletedRound>,
    /// Ranks from the most recently completed round, used for rank stability
    /// when the next round forms.
    prev_ranks: HashMap<String, u32>,
    settle_deadline: Option<SystemTime>,
    join_counter: u64,
}

impl RunRendezvous {
    pub fn new(config: RendezvousConfig) -> Self {
        // Seed the round from the wall clock so rounds stay monotonic across
        // coordinator restarts (in-memory rendezvous state does not survive a
        // restart, but workers holding an old round number must never see the
        // same or a smaller round after failover).
        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        Self {
            config,
            round: seed.max(1),
            participants: Vec::new(),
            completed: None,
            prev_ranks: HashMap::new(),
            settle_deadline: None,
            join_counter: 0,
        }
    }

    pub fn current_round(&self) -> u64 {
        self.round
    }

    pub fn config(&self) -> RendezvousConfig {
        self.config
    }

    /// A worker joins (or re-joins) the rendezvous.
    ///
    /// Joining while a round is complete signals a membership change — a
    /// healthy worker never re-joins a round it already completed — so the
    /// completed round is invalidated and a new one starts with this worker
    /// as its first participant.
    pub fn join(
        &mut self,
        worker_id: &str,
        host: &str,
        store_port: u32,
        now: SystemTime,
    ) -> RendezvousSnapshot {
        if self.completed.is_some() {
            self.begin_next_round();
        }

        let store_port = if store_port == 0 {
            DEFAULT_STORE_PORT
        } else {
            store_port
        };
        self.join_counter += 1;
        let join_order = self.join_counter;
        match self
            .participants
            .iter_mut()
            .find(|p| p.worker_id == worker_id)
        {
            Some(existing) => {
                existing.host = host.to_string();
                existing.store_port = store_port;
            }
            None => self.participants.push(Participant {
                worker_id: worker_id.to_string(),
                host: host.to_string(),
                store_port,
                join_order,
            }),
        }

        self.try_complete(now);
        self.snapshot_for(worker_id)
    }

    /// Poll the rendezvous state.  Elastic settle windows are evaluated
    /// lazily here, so no background task is required.
    pub fn state_for(&mut self, worker_id: &str, now: SystemTime) -> RendezvousSnapshot {
        if self.completed.is_none() {
            self.try_complete(now);
        }
        self.snapshot_for(worker_id)
    }

    /// Called when a registered worker transitions to FAILED.  Returns `true`
    /// when the failure invalidated a completed round (i.e. the remaining
    /// workers must re-rendezvous).
    pub fn on_worker_failed(&mut self, worker_id: &str, _now: SystemTime) -> bool {
        if let Some(completed) = &self.completed {
            if completed.ranks.contains_key(worker_id) {
                self.begin_next_round();
                self.prev_ranks.remove(worker_id);
                return true;
            }
            return false;
        }

        let before = self.participants.len();
        self.participants.retain(|p| p.worker_id != worker_id);
        if self.participants.len() != before && (self.participants.len() as u32) < self.config.min()
        {
            // Dropped below the completion threshold — reopen the settle window.
            self.settle_deadline = None;
        }
        false
    }

    fn begin_next_round(&mut self) {
        if let Some(completed) = self.completed.take() {
            self.prev_ranks = completed.ranks;
        }
        self.round += 1;
        self.participants.clear();
        self.settle_deadline = None;
    }

    fn try_complete(&mut self, now: SystemTime) {
        if self.completed.is_some() {
            return;
        }
        let count = self.participants.len() as u32;
        let min = self.config.min();
        let max = self.config.max();
        if count < min {
            self.settle_deadline = None;
            return;
        }

        let complete_now = if count >= max {
            true
        } else if self.config.is_elastic() {
            match self.settle_deadline {
                Some(deadline) => now >= deadline,
                None => {
                    self.settle_deadline = Some(now + self.config.settle_timeout);
                    false
                }
            }
        } else {
            // Fixed-size runs complete exactly at world_size (== min == max).
            true
        };
        if !complete_now {
            return;
        }

        // Rank the first `max` participants by join order; later arrivals in
        // the same round wait unranked for the next round.
        let mut selected: Vec<&Participant> = self.participants.iter().collect();
        selected.sort_by_key(|p| p.join_order);
        selected.truncate(max as usize);
        let world_size = selected.len() as u32;

        // First pass: participants keep their rank from the previous round
        // when it still fits the new world size.
        let mut ranks: HashMap<String, u32> = HashMap::new();
        let mut claimed = vec![false; world_size as usize];
        for participant in &selected {
            if let Some(&prev) = self.prev_ranks.get(&participant.worker_id) {
                if prev < world_size && !claimed[prev as usize] {
                    ranks.insert(participant.worker_id.clone(), prev);
                    claimed[prev as usize] = true;
                }
            }
        }
        // Second pass: everyone else (replacements, new workers) fills the
        // unclaimed ranks in join order.
        let mut next_free = 0u32;
        for participant in &selected {
            if ranks.contains_key(&participant.worker_id) {
                continue;
            }
            while claimed[next_free as usize] {
                next_free += 1;
            }
            claimed[next_free as usize] = true;
            ranks.insert(participant.worker_id.clone(), next_free);
        }

        let (store_addr, store_port) = selected
            .iter()
            .find(|p| ranks.get(&p.worker_id) == Some(&0))
            .map(|p| (p.host.clone(), p.store_port))
            .unwrap_or_else(|| (String::new(), DEFAULT_STORE_PORT));

        self.completed = Some(CompletedRound {
            round: self.round,
            world_size,
            ranks,
            store_addr,
            store_port,
        });
        self.settle_deadline = None;
    }

    fn snapshot_for(&self, worker_id: &str) -> RendezvousSnapshot {
        match &self.completed {
            Some(completed) => RendezvousSnapshot {
                round: completed.round,
                complete: true,
                world_size: completed.world_size,
                rank: completed.ranks.get(worker_id).copied(),
                store_addr: completed.store_addr.clone(),
                store_port: completed.store_port,
            },
            None => RendezvousSnapshot {
                round: self.round,
                complete: false,
                world_size: self.participants.len() as u32,
                rank: None,
                store_addr: String::new(),
                store_port: 0,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now() -> SystemTime {
        SystemTime::now()
    }

    fn fixed(world_size: u32) -> RunRendezvous {
        RunRendezvous::new(RendezvousConfig::fixed(world_size))
    }

    fn elastic(min: u32, max: u32, settle: Duration) -> RunRendezvous {
        RunRendezvous::new(RendezvousConfig {
            world_size: max,
            min_world_size: min,
            max_world_size: max,
            settle_timeout: settle,
        })
    }

    #[test]
    fn fixed_round_completes_at_world_size() {
        let mut rdzv = fixed(2);
        let snap = rdzv.join("w0", "hostA", 29501, now());
        assert!(!snap.complete);
        assert_eq!(snap.world_size, 1);

        let snap = rdzv.join("w1", "hostB", 29502, now());
        assert!(snap.complete);
        assert_eq!(snap.world_size, 2);
        assert!(snap.rank.is_some());

        let a = rdzv.state_for("w0", now());
        let b = rdzv.state_for("w1", now());
        let mut ranks = vec![a.rank.unwrap(), b.rank.unwrap()];
        ranks.sort_unstable();
        assert_eq!(ranks, vec![0, 1]);
        // Store endpoint belongs to the rank-0 participant.
        let rank0 = if a.rank == Some(0) {
            ("hostA", 29501)
        } else {
            ("hostB", 29502)
        };
        assert_eq!(a.store_addr, rank0.0);
        assert_eq!(a.store_port, rank0.1);
        assert_eq!(a.store_addr, b.store_addr);
    }

    #[test]
    fn ranks_assigned_in_join_order() {
        let mut rdzv = fixed(3);
        rdzv.join("first", "h1", 0, now());
        rdzv.join("second", "h2", 0, now());
        rdzv.join("third", "h3", 0, now());
        assert_eq!(rdzv.state_for("first", now()).rank, Some(0));
        assert_eq!(rdzv.state_for("second", now()).rank, Some(1));
        assert_eq!(rdzv.state_for("third", now()).rank, Some(2));
        // Default port substituted for 0.
        assert_eq!(
            rdzv.state_for("first", now()).store_port,
            DEFAULT_STORE_PORT
        );
    }

    #[test]
    fn rejoin_after_completion_starts_new_round_and_keeps_surviving_ranks() {
        let mut rdzv = fixed(2);
        rdzv.join("w0", "h0", 0, now());
        rdzv.join("w1", "h1", 0, now());
        let first_round = rdzv.state_for("w0", now()).round;
        assert_eq!(rdzv.state_for("w1", now()).rank, Some(1));

        // w1 re-joins (e.g. its process restarted) → new round forms.
        let snap = rdzv.join("w1", "h1", 0, now());
        assert!(!snap.complete);
        assert!(snap.round > first_round);

        rdzv.join("w0", "h0", 0, now());
        let w0 = rdzv.state_for("w0", now());
        let w1 = rdzv.state_for("w1", now());
        assert!(w0.complete);
        // Both workers keep their previous ranks.
        assert_eq!(w0.rank, Some(0));
        assert_eq!(w1.rank, Some(1));
    }

    #[test]
    fn failed_worker_is_replaced_and_replacement_takes_freed_rank() {
        let mut rdzv = fixed(2);
        rdzv.join("w0", "h0", 0, now());
        rdzv.join("w1", "h1", 0, now());
        assert_eq!(rdzv.state_for("w0", now()).rank, Some(0));

        // w0 (rank 0) dies → completed round invalidated.
        assert!(rdzv.on_worker_failed("w0", now()));
        let stale = rdzv.state_for("w1", now());
        assert!(!stale.complete);

        // Survivor re-joins, replacement joins.
        rdzv.join("w1", "h1", 0, now());
        rdzv.join("w2", "h2", 29777, now());
        let w1 = rdzv.state_for("w1", now());
        let w2 = rdzv.state_for("w2", now());
        assert!(w1.complete);
        // w1 keeps rank 1; the replacement takes the freed rank 0 and hosts the store.
        assert_eq!(w1.rank, Some(1));
        assert_eq!(w2.rank, Some(0));
        assert_eq!(w1.store_addr, "h2");
        assert_eq!(w1.store_port, 29777);
    }

    #[test]
    fn failure_of_unranked_worker_does_not_invalidate_round() {
        let mut rdzv = fixed(1);
        rdzv.join("w0", "h0", 0, now());
        assert!(rdzv.state_for("w0", now()).complete);
        assert!(!rdzv.on_worker_failed("stranger", now()));
        assert!(rdzv.state_for("w0", now()).complete);
    }

    #[test]
    fn elastic_round_completes_at_max_immediately() {
        let mut rdzv = elastic(2, 3, Duration::from_secs(3600));
        rdzv.join("w0", "h0", 0, now());
        rdzv.join("w1", "h1", 0, now());
        assert!(!rdzv.state_for("w0", now()).complete);
        rdzv.join("w2", "h2", 0, now());
        let snap = rdzv.state_for("w0", now());
        assert!(snap.complete);
        assert_eq!(snap.world_size, 3);
    }

    #[test]
    fn elastic_round_completes_after_settle_window_at_min() {
        let mut rdzv = elastic(2, 4, Duration::from_millis(10));
        let t0 = now();
        rdzv.join("w0", "h0", 0, t0);
        rdzv.join("w1", "h1", 0, t0);
        assert!(!rdzv.state_for("w0", t0).complete);
        // Settle window elapses → round completes with world size 2.
        let later = t0 + Duration::from_millis(50);
        let snap = rdzv.state_for("w0", later);
        assert!(snap.complete);
        assert_eq!(snap.world_size, 2);
    }

    #[test]
    fn elastic_settle_window_reopens_when_worker_fails_below_min() {
        let mut rdzv = elastic(2, 4, Duration::from_millis(10));
        let t0 = now();
        rdzv.join("w0", "h0", 0, t0);
        rdzv.join("w1", "h1", 0, t0);
        rdzv.on_worker_failed("w1", t0);
        let later = t0 + Duration::from_secs(1);
        assert!(!rdzv.state_for("w0", later).complete);
        // A replacement arrives; window restarts and then completes.
        rdzv.join("w2", "h2", 0, later);
        let even_later = later + Duration::from_secs(1);
        assert!(rdzv.state_for("w0", even_later).complete);
    }

    #[test]
    fn elastic_regrowth_after_completion_uses_new_round() {
        let mut rdzv = elastic(1, 2, Duration::from_millis(0));
        let t0 = now();
        rdzv.join("w0", "h0", 0, t0);
        let snap = rdzv.state_for("w0", t0 + Duration::from_millis(1));
        assert!(snap.complete);
        assert_eq!(snap.world_size, 1);
        let round1 = snap.round;

        // A new worker joins after completion → membership change → new round.
        let snap = rdzv.join("w1", "h1", 0, t0);
        assert!(snap.round > round1);
        rdzv.join("w0", "h0", 0, t0);
        let w0 = rdzv.state_for("w0", t0 + Duration::from_secs(1));
        assert!(w0.complete);
        assert_eq!(w0.world_size, 2);
        assert_eq!(w0.rank, Some(0));
    }

    #[test]
    fn rounds_are_monotonic_across_instances() {
        // Simulates a coordinator restart: a fresh RunRendezvous must never
        // reuse a round number a worker could still hold.
        let r1 = fixed(1).current_round();
        std::thread::sleep(Duration::from_millis(5));
        let r2 = fixed(1).current_round();
        assert!(r2 > r1);
    }
}
