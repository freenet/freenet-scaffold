//! Convergence testing framework for ComposableState implementations.
//!
//! This module provides utilities for testing that CRDT implementations
//! converge to the same state regardless of the order in which operations
//! are applied.
//!
//! # Overview
//!
//! For a proper CRDT, the following properties must hold:
//! - **Commutativity**: Applying deltas in any order produces the same result
//! - **Idempotency**: Applying the same delta twice has no additional effect
//! - **Associativity**: Grouping of delta applications doesn't matter
//!
//! # Usage
//!
//! Contracts implement the [`ConvergenceTestHarness`] trait to define how to
//! generate valid operations for their specific state structure. The framework
//! then handles permuting operations and checking convergence.
//!
//! ```ignore
//! use freenet_scaffold::convergence::*;
//!
//! struct MyHarness { /* contract-specific state */ }
//!
//! impl ConvergenceTestHarness for MyHarness {
//!     type State = MyState;
//!     type Delta = MyDelta;
//!     type Parameters = MyParams;
//!     type Operation = MyOperation;
//!
//!     fn initial_state(&self) -> (Self::State, Self::Parameters) { ... }
//!     fn generate_operation(&mut self, rng: &mut impl Rng) -> Self::Operation { ... }
//!     fn operation_to_delta(&mut self, op: &Self::Operation) -> Self::Delta { ... }
//! }
//!
//! #[test]
//! fn test_convergence() {
//!     let harness = MyHarness::new();
//!     test_operation_commutativity(harness, 10, 100);
//! }
//! ```

use crate::ComposableState;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::Debug;

/// A test harness for convergence testing of ComposableState implementations.
///
/// Implementors define how to generate valid operations for their contract,
/// accounting for cryptographic requirements like signatures and invite chains.
pub trait ConvergenceTestHarness: Clone {
    /// The ComposableState type being tested
    type State: ComposableState<
            ParentState = Self::State,
            Delta = Self::Delta,
            Parameters = Self::Parameters,
        > + Clone
        + PartialEq
        + Debug;

    /// The delta type for the state
    type Delta: Serialize + DeserializeOwned + Clone + Debug;

    /// The parameters type for the state
    type Parameters: Serialize + DeserializeOwned + Clone + Debug;

    /// An operation that can be converted to a delta.
    /// Operations are contract-specific and may require internal state
    /// (like signing keys) to generate valid deltas.
    type Operation: Clone + Debug;

    /// Returns the initial state and parameters for testing.
    fn initial_state(&self) -> (Self::State, Self::Parameters);

    /// Generates a random valid operation.
    ///
    /// The harness maintains any internal state needed to generate valid
    /// operations (e.g., signing keys for members that have been added).
    fn generate_operation<R: Rng>(&mut self, rng: &mut R) -> Self::Operation;

    /// Converts an operation to a delta that can be applied to the state.
    ///
    /// This may update internal harness state (e.g., tracking which members
    /// exist for future operations).
    fn operation_to_delta(&mut self, state: &Self::State, op: &Self::Operation) -> Self::Delta;

    /// Applies a delta to a state, returning the new state.
    fn apply_delta(
        &self,
        state: &mut Self::State,
        parameters: &Self::Parameters,
        delta: &Self::Delta,
    ) -> Result<(), String> {
        let parent = state.clone();
        state.apply_delta(&parent, parameters, &Some(delta.clone()))
    }
}

/// Trait for random number generation, compatible with the `rand` crate.
pub trait Rng {
    fn next_u64(&mut self) -> u64;

    fn gen_range(&mut self, range: std::ops::Range<usize>) -> usize {
        let len = range.end - range.start;
        if len == 0 {
            return range.start;
        }
        range.start + (self.next_u64() as usize % len)
    }

    fn gen_bool(&mut self, probability: f64) -> bool {
        (self.next_u64() as f64 / u64::MAX as f64) < probability
    }
}

/// A simple seeded RNG for reproducible tests.
#[derive(Clone)]
pub struct SimpleRng {
    state: u64,
}

impl SimpleRng {
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }
}

impl Rng for SimpleRng {
    fn next_u64(&mut self) -> u64 {
        // xorshift64
        self.state ^= self.state << 13;
        self.state ^= self.state >> 7;
        self.state ^= self.state << 17;
        self.state
    }
}

/// Result of a convergence test.
#[derive(Debug)]
pub struct ConvergenceTestResult {
    /// Whether the test passed
    pub passed: bool,
    /// Number of operation sequences tested
    pub sequences_tested: usize,
    /// If failed, the operations that caused divergence
    pub failing_operations: Option<Vec<String>>,
    /// If failed, descriptions of the divergent states
    pub divergent_states: Option<(String, String)>,
}

impl ConvergenceTestResult {
    pub fn success(sequences_tested: usize) -> Self {
        Self {
            passed: true,
            sequences_tested,
            failing_operations: None,
            divergent_states: None,
        }
    }

    pub fn failure(
        sequences_tested: usize,
        operations: Vec<String>,
        state_a: String,
        state_b: String,
    ) -> Self {
        Self {
            passed: false,
            sequences_tested,
            failing_operations: Some(operations),
            divergent_states: Some((state_a, state_b)),
        }
    }
}

/// Tests that applying operations in different orders produces the same final state.
///
/// # Arguments
/// * `harness` - The test harness for generating operations
/// * `num_operations` - Number of operations to generate per test
/// * `num_permutations` - Number of different orderings to test
/// * `seed` - Random seed for reproducibility
///
/// # Returns
/// A `ConvergenceTestResult` indicating success or failure with details.
pub fn test_operation_commutativity<H: ConvergenceTestHarness>(
    harness: H,
    num_operations: usize,
    num_permutations: usize,
    seed: u64,
) -> ConvergenceTestResult {
    let mut rng = SimpleRng::new(seed);

    // Generate operations using the harness
    let mut gen_harness = harness.clone();
    let operations: Vec<H::Operation> = (0..num_operations)
        .map(|_| gen_harness.generate_operation(&mut rng))
        .collect();

    let (initial_state, parameters) = harness.initial_state();

    // Apply operations in original order to get reference state
    let mut reference_harness = harness.clone();
    let mut reference_state = initial_state.clone();
    for op in &operations {
        let delta = reference_harness.operation_to_delta(&reference_state, op);
        if let Err(e) = reference_harness.apply_delta(&mut reference_state, &parameters, &delta) {
            // If delta application fails, that's a test setup issue, not a convergence issue
            return ConvergenceTestResult::failure(
                0,
                operations.iter().map(|o| format!("{:?}", o)).collect(),
                format!("Delta application failed: {}", e),
                String::new(),
            );
        }
    }

    // Test different permutations
    for perm_idx in 0..num_permutations {
        let permuted = permute_operations(&operations, &mut rng);

        let mut test_harness = harness.clone();
        let mut test_state = initial_state.clone();

        for op in &permuted {
            let delta = test_harness.operation_to_delta(&test_state, op);
            if let Err(e) = test_harness.apply_delta(&mut test_state, &parameters, &delta) {
                return ConvergenceTestResult::failure(
                    perm_idx,
                    permuted.iter().map(|o| format!("{:?}", o)).collect(),
                    format!("Delta application failed: {}", e),
                    String::new(),
                );
            }
        }

        if test_state != reference_state {
            return ConvergenceTestResult::failure(
                perm_idx + 1,
                permuted.iter().map(|o| format!("{:?}", o)).collect(),
                format!("{:?}", reference_state),
                format!("{:?}", test_state),
            );
        }
    }

    ConvergenceTestResult::success(num_permutations)
}

/// Tests that applying the same delta twice has no additional effect.
///
/// # Arguments
/// * `harness` - The test harness for generating operations
/// * `num_operations` - Number of operations to test
/// * `seed` - Random seed for reproducibility
pub fn test_idempotency<H: ConvergenceTestHarness>(
    harness: H,
    num_operations: usize,
    seed: u64,
) -> ConvergenceTestResult {
    let mut rng = SimpleRng::new(seed);
    let (initial_state, parameters) = harness.initial_state();

    for i in 0..num_operations {
        let mut gen_harness = harness.clone();
        let op = gen_harness.generate_operation(&mut rng);

        // Apply once
        let mut state_once = initial_state.clone();
        let delta = gen_harness.operation_to_delta(&state_once, &op);
        if let Err(e) = gen_harness.apply_delta(&mut state_once, &parameters, &delta) {
            return ConvergenceTestResult::failure(
                i,
                vec![format!("{:?}", op)],
                format!("First application failed: {}", e),
                String::new(),
            );
        }

        // Apply twice
        let mut state_twice = state_once.clone();
        if let Err(e) = gen_harness.apply_delta(&mut state_twice, &parameters, &delta) {
            return ConvergenceTestResult::failure(
                i,
                vec![format!("{:?}", op)],
                format!("Second application failed: {}", e),
                String::new(),
            );
        }

        if state_once != state_twice {
            return ConvergenceTestResult::failure(
                i + 1,
                vec![format!("{:?}", op)],
                format!("{:?}", state_once),
                format!("{:?}", state_twice),
            );
        }
    }

    ConvergenceTestResult::success(num_operations)
}

/// Tests convergence when two peers apply overlapping sets of operations.
///
/// Simulates two peers that each see some operations and then merge.
/// Both should converge to the same final state.
///
/// # Arguments
/// * `harness` - The test harness for generating operations
/// * `num_operations` - Total number of operations to generate
/// * `overlap_probability` - Probability that each operation is seen by both peers (0.0 to 1.0)
/// * `seed` - Random seed for reproducibility
pub fn test_merge_convergence<H: ConvergenceTestHarness>(
    harness: H,
    num_operations: usize,
    overlap_probability: f64,
    seed: u64,
) -> ConvergenceTestResult {
    let mut rng = SimpleRng::new(seed);

    // Generate operations
    let mut gen_harness = harness.clone();
    let operations: Vec<H::Operation> = (0..num_operations)
        .map(|_| gen_harness.generate_operation(&mut rng))
        .collect();

    let (initial_state, parameters) = harness.initial_state();

    // Assign operations to peers
    let mut peer_a_ops: Vec<&H::Operation> = Vec::new();
    let mut peer_b_ops: Vec<&H::Operation> = Vec::new();

    for op in &operations {
        let both = rng.gen_bool(overlap_probability);
        let peer_a = both || rng.gen_bool(0.5);
        let peer_b = both || !peer_a || rng.gen_bool(0.5);

        if peer_a {
            peer_a_ops.push(op);
        }
        if peer_b {
            peer_b_ops.push(op);
        }
    }

    // Apply to peer A
    let mut harness_a = harness.clone();
    let mut state_a = initial_state.clone();
    for op in &peer_a_ops {
        let delta = harness_a.operation_to_delta(&state_a, op);
        let _ = harness_a.apply_delta(&mut state_a, &parameters, &delta);
    }

    // Apply to peer B
    let mut harness_b = harness.clone();
    let mut state_b = initial_state.clone();
    for op in &peer_b_ops {
        let delta = harness_b.operation_to_delta(&state_b, op);
        let _ = harness_b.apply_delta(&mut state_b, &parameters, &delta);
    }

    // Merge A into B and B into A
    let _ = state_a.merge(&state_a.clone(), &parameters, &state_b);
    let _ = state_b.merge(&state_b.clone(), &parameters, &state_a);

    if state_a != state_b {
        return ConvergenceTestResult::failure(
            1,
            operations.iter().map(|o| format!("{:?}", o)).collect(),
            format!("{:?}", state_a),
            format!("{:?}", state_b),
        );
    }

    ConvergenceTestResult::success(1)
}

/// Fisher-Yates shuffle for generating permutations
fn permute_operations<T: Clone, R: Rng>(operations: &[T], rng: &mut R) -> Vec<T> {
    let mut result: Vec<T> = operations.to_vec();
    let n = result.len();
    for i in (1..n).rev() {
        let j = rng.gen_range(0..i + 1);
        result.swap(i, j);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Simple test state for unit testing the framework
    #[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    struct TestState {
        values: std::collections::BTreeSet<i32>,
    }

    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    struct TestDelta {
        add: Vec<i32>,
    }

    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    struct TestParams;

    impl ComposableState for TestState {
        type ParentState = TestState;
        type Summary = std::collections::BTreeSet<i32>;
        type Delta = TestDelta;
        type Parameters = TestParams;

        fn verify(&self, _: &Self::ParentState, _: &Self::Parameters) -> Result<(), String> {
            Ok(())
        }

        fn summarize(&self, _: &Self::ParentState, _: &Self::Parameters) -> Self::Summary {
            self.values.clone()
        }

        fn delta(
            &self,
            _: &Self::ParentState,
            _: &Self::Parameters,
            old: &Self::Summary,
        ) -> Option<Self::Delta> {
            let add: Vec<i32> = self.values.difference(old).copied().collect();
            if add.is_empty() {
                None
            } else {
                Some(TestDelta { add })
            }
        }

        fn apply_delta(
            &mut self,
            _: &Self::ParentState,
            _: &Self::Parameters,
            delta: &Option<Self::Delta>,
        ) -> Result<(), String> {
            if let Some(d) = delta {
                self.values.extend(d.add.iter().copied());
            }
            Ok(())
        }
    }

    #[derive(Clone, Debug)]
    struct TestOp(i32);

    #[derive(Clone)]
    struct TestHarness;

    impl ConvergenceTestHarness for TestHarness {
        type State = TestState;
        type Delta = TestDelta;
        type Parameters = TestParams;
        type Operation = TestOp;

        fn initial_state(&self) -> (Self::State, Self::Parameters) {
            (
                TestState {
                    values: std::collections::BTreeSet::new(),
                },
                TestParams,
            )
        }

        fn generate_operation<R: Rng>(&mut self, rng: &mut R) -> Self::Operation {
            TestOp((rng.next_u64() % 100) as i32)
        }

        fn operation_to_delta(&mut self, _: &Self::State, op: &Self::Operation) -> Self::Delta {
            TestDelta { add: vec![op.0] }
        }
    }

    #[test]
    fn test_simple_commutativity() {
        let harness = TestHarness;
        let result = test_operation_commutativity(harness, 10, 50, 12345);
        assert!(
            result.passed,
            "Set-based CRDT should be commutative: {:?}",
            result
        );
    }

    #[test]
    fn test_simple_idempotency() {
        let harness = TestHarness;
        let result = test_idempotency(harness, 20, 12345);
        assert!(
            result.passed,
            "Set-based CRDT should be idempotent: {:?}",
            result
        );
    }

    #[test]
    fn test_simple_merge_convergence() {
        let harness = TestHarness;
        let result = test_merge_convergence(harness, 20, 0.3, 12345);
        assert!(
            result.passed,
            "Set-based CRDT should converge after merge: {:?}",
            result
        );
    }
}
