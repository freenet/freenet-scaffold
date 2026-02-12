use super::*;
use crate as freenet_scaffold;
use serde::Deserialize;

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct ContractualI32(pub i32);

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct ContractualString(pub String);

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct TestStructParameters;

impl ComposableState for ContractualI32 {
    type ParentState = TestStruct;
    type Summary = i32;
    type Delta = i32;
    type Parameters = TestStructParameters;

    fn verify(
        &self,
        _parent_state: &Self::ParentState,
        _parameters: &Self::Parameters,
    ) -> Result<(), String> {
        Ok(())
    }

    fn summarize(
        &self,
        _parent_state: &Self::ParentState,
        _parameters: &Self::Parameters,
    ) -> Self::Summary {
        self.0
    }

    fn delta(
        &self,
        _parent_state: &Self::ParentState,
        _parameters: &Self::Parameters,
        old_state_summary: &Self::Summary,
    ) -> Option<Self::Delta> {
        Some(self.0 - old_state_summary)
    }

    fn apply_delta(
        &mut self,
        _parent_state: &Self::ParentState,
        _parameters: &Self::Parameters,
        delta: &Option<Self::Delta>,
    ) -> Result<(), String> {
        match delta {
            Some(delta) => {
                self.0 += *delta;
                Ok(())
            }
            None => Ok(()),
        }
    }
}

impl ComposableState for ContractualString {
    type ParentState = TestStruct;
    type Summary = String;
    type Delta = String;
    type Parameters = TestStructParameters;

    fn verify(
        &self,
        _parent_state: &Self::ParentState,
        _parameters: &Self::Parameters,
    ) -> Result<(), String> {
        Ok(())
    }

    fn summarize(
        &self,
        _parent_state: &Self::ParentState,
        _parameters: &Self::Parameters,
    ) -> Self::Summary {
        self.0.clone()
    }

    fn delta(
        &self,
        _parent_state: &Self::ParentState,
        _parameters: &Self::Parameters,
        old_state_summary: &Self::Summary,
    ) -> Option<Self::Delta> {
        if self.0 == *old_state_summary {
            Some(String::new())
        } else {
            Some(self.0.clone())
        }
    }

    fn apply_delta(
        &mut self,
        _parent_state: &Self::ParentState,
        _parameters: &Self::Parameters,
        delta: &Option<Self::Delta>,
    ) -> Result<(), String> {
        if let Some(delta) = delta {
            self.0 = delta.clone()
        }
        Ok(())
    }
}

#[composable]
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct TestStruct {
    number: ContractualI32,
    text: ContractualString,
}

impl TestStruct {
    fn new(number: i32, text: &str) -> Self {
        TestStruct {
            number: ContractualI32(number),
            text: ContractualString(text.to_string()),
        }
    }
}

#[test]
fn test_contractual_macro() {
    let mut test_struct = TestStruct::new(42, "hello");
    let parameters = TestStructParameters {};

    // Test verify
    assert!(test_struct.verify(&test_struct, &parameters).is_ok());

    // Test summarize
    let summary = test_struct.summarize(&test_struct, &parameters);
    assert_eq!(summary.number, 42);
    assert_eq!(summary.text, "hello");

    // Test delta
    let new_state = TestStruct::new(84, "world");
    let delta = new_state.delta(&test_struct.clone(), &parameters, &summary);
    assert_eq!(delta.clone().unwrap().number, Some(42)); // The delta should be the difference: 84 - 42 = 42
    assert_eq!(delta.clone().unwrap().text, Some("world".to_string()));

    // Test apply_delta
    assert!(test_struct
        .apply_delta(&test_struct.clone(), &parameters, &delta)
        .is_ok());
    assert_eq!(test_struct, new_state);
}

// --- post_apply_delta hook tests (separate module to avoid ComposableState import conflict) ---
mod hook_tests {
    use crate as freenet_scaffold;
    use crate::composable;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
    pub struct HookParams;

    #[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
    pub struct HookI32(pub i32);

    #[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
    pub struct HookString(pub String);

    impl freenet_scaffold::ComposableState for HookI32 {
        type ParentState = TestStructWithHook;
        type Summary = i32;
        type Delta = i32;
        type Parameters = HookParams;

        fn verify(&self, _: &Self::ParentState, _: &Self::Parameters) -> Result<(), String> {
            Ok(())
        }
        fn summarize(&self, _: &Self::ParentState, _: &Self::Parameters) -> Self::Summary {
            self.0
        }
        fn delta(
            &self,
            _: &Self::ParentState,
            _: &Self::Parameters,
            old: &Self::Summary,
        ) -> Option<Self::Delta> {
            Some(self.0 - old)
        }
        fn apply_delta(
            &mut self,
            _: &Self::ParentState,
            _: &Self::Parameters,
            delta: &Option<Self::Delta>,
        ) -> Result<(), String> {
            if let Some(d) = delta {
                self.0 += d;
            }
            Ok(())
        }
    }

    impl freenet_scaffold::ComposableState for HookString {
        type ParentState = TestStructWithHook;
        type Summary = String;
        type Delta = String;
        type Parameters = HookParams;

        fn verify(&self, _: &Self::ParentState, _: &Self::Parameters) -> Result<(), String> {
            Ok(())
        }
        fn summarize(&self, _: &Self::ParentState, _: &Self::Parameters) -> Self::Summary {
            self.0.clone()
        }
        fn delta(
            &self,
            _: &Self::ParentState,
            _: &Self::Parameters,
            old: &Self::Summary,
        ) -> Option<Self::Delta> {
            if self.0 == *old {
                None
            } else {
                Some(self.0.clone())
            }
        }
        fn apply_delta(
            &mut self,
            _: &Self::ParentState,
            _: &Self::Parameters,
            delta: &Option<Self::Delta>,
        ) -> Result<(), String> {
            if let Some(d) = delta {
                self.0 = d.clone();
            }
            Ok(())
        }
    }

    #[composable(post_apply_delta = "normalize")]
    #[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
    pub struct TestStructWithHook {
        number: HookI32,
        text: HookString,
    }

    impl TestStructWithHook {
        fn normalize(&mut self, _parameters: &HookParams) -> Result<(), String> {
            if self.number.0 < 0 {
                self.text.0 = "negative".to_string();
            }
            Ok(())
        }
    }

    #[test]
    fn test_post_apply_delta_hook_triggers() {
        let initial = TestStructWithHook {
            number: HookI32(10),
            text: HookString("hello".to_string()),
        };
        let params = HookParams;

        let modified = TestStructWithHook {
            number: HookI32(-5),
            text: HookString("hello".to_string()),
        };
        let summary = initial.summarize(&initial, &params);
        let delta = modified.delta(&initial, &params, &summary);

        let mut result = initial.clone();
        result.apply_delta(&initial, &params, &delta).unwrap();

        assert_eq!(result.number.0, -5);
        assert_eq!(
            result.text.0, "negative",
            "post_apply_delta hook should modify text when number is negative"
        );
    }

    #[test]
    fn test_post_apply_delta_hook_not_triggered() {
        let initial = TestStructWithHook {
            number: HookI32(10),
            text: HookString("hello".to_string()),
        };
        let params = HookParams;

        let modified = TestStructWithHook {
            number: HookI32(20),
            text: HookString("world".to_string()),
        };
        let summary = initial.summarize(&initial, &params);
        let delta = modified.delta(&initial, &params, &summary);

        let mut result = initial.clone();
        result.apply_delta(&initial, &params, &delta).unwrap();

        assert_eq!(result.number.0, 20);
        assert_eq!(
            result.text.0, "world",
            "hook should not modify text when number >= 0"
        );
    }
}
