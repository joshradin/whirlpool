//! Asserts provides functions for doing assertions that throw Errors if the assertion fails

/// Tests a predicate. If the predicate fails, then the error is the result.
pub fn assert_that<F, E>(predicate: F, error: E) -> Result<(), E>
where
    F: FnOnce() -> bool,
{
    if predicate() {
        Ok(())
    } else {
        Err(error)
    }
}

/// Tests a predicate. If the predicate fails, then the result of the error function is the result.
pub fn assert_that_with<F1, F2, E>(predicate: F1, error: F2) -> Result<(), E>
where
    F1: FnOnce() -> bool,
    F2: FnOnce() -> E,
{
    if predicate() {
        Ok(())
    } else {
        Err(error())
    }
}
