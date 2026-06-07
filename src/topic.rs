//! Topic pattern matching for event routing.
//!
//! Supports wildcard patterns for flexible event subscription:
//! - `*` matches a single segment (e.g., `"user.*"` matches `"user.created"` but not `"user.foo.bar"`)
//! - `**` matches multiple segments (e.g., `"user.**"` matches `"user.created"` and `"user.foo.bar"`)
//! - Exact match when no wildcards are present

/// Match a topic pattern against a concrete topic name.
///
/// # Examples
///
/// ```ignore
/// use anycms_event::topic::matches;
///
/// assert!(matches("user.created", "user.created"));
/// assert!(matches("user.*", "user.created"));
/// assert!(matches("user.**", "user.foo.bar"));
/// assert!(matches("**", "anything.at.all"));
/// assert!(!matches("user.*", "user.foo.bar"));
/// ```
pub fn matches(pattern: &str, topic: &str) -> bool {
    let pattern_parts: Vec<&str> = pattern.split('.').collect();
    let topic_parts: Vec<&str> = topic.split('.').collect();
    match_glob(&pattern_parts, &topic_parts)
}

fn match_glob(pattern: &[&str], topic: &[&str]) -> bool {
    match (pattern.first(), topic.first()) {
        (None, None) => true,
        (None, Some(_)) => false,
        (Some(&"**"), _) => {
            // ** matches zero or more segments
            if pattern.len() == 1 {
                return true;
            }
            // Try matching rest of pattern against current and remaining topic positions
            (0..=topic.len()).any(|i| match_glob(&pattern[1..], &topic[i..]))
        }
        (Some(&"*"), None) => false,
        (Some(&"*"), Some(_)) => match_glob(&pattern[1..], &topic[1..]),
        (Some(p), Some(t)) if *p == *t => match_glob(&pattern[1..], &topic[1..]),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exact_match() {
        assert!(matches("user.created", "user.created"));
        assert!(!matches("user.created", "user.deleted"));
    }

    #[test]
    fn test_single_wildcard() {
        assert!(matches("user.*", "user.created"));
        assert!(matches("user.*", "user.deleted"));
        assert!(!matches("user.*", "user.foo.bar"));
        assert!(matches("*.created", "user.created"));
        assert!(!matches("*.created", "user.deleted"));
    }

    #[test]
    fn test_double_wildcard() {
        assert!(matches("user.**", "user.created"));
        assert!(matches("user.**", "user.foo.bar"));
        assert!(matches("**", "anything.at.all"));
        assert!(matches("*.**", "user.foo.bar"));
        assert!(matches("user.**", "user"));
    }

    #[test]
    fn test_empty_patterns() {
        assert!(matches("", ""));
        assert!(!matches("", "something"));
        assert!(!matches("something", ""));
    }

    #[test]
    fn test_multiple_wildcards() {
        assert!(matches("*.created.*", "user.created.today"));
        assert!(!matches("*.created.*", "user.created"));
        assert!(matches("*.*.*", "a.b.c"));
        assert!(!matches("*.*.*", "a.b"));
    }
}
