use agent_supervisor::ResourceLimits;
use proptest::prelude::*;

proptest! {
    #[test]
    fn resource_limits_round_trip_through_json(
        timeout_ms in any::<u64>(),
        input_bytes in any::<usize>(),
        output_bytes in any::<usize>(),
        stderr_bytes in any::<usize>(),
        memory_bytes in proptest::option::of(any::<u64>()),
        max_processes in proptest::option::of(any::<u32>()),
        cpu_quota_micros in proptest::option::of(any::<u64>()),
    ) {
        let limits = ResourceLimits {
            timeout_ms,
            input_bytes,
            output_bytes,
            stderr_bytes,
            memory_bytes,
            max_processes,
            cpu_quota_micros,
        };
        let encoded = serde_json::to_value(&limits).unwrap();
        let decoded: ResourceLimits = serde_json::from_value(encoded).unwrap();
        let has_kernel_limits = decoded.has_kernel_limits();
        prop_assert_eq!(decoded, limits);
        prop_assert_eq!(
            has_kernel_limits,
            memory_bytes.is_some() || max_processes.is_some() || cpu_quota_micros.is_some()
        );
    }
}
