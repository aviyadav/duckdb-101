#!/usr/bin/env python3
"""
Demo script showing the structure and logic of the data generators
This demonstrates what the actual scripts will produce when dependencies are installed
"""

print("=" * 80)
print("PARQUET DATA GENERATOR - DEMONSTRATION")
print("=" * 80)
print()

print("📦 Required packages: polars, pyarrow")
print("💻 Install with: pip install polars pyarrow")
print()

print("=" * 80)
print("SCRIPT 1: generate_users.py")
print("=" * 80)
print()

print("Configuration:")
print("  • Users to generate: 1,500")
print("  • Output: data/users.parquet")
print("  • Multiprocessing: Uses all CPU cores")
print()

print("Fields generated:")
print("  ├─ user_id: u_0001 to u_1500")
print("  ├─ signup_at: Random timestamp (2022-01-01 to 2025-01-31)")
print("  ├─ signup_date: Date extracted from signup_at")
print("  ├─ acquisition_channel: [organic, paid_search, social, referral, email, direct]")
print("  ├─ country: 2-letter codes [US, GB, CA, AU, DE, FR, IN, BR, JP, SG, NL, ES, IT, MX, AR]")
print("  └─ user_status: [active (70%), inactive (15%), suspended (5%), deactivated (10%)]")
print()

print("Sample output:")
print("""
┌─────────┬─────────────────────┬─────────────┬─────────────────────┬─────────┬─────────────┐
│ user_id │ signup_at           │ signup_date │ acquisition_channel │ country │ user_status │
├─────────┼─────────────────────┼─────────────┼─────────────────────┼─────────┼─────────────┤
│ u_0001  │ 2023-04-15 14:23:45 │ 2023-04-15  │ organic             │ US      │ active      │
│ u_0002  │ 2022-08-22 09:17:33 │ 2022-08-22  │ paid_search         │ GB      │ active      │
│ u_0003  │ 2024-01-10 18:45:12 │ 2024-01-10  │ social              │ CA      │ inactive    │
│ u_0004  │ 2023-11-03 11:29:58 │ 2023-11-03  │ referral            │ DE      │ active      │
│ u_0005  │ 2024-06-28 20:14:07 │ 2024-06-28  │ email               │ IN      │ active      │
└─────────┴─────────────────────┴─────────────┴─────────────────────┴─────────┴─────────────┘
""")

print("Performance:")
print("  • Generation time: ~1-2 seconds")
print("  • File size: ~0.1-0.2 MB (with compression)")
print("  • Parallel processing: Work split across all CPU cores")
print()

print("=" * 80)
print("SCRIPT 2: generate_events.py")
print("=" * 80)
print()

print("Configuration:")
print("  • Events to generate: 1,000,000")
print("  • Output: data/events.parquet")
print("  • Multiprocessing: Uses all CPU cores")
print()

print("Fields generated:")
print("  ├─ user_id: References users (Pareto distribution - some users very active)")
print("  ├─ event_at: Random timestamp (2022-01-01 to 2025-02-01)")
print("  ├─ event_type: [session (50%), view (25%), click (10%), purchase (5%), ...]")
print("  └─ revenue: $5-$500 for purchase/checkout events, $0 otherwise")
print()

print("Sample output:")
print("""
┌─────────┬─────────────────────┬──────────────┬─────────┐
│ user_id │ event_at            │ event_type   │ revenue │
├─────────┼─────────────────────┼──────────────┼─────────┤
│ u_0042  │ 2023-03-15 10:23:11 │ session      │ 0.00    │
│ u_0142  │ 2023-03-15 10:45:33 │ view         │ 0.00    │
│ u_0042  │ 2023-03-15 11:12:44 │ click        │ 0.00    │
│ u_0088  │ 2023-03-15 12:03:21 │ add_to_cart  │ 0.00    │
│ u_0088  │ 2023-03-15 12:05:18 │ purchase     │ 129.99  │
│ u_0234  │ 2023-03-15 13:22:45 │ session      │ 0.00    │
│ u_0142  │ 2023-03-15 14:11:09 │ view         │ 0.00    │
│ u_0042  │ 2023-03-15 15:33:22 │ checkout     │ 45.50   │
└─────────┴─────────────────────┴──────────────┴─────────┘
""")

print("Data Characteristics:")
print("  • Power law distribution: Few users generate many events, most have few")
print("  • Sorted by event_at for optimal query performance")
print("  • Revenue only for purchase/checkout event types")
print("  • Realistic event type distribution (sessions most common)")
print()

print("Performance:")
print("  • Generation time: ~10-30 seconds (depends on CPU cores)")
print("  • File size: ~15-25 MB (with compression)")
print("  • Parallel processing: Work split across all CPU cores")
print()

print("=" * 80)
print("USAGE")
print("=" * 80)
print()
print("1. Install dependencies:")
print("   pip install polars pyarrow")
print()
print("2. Run the generators:")
print("   python generate_users.py")
print("   python generate_events.py")
print()
print("3. Output files will be in the 'data/' directory:")
print("   • data/users.parquet")
print("   • data/events.parquet")
print()
print("4. Read the data with Polars:")
print("   import polars as pl")
print("   users = pl.read_parquet('data/users.parquet')")
print("   events = pl.read_parquet('data/events.parquet')")
print()

print("=" * 80)
print("KEY FEATURES")
print("=" * 80)
print()
print("✓ Fast parallel generation using multiprocessing")
print("✓ Efficient Polars + PyArrow (no Pandas)")
print("✓ Realistic data distributions")
print("✓ Snappy compression for optimal file size")
print("✓ Sorted events for better query performance")
print("✓ Statistics and verification after generation")
print("✓ Professional output with progress indicators")
print()
print("=" * 80)
