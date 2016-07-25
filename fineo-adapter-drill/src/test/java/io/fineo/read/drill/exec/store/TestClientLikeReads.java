package io.fineo.read.drill.exec.store;

import io.fineo.read.drill.BaseFineoTest;

/**
 * Validate reads across all three sources with different varying time ranges. Things that need
 * covering are:
 * - ensuring that we don't have duplicate data when reading overlapping time ranges
 * - combining fields across json and parquet formats
 */
public class TestClientLikeReads extends BaseFineoTest {
}
