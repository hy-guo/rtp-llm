package org.flexlb.mockengine;

import org.flexlb.enums.FlexMetricType;
import org.flexlb.metric.FlexMetricTags;
import org.flexlb.metric.FlexMonitor;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class WhaleEvictionLifetimeTest {
    @Test void reportsActualEvictionsOnlyWithExplicitMockSourceAndTier() {
        var sink = mock(FlexMonitor.class);
        var monitor = new WhaleMockMonitor(sink);
        var labels = Map.of("role", "ROLE_TYPE_PREFILL", "scope", "gpu");
        monitor.reportEvent(Map.of(), labels);
        verifyNoInteractions(sink);
        monitor.reportEvent(Map.of("mock_cache_evicted_entry_age_ms", 75.0), labels);
        var tags = ArgumentCaptor.forClass(FlexMetricTags.class);
        String name = "rtp_llm_kv_cache_evicted_block_lifetime_ms";
        verify(sink).register(name, FlexMetricType.GAUGE);
        verify(sink).report(eq(name), tags.capture(), eq(75.0));
        assertEquals("DEVICE", tags.getValue().getTags().get("tier"));
        assertEquals("mock_cache_entry", tags.getValue().getTags().get("lifetime_source"));
        monitor.reportEvent(Map.of("mock_cache_evicted_entry_age_ms", Double.NaN), labels);
        monitor.reportEvent(Map.of("mock_cache_evicted_entry_age_ms", -1), labels);
        monitor.reportEvent(Map.of(), labels);
        verify(sink, times(1)).report(eq(name), any(FlexMetricTags.class), anyDouble());
        monitor.reportEvent(Map.of("mock_cache_evicted_entry_age_ms", 80.0),
                Map.of("role", "ROLE_TYPE_DECODE", "scope", "memory"));
        verify(sink, times(1)).register(name, FlexMetricType.GAUGE);
        verify(sink).report(eq(name), tags.capture(), eq(80.0));
        assertEquals("HOST", tags.getValue().getTags().get("tier"));
    }
}
