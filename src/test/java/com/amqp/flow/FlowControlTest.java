package com.amqp.flow;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.*;

class FlowControlTest {

    private FlowControl flowControl;

    @BeforeEach
    void setUp() {
        flowControl = new FlowControl();
    }

    @Test
    void testInitialState() {
        assertThat(flowControl.isConnectionBlocked()).isFalse();
        assertThat(flowControl.isChannelFlowActive()).isTrue();
        assertThat(flowControl.getMemoryUsed()).isEqualTo(0);
    }

    @Test
    void testMemoryTracking() {
        flowControl.addMemoryUsage(1024);
        assertThat(flowControl.getMemoryUsed()).isEqualTo(1024);

        flowControl.addMemoryUsage(512);
        assertThat(flowControl.getMemoryUsed()).isEqualTo(1536);

        flowControl.releaseMemoryUsage(1024);
        assertThat(flowControl.getMemoryUsed()).isEqualTo(512);
    }

    @Test
    void testMemoryHighWatermark() {
        AtomicBoolean blocked = new AtomicBoolean(false);

        flowControl.setListener(new FlowControl.FlowControlListener() {
            @Override
            public void onConnectionBlocked(String reason) {
                blocked.set(true);
            }

            @Override
            public void onConnectionUnblocked() {
                blocked.set(false);
            }

            @Override
            public void onChannelFlow(boolean active) {
            }
        });

        flowControl.setMemoryHighWatermark(1000);
        flowControl.addMemoryUsage(1500); // Exceed high watermark

        assertThat(blocked.get()).isTrue();
        assertThat(flowControl.isConnectionBlocked()).isTrue();
    }

    @Test
    void testMemoryLowWatermark() {
        AtomicBoolean blocked = new AtomicBoolean(false);

        flowControl.setListener(new FlowControl.FlowControlListener() {
            @Override
            public void onConnectionBlocked(String reason) {
                blocked.set(true);
            }

            @Override
            public void onConnectionUnblocked() {
                blocked.set(false);
            }

            @Override
            public void onChannelFlow(boolean active) {
            }
        });

        flowControl.setMemoryHighWatermark(1000);
        flowControl.setMemoryLowWatermark(500);

        flowControl.addMemoryUsage(1500); // Block
        assertThat(blocked.get()).isTrue();

        flowControl.releaseMemoryUsage(1100); // Below low watermark
        assertThat(blocked.get()).isFalse();
        assertThat(flowControl.isConnectionBlocked()).isFalse();
    }

    @Test
    void testDiskSpaceLimit() {
        AtomicBoolean blocked = new AtomicBoolean(false);

        flowControl.setListener(new FlowControl.FlowControlListener() {
            @Override
            public void onConnectionBlocked(String reason) {
                blocked.set(true);
            }

            @Override
            public void onConnectionUnblocked() {
                blocked.set(false);
            }

            @Override
            public void onChannelFlow(boolean active) {
            }
        });

        flowControl.setDiskFreeLimit(100 * 1024 * 1024); // 100 MB

        flowControl.checkDiskSpace(50 * 1024 * 1024); // 50 MB - below limit
        assertThat(blocked.get()).isTrue();

        flowControl.checkDiskSpace(250 * 1024 * 1024); // 250 MB - above limit
        assertThat(blocked.get()).isFalse();
    }

    @Test
    void testChannelFlow() {
        AtomicBoolean flowActive = new AtomicBoolean(true);

        flowControl.setListener(new FlowControl.FlowControlListener() {
            @Override
            public void onConnectionBlocked(String reason) {
            }

            @Override
            public void onConnectionUnblocked() {
            }

            @Override
            public void onChannelFlow(boolean active) {
                flowActive.set(active);
            }
        });

        flowControl.setChannelFlow(false);
        assertThat(flowActive.get()).isFalse();
        assertThat(flowControl.isChannelFlowActive()).isFalse();

        flowControl.setChannelFlow(true);
        assertThat(flowActive.get()).isTrue();
        assertThat(flowControl.isChannelFlowActive()).isTrue();
    }

    @Test
    void testReset() {
        flowControl.addMemoryUsage(1000);
        flowControl.setChannelFlow(false);

        flowControl.reset();

        assertThat(flowControl.getMemoryUsed()).isEqualTo(0);
        assertThat(flowControl.isConnectionBlocked()).isFalse();
        assertThat(flowControl.isChannelFlowActive()).isTrue();
    }
}
