package com.huawei.omniruntime.flink.runtime.state;

import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.runtime.memory.OpaqueMemoryResource;
import org.apache.flink.util.function.LongFunctionWithException;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class OmniRocksDBSharedResourcesFactoryTest {
    private static final long SLOT_BYTES = 32L * 1024 * 1024;

    private OmniRocksDBMemoryConfiguration managed() {
        return new OmniRocksDBMemoryConfiguration(true, 0, 0.5, 0.1, false);
    }

    @Test
    void statelessChainHeadUsesTheSlotBackendShareWithoutChangingItsConfig() {
        StreamConfig config = new StreamConfig(new Configuration());
        config.setStateBackendUsesManagedMemory(true);
        config.setManagedMemoryFractionOperatorOfUseCase(ManagedMemoryUseCase.STATE_BACKEND, 0);
        config.setManagedMemoryFractionOperatorOfUseCase(ManagedMemoryUseCase.OPERATOR, 1);
        Configuration taskManagerConfig = new Configuration();
        ClassLoader loader = getClass().getClassLoader();
        double fraction = OmniRocksDBSharedResourcesFactory.getStateBackendFractionOfSlot(
                config, taskManagerConfig, loader);
        assertTrue(fraction > 0 && fraction < 1);
        assertEquals(0, config.getManagedMemoryFractionOperatorUseCaseOfSlot(
                ManagedMemoryUseCase.STATE_BACKEND, taskManagerConfig, loader));
        config.setManagedMemoryFractionOperatorOfUseCase(ManagedMemoryUseCase.STATE_BACKEND, 1);
        assertEquals(config.getManagedMemoryFractionOperatorUseCaseOfSlot(
                ManagedMemoryUseCase.STATE_BACKEND, taskManagerConfig, loader), fraction);
    }

    @Test
    void backendOnlySlotUsesAllItsManagedMemory() {
        StreamConfig config = new StreamConfig(new Configuration());
        config.setStateBackendUsesManagedMemory(true);
        assertEquals(1, OmniRocksDBSharedResourcesFactory.getStateBackendFractionOfSlot(
                config, new Configuration(), getClass().getClassLoader()));
    }

    @Test
    void sharesSlotPoolWithoutReservingMemoryAndReleasesOnlyAfterLastLease() throws Exception {
        MemoryManager manager = MemoryManager.create(SLOT_BYTES, 32768);
        AtomicInteger allocations = new AtomicInteger();
        AtomicInteger releases = new AtomicInteger();
        OmniRocksDBMemoryConfiguration config = managed();
        LongFunctionWithException<OmniRocksDBSharedResources, Exception> allocator = size ->
                new OmniRocksDBSharedResources(allocations.incrementAndGet(), size, config,
                        handle -> releases.incrementAndGet());
        OpaqueMemoryResource<OmniRocksDBSharedResources> a =
                OmniRocksDBSharedResourcesFactory.acquire(manager, 0.5, config, allocator);
        OpaqueMemoryResource<OmniRocksDBSharedResources> b =
                OmniRocksDBSharedResourcesFactory.acquire(manager, 0.5, config, allocator);
        assertSame(a.getResourceHandle(), b.getResourceHandle());
        assertEquals(1, allocations.get());
        assertEquals(SLOT_BYTES / 2, a.getSize());
        assertEquals(SLOT_BYTES / 2, a.getResourceHandle().getTotalMemoryBytes());
        assertEquals(SLOT_BYTES, manager.availableMemory());
        a.close();
        assertEquals(0, releases.get());
        assertTrue(b.getResourceHandle().getHandle() > 0);
        assertEquals(SLOT_BYTES, manager.availableMemory());
        b.close();
        assertEquals(1, releases.get());
        assertEquals(SLOT_BYTES, manager.availableMemory());
        assertTrue(manager.verifyEmpty());
        manager.shutdown();
    }

    @Test
    void coexistsWithJavaManagedPoolInEitherAllocationOrder() throws Exception {
        for (boolean nativeFirst : new boolean[] {true, false}) {
            MemoryManager manager = MemoryManager.create(SLOT_BYTES, 32768);
            AtomicInteger nativeAllocations = new AtomicInteger();
            AtomicInteger nativeReleases = new AtomicInteger();
            AtomicInteger javaReleases = new AtomicInteger();
            OmniRocksDBMemoryConfiguration config = managed();
            LongFunctionWithException<OmniRocksDBSharedResources, Exception> nativeAllocator = size ->
                    new OmniRocksDBSharedResources(nativeAllocations.incrementAndGet(), size, config,
                            handle -> nativeReleases.incrementAndGet());
            OpaqueMemoryResource<OmniRocksDBSharedResources> nativeLease = nativeFirst
                    ? OmniRocksDBSharedResourcesFactory.acquire(manager, 1, config, nativeAllocator)
                    : null;
            // Use the Java RocksDB resource ID and reservation API without loading RocksDB JNI.
            try (OpaqueMemoryResource<AutoCloseable> javaLease =
                    manager.getSharedMemoryResourceForManagedMemory(
                            "state-rocks-managed-memory",
                            size -> (AutoCloseable) () -> javaReleases.incrementAndGet(), 1)) {
                if (!nativeFirst) {
                    nativeLease = OmniRocksDBSharedResourcesFactory.acquire(
                            manager, 1, config, nativeAllocator);
                }
                assertEquals(SLOT_BYTES, javaLease.getSize());
                assertEquals(SLOT_BYTES, nativeLease.getSize());
                assertEquals(0, manager.availableMemory());
                try (OpaqueMemoryResource<OmniRocksDBSharedResources> secondNativeLease =
                        OmniRocksDBSharedResourcesFactory.acquire(manager, 1, config, nativeAllocator)) {
                    assertSame(nativeLease.getResourceHandle(), secondNativeLease.getResourceHandle());
                    assertEquals(1, nativeAllocations.get());
                    nativeLease.close();
                    assertEquals(0, nativeReleases.get());
                }
                assertEquals(1, nativeReleases.get());
                assertEquals(0, javaReleases.get());
                assertEquals(0, manager.availableMemory());
            } finally {
                if (nativeLease != null) {
                    nativeLease.close();
                }
            }
            assertEquals(1, javaReleases.get());
            assertEquals(SLOT_BYTES, manager.availableMemory());
            assertTrue(manager.verifyEmpty());
            manager.shutdown();
        }
    }

    @Test
    void fixedBudgetWorksWithZeroManagedFractionAndDoesNotReserveManagedMemory() throws Exception {
        MemoryManager manager = MemoryManager.create(SLOT_BYTES, 32768);
        OmniRocksDBMemoryConfiguration config =
                new OmniRocksDBMemoryConfiguration(false, SLOT_BYTES / 4, 0.5, 0.1, true);
        AtomicLong allocatedBytes = new AtomicLong();
        try (OpaqueMemoryResource<OmniRocksDBSharedResources> lease =
                OmniRocksDBSharedResourcesFactory.acquire(manager, 0, config, size -> {
                    allocatedBytes.set(size);
                    return new OmniRocksDBSharedResources(1, size, config, handle -> {});
                })) {
            assertEquals(SLOT_BYTES / 4, allocatedBytes.get());
            assertEquals(SLOT_BYTES / 4, lease.getResourceHandle().getTotalMemoryBytes());
            assertEquals(SLOT_BYTES, manager.availableMemory());
        }
        manager.shutdown();
    }

    @Test
    void rejectsConflictingPoliciesWithoutLosingExistingLease() throws Exception {
        MemoryManager manager = MemoryManager.create(SLOT_BYTES, 32768);
        OmniRocksDBMemoryConfiguration config = managed();
        try (OpaqueMemoryResource<OmniRocksDBSharedResources> lease =
                OmniRocksDBSharedResourcesFactory.acquire(manager, 0.5, config,
                        size -> new OmniRocksDBSharedResources(1, size, config, handle -> {}))) {
            OmniRocksDBMemoryConfiguration conflict =
                    new OmniRocksDBMemoryConfiguration(true, 0, 0.6, 0.1, false);
            assertThrows(IllegalArgumentException.class, () ->
                    OmniRocksDBSharedResourcesFactory.acquire(manager, 0.5, conflict,
                            size -> { throw new AssertionError("Must reuse the existing pool"); }));
            assertThrows(IllegalArgumentException.class, () ->
                    OmniRocksDBSharedResourcesFactory.acquire(manager, 0.25, config,
                            size -> { throw new AssertionError("Must reuse the existing pool"); }));
            assertEquals(1, lease.getResourceHandle().getHandle());
        }
        assertTrue(manager.verifyEmpty());
        manager.shutdown();
    }

    @Test
    void allocationFailureLeavesManagedBudgetUntouched() {
        MemoryManager manager = MemoryManager.create(SLOT_BYTES, 32768);
        assertThrows(Exception.class, () ->
                OmniRocksDBSharedResourcesFactory.acquire(manager, 0.5, managed(),
                        size -> { throw new Exception("native allocation failed"); }));
        assertEquals(SLOT_BYTES, manager.availableMemory());
        assertTrue(manager.verifyEmpty());
        manager.shutdown();
    }

    @Test
    void separateSlotsAllocateSeparatePools() throws Exception {
        MemoryManager a = MemoryManager.create(SLOT_BYTES, 32768);
        MemoryManager b = MemoryManager.create(SLOT_BYTES, 32768);
        OmniRocksDBMemoryConfiguration config = managed();
        AtomicInteger nextHandle = new AtomicInteger();
        LongFunctionWithException<OmniRocksDBSharedResources, Exception> allocator = size ->
                new OmniRocksDBSharedResources(nextHandle.incrementAndGet(), size, config, handle -> {});
        try (OpaqueMemoryResource<OmniRocksDBSharedResources> first =
                     OmniRocksDBSharedResourcesFactory.acquire(a, 0.5, config, allocator);
             OpaqueMemoryResource<OmniRocksDBSharedResources> second =
                     OmniRocksDBSharedResourcesFactory.acquire(b, 0.5, config, allocator)) {
            assertNotSame(first.getResourceHandle(), second.getResourceHandle());
            assertNotEquals(first.getResourceHandle().getHandle(), second.getResourceHandle().getHandle());
        }
        a.shutdown();
        b.shutdown();
    }

    @Test
    void closeIsIdempotentAndClosedHandleIsRejected() {
        AtomicInteger releases = new AtomicInteger();
        OmniRocksDBSharedResources resource = new OmniRocksDBSharedResources(1, SLOT_BYTES,
                managed(), handle -> releases.incrementAndGet());
        resource.close();
        resource.close();
        assertEquals(1, releases.get());
        assertThrows(IllegalStateException.class, resource::getHandle);
    }
}
