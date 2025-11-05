package com.huawei.omniruntime.flink.runtime.api.graph.json;

import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.filesystem.RelativeFileStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalLocalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.util.StringUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Contains JUnit tests for the FlinkStateDeserializer class.
 */
public class FlinkStateDeserializerTest {

    @Test
    void testDeserializeLocalStateSnapshot() throws Exception {
        String localStateJson = "{\"isTaskDeployedAsFinished\":false,\"isTaskFinished\":false,\"stateHandleName\":\"TaskStateSnapshot\",\"stateSize\":81584,\"subtaskStatesByOperatorID\":{\"4BF7C1955FFE56E2106D666433EAF137\":{\"checkpointedSize\":0,\"inputChannelState\":{\"stateObjects\":[]},\"managedKeyedState\":{\"stateObjects\":[{\"backendIdentifier\":\"8d43f905-8fcb-491d-95df-87e4597464e4\",\"checkpointId\":11,\"checkpointedSize\":0,\"directoryStateHandle\":{\"directory\":\"/home/hudsonsheng/RocksDB/tm_localhost:35785-4d040b/localState/aid_23d801926c6066daab3c7eabda5e84e6/jid_095942d305df3523779193e8cf79060a/vtx_4bf7c1955ffe56e2106d666433EAF137_sti_0/chk_11/8d43f9058fcb491d95df87e4597464e4\",\"stateHandleName\":\"DirectoryStateHandle\",\"stateSize\":0},\"keyGroupRange\":{\"endKeyGroup\":127,\"numberOfKeyGroups\":128,\"startKeyGroup\":0},\"metaDataState\":{\"filePath\":\"file:/home/hudsonsheng/RocksDB/tm_localhost:35785-4d040b/localState/aid_23d801926c6066daab3c7eabda5e84e6/jid_095942d305df3523779193e8cf79060a/vtx_4bf7c1955ffe56e2106d666433EAF137_sti_0/chk_11/e5e08ea2-de9c-4a46-a338-73e661d67186\",\"stateHandleName\":\"FileStateHandle\",\"stateSize\":81584,\"streamStateHandleID\":{\"keyString\":\"file:/home/hudsonsheng/RocksDB/tm_localhost:35785-4d040b/localState/aid_23d801926c6066daab3c7eabda5e84e6/jid_095942d305df3523779193e8cf79060a/vtx_4bf7c1955ffe56e2106d666433EAF137_sti_0/chk_11/e5e08ea2-de9c-4a46-a338-73e661d67186\",\"stateHandleName\":\"PhysicalStateHandleID\"}},\"sharedState\":[],\"stateHandleName\":\"IncrementalLocalKeyedStateHandle\",\"stateSize\":81584}]},\"managedOperatorState\":{\"stateObjects\":[]},\"rawKeyedState\":{\"stateObjects\":[]},\"rawOperatorState\":{\"stateObjects\":[]},\"resultSubpartitionState\":{\"stateObjects\":[]},\"stateHandleName\":\"OperatorSubtaskState\",\"stateSize\":81584},\"CCB29B5204E83E8A588B3828AFAA7015\":{\"checkpointedSize\":0,\"inputChannelState\":{\"stateObjects\":[]},\"managedKeyedState\":{\"stateObjects\":[]},\"managedOperatorState\":{\"stateObjects\":[]},\"rawKeyedState\":{\"stateObjects\":[]},\"rawOperatorState\":{\"stateObjects\":[]},\"resultSubpartitionState\":{\"stateObjects\":[]},\"stateHandleName\":\"OperatorSubtaskState\",\"stateSize\":0}}}";

        TaskStateSnapshot snapshot = TaskStateSnapshotDeser.deserializeTaskStateSnapshot(localStateJson);
        assertNotNull(snapshot);
        assertFalse(snapshot.isTaskFinished());

        OperatorID operatorID = new OperatorID(StringUtils.hexStringToByte("4BF7C1955FFE56E2106D666433EAF137"));
        OperatorSubtaskState subtaskState = snapshot.getSubtaskStateByOperatorID(operatorID);
        assertNotNull(subtaskState);

        StateObjectCollection<KeyedStateHandle> keyedState = subtaskState.getManagedKeyedState();
        assertEquals(1, keyedState.size());

        assertTrue(keyedState.iterator().next() instanceof IncrementalLocalKeyedStateHandle);
        IncrementalLocalKeyedStateHandle handle = (IncrementalLocalKeyedStateHandle) keyedState.iterator().next();
        assertEquals(11, handle.getCheckpointId());
        assertEquals("8d43f905-8fcb-491d-95df-87e4597464e4", handle.getBackendIdentifier().toString());
    }

    @Test
    void testDeserializeRemoteStateSnapshot() throws Exception {
        // Use the complete JSON string for the remote state snapshot
        String remoteStateJson = "{\"isTaskDeployedAsFinished\":false,\"isTaskFinished\":false,\"stateHandleName\":\"TaskStateSnapshot\",\"stateSize\":25378,\"subtaskStatesByOperatorID\":{\"4BF7C1955FFE56E2106D666433EAF137\":{\"checkpointedSize\":0,\"inputChannelState\":{\"stateObjects\":[]},\"managedKeyedState\":{\"stateObjects\":[{\"backendIdentifier\":\"8d43f905-8fcb-491d-95df-87e4597464e4\",\"checkpointId\":1,\"checkpointedSize\":421936,\"keyGroupRange\":{\"endKeyGroup\":127,\"numberOfKeyGroups\":128,\"startKeyGroup\":0},\"metaStateHandle\":{\"filePath\":\"file:/home/hudsonsheng/flink/checkpoints/095942d305df3523779193e8cf79060a/chk-1/696275ab-57d4-482f-ae52-97bfd07d891e\",\"relativePath\":\"696275ab-57d4-482f-ae52-97bfd07d891e\",\"stateHandleName\":\"RelativeFileStateHandle\",\"stateSize\":81584,\"streamStateHandleID\":{\"keyString\":\"file:/home/hudsonsheng/flink/checkpoints/095942d305df3523779193e8cf79060a/chk-1/696275ab-57d4-482f-ae52-97bfd07d891e\",\"stateHandleName\":\"PhysicalStateHandleID\"}},\"persistedSizeOfThisCheckpoint\":421936,\"privateState\":[{\"handle\":{\"data\":\"Vvm4+BwAAQEabGV2ZWxkYi5CeXRld2lzZUNvbXBhcmF0b3KYr1imAgABAgBRgcd4BgABCQADBQQARtLoITEAAQEabGV2ZWxkYi5CeXRld2lzZUNvbXBhcmF0b3ICBQMIBADIAQHJAQlMZWZ0Q2FjaGU6cy3AMgABARpsZXZlbGRiLkJ5dGV3aXNlQ29tcGFyYXRvcgIFAwoEAMgBAskBClJpZ2h0Q2FjaGXvtbdrNwABARpsZXZlbGRiLkJ5dGV3aXNlQ29tcGFyYXRvcgIFAwwEAMgBA8kBD0VhcmxpZXN0RWxlbWVudLD4/3k6AAEBGmxldmVsZGIuQnl0ZXdpc2VDb21wYXJhdG9yAgUDDgQAyAEEyQESTGVmdENhY2hlQ2xlYW5UaW1lD5BHZzsAAQEabGV2ZWxkYi5CeXRld2lzZUNvbXBhcmF0b3ICBQMQBADIAQXJARNSaWdodENhY2hlQ2xlYW5UaW1lmNaHPDgAAQEabGV2ZWxkYi5CeXRld2lzZUNvbXBhcmF0b3ICBQMSBADIAQbJARBMZWZ0RHVwbGljYXRlUmNk0eMQ3DkAAQEabGV2ZWxkYi5CeXRld2lzZUNvbXBhcmF0b3ICBQMUBADIAQfJARFSaWdodER1cGxpY2F0ZVJjZBtIAPJDAAEBGmxldmVsZGIuQnl0ZXdpc2VDb21wYXJhdG9yAgUDFgQAyAEIyQEbTGVmdER1cGxpY2F0ZVJjZENsZWFuZXJUaW1lFa5C0EQAAQEabGV2ZWxkYi5CeXRld2lzZUNvbXBhcmF0b3ICBQMYBADIAQnJARxSaWdodER1cGxpY2F0ZVJjZENsZWFuZXJUaW1lYndGHTkAAQEabGV2ZWxkYi5CeXRld2lzZUNvbXBhcmF0b3ICBQMaBADIAQrJARFMZWZ0RGF0YVRvdGFsU2l6ZZqvcUZLAAEBGmxldmVsZGIuQnl0ZXdpc2VDb21wYXJhdG9yAgUDHAQAyAELyQEjX3RpbWVyX3N0YXRlL3Byb2Nlc3NpbmdfdXNlci10aW1lcnMatVTLRgABARpsZXZlbGRiLkJ5dGV3aXNlQ29tcGFyYXRvcgIFAx4EAMgBDMkBHl90aW1lcl9zdGF0ZS9ldmVudF91c2VyLXRpbWVyc4u+mDWhAAECBQkAAyEE1lBnACCAWTgAJmNoYW5uZWwgNDQ5LGFw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\",\"handleName\":\"file:/home/hudsonsheng/flink/checkpoints/095942d305df3523779193e8cf79060a/chk-1/632e33a6-4441-403a-98ea-e9ceb4842279\",\"stateHandleName\":\"ByteStreamStateHandle\",\"stateSize\":2602,\"streamStateHandleID\":{\"keyString\":\"file:/home/hudsonsheng/flink/checkpoints/095942d305df3523779193e8cf79060a/chk-1/632e33a6-4441-403a-98ea-e9ceb4842279\",\"stateHandleName\":\"PhysicalStateHandleID\"}},\"localPath\":\"MANIFEST-000004\",\"stateSize\":2602},{\"handle\":{\"filePath\":\"file:/home/hudsonsheng/flink/checkpoints/095942d305df3523779193e8cf79060a/chk-1/5820ceb4-5f2e-489e-8185-749713ef79ce\",\"relativePath\":\"5820ceb4-5f2e-489e-8185-749713ef79ce\",\"stateHandleName\":\"RelativeFileStateHandle\",\"stateSize\":22776,\"streamStateHandleID\":{\"keyString\":\"file:/home/hudsonsheng/flink/checkpoints/095942d305df3523779193e8cf79060a/chk-1/5820ceb4-5f2e-489e-8185-749713ef79ce\",\"stateHandleName\":\"PhysicalStateHandleID\"}},\"localPath\":\"000040.sst\",\"stateSize\":22776}],\"sharedState\":[],\"stateHandleId\":{\"keyString\":\"13bbca3d-116b-4d9f-aeff-ad55c9cb29e1\"},\"stateHandleName\":\"IncrementalRemoteKeyedStateHandle\"}]},\"managedOperatorState\":{\"stateObjects\":[]},\"rawKeyedState\":{\"stateObjects\":[]},\"rawOperatorState\":{\"stateObjects\":[]},\"resultSubpartitionState\":{\"stateObjects\":[]},\"stateHandleName\":\"OperatorSubtaskState\",\"stateSize\":25378},\"CCB29B5204E83E8A588B3828AFAA7015\":{\"checkpointedSize\":0,\"inputChannelState\":{\"stateObjects\":[]},\"managedKeyedState\":{\"stateObjects\":[]},\"managedOperatorState\":{\"stateObjects\":[]},\"rawKeyedState\":{\"stateObjects\":[]},\"rawOperatorState\":{\"stateObjects\":[]},\"resultSubpartitionState\":{\"stateObjects\":[]},\"stateHandleName\":\"OperatorSubtaskState\",\"stateSize\":0}}}";

        TaskStateSnapshot snapshot = TaskStateSnapshotDeser.deserializeTaskStateSnapshot(remoteStateJson);
        assertNotNull(snapshot);
        assertFalse(snapshot.isTaskFinished());

        OperatorID operatorID = new OperatorID(StringUtils.hexStringToByte("4BF7C1955FFE56E2106D666433EAF137"));
        OperatorSubtaskState subtaskState = snapshot.getSubtaskStateByOperatorID(operatorID);
        assertNotNull(subtaskState);

        StateObjectCollection<KeyedStateHandle> keyedState = subtaskState.getManagedKeyedState();
        assertEquals(1, keyedState.size());

        assertTrue(keyedState.iterator().next() instanceof IncrementalRemoteKeyedStateHandle);
        IncrementalRemoteKeyedStateHandle handle = (IncrementalRemoteKeyedStateHandle) keyedState.iterator().next();
        assertEquals(1, handle.getCheckpointId());
        assertEquals(2, handle.getPrivateState().size()); // Assert there are two private handles

        IncrementalKeyedStateHandle.HandleAndLocalPath privateHandle1 = handle.getPrivateState().get(0);
        assertEquals("MANIFEST-000004", privateHandle1.getLocalPath());
        assertTrue(privateHandle1.getHandle() instanceof ByteStreamStateHandle);
        ByteStreamStateHandle byteHandle = (ByteStreamStateHandle) privateHandle1.getHandle();
        // The Base64 string from the JSON decodes to 1950 bytes
        assertEquals(2602, byteHandle.getData().length);

        IncrementalKeyedStateHandle.HandleAndLocalPath privateHandle2 = handle.getPrivateState().get(1);
        assertEquals("000040.sst", privateHandle2.getLocalPath());
        assertTrue(privateHandle2.getHandle() instanceof RelativeFileStateHandle);
    }
}
