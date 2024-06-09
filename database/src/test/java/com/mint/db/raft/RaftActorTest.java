package com.mint.db.raft;

import com.mint.db.Raft;
import com.mint.db.config.NodeConfig;
import com.mint.db.dao.impl.BaseDao;
import com.mint.db.grpc.InternalGrpcActor;
import com.mint.db.grpc.server.ExternalServiceImpl;
import com.mint.db.raft.model.LogId;
import com.mint.db.replication.ReplicatedLogManager;
import com.mint.db.replication.impl.ReplicatedLogManagerImpl;
import com.mint.db.replication.model.PersistentState;
import org.awaitility.Awaitility;
import org.hamcrest.comparator.ComparatorMatcherBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static com.mint.db.util.LogUtil.protobufMessageToString;

@RunWith(PowerMockRunner.class)
class RaftActorTest {
    private static final Logger logger = LoggerFactory.getLogger(RaftActorTest.class);

    @Test
    void leaderElectionTest() throws IOException {
        Path logDir = Files.createTempDirectory("mintKVLogDir");
        NodeConfig config = new NodeConfig(
                8080,
                0,
                logDir.toString(),
                5000L,
                List.of(
                        "http://localhost:8080",
                        "http://localhost:8081",
                        "http://localhost:8082",
                        "http://localhost:8083",
                        "http://localhost:8084"
                )
        );

        List<RaftActor> cluster = new ArrayList<>(5);

        InternalGrpcActor internalGrpcActor = Mockito.mock(InternalGrpcActor.class);
        ExternalServiceImpl externalGrpcActor = Mockito.mock(ExternalServiceImpl.class);
        AtomicInteger internalGrpcActorInvocations = new AtomicInteger();
        Mockito.doAnswer(invocation -> {
            Raft.VoteRequest voteRequest = invocation.getArgument(0);
            BiConsumer<Integer, Raft.VoteResponse> onRequestVoteResult = invocation.getArgument(1);
            int nodeId = voteRequest.getCandidateId();
            logger.info("internalGrpcActor.onLeaderCandidate({})", protobufMessageToString(voteRequest));
            for (int i = 0; i < cluster.size(); i++) {
                if (i != nodeId) {
                    int finalI = i;
                    cluster.get(i).onRequestVote(voteRequest, (voteResponse -> {
                        if (nodeId == 0) {
                            Assertions.assertTrue(voteResponse.getVoteGranted());
                            Assertions.assertEquals(1, voteResponse.getTerm());
                            onRequestVoteResult.accept(finalI, voteResponse);
                        } else {
                            Assertions.assertFalse(voteResponse.getVoteGranted());
                        }
                    }));
                }
            }
            internalGrpcActorInvocations.incrementAndGet();
            return null;
        }).when(internalGrpcActor).sendVoteRequest(Mockito.any(Raft.VoteRequest.class), Mockito.any());

        for (int i = 0; i < 5; i++) {
            final NodeConfig nodeConfig = Mockito.spy(config.copy());
            nodeConfig.setNodeId(i);
            nodeConfig.setPort(8080 + i);
            Mockito.when(nodeConfig.heartbeatRandom()).thenReturn(false);

            ReplicatedLogManager<MemorySegment> replicatedLogManager =
                    new ReplicatedLogManagerImpl(nodeConfig, new PersistentState(), new BaseDao());

            StateMachine<MemorySegment> stateMachine = Mockito.mock(DaoStateMachine.class);
            Environment<MemorySegment> env = new EnvironmentImpl(nodeConfig, replicatedLogManager, stateMachine);

            RaftActor raftActor = new RaftActor(
                    internalGrpcActor,
                    env,
                    externalGrpcActor
            );
            cluster.add(raftActor);
        }
        //CHECKSTYLE.OFF: IndentationCheck
        Awaitility.await().atMost(11, TimeUnit.SECONDS)
                .untilAtomic(
                        internalGrpcActorInvocations,
                        ComparatorMatcherBuilder.comparedBy(Integer::compareTo).greaterThan(0)
                );
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .failFast(() -> internalGrpcActorInvocations.get() > 1)
                .untilAtomic(
                        internalGrpcActorInvocations,
                        ComparatorMatcherBuilder.comparedBy(Integer::compareTo).comparesEqualTo(1)
                );
        Mockito.verify(internalGrpcActor, Mockito.times(4))
                .sendAppendEntriesRequest(
                        Mockito.anyInt(), Mockito.any(Raft.AppendEntriesRequest.class), Mockito.any()
                );
        //CHECKSTYLE.ON: IndentationCheck
    }
}
